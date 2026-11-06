"""Sequential downloads with owned partials, validation and persistent pacing."""
from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import random
import re
import time
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

import requests

from .guard import Pacer
from .messages import (BODY_BREAK_TYPES, body_read_broken, describe_error, error_context,
                       error_kind)
from .names import unique_names, _numbered
from .requestlog import STAGE_BODY, log_request, tag_error
from .resolver import check_challenge
from .models import (BunkrError, ChallengeRequired, Maintenance, QueueResult,
                     RateLimited, TransientError)

# Failure kinds that get one more attempt in the end-of-round pass.
ROUND_RETRY_KINDS = ('rate_limited', 'transient', 'maintenance', 'size_mismatch')

# A broken body stream may surface as a bare urllib3/http.client error rather
# than a requests one; those must be recorded as a failure, not escape the queue.
DOWNLOAD_ERRORS = (BunkrError, requests.RequestException, OSError) + BODY_BREAK_TYPES

_URL = re.compile(r'https?://\S+')


class DownloadStopped(Exception):
    """The user asked to pause/cancel; the partial file is kept."""


def _emit(callback, event_type, record=None, **values):
    if callback:
        if event_type in ('start', 'done'):
            values = {'index': 1, 'count': 1, **values}
        callback({"type": event_type, "file_id": record.file_id if record else None,
                  "name": record.name if record else None, **values})


def _update(store, record, **values):
    store.update_file(record.album_id, record.file_id, **values)
    for key, value in values.items():
        setattr(record, key, value)


def _detail(error) -> str:
    """Short technical reason for describe_error, never carrying a signed URL."""
    if isinstance(error, requests.Timeout):
        text = '连接超时'
    elif isinstance(error, RateLimited):
        text = str(error) or '请求受限'
    elif isinstance(error, BunkrError):
        text = str(error)
    elif body_read_broken(error):
        # The headers arrived and the body then stopped: say so, instead of the
        # misleading file-write wording an OSError subclass would otherwise get.
        text = '下载中断'
    elif isinstance(error, requests.ConnectionError):
        text = '连接失败'
    elif isinstance(error, OSError):
        text = '文件写入失败'
    else:
        text = str(error)
    text = _URL.sub('[地址已省略]', text or '未知错误')
    return text.strip().splitlines()[0][:120]


def _reject_redirect(path):
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise BunkrError('目标路径是符号链接或目录，拒绝写入')


def _target(store, record, dest_dir):
    root = Path(dest_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    records = store.list_files(record.album_id)
    names = unique_names(records)
    base = names[record.file_id]
    reserved = {v.casefold() for k, v in names.items() if k != record.file_id}
    claimed = set()
    for album in store.list_albums(include_hidden=True):
        for other in store.list_files(album.album_id):
            if (other.album_id, other.file_id) != (record.album_id, record.file_id) and other.local_path:
                claimed.add(str(Path(other.local_path).absolute()).casefold())
    if record.local_path:
        previous = Path(record.local_path).absolute()
        if previous.parent == root and str(previous).casefold() not in claimed:
            _reject_redirect(previous)
            if not previous.exists() or record.status == 'done':
                return previous
    candidate = root / base
    number = 1
    while True:
        _reject_redirect(candidate)
        if (not candidate.exists() and str(candidate).casefold() not in claimed
                and (number == 1 or candidate.name.casefold() not in reserved)):
            return candidate
        number += 1
        candidate = root / _numbered(base, number)


def _partial_paths(target, record):
    ident = hashlib.sha256((record.album_id + ':' + record.file_id).encode()).hexdigest()[:16]
    number = 0
    while True:
        token = ident if not number else hashlib.sha256(f'{ident}:{number}'.encode()).hexdigest()[:16]
        suffix = f'.{token}.bunkrdl.part'
        part = target.with_name(target.name + suffix)
        meta = part.with_name(part.name + '.json')
        _reject_redirect(part)
        _reject_redirect(meta)
        owner = None
        if meta.exists():
            try:
                owner = json.loads(meta.read_text(encoding='utf-8'))
            except (ValueError, UnicodeError):
                pass
        if (isinstance(owner, dict) and owner.get('album_id') == record.album_id
                and owner.get('file_id') == record.file_id):
            return part, meta, owner
        if not part.exists() and not meta.exists():
            return part, meta, {}
        number += 1


def _retry_after(value):
    if not value:
        return None
    try:
        number = float(value)
        if not math.isfinite(number) or number < 0:
            return None
        return number
    except (ValueError, TypeError, OverflowError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return None


# Statuses that may still claim an already published file.
_CLAIM_STATUSES = ('pending', 'downloading', 'failed', 'done')
# Five redirects are followed; the sixth response is the last allowed request.
_MAX_REDIRECTS = 5

# Simple request counter so a header line and its body-read failure line match.
_REQUEST_SEQ = itertools.count(1)


def _claimed_file(record):
    """The published file at local_path that this record may claim, if any."""
    if record.status not in _CLAIM_STATUSES or not record.local_path:
        return None
    path = Path(record.local_path)
    try:
        return path if path.is_file() else None
    except OSError:
        return None


def _discard_owned_partial(path, record):
    """Delete this file's own resume data; another file's `.part` is kept."""
    part, meta, owner = _partial_paths(path, record)
    if owner:
        part.unlink(missing_ok=True)
        meta.unlink(missing_ok=True)


def _reject_error_page(path, record):
    """Refuse to claim a published file that is really a served error page.

    A known size must match, otherwise the file is not a completion candidate at
    all; the content is then judged exactly like the external folder check, so a
    JSON or text error page whose length happens to match is still rejected.
    """
    try:
        size = path.stat().st_size
    except OSError:
        return
    if record.size is not None and size != record.size:
        return
    from .reconcile import content_check
    if content_check(path, record)[0]:
        return
    _discard_owned_partial(path, record)
    raise BunkrError('本地文件内容校验失败：服务器返回错误页或内容无效')


def _completed_local(record):
    """A file finished by an earlier run that crashed before recording done.

    Only pending/downloading/failed records qualify: exported keeps its link and
    skipped/done are settled.  A known size must match; an unknown size is
    accepted only when the bytes pass the same inspection the external folder
    check uses, so an error page is never adopted (nor a manual confirmation
    silently skipped).
    """
    if record.status not in ('pending', 'downloading', 'failed') or not record.local_path:
        return None
    path = Path(record.local_path)
    try:
        if not path.is_file():
            return None
        if record.size is not None and path.stat().st_size != record.size:
            return None
    except OSError:
        return None
    from .reconcile import content_check
    acceptable, unverified = content_check(path, record)
    return path if acceptable and not unverified else None


@contextmanager
def _body_response(transport, url, headers, config, throttle, stop_event):
    """GET a file body, passing every hop through the shared request gate.

    Redirects are followed by hand (at most five) so no auto-followed request can
    bypass the gate or outlive a pause, and each hop keeps the Referer and link
    headers the CDN expects.
    """
    for hop in range(_MAX_REDIRECTS + 1):
        if stop_event is not None and stop_event.is_set():
            raise DownloadStopped()
        if throttle is not None and not throttle.acquire(stop_event):
            raise DownloadStopped()
        if stop_event is not None and stop_event.is_set():
            raise DownloadStopped()
        started = time.perf_counter()
        seq = next(_REQUEST_SEQ)
        try:
            response = transport.get(url, headers=headers, stream=True, allow_redirects=False,
                                     timeout=(config.api_timeout, config.read_timeout))
        except Exception as error:
            tag_error(error, STAGE_BODY, url)
            log_request(STAGE_BODY, 'GET', url, error=error, started=started, seq=seq,
                        config=config)
            raise
        with response:
            log_request(STAGE_BODY, 'GET', url, status=response.status_code,
                        retry_after=response.headers.get('Retry-After'),
                        started=started, seq=seq, config=config)
            if response.status_code not in (301, 302, 303, 307, 308):
                try:
                    yield response
                except DownloadStopped:
                    raise
                except Exception as error:
                    # The failure belongs to this hop, not to the original link:
                    # a redirected body must name the host it was actually read from.
                    tag_error(error, STAGE_BODY, url)
                    if not isinstance(error, BunkrError):
                        # Reading the body timed out or broke off after the headers
                        # arrived: log it with the time since this request started,
                        # under the same request number as the header line above.
                        log_request(STAGE_BODY, 'GET', url, error=error, started=started,
                                    seq=seq, config=config)
                    raise
                return
            check_challenge(response.headers, response.content)
            location = response.headers.get('Location')
            if not location:
                raise BunkrError('服务器重定向缺少地址')
            if hop == _MAX_REDIRECTS:
                raise BunkrError('服务器重定向次数过多')
            url = urljoin(url, location)


def download_file(client, store, record, dest_dir, config, on_event=None,
                  session=None, clock=time.monotonic, stop_event=None, reserve_lock=None):
    from .reconcile import content_check

    link = None
    try:
        recovered = None
        with reserve_lock if reserve_lock is not None else nullcontext():
            # A crash after os.link() but before status='done' leaves a complete
            # file at local_path; adopt it instead of downloading a second copy.
            # Its content is inspected first: an error page that survived a crash
            # must be reported as failed, never adopted as done.
            claimed = _claimed_file(record)
            if claimed is not None:
                _reject_error_page(claimed, record)
            recovered = _completed_local(record)
            if recovered is not None:
                _update(store, record, status='done', last_error=None)
            else:
                target = _target(store, record, dest_dir)
                owned_done = record.status == 'done' and record.local_path == str(target)
                _update(store, record, status='downloading', attempts=record.attempts + 1,
                        last_error=None, local_path=str(target))
        if recovered is not None:
            _emit(on_event, 'start', record, message='开始下载', local_path=str(recovered))
            _emit(on_event, 'done', record, message='此前已下载完成', local_path=str(recovered))
            return recovered
        _emit(on_event, 'start', record, message='开始下载', local_path=str(target))
        if (owned_done and target.is_file()
                and (record.size is None or target.stat().st_size == record.size)):
            _update(store, record, status='done')
            _emit(on_event, 'done', record, message='此前已下载完成', local_path=str(target))
            return target
        # An owned done file with changed size must also be preserved.
        if target.exists():
            raise BunkrError('目标文件已存在且校验不符，拒绝覆盖')
        if stop_event is not None and stop_event.is_set():
            raise DownloadStopped()
        partial, metadata, owner = _partial_paths(target, record)
        link = client.resolve(record)
        _update(store, record, cdn_name=link.cdn_name, link_url=link.url, link_expires=link.expires_at)
        remote_path = urlsplit(link.url).path
        validator = owner.get('etag') or owner.get('last_modified')
        offset = partial.stat().st_size if partial.exists() else 0
        if (owner.get('path') != remote_path or owner.get('size') != record.size
                or not validator or (record.size is not None and offset >= record.size)):
            offset = 0
        transport = session
        if transport is None:
            # Reuse the client's own Session so the file body shares its connection
            # pool and proxy settings instead of opening a new connection per file.
            # Each web slot owns one client (and session), so no Session is ever
            # used by two download threads at the same time.
            transport = getattr(client, 'session', None) or requests
        throttle = getattr(client, 'throttle', None)
        for restart in range(2):
            headers = {'User-Agent': config.user_agent, 'Referer': link.referer,
                       **link.headers, 'Accept-Encoding': 'identity'}
            if offset:
                headers.update(Range=f'bytes={offset}-', **{'If-Range': validator})
            with _body_response(transport, link.url, headers, config, throttle, stop_event) as response:
                chunks = response.iter_content(chunk_size=config.chunk_size)
                prefix = bytearray()
                while len(prefix) < 8192:
                    if stop_event is not None and stop_event.is_set():
                        raise DownloadStopped()
                    chunk = next(chunks, None)
                    if chunk is None:
                        break
                    prefix.extend(chunk)
                check_challenge(response.headers, bytes(prefix))
                basename = unquote(urlsplit(response.url).path).rsplit('/', 1)[-1].lower()
                lower = bytes(prefix).lower()
                html = ('html' in response.headers.get('Content-Type', '').lower()
                        or lower.lstrip().startswith((b'<!doctype html', b'<html')))
                # A placeholder file is maintenance whatever its status; page text
                # only counts on an error page, so a payload containing the word
                # is not misread as maintenance.
                maintenance = basename in {'maint.mp4', 'maintenance-vid.mp4'}
                if html or response.status_code in (429, 503):
                    # Scan the whole error page before classifying it: a late
                    # challenge outranks rate limits, which outrank maintenance.
                    tail = lower
                    maintenance = maintenance or b'maintenance' in tail
                    for chunk in chunks:
                        if stop_event is not None and stop_event.is_set():
                            raise DownloadStopped()
                        tail = tail[-128:] + chunk.lower()
                        check_challenge(response.headers, tail)
                        maintenance = maintenance or b'maintenance' in tail
                if response.status_code in (429, 503):
                    raw_retry = response.headers.get('Retry-After')
                    retry_after = _retry_after(raw_retry)
                    if raw_retry is not None and retry_after is None:
                        _emit(on_event, 'invalid_time', record, message='忽略非法 Retry-After')
                    detail = '：服务器维护中' if maintenance else ''
                    raise RateLimited(f'HTTP {response.status_code}{detail}', retry_after=retry_after)
                if maintenance:
                    # The placeholder was never written to the partial, so an
                    # earlier breakpoint is still usable for the next run.
                    raise Maintenance('服务器维护中：返回维护占位内容')
                response.raise_for_status()
                if html:
                    # A saved HTML error page must never be kept as a breakpoint.
                    partial.unlink(missing_ok=True)
                    metadata.unlink(missing_ok=True)
                    raise BunkrError('服务器返回网页，无法保存为文件')
                total = record.size
                expected_bytes = None
                if response.status_code == 206:
                    match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', response.headers.get('Content-Range', ''))
                    if not match:
                        raise BunkrError('服务器返回的续传范围不正确')
                    start, end, remote_total = map(int, match.groups())
                    if (start != offset or end < start or end >= remote_total
                            or (total is not None and total != remote_total)):
                        raise BunkrError('服务器返回的续传范围不正确')
                    if offset and ((owner.get('etag') and response.headers.get('ETag') != owner['etag'])
                                   or (not owner.get('etag') and response.headers.get('Last-Modified') != validator)):
                        offset = 0
                        continue
                    total = remote_total
                    expected_bytes = end - start + 1
                else:
                    offset = 0
                length = response.headers.get('Content-Length')
                if length is not None:
                    try:
                        length = int(length)
                        if length < 0 or (expected_bytes is not None and expected_bytes != length):
                            raise ValueError
                    except ValueError:
                        raise BunkrError('响应长度与续传范围不一致') from None
                    expected_bytes = length
                    if total is None:
                        total = length + offset
                owner = dict(album_id=record.album_id, file_id=record.file_id, path=remote_path,
                             size=record.size, etag=response.headers.get('ETag'),
                             last_modified=response.headers.get('Last-Modified'), expected_size=total)
                # Invalidate old bytes before publishing new ownership on a restart.
                _reject_redirect(partial)
                _reject_redirect(metadata)
                with partial.open('ab' if offset else 'wb') as output:
                    metadata.write_text(json.dumps(owner), encoding='utf-8')
                    downloaded = offset
                    last_progress = float('-inf')
                    for chunk in _with_prefix(bytes(prefix), chunks):
                        # Check every chunk so pause takes effect without waiting for a read timeout.
                        if stop_event is not None and stop_event.is_set():
                            output.flush()
                            raise DownloadStopped()
                        output.write(chunk)
                        downloaded += len(chunk)
                        now = clock()
                        if now - last_progress >= 1:
                            last_progress = now
                            _emit(on_event, 'progress', record, message='正在下载',
                                  bytes=downloaded, downloaded=downloaded, total=total)
                if stop_event is not None and stop_event.is_set():
                    raise DownloadStopped()
                # Inspect the finished body even when its length also differs, and
                # judge it as the size we actually received: a JSON or text error
                # page of exactly the expected length is not a completed file.
                # Size validation stays separate, retaining useful partial data.
                acceptable, _ = content_check(partial, replace(record, size=downloaded))
                if not acceptable:
                    partial.unlink()
                    metadata.unlink()
                    raise BunkrError('文件内容校验失败：服务器返回错误页或内容无效')
                if ((expected_bytes is not None and downloaded - offset != expected_bytes)
                        or (total is not None and downloaded != total)):
                    raise BunkrError(f'文件大小不符：预期 {total}，实际 {downloaded}')
                _reject_redirect(target)
                # Hard linking is atomic and refuses an existing destination on both OSes.
                os.link(partial, target)
                partial.unlink()
                metadata.unlink()
                _update(store, record, status='done', last_error=None)
                _emit(on_event, 'done', record, message='下载完成', local_path=str(target))
                return target
        raise BunkrError('远端资源标识发生变化，续传已停止')
    except DownloadStopped:
        # Keep the .part file and its ownership metadata for the next run.
        _update(store, record, status='pending', last_error=None)
        _emit(on_event, 'stopped', record, message='已暂停，已下载的部分会保留')
        raise
    except DOWNLOAD_ERRORS as error:
        if getattr(error, 'winerror', None) == 1909:
            raise
        if link is not None:
            tag_error(error, STAGE_BODY, link.url)
        kind = error_kind(error)
        message = describe_error(kind, _detail(error), **error_context(error))
        # A failed body download must not keep showing an old link's validity.
        _update(store, record, status='failed', last_error=message,
                link_url=None, link_expires=None)
        _emit(on_event, 'failed', record, error_kind=kind, message=message)
        if kind == 'transient':
            replacement = TransientError(_detail(error))
            tag_error(replacement, getattr(error, 'stage', None),
                      host=getattr(error, 'host', None))
            raise replacement from None
        raise


def _with_prefix(prefix, chunks):
    yield prefix
    yield from chunks


def quick_delay(config, attempt, error):
    """5s, 10s, ... respecting Retry-After up to quick_backoff_max."""
    retry_after = getattr(error, 'retry_after', None)
    if retry_after is not None:
        try:
            value = float(retry_after)
        except (TypeError, ValueError, OverflowError):
            value = float('nan')
        if math.isfinite(value) and value >= 0:
            return min(value, config.quick_backoff_max)
    return min(config.quick_backoff_max, 5.0 * 2 ** min(attempt, 10))


def _persist_backoff(pacer, store, until):
    """Record the retry deadline so a restart still waits out the remaining time."""
    deadlines = getattr(pacer, '_deadlines', None)
    if callable(deadlines):
        deadlines()
    store.set_meta('backoff_until', str(until))


def run_queue(client, store, album_id, dest_dir, config, kinds=None,
              stop_event=None, on_event=None, sleep=time.sleep, rng=random, clock=time.time,
              exclude_ids=None, only_ids=None):
    result = QueueResult()
    pacer = Pacer(store, config, sleep=sleep, clock=clock, rng=rng,
                  stop_event=stop_event, on_event=on_event)
    excluded = set(exclude_ids or ())
    only = None if only_ids is None else set(only_ids)
    records = [r for r in store.list_files(album_id, kinds=kinds)
               if r.status in ('pending', 'failed') and r.file_id not in excluded
               and (only is None or r.file_id in only)]
    failed_kinds = {}
    positions = {r.file_id: index for index, r in enumerate(records, 1)}

    def stopped():
        return stop_event is not None and stop_event.is_set()

    def run_round(batch):
        """Process files once, with per-file quick retries; False means stop/abort."""
        for index, record in enumerate(batch, 1):
            # The end-of-round pass keeps the original queue numbering.
            position = positions.get(record.file_id, index)

            def file_event(event, _index=position, _count=len(records)):
                if on_event:
                    if event['type'] in ('start', 'done'):
                        event = {**event, 'index': _index, 'count': _count}
                    on_event(event)

            if not pacer.before_file():
                return False
            limited_failure = False
            for attempt in range(config.quick_retries + 1):
                try:
                    download_file(client, store, record, dest_dir, config,
                                  on_event=file_event, stop_event=stop_event)
                    failed_kinds.pop(record.file_id, None)
                    limited_failure = False
                    break
                except DownloadStopped:
                    return False
                except ChallengeRequired as error:
                    result.aborted_reason = str(error)
                    _emit(on_event, 'aborted', record,
                          message=f'需要人机验证，队列已中止：{describe_error("challenge")}')
                    return False
                except (RateLimited, TransientError) as error:
                    kind = error_kind(error)
                    if getattr(error, 'invalid_retry_after', False):
                        _emit(on_event, 'invalid_time', record, message='忽略非法 Retry-After')
                    limited_failure = kind == 'rate_limited'
                    failed_kinds[record.file_id] = kind
                    if attempt >= config.quick_retries:
                        break
                    delay = quick_delay(config, attempt, error)
                    until = clock() + delay
                    message = describe_error(kind, _detail(error))
                    _persist_backoff(pacer, store, until)
                    _emit(on_event, 'retry', record, message=f'{message}，稍后自动重试',
                          seconds=delay, reason='backoff', until=until)
                    _emit(on_event, 'wait', record, message='请求失败，稍后自动重试',
                          seconds=delay, reason='backoff', until=until)
                    if not pacer.wait(delay):
                        return False
                except DOWNLOAD_ERRORS as error:
                    if getattr(error, 'winerror', None) == 1909:
                        raise
                    kind = error_kind(error)
                    if kind != 'challenge':
                        failed_kinds[record.file_id] = kind
                    limited_failure = False
                    break
            pacer.file_done(limited_failure and record.status != 'done')
            if stopped():
                return False
        return True

    if not run_round(records):
        result.stopped = stopped()
    elif not stopped():
        # One extra pass over this round's retryable failures.
        retryable = [r for r in store.list_files(album_id)
                     if r.file_id in failed_kinds and r.status == 'failed'
                     and failed_kinds[r.file_id] in ROUND_RETRY_KINDS]
        if retryable and not run_round(retryable):
            result.stopped = stopped()

    current = {r.file_id: r for r in store.list_files(album_id)}
    selected = [current[r.file_id] for r in records if r.file_id in current]
    result.done = sum(1 for r in selected if r.status == 'done')
    result.failed = sum(1 for r in selected if r.status == 'failed')
    result.stopped = result.stopped or stopped()
    _emit(on_event, 'finished', message='队列已停止' if result.stopped else '队列处理结束',
          done=result.done, failed=result.failed, skipped=result.skipped,
          stopped=result.stopped, aborted_reason=result.aborted_reason)
    return result
