"""Export fresh signatures in independently published, versioned volumes."""
from __future__ import annotations

import csv
import math
import os
import re
import sqlite3
import time
from contextlib import closing
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, unquote_plus, urlsplit, urlunsplit

import requests

from .names import unique_names
from .messages import describe_error, error_kind
from .models import BunkrError, ChallengeRequired, ExportResult, RateLimited, TransientError
from .worktypes import FAILED, INTERRUPTED, OK, ItemResult, WorkContext

_URL = re.compile(r'https?://\S+')


def _detail(error) -> str:
    """Short technical reason for describe_error, never carrying a signed URL."""
    if isinstance(error, requests.Timeout):
        text = '连接超时'
    elif isinstance(error, requests.ConnectionError):
        text = '连接失败'
    else:
        text = str(error) or '未知错误'
    return _URL.sub('[地址已省略]', text).strip().splitlines()[0][:120]


def _named_url(url, name):
    # Keep signature parameters byte-for-byte; replace only the filename hint.
    parts = urlsplit(url)
    query = [part for part in parts.query.split('&')
             if part and unquote_plus(part.partition('=')[0]) != 'n']
    query.append('n=' + quote(name, safe=''))
    return urlunsplit(parts._replace(query='&'.join(query)))


def download_url(record, config, names: dict[str, str] | None = None) -> str:
    """Format a saved link using the same whole-album names as CLI volumes.

    Callers with an open store should pass unique_names(store.list_files(...)).
    Otherwise read the configured database without migrations or writes.
    """
    if not record.link_url:
        return ''
    if names is None:
        uri = Path(config.state_db).expanduser().resolve().as_uri() + '?mode=ro'
        with closing(sqlite3.connect(uri, uri=True)) as db:
            rows = db.execute('SELECT file_id, name FROM files WHERE album_id=? ORDER BY rowid',
                              (record.album_id,)).fetchall()
        names = unique_names(SimpleNamespace(file_id=ident, name=name) for ident, name in rows)
    return _named_url(record.link_url, names[record.file_id])


def generate_link_once(ctx: WorkContext, album_id, file_id) -> ItemResult:
    """Resolve once and publish one saved link; the coordinator owns retries."""
    if ctx.stop_event.is_set():
        return ItemResult(INTERRUPTED)
    record = next((r for r in ctx.store.list_files(album_id) if r.file_id == file_id), None)
    if record is None:
        return ItemResult(FAILED, 'not_found', '文件不存在，请重新添加相册')
    ctx.store.update_file(album_id, file_id, attempts=record.attempts + 1)
    try:
        link = ctx.client.resolve(record)
        remaining = None if link.expires_at is None else link.expires_at - time.time()
        if remaining is None or not math.isfinite(remaining):
            raise BunkrError('下载链接有效期未知，请重新获取下载链接')
        if remaining < 600:
            raise BunkrError('下载链接剩余不足 10 分钟，请重新获取下载链接')
    except Exception as error:
        if getattr(error, 'winerror', None) == 1909:
            raise
        # A challenge must still reach the coordinator when pause races a request.
        if ctx.stop_event.is_set() and not isinstance(error, ChallengeRequired):
            return ItemResult(INTERRUPTED)
        kind = error_kind(error)
        message = describe_error(kind, _detail(error))
        ctx.store.update_file(album_id, file_id, status='failed', last_error=message,
                              link_url=None, link_expires=None)
        ctx.store.remember_action(album_id, file_id, 'link')
        return ItemResult(FAILED, kind, message, getattr(error, 'retry_after', None))
    # A finished signature is useful even if pause arrived during the request.
    ctx.store.update_file(album_id, file_id, status='exported', last_error=None,
                          link_url=link.url, link_expires=link.expires_at, cdn_name=link.cdn_name)
    ctx.on_event(dict(type='link', album_id=album_id, file_id=file_id, action='link',
                      name=record.name, message='链接已生成', link_expires=link.expires_at))
    return ItemResult(OK)


def quick_delay(config, attempt, retry_after):
    """5s, 10s, ... respecting Retry-After up to quick_backoff_max."""
    if retry_after is not None:
        try:
            value = float(retry_after)
        except (TypeError, ValueError, OverflowError):
            value = float('nan')
        if math.isfinite(value) and value >= 0:
            return min(value, config.quick_backoff_max)
    return min(config.quick_backoff_max, 5.0 * 2 ** min(attempt, 10))


def _local_time(timestamp) -> str:
    return (datetime.fromtimestamp(timestamp, timezone.utc).astimezone().isoformat(timespec='seconds')
            if timestamp is not None else '未知')


def _new_directory(out_dir):
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().replace(microsecond=0)
    # Reserve atomically, including when two exports start within the same second.
    while True:
        destination = root / stamp.strftime('%Y%m%d-%H%M%S')
        try:
            destination.mkdir()
            return destination
        except FileExistsError:
            stamp += timedelta(seconds=1)


def _publish(destination, writers, before_publish=None):
    """Stage every file before publishing any; states are committed by the caller."""
    staged, published = [], []
    try:
        for name, encoding, write in writers:
            target = destination / name
            temporary = destination / (name + '.tmp')
            staged.append(temporary)
            with temporary.open('x', encoding=encoding, newline='') as stream:
                write(stream)
                stream.flush()
                os.fsync(stream.fileno())
            published.append(target)
        if before_publish is not None and not before_publish():
            for path in staged:
                path.unlink(missing_ok=True)
            return []
        for temporary, target in zip(staged, published):
            temporary.rename(target)
    except BaseException:
        # This directory belongs to this invocation; previous exports are untouched.
        for path in staged + published:
            path.unlink(missing_ok=True)
        raise
    return published


def export_links(client, store, album_id, out_dir, config, kinds=('video',),
                 on_event=None, stop_event=None, pacer=None, file_ids=None) -> ExportResult:
    if pacer is None:
        from .guard import Pacer
        pacer = Pacer(store, config, stop_event=stop_event, on_event=on_event)
    records = store.list_files(album_id)
    names = unique_names(records)
    selected = None if file_ids is None else set(file_ids)
    eligible = [r for r in records if r.status in ('pending', 'failed', 'exported')
                and (kinds is None or r.kind in kinds)
                and (selected is None or r.file_id in selected)]
    indices = {r.file_id: index for index, r in enumerate(eligible, 1)}
    destination = _new_directory(out_dir)
    result = ExportResult(out_dir=str(destination))
    batch_size = max(1, int(getattr(config, 'export_batch_size', 50)))
    pending, volumes = [], []

    def stopped():
        return stop_event is not None and stop_event.is_set()

    def emit(kind, record=None, **fields):
        if on_event:
            if kind in ('start', 'done'):
                fields = dict(index=indices[record.file_id], count=len(eligible), **fields)
            on_event(dict(type=kind, file_id=record.file_id if record else None,
                          name=record.name if record else None, **fields))

    def failure(record, message, error=None):
        error = error if error is not None else BunkrError(message)
        kind = error_kind(error)
        emit('failed', record, error_kind=kind, message=message)

    def fail(record, message, error=None):
        store.update_file(album_id, record.file_id, status='failed',
                          last_error=message, link_url=None, link_expires=None)
        store.remember_action(album_id, record.file_id, 'link')
        result.failed += 1
        failure(record, message, error)

    def flush():
        now = time.time()
        valid = []
        for record, link in pending:
            if link.expires_at is not None and link.expires_at <= now:
                fail(record, '签名已过期，请重新生成下载链接')
            else:
                valid.append((record, link))
        pending.clear()
        if not valid or stopped():
            return
        expirations = [link.expires_at for _, link in valid if link.expires_at is not None]
        expires = min(expirations) if expirations else None
        header = (f'# 生成时间：{_local_time(now)}\n'
                  f'# 最早过期时间：{_local_time(expires)}\n')
        prefix = f'part-{len(volumes) + 1:02d}-'

        def links(stream, aria=False):
            stream.write(header)
            for record, link in valid:
                url = link.url
                if config.link_name_param:
                    url = _named_url(url, names[record.file_id])
                stream.write(url + '\n')
                if aria:
                    ua = next((v for k, v in link.headers.items() if k.lower() == 'user-agent'),
                              config.user_agent)
                    stream.write(f'  out={names[record.file_id]}\n'
                                 f'  referer={link.referer}\n  header=User-Agent: {ua}\n')

        def mapping(stream):
            stream.write(header)
            writer = csv.writer(stream)
            writer.writerow(['cdn_name', 'name', 'size', 'file_id', 'expires_at'])
            for record, link in valid:
                writer.writerow([link.cdn_name, names[record.file_id], record.size,
                                 record.file_id, _local_time(link.expires_at)])

        def ready():
            # fsync may be slow: recheck after ALL staging, immediately before rename.
            now = time.time()
            rejected = [(r, link) for r, link in valid
                        if link.expires_at is not None and link.expires_at - now < 600]
            if not rejected:
                return True
            for record, _ in rejected:
                result.failed += 1
                failure(record, '签名剩余不足 10 分钟，请重新生成下载链接')
            valid[:] = [(r, link) for r, link in valid if (r, link) not in rejected]
            return False

        paths = _publish(destination, [
            (prefix + 'links.txt', 'utf-8', links),
            (prefix + 'aria2.txt', 'utf-8', lambda stream: links(stream, True)),
            (prefix + 'rename_map.csv', 'utf-8-sig', mapping)], before_publish=ready)
        if not paths:
            pending.extend(valid)
            if pending:
                flush()
            return
        result.files.extend(str(path) for path in paths)
        volumes.append((paths, expires))
        # Publish the whole volume before any status or user callback can observe it.
        # Published CSV and link files remain recovery evidence if this transaction fails.
        store.update_files([(album_id, record.file_id,
                             dict(status='exported', last_error=None, link_url=link.url,
                                  link_expires=link.expires_at, cdn_name=link.cdn_name))
                            for record, link in valid])
        result.exported += len(valid)
        for record, _ in valid:
            emit('exported', record, message='已写出签名直链分卷', out_dir=str(destination))
            emit('done', record, message='签名直链已生成', out_dir=str(destination))

    for record in eligible:
        if stopped() or not pacer.before_file() or stopped():
            result.stopped = True
            break
        limited = False
        emit('start', record, message='正在生成签名直链')
        try:
            for attempt in range(config.quick_retries + 1):
                if stopped():
                    result.stopped = True
                    break
                try:
                    link = client.resolve(record)
                except ChallengeRequired as error:
                    record.attempts += 1
                    store.update_file(album_id, record.file_id, attempts=record.attempts)
                    message = describe_error('challenge')
                    result.stopped = True
                    result.aborted_reason = message
                    fail(record, message, error)
                    emit('challenge', record, message=message)
                    break
                except (RateLimited, TransientError) as error:
                    if getattr(error, 'invalid_retry_after', False):
                        emit('invalid_time', record, message='忽略非法 Retry-After')
                    record.attempts += 1
                    store.update_file(album_id, record.file_id, attempts=record.attempts)
                    kind = error_kind(error)
                    message = describe_error(kind, _detail(error))
                    if attempt < config.quick_retries:
                        delay = quick_delay(config, attempt, getattr(error, 'retry_after', None))
                        until = getattr(pacer, 'clock', time.time)() + delay
                        emit('retry', record, message=f'{message}，稍后自动重试',
                             seconds=delay, reason='backoff', until=until)
                        emit('wait', record, message='请求失败，稍后自动重试',
                             seconds=delay, reason='backoff', until=until)
                        if stopped() or not pacer.wait(delay):
                            result.stopped = True
                            break
                        continue
                    limited = kind == 'rate_limited'
                    fail(record, message, error)
                    break
                except Exception as error:
                    if getattr(error, 'winerror', None) == 1909:
                        raise
                    record.attempts += 1
                    store.update_file(album_id, record.file_id, attempts=record.attempts)
                    fail(record, describe_error(error_kind(error), _detail(error)), error)
                    break
                else:
                    pending.append((record, link))
                    break
        finally:
            pacer.file_done(rate_limited=limited)
        if result.stopped or stopped():
            result.stopped = True
            break
        if len(pending) >= batch_size:
            flush()
    if not result.stopped and not stopped():
        flush()
    result.stopped = result.stopped or stopped()

    def index(stream):
        stream.write('# 下载链接分卷及最早过期时间\n')
        for paths, expires in volumes:
            stream.write(f'{", ".join(path.name for path in paths)}\t{_local_time(expires)}\n')
        if result.stopped:
            stream.write('# 已停止；仅列出已写好的分卷，未写出条目保持原状态\n')
        stream.write(f'# 已生成：{result.exported}；失败：{result.failed}\n')

    result.files.extend(str(path) for path in _publish(destination, [('index.txt', 'utf-8', index)]))
    return result
