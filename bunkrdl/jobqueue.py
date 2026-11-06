"""Persistent FIFO coordinator with independent image and video execution slots."""
from __future__ import annotations

import copy
import json
import threading
import time
from collections import Counter, deque
from types import SimpleNamespace

from . import messages
from .guard import JobLock
from .limiter import LaneLimiter, RateLimitGate, RequestThrottle
from .models import STATUSES, JobBusy
from .worktypes import (ACTIONS, DOWNLOAD, FAILED, INTERRUPTED, LINK, OK, QUEUED,
                        RUNNING, Busy, EnqueueResult, ItemResult, WorkContext, lane_of)

PAUSE_KEY = 'web:pause_state'
RATE_LIMIT_PAUSE = '被网站连续限速，已暂停。建议降低并发或等一会儿再继续'


class _StoreClosed(Exception):
    pass


class _StoreLease:
    """Revoke late workers before the owner closes the underlying database."""
    def __init__(self, store):
        self.store = store
        self.lock = threading.RLock()
        self.closed = False

    def __getattr__(self, name):
        def call(*args, **kwargs):
            with self.lock:
                if self.closed:
                    raise _StoreClosed('状态库访问已结束')
                return getattr(self.store, name)(*args, **kwargs)
        return call

    def revoke(self):
        with self.lock:
            self.closed = True


class Coordinator:
    SAMPLE_WINDOW = 5.0

    def __init__(self, config, db_path, store, client_factory, *, ops=None, clock=time.time):
        self.config, self.db_path, self.store = config, db_path, store
        self.client_factory, self.clock = client_factory, clock
        if ops is None:
            from . import export, flows
            ops = SimpleNamespace(download_once=flows.download_once,
                                  generate_link_once=export.generate_link_once)
        self.ops = ops
        self._lock = threading.RLock()
        self._wake = threading.Condition(self._lock)
        self._lease = _StoreLease(store)
        self._reserve_lock = threading.Lock()
        self._throttle = RequestThrottle(config.web_request_interval)
        # Bunkr limits the exit IP, so one gate covers both lanes.
        self._gate = RateLimitGate(config.web_cooldown, self._lease,
                                   cap=config.ratelimit_cooldown_max, clock=clock)
        self._lanes = {
            'image': LaneLimiter('image', config.image_concurrency, self._lease,
                                 gap_min=config.image_gap, clock=clock, gate=self._gate),
            'video': LaneLimiter('video', config.video_concurrency, self._lease,
                                 gap_min=config.video_gap_min, gap_max=config.video_gap_max,
                                 clock=clock, gate=self._gate),
        }
        self._items, self._active, self._clients = {}, {}, {}
        self._events, self._failures = deque(maxlen=500), deque(maxlen=20)
        self._sequence = self._consecutive = 0
        self._paused = self._closed = self._started = self._initialized = False
        self._pause_kind = self._pause_reason = None
        self._job_lock = self._thread = None

    def _own(self):
        if self._job_lock is None:
            lock = JobLock(self.db_path)
            lock.__enter__()
            self._job_lock = lock

    def _release(self):
        if self._job_lock is not None and not self._active:
            self._job_lock.__exit__(None, None, None)
            self._job_lock = None

    def _ensure_open(self):
        if self._closed:
            raise Busy('正在关闭，请稍后再试')

    def _initialize(self):
        if self._initialized:
            return
        self._own()
        try:
            self.store.reset_running_work()
            self.store.recover_interrupted()
            self._items = {w.work_id: w for w in self.store.list_work_items()}
            for lane in self._lanes.values():
                lane.load()
            self._gate.load()
            self._initialized = True
        finally:
            self._release()

    def start(self):
        with self._wake:
            self._ensure_open()
            if self._started:
                return
            self._items = {w.work_id: w for w in self.store.list_work_items()}
            if self._items:
                kind, reason = self._saved_pause()
                self._pause(kind, reason)
            try:
                self._initialize()
            except JobBusy:
                pass
            self._started = True
            self._thread = threading.Thread(target=self._run, name='bunkrdl-coordinator', daemon=True)
            self._thread.start()

    def _saved_pause(self):
        """Pause reason persisted by the previous run, else the generic restart notice."""
        try:
            data = json.loads(self.store.get_meta(PAUSE_KEY) or 'null')
            if (isinstance(data, dict) and data.get('kind') in ('user', 'failures', 'challenge', 'ratelimit')
                    and isinstance(data.get('reason'), str) and data['reason']):
                return data['kind'], data['reason']
        except Exception:
            pass
        return 'restart', '程序已重启，点“继续”处理保留的文件'

    def _persist_pause(self, value):
        try:
            self.store.set_meta(PAUSE_KEY, json.dumps(value, ensure_ascii=False) if value else '')
        except Exception:
            pass  # a closing store must not break pausing

    def close(self, timeout=30):
        deadline = time.monotonic() + max(0, timeout)
        with self._wake:
            self._closed = True
            for active in self._active.values():
                active['stop'].set()
            self._wake.notify_all()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(max(0, deadline - time.monotonic()))
        with self._wake:
            self._lease.revoke()
            self._release()

    def mutate(self, fn, file_keys=()):
        with self._wake:
            self._ensure_open()
            keys = set(file_keys)
            if any((w.album_id, w.file_id) in keys for w in self._items.values()):
                raise Busy('选中的文件正在处理或排队，请等待结束或取消后再操作')
            self._own()
            try:
                return fn()
            finally:
                self._release()

    def ensure_album_idle(self, album_id):
        """Reject rewriting an album while any of its work items are queued or running."""
        with self._wake:
            self._ensure_open()
            if any(w.album_id == album_id for w in self._items.values()):
                raise Busy('这个相册还有排队或正在处理的文件，请等待结束或取消排队后再添加')

    def ensure_files_idle(self, file_keys):
        """Fail fast (before slow scans) when any (album, file) has queued/running work."""
        with self._wake:
            self._ensure_open()
            keys = set(file_keys)
            if any((w.album_id, w.file_id) in keys for w in self._items.values()):
                raise Busy('选中的文件正在处理或排队，请等待结束或取消后再操作')

    def new_client(self):
        """一次性的查询客户端（如添加相册），与调度器共用同一个请求节流器。"""
        return self.client_factory(self._throttle)

    def cooldown_remaining(self):
        """Seconds left on the shared exit-IP cooldown; 0 when a request may go."""
        with self._lock:
            return self._gate.remaining()

    def register_rate_limit(self, retry_after=None):
        """Record a 429/503 that arrived outside a work item (e.g. adding an album).

        The same IP-wide gate owns the cooldown, so the exponential backoff, the
        dispatch stop and the four-in-a-row pause apply here too. Returns the
        cooldown deadline so the caller can tell the user how long to wait.
        """
        with self._wake:
            self._ensure_open()
            until = self._gate.limited(retry_after)
            self._emit('cooldown', until=until,
                       message='被网站限速，暂停派发新文件，稍后自动继续')
            if self._gate.streak >= max(1, self.config.ratelimit_pause_after):
                self._pause('ratelimit', RATE_LIMIT_PAUSE)
            self._wake.notify_all()
            return until

    @staticmethod
    def _action_key(album_id, file_id):
        return f'web:{album_id}:last_action:{file_id}'

    def enqueue(self, album_id, file_ids, action):
        if action not in ACTIONS:
            raise ValueError('动作须为 download 或 link')
        if not isinstance(file_ids, (list, tuple)) or any(not isinstance(i, str) or not i for i in file_ids):
            raise ValueError('file_ids 须为文件 id 数组')
        with self._wake:
            self._ensure_open()
            if not self._initialized:
                self._initialize()
            self._own()
            try:
                records = {r.file_id: r for r in self.store.list_files(album_id)}
                if self.store.get_album(album_id) is None or set(file_ids) - records.keys():
                    raise ValueError('相册不存在或包含不属于此相册的文件')
                result, rows = EnqueueResult(), []
                existing = {(w.album_id, w.file_id): w for w in self._items.values()}
                for file_id in dict.fromkeys(file_ids):
                    record = records[file_id]
                    work = existing.get((album_id, file_id))
                    if work is not None:
                        result.work_ids.append(work.work_id)
                        if work.action == action:
                            result.already += 1
                        elif work.state == RUNNING:
                            result.busy += 1
                        else:
                            self.store.replace_work_action(work.work_id, action)
                            work.action = action
                            result.replaced += 1
                        continue
                    if record.status not in ('pending', 'failed', 'exported'):
                        result.not_applicable += 1
                        continue
                    rows.append((album_id, file_id, action, lane_of(record.kind)))
                for work in self.store.add_work_items(rows):
                    self._items[work.work_id] = work
                    result.work_ids.append(work.work_id)
                    result.added += 1
                self._wake.notify_all()
                return result
            finally:
                self._release()

    def retry_failed(self, album_id):
        with self._lock:
            self._ensure_open()
            result = EnqueueResult()
            for record in self.store.list_files(album_id, status='failed'):
                action = getattr(record, 'last_action', None) or self.store.get_meta(
                    self._action_key(album_id, record.file_id))
                current = self.enqueue(album_id, [record.file_id], LINK if action == LINK else DOWNLOAD)
                for key in ('added', 'already', 'replaced', 'busy', 'not_applicable'):
                    setattr(result, key, getattr(result, key) + getattr(current, key))
                result.work_ids.extend(current.work_ids)
            return result

    def cancel_queued(self, album_id, file_ids):
        """Cancel not-yet-started work items for the selected files.

        Enqueueing never changes a file's status, so deleting a not-yet-started
        work item restores the pre-enqueue view (pending, failed or exported)
        without a second write. A file that already started once and was
        interrupted by pause is already pending (its old link is consumed by
        design), so cancelling its requeued item leaves it pending too. Running
        files are rejected atomically: the caller must wait or pause
        everything, never race a slot.
        """
        if not isinstance(file_ids, (list, tuple)) or any(
                not isinstance(i, str) or not i for i in file_ids):
            raise ValueError('file_ids 须为文件 id 数组')
        wanted = set(file_ids)
        with self._wake:
            self._ensure_open()
            if not self._initialized:
                self._initialize()
            self._own()
            try:
                records = {r.file_id: r for r in self.store.list_files(album_id)}
                if self.store.get_album(album_id) is None or wanted - records.keys():
                    raise ValueError('相册不存在或包含不属于此相册的文件')
                targets = [w for w in self._items.values()
                           if w.album_id == album_id and w.file_id in wanted]
                # The dispatch path flips state and registers the active slot
                # while holding _wake, so this check cannot miss a picked file.
                if any(w.state != QUEUED or w.work_id in self._active for w in targets):
                    raise Busy('正在处理，请稍后或使用“暂停全部”')
                self.store.delete_work_items([w.work_id for w in targets])
                for work in targets:
                    self._items.pop(work.work_id, None)
                    self._emit('cancelled', work, message='已取消排队')
                if targets:
                    self._wake.notify_all()
                return len(targets)
            finally:
                self._release()

    def cancel_album(self, album_id):
        with self._wake:
            self._ensure_open()
            targets = [w for w in self._items.values() if w.album_id == album_id]
            if not targets:
                return 0
            self._own()
            try:
                queued = [w.work_id for w in targets if w.work_id not in self._active]
                self.store.delete_work_items(queued)
                for work_id in queued:
                    work = self._items.pop(work_id)
                    self._emit('cancelled', work, message='已取消排队')
                for work in targets:
                    active = self._active.get(work.work_id)
                    if active:
                        active['cancelled'] = True
                        active['stop'].set()
                self._wake.notify_all()
                return len(targets)
            finally:
                self._release()

    PURGE_BUSY = '还有文件在排队或处理，请先取消排队或暂停后再清除'

    def _require_no_work(self, album_id, file_ids=None):
        """Reject a purge while any affected work item is queued or running.

        Runs in the same _wake critical section as dispatch, so a file can not
        be picked between the check and the delete.
        """
        wanted = None if file_ids is None else set(file_ids)
        for work in self._items.values():
            if work.album_id == album_id and (wanted is None or work.file_id in wanted):
                raise Busy(self.PURGE_BUSY)

    def purge_album(self, album_id):
        """Delete every record of one album; files on disk are never touched."""
        with self._wake:
            self._ensure_open()
            if not self._initialized:
                self._initialize()
            self._own()
            try:
                self._require_no_work(album_id)
                deleted = self.store.delete_album_records(album_id)
                self._forget(album_id)
                return deleted
            finally:
                self._release()

    def purge_files(self, album_id, file_ids=None, statuses=None):
        """Delete selected file records of one album; returns the deleted count."""
        if file_ids is None and statuses is None:
            raise ValueError('须指定文件 id 或状态')
        if file_ids is not None and (not isinstance(file_ids, (list, tuple))
                                     or any(not isinstance(i, str) or not i for i in file_ids)):
            raise ValueError('file_ids 须为文件 id 数组')
        if statuses is not None and (not isinstance(statuses, (list, tuple))
                                     or any(s not in STATUSES for s in statuses)):
            raise ValueError('未知的文件状态')
        with self._wake:
            self._ensure_open()
            if not self._initialized:
                self._initialize()
            self._own()
            try:
                targets = self.store.match_file_ids(album_id, file_ids=file_ids,
                                                    statuses=statuses)
                if not targets:
                    return 0
                self._require_no_work(album_id, targets)
                deleted = self.store.delete_files(album_id, file_ids=targets)
                self._forget(album_id, targets)
                return deleted
            finally:
                self._release()

    def _forget(self, album_id, file_ids=None):
        """Drop in-memory traces of records that no longer exist."""
        wanted = None if file_ids is None else set(file_ids)

        def gone(entry):
            return entry['album_id'] == album_id and (wanted is None or entry['file_id'] in wanted)

        kept = [entry for entry in self._failures if not gone(entry)]
        if len(kept) != len(self._failures):
            self._failures = deque(kept, maxlen=20)
            # The failure latch counted files whose records are gone now.
            self._consecutive = 0
        events = [event for event in self._events
                  if not (event['type'] == 'failed' and gone(event))]
        if len(events) != len(self._events):
            self._events.clear()
            self._events.extend(events)

    def _pause(self, kind, reason, work=None):
        if self._paused and kind != 'challenge':
            return
        self._paused, self._pause_kind, self._pause_reason = True, kind, reason
        if kind != 'restart':
            self._persist_pause({'kind': kind, 'reason': reason})
        for active in self._active.values():
            active['stop'].set()
        self._emit('paused', work, message=reason, pause_kind=kind)

    def pause_all(self, reason='user'):
        with self._wake:
            self._ensure_open()
            self._pause('user', '用户已暂停' if reason == 'user' else str(reason))
            self._wake.notify_all()
            return self.snapshot()

    def pause_for_challenge(self):
        """Pause all lanes when a request outside a work item hits a challenge."""
        with self._wake:
            self._pause('challenge', messages.describe_error('challenge'))
            self._wake.notify_all()
            return self.snapshot()

    def resume_all(self):
        with self._wake:
            self._ensure_open()
            limited = self._pause_kind == 'ratelimit'
            self._paused, self._pause_kind, self._pause_reason = False, None, None
            self._persist_pause(None)
            self._consecutive = 0
            if limited:
                # After an explicit continue, start over from the smallest
                # cooldown and let a single file test the water first.
                self._gate.reset()
            self._emit('resumed', message='已继续')
            self._wake.notify_all()
            return self.snapshot()

    def set_concurrency(self, lane, value):
        if lane not in self._lanes:
            raise ValueError('通道须为 image 或 video')
        self.mutate(lambda: self._lanes[lane].set_concurrency(value))
        with self._wake:
            self._wake.notify_all()
            return self.snapshot()

    def snapshot(self):
        with self._lock:
            active = [dict(a['public']) for a in self._active.values()]
            now = self.clock()
            for item in active:
                samples = self._active[item['work_id']]['samples']
                if samples and now - samples[-1][0] > self.SAMPLE_WINDOW:
                    item['speed'] = 0
            queued = Counter(w.album_id for w in self._items.values() if w.state == QUEUED)
            return {'paused': self._paused, 'pause_kind': self._pause_kind,
                    'pause_reason': self._pause_reason,
                    'stopping': self._closed or any(a['stop'].is_set() for a in self._active.values()),
                    'lanes': {key: lane.snapshot() for key, lane in self._lanes.items()},
                    'active': active, 'speed': sum(a['speed'] for a in active),
                    'queued': sum(queued.values()), 'queued_by_album': dict(queued),
                    'consecutive_failures': self._consecutive, 'recent_failures': copy.deepcopy(list(self._failures))}

    def events(self, since=0):
        with self._lock:
            return {'events': copy.deepcopy([e for e in self._events if e['sequence'] > since]),
                    'next': self._sequence}

    def _emit(self, kind, work=None, **fields):
        self._sequence += 1
        identity = dict(work_id=None, album_id=None, file_id=None, action=None, name=None)
        if work is not None:
            identity.update(work_id=work.work_id, album_id=work.album_id,
                            file_id=work.file_id, action=work.action)
            active = self._active.get(work.work_id)
            identity['name'] = active['public']['name'] if active else work.file_id
        self._events.append(dict(identity, **{'type': kind, 'sequence': self._sequence,
                                             'time': self.clock(), 'message': '', **fields}))

    def _handle(self, work, event):
        with self._lock:
            active = self._active.get(work.work_id)
            if active is None or self._lease.closed:
                return
            kind = event.get('type')
            payload = {k: v for k, v in event.items()
                       if k not in ('type', 'work_id', 'album_id', 'file_id', 'action', 'name')}
            if kind == 'progress':
                public, samples = active['public'], active['samples']
                now, amount = self.clock(), event.get('bytes', event.get('downloaded', 0)) or 0
                if samples and amount < samples[-1][1]:
                    samples.clear()
                samples.append((now, amount))
                while len(samples) > 2 and samples[1][0] <= now - self.SAMPLE_WINDOW:
                    samples.popleft()
                elapsed = now - samples[0][0]
                public.update(bytes=amount, total=event.get('total', public['total']),
                              speed=max(0, (amount - samples[0][1]) / elapsed) if elapsed > 0 else 0)
                # Progress reaches the page through snapshot(); keeping it out of
                # the event ring stops large files from evicting failed/paused.
            elif kind == 'link':
                active['linked'] = True
                self._emit('link', work, **payload)
            # Terminal callbacks never settle a result a second time.

    def _run(self):
        try:
            with self._wake:
                while True:
                    if self._closed and not self._active:
                        return
                    if not self._closed:
                        try:
                            self._initialize()
                            if not self._paused:
                                self._dispatch()
                        except JobBusy:
                            pass
                        except Exception as error:
                            if getattr(error, 'winerror', None) == 1909:
                                self._closed = True
                            self._pause('failures', messages.describe_error('other', str(error)))
                    self._release()
                    self._wake.wait(0.05)
        finally:
            with self._lock:
                self._release()

    def _dispatch(self):
        gate = self._gate
        # A cooling gate stops every lane; probe mode lets exactly one file
        # through until it either succeeds or is rate-limited again.
        if not gate.allows(len(self._active)):
            return
        started = 0
        for work in sorted(self._items.values(), key=lambda w: w.seq):
            if work.state != QUEUED:
                continue
            if gate.probe and started:
                break
            busy = {a['slot'] for a in self._active.values() if a['public']['lane'] == work.lane}
            slot = self._lanes[work.lane].available_slot(busy)
            if slot is None:
                continue
            self._own()
            records = {r.file_id: r for r in self.store.list_files(work.album_id)}
            record = records.get(work.file_id)
            if record is None or record.status not in ('pending', 'failed', 'exported'):
                self.store.delete_work_items([work.work_id])
                del self._items[work.work_id]
                continue
            self.store.update_work_item(work.work_id, state=RUNNING)
            self.store.remember_action(work.album_id, work.file_id, work.action)
            work.state = RUNNING
            public = dict(work_id=work.work_id, album_id=work.album_id, file_id=work.file_id,
                          name=record.name, action=work.action, lane=work.lane, bytes=0,
                          total=record.size, speed=0)
            active = dict(public=public, samples=deque(), stop=threading.Event(),
                          slot=slot, cancelled=False, linked=False)
            self._active[work.work_id] = active
            started += 1
            self._emit('started', work, message='开始下载' if work.action == DOWNLOAD else '正在获取下载链接')
            thread = threading.Thread(target=self._execute, args=(work, active), daemon=True,
                                      name=f'bunkrdl-{work.lane}-{slot}')
            active['thread'] = thread
            try:
                thread.start()
            except Exception:
                self._active.pop(work.work_id, None)
                self.store.update_work_item(work.work_id, state=QUEUED)
                work.state = QUEUED
                raise

    def _execute(self, work, active):
        result = ItemResult(INTERRUPTED)
        fatal = False
        try:
            key = (work.lane, active['slot'])
            client = self._clients.get(key)
            if client is None:
                client = self.new_client()
                self._clients[key] = client
            ctx = WorkContext(self.config, self._lease, client, active['stop'],
                              lambda e: self._handle(work, e), self._reserve_lock)
            op = self.ops.download_once if work.action == DOWNLOAD else self.ops.generate_link_once
            if not active['stop'].is_set():
                result = op(ctx, work.album_id, work.file_id)
            if not isinstance(result, ItemResult) or result.outcome not in (OK, FAILED, INTERRUPTED):
                raise ValueError('单文件操作返回了无效结果')
        except _StoreClosed:
            pass
        except Exception as error:
            fatal = getattr(error, 'winerror', None) == 1909
            kind = messages.error_kind(error)
            result = ItemResult(FAILED, kind, messages.describe_error(kind, str(error)),
                                getattr(error, 'retry_after', None))
        finally:
            with self._wake:
                try:
                    if not self._lease.closed:
                        self._settle(work, active, result)
                    if fatal:
                        self._closed = True
                        for other in self._active.values():
                            other['stop'].set()
                except Exception as error:
                    self._pause('failures', messages.describe_error('other', str(error)), work)
                    if getattr(error, 'winerror', None) == 1909:
                        self._closed = True
                finally:
                    self._active.pop(work.work_id, None)
                    self._lanes[work.lane].finished(active['slot'])
                    self._release()
                    self._wake.notify_all()

    def _settle(self, work, active, result):
        if result.outcome == INTERRUPTED and not active['cancelled']:
            self.store.update_work_item(work.work_id, state=QUEUED)
            work.state = QUEUED
            return
        if result.outcome == FAILED:
            # Also cover client construction and unexpected operation errors.
            self.store.update_file(work.album_id, work.file_id, status='failed',
                                   last_error=result.message or messages.describe_error(result.error_kind))
        self.store.delete_work_items([work.work_id])
        self._items.pop(work.work_id, None)
        cancelled = active['cancelled']
        if result.outcome == OK and cancelled:
            # A cancelled link job can finish resolving before it sees the stop
            # flag; report it as cancelled only, never as done.
            self._emit('cancelled', work, message='已取消')
            return
        if result.outcome == OK:
            self._gate.succeeded()
            self._consecutive = 0
            if work.action == LINK and not active['linked']:
                self._emit('link', work, message='链接已生成')
            self._emit('done', work, message='下载完成' if work.action == DOWNLOAD else '链接已生成')
        elif result.outcome == FAILED:
            kind = result.error_kind or 'other'
            message = result.message or messages.describe_error(kind)
            self._failures.append(dict(album_id=work.album_id, file_id=work.file_id,
                                       name=active['public']['name'], action=work.action,
                                       error_kind=kind, message=message, time=self.clock()))
            self._emit('failed', work, error_kind=kind, message=message)
            if kind == 'rate_limited':
                until = self._gate.limited(result.retry_after)
                self._emit('cooldown', work, until=until, lane=work.lane,
                           message='被网站限速，暂停派发新文件，稍后自动继续')
                if self._gate.streak >= max(1, self.config.ratelimit_pause_after):
                    self._pause('ratelimit', RATE_LIMIT_PAUSE, work)
            else:
                # The site answered without throttling us, so the probe is
                # over; rate limiting never counts towards the failure latch.
                self._gate.failed()
                self._consecutive += 1
                if kind == 'challenge':
                    self._pause('challenge', message, work)
                elif self._consecutive >= max(1, self.config.fail_pause_after):
                    self._pause('failures', f'连续 {self._consecutive} 个文件失败：{message}', work)
        if cancelled:
            self._emit('cancelled', work, message='已取消')
