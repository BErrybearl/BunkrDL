"""Process ownership and persistent, interruptible queue pacing."""
import os
import math
import random
import time
from pathlib import Path

from .models import JobBusy


class JobLock:
    def __init__(self, db_path):
        self.path = Path(str(Path(db_path).expanduser().resolve()) + '.lock')
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open('a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                if self.path.stat().st_size == 0:
                    handle.write(b'0')
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            if getattr(exc, 'winerror', None) == 1909:
                raise
            raise JobBusy('已有其他任务运行中，请等待任务结束') from None
        self.handle = handle
        return self

    def __exit__(self, *args):
        if self.handle is not None:
            try:
                if os.name == 'nt':
                    import msvcrt
                    self.handle.seek(0)
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None


class Pacer:
    def __init__(self, store, config, *, sleep=time.sleep, clock=time.time, rng=random,
                 stop_event=None, on_event=None):
        self.store, self.config = store, config
        self.sleep, self.clock, self.rng = sleep, clock, rng
        self.stop_event, self.on_event = stop_event, on_event

    def _get(self, key):
        raw = self.store.get_meta(key)
        if raw is None:
            return 0.0
        try:
            value = float(raw)
            if math.isfinite(value) and value >= 0:
                return value
        except (TypeError, ValueError, OverflowError):
            pass
        if self.on_event:
            self.on_event({'type': 'invalid_time', 'message': '忽略非法持久值：' + key})
        return 0.0

    def _set(self, key, value):
        self.store.set_meta(key, str(value))

    def _stopped(self):
        return self.stop_event is not None and self.stop_event.is_set()

    def _event(self, kind, seconds, reason=None, until=None):
        if self.on_event and seconds > 0:
            self.on_event({'type': kind, 'seconds': seconds,
                           'reason': reason or kind,
                           'until': self.clock() + seconds if until is None else until,
                           'message': '连续失败，队列冷却中' if kind == 'cooldown' else '等待后继续'})

    def wait(self, seconds):
        try:
            remaining = float(seconds)
        except (ValueError, TypeError, OverflowError):
            remaining = float('nan')
        if not math.isfinite(remaining) or remaining < 0:
            if self.on_event:
                self.on_event({'type': 'invalid_time', 'message': '忽略非法等待时间'})
            remaining = 0.0
        while remaining > 0 and not self._stopped():
            step = min(0.25, remaining)
            self.sleep(step)
            remaining -= step
            self._deadlines()
        return not self._stopped()

    def _deadlines(self):
        now = self.clock()
        last = self._get('pacer_recorded_at') if self.store.get_meta('pacer_recorded_at') is not None else self._get('last_file_done_at')
        deadlines = [self._get(key) for key in ('cooldown_until', 'next_allowed_at', 'backoff_until')]
        if now < last:
            deadlines = [now + max(0, deadline - last) if deadline else 0 for deadline in deadlines]
            for key, deadline in zip(('cooldown_until', 'next_allowed_at', 'backoff_until'), deadlines):
                self._set(key, deadline)
            if self.on_event:
                self.on_event({'type': 'clock_rollback', 'message': '时钟回拨，已按剩余等待时间恢复'})
        self._set('pacer_recorded_at', now)
        self._wait_deadlines = deadlines
        return max(deadlines) - now

    def before_file(self):
        delay = max(0.0, self._deadlines())
        # Explain the governing deadline without changing how long we wait.
        deadlines = list(zip(('cooldown', 'interval', 'backoff'), self._wait_deadlines))
        reason, until = max(deadlines, key=lambda item: item[1])
        count = int(self._get('files_processed'))
        if (reason == 'interval' and count > 0 and self.config.batch_size > 0
                and count % self.config.batch_size == 0 and self.config.batch_pause > 0):
            reason = 'batch'
        self._event('wait', delay, reason=reason, until=until)
        return self.wait(delay)

    def backoff_delay(self, retry, retry_after=None):
        if retry_after is not None:
            try:
                value = float(retry_after)
            except (ValueError, TypeError, OverflowError):
                value = float('nan')
            if math.isfinite(value) and value >= 0:
                self._deadlines()
                self._set('backoff_until', self.clock() + value)
                return value
            if self.on_event:
                self.on_event({'type': 'invalid_time', 'message': '忽略非法 Retry-After'})
        base = min(self.config.backoff_max, self.config.backoff_base * 2 ** min(retry - 1, 30))
        delay = min(self.config.backoff_max, base + self.rng.uniform(0, base * .2))
        self._deadlines()
        self._set('backoff_until', self.clock() + delay)
        return delay

    def file_done(self, rate_limited):
        self._deadlines()
        now = self.clock()
        count = int(self._get('files_processed')) + 1
        self._set('files_processed', count)
        delay = self.rng.uniform(self.config.file_delay_min, self.config.file_delay_max)
        if self.config.batch_size > 0 and count % self.config.batch_size == 0:
            delay += self.config.batch_pause
        self._set('last_file_done_at', now)
        self._set('next_allowed_at', now + delay)
        failures = int(self._get('consecutive_limit_failures')) + 1 if rate_limited else 0
        if self.config.consecutive_limit_failures > 0 and failures >= self.config.consecutive_limit_failures:
            self._set('cooldown_until', now + self.config.cooldown)
            failures = 0
            self._event('cooldown', self.config.cooldown)
        self._set('consecutive_limit_failures', failures)
