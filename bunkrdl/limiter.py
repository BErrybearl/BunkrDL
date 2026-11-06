"""Shared request spacing, per-lane slot pacing and the IP-wide rate-limit gate."""
import math
import random
import threading
import time


def _seconds(value, default=0.0):
    try:
        number = float(value)
        return number if math.isfinite(number) and number >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


class RequestThrottle:
    def __init__(self, interval: float, clock=time.monotonic, sleep=time.sleep):
        self.interval = _seconds(interval)
        self.clock, self.sleep = clock, sleep
        self._lock = threading.Lock()
        self._last = None

    def acquire(self, stop_event=None) -> bool:
        while True:
            if stop_event is not None and stop_event.is_set():
                return False
            with self._lock:
                now = self.clock()
                delay = 0 if self._last is None else self._last + self.interval - now
                if delay <= 0:
                    if stop_event is not None and stop_event.is_set():
                        return False
                    self._last = now
                    return True
            # Cancellation must not reserve a future request start.
            self.sleep(min(delay, 0.05))


class RateLimitGate:
    """One cooldown for the whole exit IP, shared by every lane.

    Bunkr throttles the egress IP, not a file kind, so a 429/503 while cooling
    must stop every lane. The gate also remembers how many rate-limited
    results arrived in a row and the resulting deadline, so a restart neither
    loses the cooldown nor restarts the backoff at the smallest step.

    After a cooldown the gate stays in probe mode: exactly one file runs until
    it proves the site answers again. Any success clears the streak; a probe
    that is rate-limited again cools down one step longer.
    """
    DEADLINE = 'web:ratelimit:until'
    STREAK = 'web:ratelimit:streak'
    # Kept apart from the streak: an in-flight success during a cooldown clears
    # the streak but still owes the one-file probe, and a restart must know it.
    PROBE = 'web:ratelimit:probe'
    # v3.1 kept one cooldown per lane; an unexpired one is folded in on load.
    LEGACY_DEADLINES = ('web:lane:image:cooldown_until', 'web:lane:video:cooldown_until')
    MAX_STEP = 60   # keeps 2**(step-1) cheap even if the stored streak is absurd

    def __init__(self, base, store, *, cap=900.0, clock=time.time):
        self.base = _seconds(base)
        self.cap = _seconds(cap, 900.0)
        self.store, self.clock = store, clock
        self._lock = threading.RLock()
        self.until = 0.0
        self.streak = 0
        self.probe = False

    def load(self):
        with self._lock:
            self.until = max(self.until, _seconds(self.store.get_meta(self.DEADLINE)))
            legacy = max(_seconds(self.store.get_meta(key)) for key in self.LEGACY_DEADLINES)
            folded = legacy > self.clock()
            if folded:
                self.until = max(self.until, legacy)
            self.streak = int(_seconds(self.store.get_meta(self.STREAK)))
            stored = self.store.get_meta(self.PROBE)
            if stored is None:
                # Older databases only have the streak; before v3.2 that was the
                # only evidence a probe was still owed.
                self.probe = self.streak > 0 or folded
            else:
                self.probe = str(stored).strip() not in ('', '0', 'False', 'false') or folded
            if folded or stored is None:
                # Publish the derived state so a later restart reads it directly
                # and the legacy lane keys can be ignored from now on.
                self._save()
            return self

    def cooling(self):
        with self._lock:
            return self.until > self.clock()

    def allows(self, running):
        """Whether another file may start while ``running`` files are active."""
        with self._lock:
            if self.until > self.clock():
                return False
            return not (self.probe and running)

    def delay(self, retry_after=None):
        """Cooldown length: the capped backoff step, or Retry-After if longer.

        The cap only bounds our own exponential step. A site asking for more
        than ``cap`` seconds must still be honoured, so it is applied after the
        cap instead of being truncated by it.
        """
        step = min(max(1, self.streak), self.MAX_STEP)
        backoff = min(self.cap, self.base * (2 ** (step - 1)))
        return max(backoff, _seconds(retry_after))

    def limited(self, retry_after=None):
        """Account for one rate-limited result; returns the new deadline."""
        with self._lock:
            self.streak += 1
            self.until = max(self.until, self.clock() + self.delay(retry_after))
            self.probe = True
            self._save()
            return self.until

    def succeeded(self):
        """Any finished file clears the streak; a probe is still owed while cooling.

        A success that lands during a cooldown (an in-flight download) already
        proves the site answers, but the rule after the cooldown is still one
        single probe file, so the gate stays in probe mode until then.
        """
        with self._lock:
            state = (0, self.until > self.clock())
            if state == (self.streak, self.probe):
                return
            self.streak, self.probe = state
            self._save()

    def failed(self):
        """A non-rate-limit failure proves the site answers: stop probing."""
        with self._lock:
            if self.until > self.clock() or not self.probe:
                return
            self.probe = False
            self._save()

    def remaining(self):
        """Seconds left in the cooldown; 0 when a request may go out again."""
        with self._lock:
            return max(0.0, self.until - self.clock())

    def reset(self):
        """Explicit user "continue": clear the streak, keep an unexpired cooldown.

        Skipping the rest of a cooldown the site demanded would only earn the
        next 429, and a 900 s deadline must not shrink to the 120 s minimum. So
        the deadline is kept as is (dropped only when already past); once it
        passes, exactly one probe file goes out first and the backoff restarts
        from the smallest step because the streak is gone.
        """
        with self._lock:
            self.streak = 0
            self.probe = True
            if self.until <= self.clock():
                self.until = 0.0
            self._save()

    def snapshot(self):
        with self._lock:
            cooling = self.until > self.clock()
            return {'cooling': cooling, 'until': self.until if cooling else None,
                    'streak': self.streak, 'probe': self.probe}

    def _save(self):
        self.store.set_meta(self.DEADLINE, str(self.until))
        self.store.set_meta(self.STREAK, str(self.streak))
        self.store.set_meta(self.PROBE, '1' if self.probe else '0')


class LaneLimiter:
    """Each slot owns a post-file deadline; the shared gate stops or damps the lane."""
    def __init__(self, lane, setting, store, *, gap_min=0, gap_max=None,
                 clock=time.time, rng=random, gate=None):
        if lane not in ('image', 'video'):
            raise ValueError('未知通道')
        self.lane, self.store, self.clock, self.rng = lane, store, clock, rng
        self.gate = gate
        self.maximum = 6 if lane == 'image' else 3
        self._lock = threading.RLock()
        self.gap_min = _seconds(gap_min)
        self.gap_max = max(self.gap_min, _seconds(gap_max, self.gap_min))
        self.setting = self.validate(setting)
        self.ready_at = [0.0] * self.maximum
        # Last gate state seen here: cooling or probing. A cooldown outlives
        # every slot's post-file deadline, so leaving it must re-space the slots.
        self._restricted = False

    def key(self, field):
        return f'web:lane:{self.lane}:{field}'

    def validate(self, value):
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= self.maximum:
            raise ValueError(f'同时下载数须为 1 至 {self.maximum} 的整数')
        return value

    def load(self):
        with self._lock:
            try:
                self.setting = self.validate(int(self.store.get_meta(self.key('setting'))))
            except (ValueError, TypeError):
                pass

    def set_concurrency(self, value):
        value = self.validate(value)
        with self._lock:
            self.store.set_meta(self.key('setting'), str(value))
            self.setting = value

    def snapshot(self):
        with self._lock:
            state = self.gate.snapshot() if self.gate is not None else None
            if state is not None and state['cooling']:
                # A cooling gate starts no file at all, in any lane.
                effective, until = 0, state['until']
            elif state is not None and state['probe']:
                effective, until = 1, None
            else:
                effective, until = self.setting, None
            return {'setting': self.setting, 'effective': effective, 'cooldown_until': until}

    def available_slot(self, busy):
        with self._lock:
            if self.gate is not None:
                state = self.gate.snapshot()
                restricted = bool(state['cooling'] or state['probe'])
                if self._restricted and not restricted:
                    # Recovery: while the gate was cooling or probing no slot
                    # refreshed its deadline, so all of them look ready at
                    # once. Re-space every slot by its own gap before any one
                    # of them starts again.
                    self.stagger()
                self._restricted = restricted
                if not self.gate.allows(len(busy)):
                    return None
            if len(busy) >= self.setting:
                return None
            return next((i for i in range(self.setting)
                         if i not in busy and self.ready_at[i] <= self.clock()), None)

    def stagger(self, now=None):
        """Give every slot a fresh post-file gap, one gap apart from each other.

        Slot 0 may start at once (the caller reserves it right away); the rest
        follow one random gap later each, so a recovered lane never fires all
        slots in the same instant. Returns the new deadlines for inspection.
        """
        with self._lock:
            cursor = self.clock() if now is None else now
            for slot in range(self.maximum):
                self.ready_at[slot] = cursor
                cursor += self.rng.uniform(self.gap_min, self.gap_max)
            return list(self.ready_at)

    def finished(self, slot):
        with self._lock:
            self.ready_at[slot] = self.clock() + self.rng.uniform(self.gap_min, self.gap_max)
