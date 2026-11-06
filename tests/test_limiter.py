"""Offline limiter contracts; pacing uses only a virtual clock."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import threading
import unittest
from types import SimpleNamespace

from bunkrdl.limiter import LaneLimiter, RateLimitGate, RequestThrottle


class Clock:
    def __init__(self):
        self.now = 100.0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def advance(self, seconds):
        with self.lock:
            self.now += seconds


class LimiterTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.meta = {}
        self.store = SimpleNamespace(get_meta=self.meta.get, set_meta=self.meta.__setitem__)

    def test_request_starts_are_spaced(self):
        throttle = RequestThrottle(.2, clock=self.clock, sleep=self.clock.advance)
        starts = []
        for _ in range(5):
            self.assertTrue(throttle.acquire())
            starts.append(self.clock())
        for left, right in zip(starts, starts[1:]):
            self.assertGreaterEqual(right - left + 1e-9, .2)

    def test_stop_during_wait_does_not_reserve_future_start(self):
        stop = threading.Event()
        def sleep(seconds):
            stop.set()
        throttle = RequestThrottle(1, clock=self.clock, sleep=sleep)
        self.assertTrue(throttle.acquire())
        self.assertFalse(throttle.acquire(stop))
        self.clock.advance(1)
        stop.clear()
        self.assertTrue(throttle.acquire(stop))

    def test_threaded_starts_share_one_timeline(self):
        throttle = RequestThrottle(.2, clock=self.clock, sleep=self.clock.advance)
        threads = [threading.Thread(target=throttle.acquire) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        self.assertGreaterEqual(self.clock() + 1e-8, 102.2)

    def gate(self, base=120, cap=900):
        return RateLimitGate(base, self.store, cap=cap, clock=self.clock)

    def lane(self, kind='video', setting=2, gate=None):
        return LaneLimiter(kind, setting, self.store, gap_min=1, gap_max=2,
                           clock=self.clock, rng=SimpleNamespace(uniform=lambda a, b: b),
                           gate=gate)

    def test_gate_backoff_doubles_from_the_configured_base_and_caps(self):
        gate = self.gate()
        seen = []
        for _ in range(4):
            gate.limited()
            seen.append(gate.until - self.clock())
        # 120, 240, 480, then 900 instead of 960.
        self.assertEqual(seen, [120, 240, 480, 900])
        self.assertEqual(gate.streak, 4)
        gate.limited()
        self.assertEqual(gate.until - self.clock(), 900)
        self.assertEqual(gate.delay(), 900)

    def test_gate_retry_after_is_a_floor_not_a_ceiling(self):
        gate = self.gate()
        self.assertEqual(gate.limited(300) - self.clock(), 300)
        # The next step (240) is smaller than the 300 s the site asked for.
        self.assertEqual(gate.limited(60) - self.clock(), 300)

    def test_retry_after_longer_than_the_cap_is_not_truncated(self):
        gate = self.gate()
        # The 900 s ceiling bounds our own backoff only; a site asking for
        # 1800 s keeps the whole 1800 s.
        self.assertEqual(gate.limited(1800) - self.clock(), 1800)
        self.assertEqual(gate.snapshot()['until'], self.clock() + 1800)
        gate.succeeded()
        self.clock.advance(1800)
        self.assertFalse(gate.cooling())
        # The streak was cleared, so the backoff restarts at the base step.
        self.assertEqual(gate.limited() - self.clock(), 120)

    def test_gate_success_clears_streak_and_probe(self):
        gate = self.gate()
        gate.limited()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=220.0, streak=1, probe=True))
        gate.succeeded()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=220.0, streak=0, probe=True))
        self.clock.advance(120)
        # A success during the cooldown still owes the single post-cooldown probe.
        self.assertEqual(gate.snapshot(), dict(cooling=False, until=None, streak=0, probe=True))
        gate.succeeded()
        self.assertEqual(gate.snapshot(), dict(cooling=False, until=None, streak=0, probe=False))
        self.assertEqual(gate.delay(), 120)

    def test_gate_allows_nothing_while_cooling_and_one_probe_afterwards(self):
        gate = self.gate()
        gate.limited()
        self.assertTrue(gate.cooling())
        for running in (0, 1, 2):
            self.assertFalse(gate.allows(running))
        self.clock.advance(120)
        self.assertTrue(gate.allows(0))
        self.assertFalse(gate.allows(1))
        gate.failed()
        # A failure that is not a rate limit proves the site still answers.
        self.assertTrue(gate.allows(2))

    def test_gate_persists_the_need_to_probe_across_restarts(self):
        gate = self.gate()
        gate.limited()
        # An in-flight file finishes while the gate is cooling: the streak is
        # gone, but the single post-cooldown probe is still owed.
        gate.succeeded()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=220.0, streak=0, probe=True))
        self.assertEqual(self.meta[RateLimitGate.STREAK], '0')
        self.assertEqual(self.meta[RateLimitGate.PROBE], '1')
        restored = self.gate()
        restored.load()
        self.assertEqual(restored.snapshot(), dict(cooling=True, until=220.0, streak=0, probe=True))
        self.clock.advance(120)
        self.assertFalse(restored.cooling())
        # Still only one probe file, not the whole configured concurrency.
        self.assertTrue(restored.allows(0))
        self.assertFalse(restored.allows(1))
        restored.succeeded()
        self.assertTrue(restored.allows(2))
        self.assertEqual(self.meta[RateLimitGate.PROBE], '0')
        again = self.gate()
        again.load()
        self.assertEqual(again.snapshot(), dict(cooling=False, until=None, streak=0, probe=False))

    def test_old_database_without_the_probe_key_still_restores_a_probe(self):
        self.meta[RateLimitGate.DEADLINE] = '220'
        self.meta[RateLimitGate.STREAK] = '1'
        gate = self.gate().load()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=220.0, streak=1, probe=True))

    def test_upgrade_folds_an_unexpired_lane_cooldown_into_the_shared_gate(self):
        # v3.1 stored one cooldown per lane; the newer shared gate knows only
        # one, so the longest unexpired lane deadline must be adopted on load.
        self.meta['web:lane:image:cooldown_until'] = '300'
        self.meta['web:lane:video:cooldown_until'] = '260'
        gate = self.gate().load()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=300.0, streak=0, probe=True))
        self.assertFalse(gate.allows(0))
        # The merged deadline is published, so the old keys can be ignored now.
        self.assertEqual(self.meta[RateLimitGate.DEADLINE], '300.0')
        self.assertEqual(self.meta[RateLimitGate.PROBE], '1')
        self.clock.advance(199)
        self.assertFalse(gate.allows(0))
        self.clock.advance(1)
        self.assertTrue(gate.allows(0))
        self.assertFalse(gate.allows(1))
        another = self.gate().load()
        self.assertEqual(another.snapshot(), dict(cooling=False, until=None, streak=0, probe=True))

    def test_expired_legacy_lane_cooldowns_are_ignored(self):
        self.meta['web:lane:image:cooldown_until'] = '90'
        gate = self.gate().load()
        self.assertEqual(gate.snapshot(), dict(cooling=False, until=None, streak=0, probe=False))
        self.assertTrue(gate.allows(2))

    def test_gate_persists_deadline_and_streak_across_restarts(self):
        gate = self.gate()
        gate.limited(30)
        self.clock.advance(10)
        gate.limited()
        self.assertEqual(float(self.meta[RateLimitGate.DEADLINE]), 350)
        self.assertEqual(self.meta[RateLimitGate.STREAK], '2')
        restored = self.gate()
        restored.load()
        self.assertEqual(restored.snapshot(), dict(cooling=True, until=350.0, streak=2, probe=True))
        self.assertFalse(restored.allows(0))
        self.clock.advance(350)
        self.assertTrue(restored.allows(0))
        self.assertFalse(restored.allows(1))
        # The streak survives too, so the next cooldown is the third step.
        self.assertEqual(restored.limited() - self.clock(), 480)

    def test_gate_reset_clears_the_streak_but_keeps_the_unexpired_cooldown(self):
        gate = self.gate()
        for _ in range(3):
            gate.limited()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=580.0, streak=3, probe=True))
        # "继续" after the fourth-in-a-row pause: the remaining 480 s are kept
        # in full (not shortened to the 120 s minimum) and only the streak goes.
        gate.reset()
        self.assertEqual(gate.snapshot(), dict(cooling=True, until=580.0, streak=0, probe=True))
        self.assertFalse(gate.allows(0))
        self.assertEqual(self.meta[RateLimitGate.STREAK], '0')
        self.assertEqual(self.meta[RateLimitGate.PROBE], '1')
        self.clock.advance(480)
        # The cooldown is over: exactly one probe goes out.
        self.assertTrue(gate.allows(0))
        self.assertFalse(gate.allows(1))
        # The streak is gone, so a new limit starts the backoff at the base.
        self.assertEqual(gate.limited() - self.clock(), 120)

    def test_gate_reset_after_an_expired_cooldown_goes_straight_to_one_probe(self):
        gate = self.gate()
        gate.limited()
        self.clock.advance(120)
        gate.reset()
        self.assertEqual(gate.snapshot(), dict(cooling=False, until=None, streak=0, probe=True))
        self.assertTrue(gate.allows(0))
        self.assertFalse(gate.allows(1))

    def test_lane_snapshot_and_slots_follow_the_shared_gate(self):
        gate = self.gate()
        lane = self.lane('image', 3, gate)
        self.assertEqual(lane.snapshot(), dict(setting=3, effective=3, cooldown_until=None))
        gate.limited()
        # The limitation is on the exit IP: even the other lane reports zero.
        other = self.lane('video', 2, gate)
        self.assertEqual(lane.snapshot(), dict(setting=3, effective=0, cooldown_until=220.0))
        self.assertEqual(other.snapshot(), dict(setting=2, effective=0, cooldown_until=220.0))
        self.assertIsNone(lane.available_slot(set()))
        self.clock.advance(120)
        self.assertEqual(lane.snapshot(), dict(setting=3, effective=1, cooldown_until=None))
        self.assertEqual(lane.available_slot(set()), 0)
        self.assertIsNone(lane.available_slot({0}))
        gate.succeeded()
        self.assertEqual(lane.snapshot(), dict(setting=3, effective=3, cooldown_until=None))
        # Recovery re-spaces the slots, so the next one waits its own gap.
        self.assertIsNone(lane.available_slot({0}))
        self.clock.advance(2)
        self.assertEqual(lane.available_slot({0}), 1)

    def test_slots_have_independent_post_file_gaps(self):
        lane = self.lane()
        lane.finished(0)
        self.assertEqual(lane.available_slot(set()), 1)
        self.assertIsNone(lane.available_slot({1}))
        self.clock.advance(2)
        self.assertEqual(lane.available_slot({1}), 0)

    def test_gate_remaining_matches_the_deadline(self):
        gate = self.gate()
        self.assertEqual(gate.remaining(), 0)
        gate.limited()
        self.assertEqual(gate.remaining(), 120)
        self.clock.advance(30)
        self.assertEqual(gate.remaining(), 90)
        self.clock.advance(90)
        self.assertEqual(gate.remaining(), 0)

    def test_cooldown_recovery_spaces_every_slot_by_its_own_gap(self):
        gate = self.gate()
        lane = self.lane('image', 4, gate)
        gate.limited()
        self.clock.advance(120)
        # The probe starts at once; the other slots are still stale but idle.
        self.assertEqual(lane.available_slot(set()), 0)
        self.assertIsNone(lane.available_slot({0}))
        gate.succeeded()
        # Back at the configured concurrency: slot 0 may go now, the rest follow
        # one gap (2 s here) apart instead of all firing at the same instant.
        self.assertEqual(lane.available_slot(set()), 0)
        self.assertEqual(lane.ready_at[:4], [220.0, 222.0, 224.0, 226.0])
        self.assertIsNone(lane.available_slot({0}))
        self.clock.advance(1.5)
        self.assertIsNone(lane.available_slot({0}))
        self.clock.advance(.5)
        self.assertEqual(lane.available_slot({0}), 1)
        self.assertIsNone(lane.available_slot({0, 1}))
        self.clock.advance(2)
        self.assertEqual(lane.available_slot({0, 1}), 2)

    def test_probe_success_restaggers_even_when_only_one_slot_was_used(self):
        gate = self.gate()
        lane = self.lane('image', 3, gate)
        lane.finished(0)
        self.assertEqual(lane.ready_at[0], 102.0)
        gate.limited()
        for running in (0, 1):
            self.assertIsNone(lane.available_slot(set(range(running))))
        self.clock.advance(120)
        self.assertEqual(lane.available_slot(set()), 0)
        gate.succeeded()
        # A finished slot's old deadline is stale too, so recovery resets all of
        # them: 0 at once, then 2 s and 4 s later.
        self.assertEqual(lane.available_slot(set()), 0)
        self.assertEqual(lane.ready_at[:3], [220.0, 222.0, 224.0])
        self.assertIsNone(lane.available_slot({0}))

    def test_a_failed_probe_also_restaggers_when_the_site_answers(self):
        gate = self.gate()
        lane = self.lane('image', 3, gate)
        gate.limited()
        self.clock.advance(120)
        self.assertEqual(lane.available_slot(set()), 0)
        gate.failed()
        # A non-rate-limit failure ends the probe: the burst must stay spaced.
        self.assertEqual(lane.available_slot(set()), 0)
        self.assertEqual(lane.ready_at[:3], [220.0, 222.0, 224.0])
        self.assertIsNone(lane.available_slot({0}))
        # Normal, un-gated operation is untouched: a finished slot waits its gap.
        self.clock.advance(10)
        lane.finished(1)
        self.assertEqual(lane.ready_at[1], 232.0)
        self.assertIsNone(lane.available_slot({0, 2}))
        self.clock.advance(2)
        self.assertEqual(lane.available_slot({0, 2}), 1)

    def test_concurrency_limits_and_bad_persistence(self):
        for kind, maximum in (('image', 6), ('video', 3)):
            lane = self.lane(kind, maximum)
            for bad in (0, maximum + 1, True, 1.5, '2'):
                with self.assertRaises(ValueError):
                    lane.set_concurrency(bad)
            self.meta[lane.key('setting')] = 'invalid'
            lane.load()
            self.assertEqual(lane.snapshot()['setting'], maximum)
            self.assertIsNone(lane.snapshot()['cooldown_until'])
