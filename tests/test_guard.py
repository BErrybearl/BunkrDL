import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import subprocess
import sys
import threading
import unittest
from pathlib import Path

from bunkrdl.config import Config
from bunkrdl.guard import JobLock, Pacer
from bunkrdl.models import JobBusy
from bunkrdl.store import Store


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / 'state.db')
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.now = 1000.0
        self.waits = []
        self.events = []
        self.config = Config(file_delay_min=3, file_delay_max=3, batch_size=2,
                             batch_pause=30, consecutive_limit_failures=2, cooldown=600)

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds

    def pacer(self, **kwargs):
        return Pacer(self.store, self.config, clock=lambda: self.now, sleep=self.sleep,
                     on_event=self.events.append, **kwargs)

    def test_nonblocking_lock_across_processes_and_release_on_exit(self):
        script = ('import sys, os\nfrom bunkrdl.guard import JobLock\n'
                  'from bunkrdl.models import JobBusy\ntry:\n'
                  ' lock=JobLock(sys.argv[1]); lock.__enter__()\n'
                  'except JobBusy:\n sys.exit(4)\nos._exit(0)\n')
        def child():
            return subprocess.run([sys.executable, '-c', script, self.path],
                                  capture_output=True, timeout=10).returncode
        with JobLock(self.path):
            with self.assertRaises(JobBusy):
                with JobLock(self.path):
                    pass
            self.assertEqual(child(), 4)
        self.assertEqual(child(), 0)
        with JobLock(self.path):
            pass

    def test_deadlines_batch_and_cooldown_survive_reopening(self):
        pacer = self.pacer()
        self.assertTrue(pacer.before_file())
        self.assertEqual(self.waits, [])
        pacer.file_done(True)
        other = Store(self.path)
        try:
            pacer = Pacer(other, self.config, clock=lambda: self.now, sleep=self.sleep)
            self.assertTrue(pacer.before_file())
            self.assertEqual(sum(self.waits), 3)
            pacer.file_done(True)
            self.assertEqual(float(other.get_meta('cooldown_until')), self.now + 600)
            self.assertEqual(float(other.get_meta('next_allowed_at')), self.now + 33)
            pacer = self.pacer()
            self.assertTrue(pacer.before_file())
            self.assertEqual(sum(self.waits), 603)
            pacer.file_done(False)
            self.assertEqual(self.store.get_meta('consecutive_limit_failures'), '0')
        finally:
            other.close()

    def test_batch_pause_persists_without_cooldown(self):
        self.pacer().file_done(False)
        self.pacer().before_file()
        self.pacer().file_done(False)
        self.pacer().before_file()
        self.assertEqual(sum(self.waits), 36)

    def test_interval_and_batch_wait_events_after_reopen(self):
        self.pacer().file_done(False)
        self.pacer().before_file()
        self.pacer().file_done(False)
        self.pacer().before_file()
        waits = [e for e in self.events if e['type'] == 'wait']
        self.assertEqual([(e['reason'], e['until'], e['seconds']) for e in waits],
                         [('interval', 1003, 3), ('batch', 1036, 33)])

    def test_wait_reports_governing_persisted_deadline(self):
        self.store.set_meta('next_allowed_at', '1003')
        self.store.set_meta('backoff_until', '1012')
        self.pacer().before_file()
        self.assertEqual((self.events[-1]['reason'], self.events[-1]['until']), ('backoff', 1012))
        self.store.set_meta('cooldown_until', '1600')
        self.pacer().before_file()
        self.assertEqual((self.events[-1]['type'], self.events[-1]['reason'], self.events[-1]['until']),
                         ('wait', 'cooldown', 1600))

    def test_rollback_wait_event_uses_adjusted_unix_deadline(self):
        self.pacer().file_done(False)
        self.now = 0
        self.pacer().before_file()
        event = next(e for e in self.events if e['type'] == 'wait')
        self.assertEqual((event['reason'], event['until'], event['seconds']), ('interval', 3, 3))

    def test_stop_interrupts_persistent_wait_without_consuming_deadline(self):
        self.store.set_meta('cooldown_until', str(self.now + 600))
        stop = threading.Event()
        pacer = self.pacer(stop_event=stop)
        def sleep(seconds):
            self.sleep(seconds)
            stop.set()
        pacer.sleep = sleep
        self.assertFalse(pacer.before_file())
        self.assertEqual(sum(self.waits), .25)
        self.assertEqual(float(self.store.get_meta('cooldown_until')), 1600)

    def test_backoff_jitter_cap_and_retry_after(self):
        class Rng:
            def uniform(self, low, high):
                return high
        pacer = self.pacer(rng=Rng())
        self.assertEqual(pacer.backoff_delay(1), 6)
        self.assertEqual(pacer.backoff_delay(2), 12)
        self.assertEqual(pacer.backoff_delay(20), 120)
        self.assertEqual(pacer.backoff_delay(1, 300), 300)
        self.assertEqual(pacer.backoff_delay(1, 0), 0)

    def test_cooldown_event_and_transient_failure_counter(self):
        self.pacer().file_done(True)
        self.pacer().file_done(True)
        self.assertEqual([e['type'] for e in self.events], ['cooldown'])
        self.assertEqual((self.events[0]['reason'], self.events[0]['until']), ('cooldown', 1600))
        self.assertEqual(float(self.store.get_meta('cooldown_until')), 1600)


class TimeBoundaryTests(unittest.TestCase):
    setUp = GuardTests.setUp
    sleep = GuardTests.sleep
    pacer = GuardTests.pacer
    def test_clock_rollback_restores_three_seconds(self):
        self.pacer().file_done(False)
        self.now = 0
        self.pacer().before_file()
        self.assertEqual(sum(self.waits), 3)
        self.assertTrue(any(e['type'] == 'clock_rollback' for e in self.events))

    def test_invalid_persisted_values_ignored_with_events(self):
        for raw in ('inf', '-1', 'nan', 'broken'):
            self.store.set_meta('next_allowed_at', raw)
            self.assertTrue(self.pacer().before_file())
        self.assertEqual(self.waits, [])
        self.assertEqual(sum(e['type'] == 'invalid_time' for e in self.events), 4)

    def test_invalid_retry_after_uses_finite_backoff_and_event(self):
        for value in (float('inf'), float('nan'), -1, 'bad'):
            self.assertGreater(self.pacer().backoff_delay(1, value), 0)
        self.assertEqual(sum(e['type'] == 'invalid_time' for e in self.events), 4)

    def test_interrupted_backoff_survives_reopen_and_file_done(self):
        stop = threading.Event()
        pacer = self.pacer(stop_event=stop)
        delay = pacer.backoff_delay(1, 600)
        def sleep(seconds):
            self.sleep(seconds)
            stop.set()
        pacer.sleep = sleep
        self.assertFalse(pacer.wait(delay))
        pacer.file_done(False)
        with_store = Store(self.path)
        try:
            resumed = Pacer(with_store, self.config, clock=lambda: self.now, sleep=self.sleep)
            self.assertTrue(resumed.before_file())
            self.assertEqual(sum(self.waits), 600)
        finally:
            with_store.close()

    def test_rollback_then_stop_preserves_remaining_deadline_at_zero_epoch(self):
        self.pacer().file_done(False)
        self.now = 0
        stop = threading.Event()
        pacer = self.pacer(stop_event=stop)
        def sleep(seconds):
            self.sleep(seconds)
            stop.set()
        pacer.sleep = sleep
        self.assertFalse(pacer.before_file())
        self.assertEqual(float(self.store.get_meta('next_allowed_at')), 3)
        self.assertTrue(self.pacer().before_file())
        self.assertEqual(sum(self.waits), 3)
