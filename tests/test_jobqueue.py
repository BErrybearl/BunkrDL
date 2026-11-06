"""Coordinator contracts with fake persistence and manually completed file ops."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import threading
import time
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from bunkrdl.config import Config
from bunkrdl.jobqueue import Coordinator
from bunkrdl.models import FileRecord, JobBusy
from bunkrdl.worktypes import (Busy, WorkItem, ItemResult, OK, FAILED, INTERRUPTED)
from test_limiter import Clock


class FakeStore:
    def __init__(self):
        self.meta, self.work, self.records = {}, {}, {}
        self.next_id = self.recovered = self.reset = self.calls_after_close = 0
        self.closed = False
        self.lock = threading.RLock()

    def add_files(self, album, ids, kind='image', status='pending'):
        for file_id in ids:
            self.records[album, file_id] = FileRecord(file_id, file_id + '.jpg', file_id,
                'https://example.test/f/' + file_id, album_id=album, kind=kind, status=status, size=1000)

    def get_album(self, album):
        return SimpleNamespace(album_id=album) if any(a == album for a, _ in self.records) else None

    def list_files(self, album, status=None):
        return [replace(r) for (a, _), r in self.records.items()
                if a == album and (status is None or r.status == status)]

    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        if self.closed:
            self.calls_after_close += 1
            raise AssertionError('store used after close')
        self.meta[key] = str(value)

    def update_file(self, album, file_id, **fields):
        if self.closed:
            self.calls_after_close += 1
            raise AssertionError('store used after close')
        for key, value in fields.items():
            setattr(self.records[album, file_id], key, value)

    def add_work_items(self, rows):
        added = []
        for album, file_id, action, lane in rows:
            if any((w.album_id, w.file_id) == (album, file_id) for w in self.work.values()):
                continue
            self.next_id += 1
            work = WorkItem(self.next_id, album, file_id, action, self.next_id, lane=lane)
            self.work[work.work_id] = work
            added.append(replace(work))
        return added

    def list_work_items(self, states=None):
        return [replace(w) for w in sorted(self.work.values(), key=lambda w: w.seq)
                if states is None or w.state in states]

    def update_work_item(self, work_id, **fields):
        if self.closed:
            self.calls_after_close += 1
            raise AssertionError('store used after close')
        for key, value in fields.items():
            setattr(self.work[work_id], key, value)

    def replace_work_action(self, work_id, action):
        assert self.work[work_id].state == 'queued'
        self.work[work_id].action = action

    def delete_work_items(self, ids):
        if self.closed:
            self.calls_after_close += 1
            raise AssertionError('store used after close')
        for work_id in ids:
            self.work.pop(work_id, None)

    def reset_running_work(self):
        self.reset += 1
        for work in self.work.values():
            work.state = 'queued'

    def recover_interrupted(self):
        self.recovered += 1
        for record in self.records.values():
            if record.status == 'downloading':
                record.status = 'pending'

    def match_file_ids(self, album, file_ids=None, statuses=None):
        return [file_id for (owner, file_id), record in self.records.items()
                if owner == album
                and (file_ids is None or file_id in file_ids)
                and (statuses is None or record.status in statuses)]

    def delete_files(self, album, file_ids=None, statuses=None):
        if self.closed:
            self.calls_after_close += 1
            raise AssertionError('store used after close')
        removed = self.match_file_ids(album, file_ids, statuses)
        for file_id in removed:
            self.records.pop((album, file_id), None)
            self.meta.pop(f'web:{album}:last_action:{file_id}', None)
        for work_id, work in list(self.work.items()):
            if work.album_id == album and work.file_id in removed:
                del self.work[work_id]
        return len(removed)

    def delete_album_records(self, album):
        if self.closed:
            self.calls_after_close += 1
            raise AssertionError('store used after close')
        removed = self.match_file_ids(album)
        for file_id in removed:
            self.records.pop((album, file_id), None)
        for work_id, work in list(self.work.items()):
            if work.album_id == album:
                del self.work[work_id]
        for key in [key for key in self.meta if key.startswith(f'web:{album}:')]:
            del self.meta[key]
        return len(removed)


class FakeJobLock:
    held = False
    enters = exits = 0
    def __init__(self, path):
        pass

    def __enter__(self):
        if type(self).held:
            raise JobBusy('已有其他任务运行中')
        type(self).held = True
        type(self).enters += 1
        return self

    def __exit__(self, *args):
        type(self).held = False
        type(self).exits += 1


class Ops:
    def __init__(self):
        self.condition = threading.Condition()
        self.calls = []
        self.gates = {}
        self.results = {}
        self.late_write = False

    def download_once(self, ctx, album, file_id):
        return self.run(ctx, album, file_id, 'download')

    def generate_link_once(self, ctx, album, file_id):
        return self.run(ctx, album, file_id, 'link')

    def run(self, ctx, album, file_id, action):
        with self.condition:
            gate = threading.Event()
            key = (album, file_id)
            self.gates[key] = gate
            self.calls.append((album, file_id, action, ctx))
            self.condition.notify_all()
        if not gate.wait(5):
            raise AssertionError('test did not finish operation')
        result = self.results.get(key, ItemResult(INTERRUPTED))
        if self.late_write:
            ctx.store.update_file(album, file_id, status='pending')
        elif result.outcome == OK:
            ctx.store.update_file(album, file_id, status='done' if action == 'download' else 'exported')
            if action == 'link':
                ctx.on_event({'type': 'link', 'message': '链接已生成', 'url': 'https://example.test/direct'})
        elif result.outcome == FAILED:
            ctx.store.update_file(album, file_id, status='failed', last_error=result.message)
            ctx.on_event({'type': 'failed', 'error_kind': result.error_kind})
        return result

    def wait_calls(self, count):
        with self.condition:
            if not self.condition.wait_for(lambda: len(self.calls) >= count, 2):
                raise AssertionError(f'expected {count} calls, got {len(self.calls)}')

    def finish(self, album, file_id, result=None):
        with self.condition:
            self.results[album, file_id] = result or ItemResult(OK)
            self.gates[album, file_id].set()

    def finish_all(self):
        with self.condition:
            for key, gate in self.gates.items():
                self.results.setdefault(key, ItemResult(OK))
                gate.set()


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        FakeJobLock.held = False
        FakeJobLock.enters = FakeJobLock.exits = 0
        self.patch = patch('bunkrdl.jobqueue.JobLock', FakeJobLock)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.store, self.ops, self.clock = FakeStore(), Ops(), Clock()
        self.config = Config(image_concurrency=2, video_concurrency=2,
                             image_gap=0, video_gap_min=0, video_gap_max=0)
        self.clients = []
        def factory(throttle, fail_fast=True):
            client = SimpleNamespace(throttle=throttle, fail_fast=fail_fast)
            self.clients.append(client)
            return client
        self.q = Coordinator(self.config, 'fake.db', self.store, factory, ops=self.ops, clock=self.clock)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if not self.q._closed:
            self.q.pause_all()
        self.ops.finish_all()
        self.q.close(2)

    def wait(self, predicate):
        with self.q._wake:
            self.assertTrue(self.q._wake.wait_for(predicate, 2), self.q.snapshot())

    def idle(self):
        self.wait(lambda: not self.q.snapshot()['active'])

    def quiet(self, seconds=.2):
        """Give the dispatch loop a chance to (not) start anything."""
        with self.q._wake:
            self.q._wake.notify_all()
        time.sleep(seconds)

    def add(self, ids=('1', '2', '3', '4'), album='a', kind='image'):
        self.store.add_files(album, ids, kind)

    def begin(self, ids=('1', '2', '3', '4'), action='download', album='a'):
        self.q.start()
        return self.q.enqueue(album, list(ids), action)

    def test_fifo_independent_lanes_and_one_held_lock(self):
        self.add(('i1', 'i2', 'i3'))
        self.add(('v1', 'v2', 'v3'), album='b', kind='video')
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['i1', 'i2', 'i3'], 'download')
        self.q.enqueue('b', ['v1', 'v2', 'v3'], 'link')
        before = FakeJobLock.enters
        self.q.resume_all()
        self.ops.wait_calls(4)
        self.assertEqual([e['file_id'] for e in self.q.events()['events'] if e['type'] == 'started'],
                         ['i1', 'i2', 'v1', 'v2'])
        self.assertEqual(len(self.q.snapshot()['active']), 4)
        self.assertEqual(FakeJobLock.enters - before, 1)
        self.assertEqual(len({id(c.throttle) for c in self.clients}), 1)
        self.assertTrue(all(c.fail_fast for c in self.clients))
        self.ops.finish('a', 'i2')
        self.ops.wait_calls(5)
        self.assertEqual(self.ops.calls[-1][1], 'i3')
        self.q.cancel_album('b')
        self.ops.finish_all()
        self.idle()
        self.assertFalse(FakeJobLock.held)
        self.assertEqual((self.store.reset, self.store.recovered), (1, 1))

    def test_duplicates_replacement_busy_and_status_filter(self):
        self.add()
        self.store.records['a', '4'].status = 'done'
        self.q.start()
        self.q.pause_all()
        self.assertEqual(self.q.enqueue('a', ['1', '1', '4'], 'download').added, 1)
        self.assertEqual(self.q.enqueue('a', ['1'], 'download').already, 1)
        self.assertEqual(self.q.enqueue('a', ['1'], 'link').replaced, 1)
        self.assertEqual(self.q.enqueue('a', ['4'], 'link').not_applicable, 1)
        self.q.resume_all()
        self.ops.wait_calls(1)
        self.assertEqual(self.ops.calls[0][2], 'link')
        self.assertEqual(self.q.enqueue('a', ['1'], 'download').busy, 1)
        self.assertEqual(self.q.enqueue('a', ['1'], 'link').already, 1)
        self.ops.finish_all()
        self.idle()

    def test_mutate_checks_queued_and_active_and_uses_lock(self):
        self.add()
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1'], 'download')
        with self.assertRaises(Busy):
            self.q.mutate(lambda: None, [('a', '1')])
        self.assertTrue(self.q.mutate(lambda: FakeJobLock.held, [('a', '2')]))
        self.assertFalse(FakeJobLock.held)
        self.q.resume_all()
        self.ops.wait_calls(1)
        with self.assertRaises(Busy):
            self.q.mutate(lambda: None, [('a', '1')])
        before = FakeJobLock.enters
        self.assertEqual(self.q.mutate(lambda: 7), 7)
        self.assertEqual(FakeJobLock.enters, before)
        self.assertTrue(FakeJobLock.held)

    def test_cli_lock_defers_recovery_and_execution(self):
        self.add(('1',))
        self.store.add_work_items([('a', '1', 'download', 'image')])
        FakeJobLock.held = True
        self.q.start()
        self.assertEqual(self.store.recovered, 0)
        self.assertEqual(self.q.snapshot()['pause_kind'], 'restart')
        self.q.resume_all()
        self.assertFalse(self.ops.calls)
        with self.assertRaises(JobBusy):
            self.q.mutate(lambda: None)
        FakeJobLock.held = False
        with self.q._wake:
            self.q._wake.notify_all()
        self.ops.wait_calls(1)
        self.assertEqual(self.store.recovered, 1)

    def test_restart_recovers_once_and_waits_for_resume(self):
        self.add(('1',))
        self.store.records['a', '1'].status = 'downloading'
        work = self.store.add_work_items([('a', '1', 'link', 'image')])[0]
        self.store.update_work_item(work.work_id, state='running')
        self.q.start()
        self.q.start()
        self.assertEqual(self.q.snapshot()['pause_kind'], 'restart')
        self.assertEqual(self.q.snapshot()['queued'], 1)
        self.assertFalse(self.ops.calls)
        self.q.resume_all()
        self.ops.wait_calls(1)
        self.ops.finish_all()
        self.idle()
        self.assertEqual((self.store.reset, self.store.recovered), (1, 1))

    def test_pause_interrupt_requeues_without_failure_and_resumes(self):
        self.add(('1', '2', '3'))
        self.begin(('1', '2', '3'))
        self.ops.wait_calls(2)
        snap = self.q.pause_all()
        self.assertTrue(snap['stopping'])
        self.assertTrue(all(c[3].stop_event.is_set() for c in self.ops.calls))
        self.ops.finish('a', '1', ItemResult(INTERRUPTED))
        self.ops.finish('a', '2', ItemResult(INTERRUPTED))
        self.idle()
        self.assertEqual(self.q.snapshot()['queued'], 3)
        self.assertEqual(self.q.snapshot()['consecutive_failures'], 0)
        self.assertFalse(FakeJobLock.held)
        self.q.resume_all()
        self.ops.wait_calls(4)
        self.assertEqual([e['file_id'] for e in self.q.events()['events'] if e['type'] == 'started'],
                         ['1', '2', '1', '2'])
        self.q.cancel_album('a')
        self.ops.finish_all()
        self.idle()

    def test_three_failures_latch_even_when_late_success_arrives(self):
        self.add(('1', '2', '3', '4', '5', '6'))
        self.q.set_concurrency('image', 4)
        self.begin(('1', '2', '3', '4', '5', '6'))
        self.ops.wait_calls(4)
        # Shrink while four old slots are still active, preventing replacements.
        self.q.set_concurrency('image', 1)
        for count, file_id in enumerate(('1', '2', '3'), 1):
            self.ops.finish('a', file_id, ItemResult(FAILED, 'transient', '网络不稳定'))
            self.wait(lambda: self.q.snapshot()['consecutive_failures'] == count)
        self.assertEqual(self.q.snapshot()['pause_kind'], 'failures')
        self.ops.finish('a', '4')
        self.idle()
        self.assertTrue(self.q.snapshot()['paused'])
        self.assertEqual(self.q.snapshot()['consecutive_failures'], 0)
        self.assertEqual(len(self.ops.calls), 4)
        self.assertEqual(len([e for e in self.q.events()['events'] if e['type'] == 'failed']), 3)
        self.assertFalse(self.q.resume_all()['paused'])
        self.ops.wait_calls(5)
        self.q.cancel_album('a')
        self.ops.finish_all()

    def test_success_resets_consecutive_count(self):
        self.add(('1', '2', '3'))
        self.q.set_concurrency('image', 1)
        self.begin(('1', '2', '3'))
        for index, file_id in enumerate(('1', '2', '3'), 1):
            self.ops.wait_calls(index)
            self.ops.finish('a', file_id, ItemResult(OK) if file_id == '2' else ItemResult(FAILED, 'other', '失败'))
        self.idle()
        self.assertEqual(self.q.snapshot()['consecutive_failures'], 1)
        self.assertFalse(self.q.snapshot()['paused'])

    def test_challenge_pauses_both_lanes_immediately(self):
        self.add(('1', '2', '3'))
        self.add(('v',), album='b', kind='video')
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2', '3'], 'link')
        self.q.enqueue('b', ['v'], 'download')
        self.q.resume_all()
        self.ops.wait_calls(3)
        self.ops.finish('a', '1', ItemResult(FAILED, 'challenge', '网站要求人机验证'))
        self.wait(lambda: self.q.snapshot()['pause_kind'] == 'challenge')
        self.assertTrue(all(c[3].stop_event.is_set() for c in self.ops.calls))
        self.ops.finish_all()
        self.idle()
        self.assertEqual(len(self.ops.calls), 3)
        self.assertTrue(self.q.snapshot()['paused'])

    def test_rate_limit_cooling_stops_every_lane_then_probes_once(self):
        self.add(('1', '2', '3', '4'))
        self.add(('v1', 'v2'), album='b', kind='video')
        self.q.start()
        self.q.set_concurrency('video', 1)
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2', '3', '4'], 'download')
        self.q.enqueue('b', ['v1', 'v2'], 'download')
        self.q.resume_all()
        self.ops.wait_calls(3)
        self.ops.finish('a', '1', ItemResult(FAILED, 'rate_limited', '被网站限速（429）', 90))
        self.wait(lambda: self.q.snapshot()['lanes']['image']['effective'] == 0)
        # The limit is on the exit IP: both lanes report zero and its deadline
        # is max(Retry-After 90, base 120) = 120 s after the failure.
        self.assertEqual(self.q.snapshot()['lanes']['video']['effective'], 0)
        self.assertEqual(self.q.snapshot()['lanes']['image']['cooldown_until'], 220)
        self.assertEqual(self.q.snapshot()['lanes']['video']['cooldown_until'], 220)
        self.assertEqual(float(self.store.meta['web:ratelimit:until']), 220)
        self.assertEqual(self.store.meta['web:ratelimit:streak'], '1')
        # Files already running keep their body download going.
        self.assertTrue(all(not call[3].stop_event.is_set() for call in self.ops.calls))
        self.ops.finish('a', '2')
        self.wait(lambda: len(self.q.snapshot()['active']) == 1)
        self.quiet()
        self.assertEqual(len(self.ops.calls), 3)
        self.assertEqual(self.q.snapshot()['queued'], 3)
        self.ops.finish('b', 'v1')
        self.idle()
        # Still cooling with an empty queue view: nothing starts in either lane.
        self.quiet()
        self.assertEqual(len(self.ops.calls), 3)
        self.clock.advance(120)
        with self.q._wake:
            self.q._wake.notify_all()
        # Cooldown over: exactly one probe file goes out, not the whole queue.
        self.ops.wait_calls(4)
        self.assertEqual(self.ops.calls[-1][1], '3')
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 1)
        self.quiet()
        self.assertEqual(len(self.ops.calls), 4)
        self.ops.finish('a', '3')
        # Only the probe's success restores the configured concurrency.
        self.ops.wait_calls(6)
        self.assertEqual(self.store.meta['web:ratelimit:streak'], '0')
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 2)
        self.assertEqual(self.q.snapshot()['lanes']['video']['effective'], 1)
        self.assertEqual(sorted(call[1] for call in self.ops.calls[4:]), ['4', 'v2'])
        self.ops.finish_all()
        self.idle()

    def test_recovery_after_a_cooldown_starts_slots_one_gap_apart(self):
        ids = tuple(str(i) for i in range(1, 9))
        self.add(ids)
        lane = self.q._lanes['image']
        lane.gap_min = lane.gap_max = 1.0
        self.q.set_concurrency('image', 3)
        self.begin(ids)
        self.ops.wait_calls(3)
        self.ops.finish('a', '1', ItemResult(FAILED, 'rate_limited', '被网站限速（429）'))
        self.wait(lambda: self.q.snapshot()['lanes']['image']['effective'] == 0)
        self.ops.finish('a', '2')
        self.ops.finish('a', '3')
        self.idle()
        self.clock.advance(120)
        with self.q._wake:
            self.q._wake.notify_all()
        self.ops.wait_calls(4)
        self.ops.finish('a', '4')
        # The probe's success restores all three slots, but they must not fire in
        # the same instant: each waits one of its own post-file gaps after the
        # previous one instead of all the stale slots going at once.
        self.ops.wait_calls(5)
        self.quiet()
        self.assertEqual(len(self.ops.calls), 5)
        self.clock.advance(1)
        with self.q._wake:
            self.q._wake.notify_all()
        self.ops.wait_calls(6)
        self.quiet()
        self.assertEqual(len(self.ops.calls), 6)
        self.q.cancel_album('a')
        self.ops.finish_all()
        self.idle()

    def test_rate_limit_outside_a_work_item_uses_the_shared_gate(self):
        self.add(('1', '2'))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2'], 'download')
        self.assertEqual(self.q.cooldown_remaining(), 0)
        # Adding an album parses its page outside the work queue; a 429 there
        # still belongs to the same IP-wide gate.
        self.assertEqual(self.q.register_rate_limit(45), 220.0)
        self.assertEqual(self.q.cooldown_remaining(), 120.0)
        self.assertEqual(self.store.meta['web:ratelimit:until'], '220.0')
        self.assertEqual(self.store.meta['web:ratelimit:streak'], '1')
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 0)
        self.assertEqual(self.q.snapshot()['lanes']['video']['effective'], 0)
        self.assertTrue(any(event['type'] == 'cooldown' for event in self.q.events()['events']))
        self.q.resume_all()
        self.quiet()
        # “继续” must not skip a cooldown that has not expired yet.
        self.assertFalse(self.ops.calls)
        self.clock.advance(120)
        with self.q._wake:
            self.q._wake.notify_all()
        self.ops.wait_calls(1)
        self.q.cancel_album('a')
        self.ops.finish_all()
        self.idle()

    def test_four_rate_limits_outside_a_work_item_pause_with_ratelimit_reason(self):
        self.q.start()
        for _ in range(4):
            self.q.register_rate_limit()
        self.wait(lambda: self.q.snapshot()['pause_kind'] == 'ratelimit')
        snapshot = self.q.snapshot()
        self.assertTrue(snapshot['paused'])
        self.assertEqual(snapshot['pause_reason'],
                         '被网站连续限速，已暂停。建议降低并发或等一会儿再继续')
        self.assertEqual(snapshot['consecutive_failures'], 0)
        self.assertFalse(self.q.resume_all()['paused'])
        self.assertEqual(self.store.meta['web:ratelimit:streak'], '0')
        # “继续” clears the streak but never shortens a 900 s cooldown that has
        # not expired: only the minimum step (120 s) would have been wrong.
        self.assertEqual(self.q.cooldown_remaining(), 900)
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 0)
        self.clock.advance(900)
        # The cooldown is over now and exactly one probe file may start.
        self.assertEqual(self.q.cooldown_remaining(), 0)
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 1)

    def test_concurrent_slots_never_share_a_client(self):
        class TrackingOps(Ops):
            def __init__(self):
                super().__init__()
                self.live, self.shared = [], []

            def run(self, ctx, album, file_id, action):
                with self.condition:
                    if any(live is ctx.client for live in self.live):
                        self.shared.append(ctx.client)
                    self.live.append(ctx.client)
                try:
                    return super().run(ctx, album, file_id, action)
                finally:
                    with self.condition:
                        self.live.remove(ctx.client)

        self.ops = TrackingOps()
        self.q.ops = self.ops
        self.add(('1', '2', '3', '4'))
        self.begin(('1', '2', '3', '4'))
        self.ops.wait_calls(2)
        self.assertEqual(len({id(call[3].client) for call in self.ops.calls}), 2)
        self.assertIs(self.ops.calls[0][3].client.throttle, self.ops.calls[1][3].client.throttle)
        self.ops.finish('a', '1')
        self.ops.wait_calls(3)
        self.ops.finish('a', '2')
        self.ops.finish('a', '3')
        self.ops.wait_calls(4)
        self.ops.finish('a', '4')
        self.idle()
        # A freed slot reuses its own client, never another slot's, and never
        # while the other one is still running.
        self.assertEqual(self.ops.shared, [])
        self.assertEqual(len({id(call[3].client) for call in self.ops.calls}), 2)

    def test_rate_limited_probe_escalates_to_the_next_cooldown_step(self):
        self.add(('1', '2'))
        self.store.meta['web:ratelimit:until'] = '90'
        self.store.meta['web:ratelimit:streak'] = '1'
        self.begin(('1', '2'))
        self.ops.wait_calls(1)
        self.assertEqual(self.ops.calls[-1][1], '1')
        self.quiet()
        self.assertEqual(len(self.ops.calls), 1)
        self.ops.finish('a', '1', ItemResult(FAILED, 'rate_limited', '被网站限速（429）'))
        self.wait(lambda: self.q.snapshot()['lanes']['image']['effective'] == 0)
        # Second step of the backoff: 120 * 2 = 240 s from now (100).
        self.assertEqual(float(self.store.meta['web:ratelimit:streak']), 2)
        self.assertEqual(float(self.store.meta['web:ratelimit:until']), 340)
        self.quiet()
        self.assertEqual(len(self.ops.calls), 1)
        self.q.cancel_album('a')

    def test_four_consecutive_rate_limits_pause_with_ratelimit_reason(self):
        self.add(('1', '2', '3', '4', '5'))
        self.q.set_concurrency('image', 1)
        self.begin(('1', '2', '3', '4', '5'))
        for index, seconds in enumerate((120, 240, 480), 1):
            self.ops.wait_calls(index)
            self.ops.finish('a', str(index), ItemResult(FAILED, 'rate_limited', '被网站限速（429）'))
            self.wait(lambda: self.q.snapshot()['lanes']['image']['effective'] == 0)
            self.assertFalse(self.q.snapshot()['paused'])
            self.clock.advance(seconds)
            with self.q._wake:
                self.q._wake.notify_all()
        self.ops.wait_calls(4)
        self.ops.finish('a', '4', ItemResult(FAILED, 'rate_limited', '被网站限速（429）'))
        self.wait(lambda: self.q.snapshot()['pause_kind'] == 'ratelimit')
        snapshot = self.q.snapshot()
        self.assertTrue(snapshot['paused'])
        self.assertEqual(snapshot['pause_reason'], '被网站连续限速，已暂停。建议降低并发或等一会儿再继续')
        # Rate limiting still never counts towards the three-failure latch.
        self.assertEqual(snapshot['consecutive_failures'], 0)
        self.assertEqual(snapshot['lanes']['image']['effective'], 0)
        self.assertTrue(any(event['type'] == 'paused' and event.get('pause_kind') == 'ratelimit'
                            for event in self.q.events()['events']))
        self.quiet()
        self.assertEqual(len(self.ops.calls), 4)
        # "继续" clears the streak but keeps the 900 s step-4 cooldown that has
        # not expired yet; only after it does a single probe file go out.
        self.q.resume_all()
        self.quiet()
        self.assertEqual(len(self.ops.calls), 4)
        self.assertEqual(self.q.cooldown_remaining(), 900)
        self.assertEqual(float(self.store.meta['web:ratelimit:streak']), 0)
        self.clock.advance(900)
        with self.q._wake:
            self.q._wake.notify_all()
        self.ops.wait_calls(5)
        self.quiet()
        self.assertEqual(len(self.ops.calls), 5)
        self.q.cancel_album('a')
        self.ops.finish_all()
        self.idle()

    def test_progress_events_do_not_evict_pause_and_failure_events(self):
        self.add(('1',))
        self.begin(('1',))
        self.ops.wait_calls(1)
        ctx = self.ops.calls[0][3]
        self.q.pause_all()
        for index in range(600):
            ctx.on_event({'type': 'progress', 'bytes': index, 'total': 1000})
        kinds = [event['type'] for event in self.q.events()['events']]
        self.assertIn('paused', kinds)
        self.ops.finish_all()
        self.idle()

    def test_cancelled_link_job_is_reported_as_cancelled_not_done(self):
        self.add(('1',))
        self.begin(('1',), action='link')
        self.ops.wait_calls(1)
        self.q.cancel_album('a')
        # The resolver can return before it notices the stop flag; the settle
        # path must still report a cancellation instead of a finished link.
        self.ops.finish('a', '1')
        self.wait(lambda: not self.q.snapshot()['active'])
        kinds = [event['type'] for event in self.q.events()['events']]
        self.assertIn('cancelled', kinds)
        self.assertNotIn('done', kinds)
        self.ops.finish_all()
        self.idle()

    def test_cancel_one_album_does_not_stop_other_album(self):
        self.add(('1', '2', '3'))
        self.add(('v',), album='b', kind='video')
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2', '3'], 'download')
        self.q.enqueue('b', ['v'], 'link')
        self.q.resume_all()
        self.ops.wait_calls(3)
        self.assertEqual(self.q.cancel_album('a'), 3)
        self.assertFalse(next(c[3].stop_event.is_set() for c in self.ops.calls if c[0] == 'b'))
        self.ops.finish_all()
        self.idle()
        self.assertEqual(self.q.snapshot()['queued'], 0)
        self.assertEqual(len([e for e in self.q.events()['events'] if e['type'] == 'cancelled']), 3)
        self.assertEqual(self.store.records['b', 'v'].status, 'exported')

    def test_cancel_queued_single_multiple_and_requeue_keeps_status(self):
        self.add(('1', '2', '3'))
        self.add(('v',), album='b', kind='video')
        self.store.records['a', '2'].status = 'failed'
        self.store.records['a', '2'].last_error = '连接超时'
        self.store.records['a', '3'].status = 'exported'
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2', '3'], 'download')
        self.q.enqueue('b', ['v'], 'link')
        self.assertEqual(self.q.cancel_queued('a', ['1']), 1)
        self.assertEqual(self.q.snapshot()['queued_by_album'], {'a': 2, 'b': 1})
        self.assertEqual(self.store.records['a', '1'].status, 'pending')
        self.assertEqual(self.q.cancel_queued('a', ['2', '3', '3']), 2)
        self.assertEqual(self.q.snapshot()['queued'], 1)
        self.assertEqual((self.store.records['a', '2'].status,
                          self.store.records['a', '2'].last_error), ('failed', '连接超时'))
        self.assertEqual(self.store.records['a', '3'].status, 'exported')
        self.assertEqual([w.file_id for w in self.store.list_work_items()], ['v'])
        self.assertEqual(self.q.enqueue('a', ['1', '2', '3'], 'download').added, 3)
        self.assertEqual(self.q.snapshot()['queued'], 4)
        self.q.cancel_album('a')
        self.q.cancel_album('b')

    def test_cancel_queued_rejects_running_selection_atomically(self):
        self.add(('1', '2', '3'))
        self.q.set_concurrency('image', 1)
        self.begin(('1', '2', '3'))
        self.ops.wait_calls(1)
        with self.assertRaises(Busy) as caught:
            self.q.cancel_queued('a', ['1', '2'])
        self.assertIn('正在处理，请稍后或使用“暂停全部”', str(caught.exception))
        self.assertEqual([w.file_id for w in self.store.list_work_items()], ['1', '2', '3'])
        self.assertFalse(self.ops.calls[0][3].stop_event.is_set())
        self.assertEqual(self.q.cancel_queued('a', ['2']), 1)
        self.assertEqual(self.q.snapshot()['active'][0]['file_id'], '1')
        self.ops.finish('a', '1')
        self.ops.wait_calls(2)
        self.ops.finish_all()
        self.idle()

    def test_cancel_queued_validates_album_ownership_under_lock(self):
        self.add(('1',))
        self.add(('x',), album='b')
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1'], 'download')
        for album, ids in (('a', ['missing']), ('b', ['1']), ('missing', ['1'])):
            with self.assertRaises(ValueError):
                self.q.cancel_queued(album, ids)
        self.assertEqual(self.q.snapshot()['queued'], 1)
        self.assertEqual(self.q.cancel_queued('a', ['1']), 1)

    def test_cancel_queued_holds_process_lock(self):
        self.add(('1',))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1'], 'download')
        original = self.store.list_files

        def list_files(*args, **kwargs):
            self.assertTrue(FakeJobLock.held)
            return original(*args, **kwargs)

        with patch.object(self.store, 'list_files', list_files):
            self.assertEqual(self.q.cancel_queued('a', ['1']), 1)
        self.assertFalse(FakeJobLock.held)

    def test_cancel_queued_does_not_interleave_with_dispatch(self):
        self.add(('1',))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1'], 'download')
        entered, release = threading.Event(), threading.Event()
        original = self.q._dispatch

        def slow_dispatch():
            entered.set()
            release.wait(2)
            original()

        self.q._dispatch = slow_dispatch
        self.q.resume_all()
        self.assertTrue(entered.wait(2))
        outcome = {}

        def cancel():
            try:
                outcome['cancelled'] = self.q.cancel_queued('a', ['1'])
            except Busy as error:
                outcome['error'] = str(error)

        thread = threading.Thread(target=cancel)
        thread.start()
        thread.join(.2)
        # The cancel request waits for the dispatch lock; once the slot has the
        # file it must fail instead of deleting a running work item.
        self.assertTrue(thread.is_alive())
        release.set()
        thread.join(2)
        self.assertIn('正在处理', outcome.get('error', ''))
        self.ops.wait_calls(1)
        self.assertEqual(self.q.snapshot()['active'][0]['file_id'], '1')
        self.ops.finish_all()
        self.idle()

    def test_retry_failed_remembers_original_action(self):
        self.add(('1', '2'))
        self.q.start()
        self.q.enqueue('a', ['1'], 'link')
        self.q.enqueue('a', ['2'], 'download')
        self.ops.wait_calls(2)
        for file_id in ('1', '2'):
            self.ops.finish('a', file_id, ItemResult(FAILED, 'transient', '网络不稳定'))
        self.idle()
        self.q.pause_all()
        result = self.q.retry_failed('a')
        self.assertEqual(result.added, 2)
        self.assertEqual({w.file_id: w.action for w in self.store.list_work_items()},
                         {'1': 'link', '2': 'download'})

    def test_speed_is_sampled_per_file_then_summed_and_expires(self):
        self.add(('1', '2'))
        self.begin(('1', '2'))
        self.ops.wait_calls(2)
        contexts = {c[1]: c[3] for c in self.ops.calls}
        contexts['1'].on_event(dict(type='progress', bytes=100, total=1000))
        contexts['2'].on_event(dict(type='progress', bytes=400, total=1000))
        self.clock.advance(2)
        contexts['1'].on_event(dict(type='progress', bytes=300, total=1000))
        contexts['2'].on_event(dict(type='progress', bytes=800, total=1000))
        self.assertEqual(self.q.snapshot()['speed'], 300)
        contexts['1'].on_event(dict(type='progress', bytes=0, total=1000))
        self.assertEqual(self.q.snapshot()['speed'], 200)
        self.clock.advance(6)
        self.assertEqual(self.q.snapshot()['speed'], 0)

    def test_events_ring_cursor_and_snapshot_are_detached(self):
        self.q.start()
        for _ in range(260):
            self.q.pause_all()
            self.q.resume_all()
        batch = self.q.events()
        self.assertEqual(len(batch['events']), 500)
        self.assertEqual(batch['next'], 520)
        self.assertEqual(len(self.q.events(518)['events']), 2)
        batch['events'][-1]['message'] = 'mutated'
        self.assertEqual(self.q.events(519)['events'][0]['message'], '已继续')
        self.assertEqual(set(self.q.snapshot()), {'paused', 'pause_kind', 'pause_reason', 'stopping',
            'lanes', 'active', 'speed', 'queued', 'queued_by_album', 'consecutive_failures', 'recent_failures'})
        self.assertTrue(all({'work_id', 'album_id', 'file_id', 'action', 'name', 'sequence', 'time', 'type', 'message'}
                            <= e.keys() for e in batch['events']))

    def test_close_waits_until_workers_settle_then_releases_lock(self):
        self.add(('1', '2', '3'))
        self.begin(('1', '2', '3'))
        self.ops.wait_calls(2)
        closed = threading.Event()
        thread = threading.Thread(target=lambda: (self.q.close(2), closed.set()))
        thread.start()
        self.wait(lambda: self.q.snapshot()['stopping'])
        self.assertFalse(closed.is_set())
        self.assertTrue(FakeJobLock.held)
        for call in self.ops.calls:
            self.ops.finish(call[0], call[1], ItemResult(INTERRUPTED))
        thread.join(2)
        self.assertTrue(closed.is_set())
        self.assertFalse(FakeJobLock.held)
        self.assertEqual(len(self.store.work), 3)
        self.assertTrue(all(w.state == 'queued' for w in self.store.work.values()))
        self.assertEqual(len(self.ops.calls), 2)

    def test_close_timeout_revokes_all_late_store_access(self):
        self.add(('1',))
        self.begin(('1',))
        self.ops.wait_calls(1)
        self.q.close(0)
        self.assertTrue(FakeJobLock.held)
        self.store.closed = self.ops.late_write = True
        self.ops.finish_all()
        self.idle()
        self.assertEqual(self.store.calls_after_close, 0)
        self.assertFalse(FakeJobLock.held)
        self.assertEqual(next(iter(self.store.work.values())).state, 'running')
        with self.assertRaises(Busy):
            self.q.enqueue('a', ['1'], 'download')

    def test_per_slot_gap_and_client_reuse(self):
        self.add(('1', '2'))
        self.q._lanes['image'].gap_min = self.q._lanes['image'].gap_max = .2
        self.q.set_concurrency('image', 1)
        self.begin(('1', '2'))
        self.ops.wait_calls(1)
        self.ops.finish('a', '1')
        self.idle()
        self.assertEqual(len(self.ops.calls), 1)
        self.clock.advance(.2)
        with self.q._wake:
            self.q._wake.notify_all()
        self.ops.wait_calls(2)
        self.assertIs(self.ops.calls[0][3].client, self.ops.calls[1][3].client)
        self.assertIs(self.ops.calls[0][3].reserve_lock, self.ops.calls[1][3].reserve_lock)

    def test_invalid_inputs_have_no_partial_enqueue(self):
        self.add()
        self.q.start()
        for action, ids in (('links', ['1']), ('download', ['1', 'missing']), ('download', '1')):
            with self.assertRaises(ValueError):
                self.q.enqueue('a', ids, action)
        self.assertEqual(self.q.snapshot()['queued'], 0)
        with self.assertRaises(ValueError):
            self.q.set_concurrency('other', 2)
        with self.assertRaises(ValueError):
            self.q.set_concurrency('video', 4)

    def test_enqueue_reads_file_state_under_process_lock(self):
        self.add(('1',))
        self.q.start()
        self.q.pause_all()
        original = self.store.list_files

        def list_files(*args, **kwargs):
            self.assertTrue(FakeJobLock.held)
            return original(*args, **kwargs)

        with patch.object(self.store, 'list_files', list_files):
            self.assertEqual(self.q.enqueue('a', ['1'], 'download').added, 1)
        self.assertFalse(FakeJobLock.held)

    def test_worker_start_failure_preserves_queue_and_releases_lock(self):
        self.add(('1',))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1'], 'download')
        with patch('bunkrdl.jobqueue.threading.Thread.start', side_effect=RuntimeError('线程启动失败')):
            self.q.resume_all()
            self.wait(lambda: self.q.snapshot()['pause_kind'] == 'failures')
        self.assertEqual(self.q.snapshot()['active'], [])
        self.assertEqual(self.q.snapshot()['queued'], 1)
        self.assertEqual(next(iter(self.store.work.values())).state, 'queued')
        self.assertFalse(FakeJobLock.held)
        self.q.resume_all()
        self.ops.wait_calls(1)
        self.ops.finish_all()
        self.idle()

    def test_client_failure_is_recorded_and_retry_keeps_link_action(self):
        self.add(('1',))

        def broken_factory(*args, **kwargs):
            raise RuntimeError('客户端创建失败')

        self.q.client_factory = broken_factory
        self.begin(('1',), action='link')
        self.wait(lambda: self.q.snapshot()['consecutive_failures'] == 1)
        self.assertEqual(self.store.records['a', '1'].status, 'failed')
        self.assertIn('客户端创建失败', self.store.records['a', '1'].last_error)
        self.assertFalse(FakeJobLock.held)
        self.q.pause_all()
        self.assertEqual(self.q.retry_failed('a').added, 1)
        self.assertEqual(next(iter(self.store.work.values())).action, 'link')

    def test_callback_failures_do_not_settle_before_final_result(self):
        self.add(('1',))
        self.begin(('1',))
        self.ops.wait_calls(1)
        ctx = self.ops.calls[0][3]
        ctx.on_event(dict(type='failed', error_kind='challenge'))
        ctx.on_event(dict(type='aborted'))
        self.assertFalse(self.q.snapshot()['paused'])
        self.assertEqual(self.q.snapshot()['consecutive_failures'], 0)
        self.ops.finish('a', '1', ItemResult(FAILED, 'challenge', '网站要求人机验证'))
        self.wait(lambda: self.q.snapshot()['pause_kind'] == 'challenge')
        failures = [e for e in self.q.events()['events'] if e['type'] == 'failed']
        self.assertEqual(len(failures), 1)
        self.assertEqual(self.q.snapshot()['consecutive_failures'], 1)

    def test_restart_keeps_cooldown_and_streak_and_blocks_every_lane(self):
        self.add(('1', '2'))
        self.add(('v',), album='b', kind='video')
        self.store.meta['web:ratelimit:until'] = '160'
        self.store.meta['web:ratelimit:streak'] = '2'
        self.store.add_work_items([('a', '1', 'download', 'image'),
                                   ('a', '2', 'download', 'image'),
                                   ('b', 'v', 'link', 'video')])
        self.q.start()
        self.assertEqual(self.q.snapshot()['pause_kind'], 'restart')
        # The persisted cooldown and streak survive the restart.
        self.assertEqual(self.q._gate.streak, 2)
        self.assertEqual(self.store.meta['web:ratelimit:streak'], '2')
        self.q.resume_all()
        self.quiet()
        # Resuming after a restart does not clear the cooldown: no lane runs.
        self.assertFalse(self.ops.calls)
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 0)
        self.assertEqual(self.q.snapshot()['lanes']['video']['effective'], 0)
        self.clock.advance(60)
        with self.q._wake:
            self.q._wake.notify_all()
        # Cooldown over: one probe file, then the configured concurrency.
        self.ops.wait_calls(1)
        self.assertEqual(self.ops.calls[0][1], '1')
        self.quiet()
        self.assertEqual(len(self.ops.calls), 1)
        self.ops.finish('a', '1')
        self.ops.wait_calls(3)
        self.assertEqual(sorted(call[1] for call in self.ops.calls[1:]), ['2', 'v'])
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 2)
        self.ops.finish_all()
        self.idle()

    def test_lowering_concurrency_does_not_interrupt_existing_slots(self):
        self.add(('1', '2', '3'))
        self.begin(('1', '2', '3'))
        self.ops.wait_calls(2)
        self.q.set_concurrency('image', 1)
        self.assertTrue(all(not call[3].stop_event.is_set() for call in self.ops.calls))
        self.ops.finish('a', '1')
        self.wait(lambda: len(self.q.snapshot()['active']) == 1)
        self.assertEqual(len(self.ops.calls), 2)
        self.ops.finish('a', '2')
        self.ops.wait_calls(3)
        self.assertEqual(self.ops.calls[-1][1], '3')

    def test_purge_album_rejects_queued_and_running_work(self):
        self.add(('1', '2'))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2'], 'download')
        for ids in (['1'], ['2']):
            with self.assertRaises(Busy) as caught:
                self.q.purge_album('a')
            self.assertEqual(str(caught.exception), '还有文件在排队或处理，请先取消排队或暂停后再清除')
            self.assertEqual(self.q.cancel_queued('a', ids), 1)
        self.assertEqual(len(self.store.list_files('a')), 2)
        self.assertEqual(self.q.purge_album('a'), 2)
        self.assertIsNone(self.store.get_album('a'))
        self.assertEqual(self.store.list_files('a'), [])
        self.assertEqual(self.q.snapshot()['queued'], 0)
        # A missing album is not an error.
        self.assertEqual(self.q.purge_album('missing'), 0)

    def test_purge_rejects_a_running_file_and_keeps_the_download(self):
        self.add(('1', '2'))
        self.q.set_concurrency('image', 1)
        self.begin(('1', '2'))
        self.ops.wait_calls(1)
        with self.assertRaises(Busy) as caught:
            self.q.purge_files('a', file_ids=['1'])
        self.assertIn('排队或处理', str(caught.exception))
        self.assertEqual(len(self.store.list_files('a')), 2)
        self.assertFalse(self.ops.calls[0][3].stop_event.is_set())
        # A queued sibling is rejected too.
        with self.assertRaises(Busy):
            self.q.purge_files('a', statuses=['pending'])
        self.ops.finish('a', '1')
        self.ops.wait_calls(2)
        self.assertEqual(self.q.purge_files('a', file_ids=['1']), 1)
        self.assertEqual([r.file_id for r in self.store.list_files('a')], ['2'])
        self.ops.finish_all()
        self.idle()

    def test_purge_files_by_status_and_unknown_input(self):
        self.add(('1', '2', '3'))
        self.store.records['a', '1'].status = 'done'
        self.store.records['a', '2'].status = 'failed'
        self.store.records['a', '3'].status = 'failed'
        self.q.start()
        self.assertEqual(self.q.purge_files('a', statuses=['failed']), 2)
        self.assertEqual([r.file_id for r in self.store.list_files('a')], ['1'])
        self.assertEqual(self.q.purge_files('a', statuses=['failed']), 0)
        self.assertEqual(self.q.purge_files('a', file_ids=['missing']), 0)
        self.assertEqual(self.q.purge_files('missing', file_ids=['1']), 0)
        with self.assertRaises(ValueError):
            self.q.purge_files('a', statuses=['nope'])
        self.assertEqual([r.file_id for r in self.store.list_files('a')], ['1'])
        # Album-level purge deletes the remainder without touching other albums.
        self.add(('x',), album='b')
        self.assertEqual(self.q.purge_album('a'), 1)
        self.assertEqual(self.store.list_files('a'), [])
        self.assertEqual([r.file_id for r in self.store.list_files('b')], ['x'])

    def test_purge_clears_failure_memory_last_action_and_event_banner(self):
        self.add(('1', '2'))
        self.q.set_concurrency('image', 1)
        self.begin(('1',), action='link')
        self.ops.wait_calls(1)
        self.ops.finish('a', '1', ItemResult(FAILED, 'other', '网站返回了意外内容'))
        self.wait(lambda: self.q.snapshot()['consecutive_failures'] == 1)
        self.assertEqual(self.store.meta['web:a:last_action:1'], 'link')
        self.assertEqual(len(self.q.snapshot()['recent_failures']), 1)
        self.assertTrue(any(e['type'] == 'failed' for e in self.q.events()['events']))
        self.q.pause_all()
        cursor = self.q.events()['next']
        self.assertEqual(self.q.purge_files('a', file_ids=['1']), 1)
        self.assertEqual(self.q.snapshot()['consecutive_failures'], 0)
        self.assertEqual(self.q.snapshot()['recent_failures'], [])
        self.assertFalse(any(e['type'] == 'failed' for e in self.q.events()['events']))
        self.assertNotIn('web:a:last_action:1', self.store.meta)
        # The event cursor never rewinds, so the page keeps receiving new events.
        self.assertEqual(self.q.events()['next'], cursor)
        # Purging the whole album drops the rest of its meta namespace and
        # keeps the global keys (rate-limit gate, lane settings).
        self.store.meta['web:a:folder'] = 'external'
        self.store.meta['web:ratelimit:until'] = '90'
        self.assertEqual(self.q.purge_album('a'), 1)
        self.assertNotIn('web:a:folder', self.store.meta)
        self.assertNotIn('web:a:last_action:1', self.store.meta)
        self.assertEqual(self.store.meta['web:ratelimit:until'], '90')
        self.assertEqual(self.store.meta['web:lane:image:setting'], '1')

    def test_purge_and_cancel_queue_are_serialised_by_the_scheduler_lock(self):
        self.add(('1', '2'))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1', '2'], 'download')
        entered, release = threading.Event(), threading.Event()
        original = self.store.delete_work_items

        def slow_delete(ids):
            entered.set()
            release.wait(2)
            original(ids)

        outcome = {}
        with patch.object(self.store, 'delete_work_items', slow_delete):
            cancelling = threading.Thread(target=lambda: outcome.update(
                cancelled=self.q.cancel_queued('a', ['1', '2'])))
            cancelling.start()
            self.assertTrue(entered.wait(2))
            purging = threading.Thread(target=lambda: outcome.update(purged=self.q.purge_album('a')))
            purging.start()
            purging.join(.2)
            # The purge waits for the cancel to finish instead of racing it.
            self.assertTrue(purging.is_alive())
            release.set()
            cancelling.join(2)
            purging.join(2)
        self.assertEqual(outcome, {'cancelled': 2, 'purged': 2})
        self.assertIsNone(self.store.get_album('a'))

    def test_purge_holds_the_process_lock_and_reads_inside_it(self):
        self.add(('1',))
        self.q.start()
        original = self.store.match_file_ids

        def match(*args, **kwargs):
            self.assertTrue(FakeJobLock.held)
            return original(*args, **kwargs)

        with patch.object(self.store, 'match_file_ids', match):
            self.assertEqual(self.q.purge_files('a', file_ids=['1']), 1)
        self.assertFalse(FakeJobLock.held)
        self.assertEqual(self.q.purge_album('a'), 0)

    def test_purge_does_not_interleave_with_dispatch(self):
        self.add(('1',))
        self.q.start()
        self.q.pause_all()
        self.q.enqueue('a', ['1'], 'download')
        entered, release = threading.Event(), threading.Event()
        original = self.q._dispatch

        def slow_dispatch():
            entered.set()
            release.wait(2)
            original()

        self.q._dispatch = slow_dispatch
        self.q.resume_all()
        self.assertTrue(entered.wait(2))
        outcome = {}

        def purge():
            try:
                outcome['purged'] = self.q.purge_album('a')
            except Busy as error:
                outcome['error'] = str(error)

        thread = threading.Thread(target=purge)
        thread.start()
        thread.join(.2)
        self.assertTrue(thread.is_alive())
        release.set()
        thread.join(2)
        self.assertIn('排队或处理', outcome.get('error', ''))
        self.ops.wait_calls(1)
        self.assertEqual(self.q.snapshot()['active'][0]['file_id'], '1')
        self.ops.finish_all()
        self.idle()
