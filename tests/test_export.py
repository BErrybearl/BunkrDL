"""Offline regression checks; scheduling is injected through the shared interface."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import csv
import sqlite3
import sys
import threading
import types
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

import requests

from bunkrdl.config import Config
from bunkrdl.export import download_url, export_links, generate_link_once
from bunkrdl.worktypes import FAILED, INTERRUPTED, OK, WorkContext
from bunkrdl.names import safe_name, unique_names
from bunkrdl.models import (BunkrError, ChallengeRequired, ExportResult, FileRecord,
                            Maintenance, NotFound, RateLimited, ResolvedLink, TransientError)


def record(file_id, name='片段.mp4', **fields):
    return FileRecord(file_id, name, file_id, 'https://example.test/f/' + file_id,
                      album_id='album', kind='video', **fields)


class MemoryStore:
    def __init__(self, records):
        self.records = [replace(r) for r in records]
        self.meta = {}

    def remember_action(self, album_id, file_id, action):
        self.meta[f'web:{album_id}:last_action:{file_id}'] = action

    def list_files(self, album_id, status=None, kinds=None):
        return [replace(r) for r in self.records if r.album_id == album_id
                and (status is None or r.status == status)
                and (kinds is None or r.kind in kinds)]

    def update_files(self, updates):
        for album_id, file_id, values in updates:
            self.update_file(album_id, file_id, **values)

    def update_file(self, album_id, file_id, **fields):
        row = next(r for r in self.records if r.album_id == album_id and r.file_id == file_id)
        for key, value in fields.items():
            setattr(row, key, value)


class FakeClient:
    def __init__(self, outcomes=None):
        self.outcomes = outcomes or {}
        self.calls = []

    def resolve(self, file):
        self.calls.append(file.file_id)
        outcomes = self.outcomes.get(file.file_id, [])
        value = outcomes.pop(0) if outcomes else ResolvedLink(
            'https://cdn.test/' + file.file_id + '?token=fresh',
            'https://referer.test/', file.file_id + '.mp4', 2000000000)
        if isinstance(value, BaseException):
            raise value
        return value


class FakePacer:
    def __init__(self):
        self.before = Mock(return_value=True)
        self.wait = Mock(return_value=True)
        self.backoff_delay = Mock(side_effect=lambda retry, retry_after: retry_after if retry_after is not None else retry * 10)
        self.file_done = Mock()

    def before_file(self):
        return self.before()


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name)
        self.config = Config()
        self.pacer = FakePacer()

    def export(self, store, client=None, **kwargs):
        return export_links(client or FakeClient(), store, 'album', self.output,
                            self.config, pacer=self.pacer, **kwargs)

    def part(self, result, name, part=1):
        return Path(result.out_dir) / f'part-{part:02d}-{name}'

    def test_safe_name_and_unique_names(self):
        self.assertEqual(safe_name('<a>:"/\\|?*\x00\x1f.mp4. '), 'a.mp4')
        self.assertEqual(safe_name('CON.mp4'), '_CON.mp4')
        rows = [record('1', 'a.mp4'), record('2', 'A.mp4'), record('3', 'a (2).mp4')]
        self.assertEqual(unique_names(rows), {'1': 'a.mp4', '2': 'A (3).mp4', '3': 'a (2).mp4'})

    def test_start_done_indices_and_done_only_after_publication(self):
        store = MemoryStore([record('1'), record('2', status='done'), record('3')])
        events = []
        def event(e):
            events.append(e)
            if e['type'] == 'done':
                self.assertTrue(list(Path(e['out_dir']).glob('part-*-links.txt')))
                self.assertEqual(store.records[0].status, 'exported')
                self.assertEqual(store.records[2].status, 'exported')
        self.export(store, on_event=event)
        self.assertEqual([(e['type'], e['file_id'], e['name'], e['index'], e['count'])
                          for e in events if e['type'] in ('start', 'done')],
                         [('start', '1', '片段.mp4', 1, 2), ('start', '3', '片段.mp4', 2, 2),
                          ('done', '1', '片段.mp4', 1, 2), ('done', '3', '片段.mp4', 2, 2)])

    def test_failed_events_preserve_exception_kind(self):
        self.config.quick_retries = 0
        cases = [(Maintenance('维护'), 'maintenance'), (NotFound('删除'), 'not_found'),
                 (ChallengeRequired('验证'), 'challenge'), (RateLimited('HTTP 429'), 'rate_limited'),
                 (TransientError('连接超时'), 'transient'), (BunkrError('大小不符'), 'size_mismatch'),
                 (BunkrError('其他'), 'other')]
        for error, kind in cases:
            with self.subTest(kind=kind):
                events = []
                store = MemoryStore([record('1')])
                self.export(store, FakeClient({'1': [error]}), on_event=events.append)
                event = next(e for e in events if e['type'] == 'failed')
                self.assertEqual((event['file_id'], event['name'], event['error_kind']),
                                 ('1', '片段.mp4', kind))
                self.assertEqual(event['message'], store.records[0].last_error)
                self.assertNotIn('token', event['message'])
                self.assertFalse(any(e['type'] == 'done' for e in events))

    def test_failure_messages_are_specific_chinese(self):
        self.config.quick_retries = 0
        for error, text in ((RateLimited('HTTP 429'), '被网站限速（429）'),
                            (RateLimited('HTTP 503'), '服务器繁忙（503）'),
                            (TransientError('连接超时'), '连接超时（可能是代理或网络问题）')):
            with self.subTest(error=type(error).__name__):
                store = MemoryStore([record('1')])
                events = []
                self.export(store, FakeClient({'1': [error]}), on_event=events.append)
                self.assertIn(text, next(e for e in events if e['type'] == 'failed')['message'])
                self.assertIn(text, store.records[0].last_error)

    def test_backoff_wait_event_has_deadline_and_keeps_retry_event(self):
        events = []
        with patch('bunkrdl.export.time.time', return_value=1000):
            self.export(MemoryStore([record('1')]),
                        FakeClient({'1': [RateLimited(retry_after=12)]}), on_event=events.append)
        waits = [e for e in events if e['type'] in ('retry', 'wait')]
        self.assertEqual([e['type'] for e in waits], ['retry', 'wait'])
        for event in waits:
            self.assertEqual((event['reason'], event['until'], event['seconds']), ('backoff', 1012, 12))

    def test_formats_encoding_headers_and_store(self):
        store = MemoryStore([record('1', '片?段.mp4', size=42), record('2', '片段.mp4')])
        link = ResolvedLink('https://cdn.test/1?token=new', 'https://ref.test/', 'random.mp4',
                            1900000000, {'user-agent': '测试 UA'})
        result = self.export(store, FakeClient({'1': [link]}))
        self.assertIsInstance(result, ExportResult)
        self.assertEqual((result.exported, result.failed), (2, 0))
        self.assertEqual(len(result.files), 4)
        aria = self.part(result, 'aria2.txt').read_text(encoding='utf-8')
        self.assertIn('最早过期时间：', aria)
        self.assertIn('  out=片段.mp4\n  referer=https://ref.test/\n  header=User-Agent: 测试 UA', aria)
        self.assertIn('  out=片段 (2).mp4', aria)
        self.assertIn(self.config.user_agent, aria)
        links = self.part(result, 'links.txt').read_text(encoding='utf-8')
        self.assertIn('&n=%E7%89%87%E6%AE%B5.mp4', links)
        self.assertEqual([line for line in links.splitlines() if not line.startswith('#')],
                         [download_url(row, self.config, unique_names(store.records))
                          for row in store.records])
        mapping = self.part(result, 'rename_map.csv')
        self.assertTrue(mapping.read_bytes().startswith(b'\xef\xbb\xbf'))
        with mapping.open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(line for line in stream if not line.startswith('#')))
        self.assertEqual(rows[0]['cdn_name'], 'random.mp4')
        self.assertEqual(rows[0]['size'], '42')
        self.assertEqual(rows[1]['size'], '')
        self.assertEqual(store.records[0].link_url, link.url)
        self.assertEqual(store.records[0].link_expires, link.expires_at)
        self.assertTrue(all(r.status == 'exported' and r.attempts == 0 for r in store.records))

    def test_filters_and_reexport_refresh(self):
        records = [record(str(i), status=status) for i, status in enumerate(
            ['pending', 'failed', 'exported', 'done', 'skipped', 'downloading'])]
        image = record('7')
        image.kind = 'image'
        records.append(image)
        store, client = MemoryStore(records), FakeClient()
        self.assertEqual(self.export(store, client).exported, 3)
        self.assertEqual(client.calls, ['0', '1', '2'])
        self.assertEqual(self.pacer.before.call_count, 3)

    def test_link_name_param_can_be_disabled(self):
        self.config.link_name_param = False
        result = self.export(MemoryStore([record('1')]))
        text = self.part(result, 'links.txt').read_text(encoding='utf-8')
        self.assertNotIn('&n=', text)
        self.assertFalse(result.stopped)

    def test_shared_pacer_created_with_full_config_and_events(self):
        factory = Mock(return_value=self.pacer)
        module = types.ModuleType('bunkrdl.guard')
        module.Pacer = factory
        stop, events, store = threading.Event(), Mock(), MemoryStore([record('1'), record('2')])
        with patch.dict(sys.modules, {'bunkrdl.guard': module}):
            export_links(FakeClient(), store, 'album', self.output, self.config,
                         on_event=events, stop_event=stop)
        factory.assert_called_once_with(store, self.config, stop_event=stop, on_event=events)
        self.assertEqual((self.config.file_delay_min, self.config.file_delay_max), (3, 6))
        self.assertEqual(self.pacer.before.call_count, 2)
        self.assertEqual(self.pacer.file_done.call_count, 2)

    def test_retry_budget_is_per_run_and_success_does_not_increment_attempts(self):
        self.config.quick_retries = 2
        store = MemoryStore([record('1', attempts=100)])
        client = FakeClient({'1': [RateLimited(retry_after=12), TransientError('连接超时')]})
        self.assertEqual(self.export(store, client).exported, 1)
        self.assertEqual(client.calls, ['1', '1', '1'])
        self.assertEqual(store.records[0].attempts, 102)
        self.assertEqual([c.args[0] for c in self.pacer.wait.call_args_list], [12, 10])
        self.pacer.file_done.assert_called_once_with(rate_limited=False)

    def test_exhausted_transient_failure_counted_and_reported_to_pacer(self):
        store = MemoryStore([record('1', attempts=100)])
        client = FakeClient({'1': [TransientError()] * 3})
        result = self.export(store, client)
        self.assertEqual((result.exported, result.failed), (0, 1))
        self.assertEqual(client.calls, ['1', '1', '1'])
        self.assertEqual(store.records[0].attempts, 103)
        self.pacer.file_done.assert_called_once_with(rate_limited=False)
        self.assertEqual([c.args[0] for c in self.pacer.wait.call_args_list], [5, 10])

    def test_retry_exhaustion_is_not_retried_again_by_the_exporter(self):
        store = MemoryStore([record('1'), record('2')])
        client = FakeClient({'1': [RateLimited('HTTP 429')] * 3})
        result = self.export(store, client)
        self.assertEqual((result.exported, result.failed), (1, 1))
        self.assertEqual(client.calls, ['1', '1', '1', '2'])

    def test_file_ids_limit_the_export(self):
        records = [record(str(i), status=status) for i, status in enumerate(
            ['pending', 'exported', 'failed', 'done'])]
        store, client = MemoryStore(records), FakeClient()
        result = self.export(store, client, file_ids=['2', '0'])
        self.assertEqual(result.exported, 2)
        self.assertEqual(client.calls, ['0', '2'])
        self.assertEqual([r.status for r in store.records], ['exported', 'exported', 'exported', 'done'])

    def test_ordinary_failure_is_not_retried_and_clears_signature(self):
        store = MemoryStore([record('1', link_url='stale', link_expires=1), record('2')])
        client = FakeClient({'1': [BunkrError('bad')]})
        result = self.export(store, client)
        self.assertEqual((result.exported, result.failed), (1, 1))
        self.assertEqual(client.calls, ['1', '2'])
        self.assertIsNone(store.records[0].link_url)
        self.assertIsNone(store.records[0].link_expires)
        self.pacer.wait.assert_not_called()

    def test_stop_before_first_file(self):
        stop = threading.Event()
        stop.set()
        client = FakeClient()
        result = self.export(MemoryStore([record('1')]), client, stop_event=stop)
        self.assertTrue(result.stopped)
        self.assertEqual(client.calls, [])
        self.pacer.before.assert_not_called()

    def test_stop_during_pacing_never_resolves(self):
        stop = threading.Event()
        self.pacer.before.side_effect = lambda: (stop.set() or True)
        client = FakeClient()
        result = self.export(MemoryStore([record('1')]), client, stop_event=stop)
        self.assertTrue(result.stopped)
        self.assertEqual(client.calls, [])

    def test_stop_during_retry_never_resolves_again(self):
        self.pacer.wait.return_value = False
        store, client = MemoryStore([record('1'), record('2')]), FakeClient({'1': [RateLimited()]})
        result = self.export(store, client)
        self.assertTrue(result.stopped)
        self.assertEqual(client.calls, ['1'])
        self.assertEqual([r.status for r in store.records], ['pending', 'pending'])

    def test_stop_is_checked_before_retry_even_if_pacer_returns_true(self):
        stop = threading.Event()
        self.pacer.wait.side_effect = lambda seconds: (stop.set() or True)
        client = FakeClient({'1': [TransientError()]})
        result = self.export(MemoryStore([record('1')]), client, stop_event=stop)
        self.assertTrue(result.stopped)
        self.assertEqual(client.calls, ['1'])

    def test_challenge_immediately_stops_and_preserves_published_volume(self):
        self.config.export_batch_size = 1
        store = MemoryStore([record('1'), record('2'), record('3')])
        client = FakeClient({'2': [ChallengeRequired()]})
        result = self.export(store, client)
        self.assertTrue(result.stopped)
        self.assertIn('人机验证', result.aborted_reason)
        self.assertEqual((result.exported, result.failed), (1, 1))
        self.assertEqual(client.calls, ['1', '2'])
        self.assertTrue(self.part(result, 'links.txt').exists())
        self.assertEqual(store.records[2].status, 'pending')

    def test_challenge_discards_unpublished_signatures(self):
        store = MemoryStore([record('1'), record('2')])
        result = self.export(store, FakeClient({'2': [ChallengeRequired()]}))
        self.assertEqual(result.exported, 0)
        self.assertEqual(store.records[0].status, 'pending')

    def test_stop_discards_unpublished_volume_preserving_all_fields(self):
        store = MemoryStore([record('1', status='exported', link_url='old', link_expires=1), record('2')])
        self.pacer.before.side_effect = [True, False]
        result = self.export(store)
        self.assertTrue(result.stopped)
        self.assertEqual((store.records[0].status, store.records[0].link_url, store.records[0].link_expires),
                         ('exported', 'old', 1))
        self.assertEqual(result.exported, 0)

    def test_each_volume_is_published_before_states_and_callbacks(self):
        self.config.export_batch_size = 2
        store = MemoryStore([record(str(i)) for i in range(5)])
        observed = []

        def event(e):
            if e['type'] == 'exported':
                folder = Path(e['out_dir'])
                self.assertEqual(len(list(folder.glob('part-01-*'))), 3)
                self.assertFalse(list(folder.glob('*.tmp')))
                self.assertTrue(all(r.status == 'exported' for r in store.records[:2]))
                observed.append(e['file_id'])

        result = self.export(store, on_event=event)
        self.assertEqual(result.exported, 5)
        self.assertEqual(len(result.files), 10)
        self.assertEqual(observed, [str(i) for i in range(5)])
        index = (Path(result.out_dir) / 'index.txt').read_text(encoding='utf-8')
        self.assertIn('part-03-links.txt', index)
        self.assertIn('最早过期时间', index)
        self.assertIn('下载链接分卷', index)
        self.assertNotIn('导出', index)

    def test_interrupt_at_callback_keeps_whole_volume_consistent(self):
        self.config.export_batch_size = 2
        store = MemoryStore([record('1'), record('2'), record('3')])

        def interrupt(e):
            if e['type'] == 'exported':
                raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self.export(store, on_event=interrupt)
        self.assertEqual([r.status for r in store.records], ['exported', 'exported', 'pending'])
        folder = next(self.output.iterdir())
        self.assertEqual(len(list(folder.glob('part-01-*'))), 3)

    def test_failed_write_does_not_change_states_or_previous_exports(self):
        first = self.export(MemoryStore([record('old')]))
        previous = {p: Path(p).read_bytes() for p in first.files}
        store = MemoryStore([record('1')])
        original_open = Path.open

        def broken(path, *args, **kwargs):
            if path.name == 'part-01-aria2.txt.tmp':
                raise OSError('disk full')
            return original_open(path, *args, **kwargs)

        with patch.object(Path, 'open', broken), self.assertRaises(OSError):
            self.export(store)
        self.assertEqual(store.records[0].status, 'pending')
        self.assertEqual(previous, {p: Path(p).read_bytes() for p in previous})
        for folder in self.output.iterdir():
            if str(folder) != first.out_dir:
                self.assertEqual(list(folder.iterdir()), [])

    def test_rename_failure_leaves_unpublished_volume_unchanged(self):
        store = MemoryStore([record('1')])
        rename = Path.rename

        def broken(path, target):
            if path.name.endswith('aria2.txt.tmp'):
                raise OSError('rename failed')
            return rename(path, target)

        with patch.object(Path, 'rename', broken), self.assertRaises(OSError):
            self.export(store)
        self.assertEqual(store.records[0].status, 'pending')
        self.assertEqual(list(next(self.output.iterdir()).iterdir()), [])

    def test_repeated_exports_use_new_timestamp_directories(self):
        store = MemoryStore([record('1')])
        first = self.export(store)
        before = {p: Path(p).read_bytes() for p in first.files}
        second = self.export(store)
        self.assertNotEqual(first.out_dir, second.out_dir)
        self.assertRegex(Path(second.out_dir).name, r'^\d{8}-\d{6}$')
        self.assertEqual(before, {p: Path(p).read_bytes() for p in before})

    def test_default_volume_size_is_fifty(self):
        result = self.export(MemoryStore([record(str(i)) for i in range(51)]))
        self.assertEqual((result.exported, len(result.files)), (51, 7))
        for part, count in ((1, 50), (2, 1)):
            lines = self.part(result, 'links.txt', part).read_text(encoding='utf-8').splitlines()
            self.assertEqual(len([line for line in lines if not line.startswith('#')]), count)

    def test_later_volume_write_failure_preserves_completed_volume(self):
        self.config.export_batch_size = 1
        store = MemoryStore([record('1'), record('2')])
        original_open = Path.open

        def broken(path, *args, **kwargs):
            if path.name == 'part-02-links.txt.tmp':
                raise OSError('disk full')
            return original_open(path, *args, **kwargs)

        with patch.object(Path, 'open', broken), self.assertRaises(OSError):
            self.export(store)
        self.assertEqual([r.status for r in store.records], ['exported', 'pending'])
        folder = next(self.output.iterdir())
        self.assertEqual(len(list(folder.glob('part-01-*'))), 3)
        self.assertFalse(list(folder.glob('part-02-*')))

    def test_expired_at_publication_is_explicitly_failed_without_resigning(self):
        expired = ResolvedLink('https://cdn.test/expired', 'r', 'old.mp4', 100)
        store, client = MemoryStore([record('1'), record('2')]), FakeClient({'1': [expired]})
        with patch('bunkrdl.export.time.time', return_value=200):
            result = self.export(store, client)
        self.assertEqual((result.exported, result.failed), (1, 1))
        self.assertEqual(client.calls, ['1', '2'])
        self.assertIn('已过期', store.records[0].last_error)
        self.assertIn('重新生成下载链接', store.records[0].last_error)
        self.assertNotIn('导出', store.records[0].last_error)
        self.assertNotIn('/expired', self.part(result, 'links.txt').read_text(encoding='utf-8'))

    def test_expiry_checked_after_entire_volume_resolved(self):
        # The first signature is initially live but expires while signing the second.
        now = [50]
        client = FakeClient({'1': [ResolvedLink('https://cdn.test/old', 'r', 'a', 100)]})
        resolve = client.resolve

        def delayed(record):
            if record.file_id == '2':
                now[0] = 150
            return resolve(record)

        client.resolve = delayed
        with patch('bunkrdl.export.time.time', side_effect=lambda: now[0]):
            result = self.export(MemoryStore([record('1'), record('2')]), client)
        self.assertEqual((result.exported, result.failed), (1, 1))

    def test_expiry_after_fsync_filters_and_restages_without_state_changes(self):
        now = [0]
        store = MemoryStore([record('1', status='exported', link_url='old', link_expires=2), record('2')])
        before = replace(store.records[0])
        client = FakeClient({'1': [ResolvedLink('https://cdn.test/short', 'r', 'short.mp4', 1200)]})
        def slow_sync(fd):
            now[0] = 601
        events = []
        with patch('bunkrdl.export.time.time', side_effect=lambda: now[0]), patch('bunkrdl.export.os.fsync', side_effect=slow_sync):
            result = self.export(store, client, on_event=events.append)
        self.assertEqual((result.exported, result.failed), (1, 1))
        self.assertEqual(store.records[0], before)
        self.assertNotIn('/short', self.part(result, 'links.txt').read_text(encoding='utf-8'))
        self.assertTrue(any('10 分钟' in e.get('message', '') for e in events))
        message = next(e['message'] for e in events if '10 分钟' in e.get('message', ''))
        self.assertIn('重新生成下载链接', message)
        self.assertNotIn('导出', message)
        self.assertFalse(list(Path(result.out_dir).glob('*.tmp')))

    def test_volume_transaction_rolls_back_and_keeps_published_recovery_files(self):
        from bunkrdl.store import Store
        from bunkrdl.models import Album, AlbumFile
        store = Store(':memory:')
        self.addCleanup(store.close)
        store.upsert_album(Album('album', 'title', 'url', [
            AlbumFile(str(i), str(i) + '.mp4', str(i), 'url', kind='video') for i in (1, 2)]))
        store._db.execute("""CREATE TRIGGER fail_second BEFORE UPDATE OF status ON files
            WHEN NEW.file_id='2' AND NEW.status='exported'
            BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END""")
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.export(store)
        self.assertEqual([r.status for r in store.list_files('album')], ['pending', 'pending'])
        folder = next(self.output.iterdir())
        self.assertEqual(len(list(folder.glob('part-01-*'))), 3)
        mapping = (folder / 'part-01-rename_map.csv').read_text(encoding='utf-8-sig')
        self.assertIn('1.mp4', mapping)
        self.assertIn('2.mp4', mapping)

    def test_unknown_expiry_allowed_and_empty_export_has_index_only(self):
        client = FakeClient({'1': [ResolvedLink('https://cdn.test/1', 'r', 'a')]})
        result = self.export(MemoryStore([record('1')]), client)
        self.assertEqual(result.exported, 1)
        self.assertIn('最早过期时间：未知', self.part(result, 'links.txt').read_text(encoding='utf-8'))
        empty = self.export(MemoryStore([]))
        self.assertEqual([Path(p).name for p in empty.files], ['index.txt'])


class SingleLinkTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore([record('1'), record('2')])
        self.client = FakeClient()
        self.events = []
        self.stop = threading.Event()
        self.ctx = WorkContext(Config(), self.store, self.client, self.stop,
                               self.events.append, threading.Lock())

    def run_item(self, ident='1'):
        with patch('bunkrdl.export.time.time', return_value=1000):
            return generate_link_once(self.ctx, 'album', ident)

    def test_real_resolver_503_maintenance_reaches_link_result_with_retry_after(self):
        from bunkrdl.resolver import BunkrClient
        class Adapter(requests.adapters.BaseAdapter):
            def send(self, request, **kwargs):
                response = requests.Response()
                response.status_code = 503
                response.url = request.url
                response._content = b'<html>Server under maintenance</html>'
                response._content_consumed = True
                response.headers['Retry-After'] = '120'
                return response
            def close(self):
                pass
        with requests.Session() as session:
            session.mount('https://', Adapter())
            self.ctx.client = BunkrClient(Config(request_interval=0), session, fail_fast=True)
            result = self.run_item()
        self.assertEqual((result.outcome, result.error_kind, result.retry_after),
                         (FAILED, 'rate_limited', 120))
        self.assertEqual(self.store.records[0].status, 'failed')
        self.assertIn('503', self.store.records[0].last_error)
        self.assertEqual(self.events, [])

    def test_saved_before_event_and_survives_later_failure(self):
        def observe(event):
            saved = self.store.records[0]
            self.assertEqual((saved.status, saved.link_expires, saved.cdn_name),
                             ('exported', 2000000000, '1.mp4'))
            self.assertTrue(saved.link_url)
            self.events.append(event)
        self.ctx.on_event = observe
        self.assertEqual(self.run_item().outcome, OK)
        self.client.outcomes['2'] = [TransientError('连接超时')]
        self.assertEqual(self.run_item('2').outcome, FAILED)
        self.assertEqual(self.store.records[0].status, 'exported')
        self.assertEqual(self.client.calls, ['1', '2'])
        event, = self.events
        self.assertEqual((event['type'], event['album_id'], event['file_id'], event['action']),
                         ('link', 'album', '1', 'link'))
        self.assertEqual(event['message'], '链接已生成')

    def test_ten_minute_boundary_and_invalid_expiry(self):
        for expires, outcome in [(1599.9, FAILED), (1600, OK), (999, FAILED),
                                 (None, FAILED), (float('nan'), FAILED), (float('inf'), FAILED)]:
            with self.subTest(expires=expires):
                self.store.records[0].link_url = 'stale'
                self.client.outcomes['1'] = [ResolvedLink('https://cdn.test/fresh', 'r', '1', expires)]
                result = self.run_item()
                self.assertEqual(result.outcome, outcome)
                self.assertEqual(self.store.records[0].status, 'exported' if outcome == OK else 'failed')
                if outcome == FAILED:
                    self.assertIsNone(self.store.records[0].link_url)
                    self.assertIsNone(self.store.records[0].link_expires)
        self.assertEqual(len(self.client.calls), 6)

    def test_errors_classified_without_retries(self):
        cases = [(ChallengeRequired(), 'challenge'), (RateLimited('HTTP 503', 75), 'rate_limited'),
                 (TransientError('连接超时'), 'transient'), (Maintenance(), 'maintenance'),
                 (NotFound(), 'not_found'), (requests.Timeout('private'), 'transient'),
                 (requests.ConnectionError('private'), 'transient'),
                 (BunkrError('https://cdn.test/?token=secret'), 'other')]
        for error, kind in cases:
            with self.subTest(kind=kind):
                self.client.outcomes['1'] = [error]
                result = self.run_item()
                self.assertEqual((result.outcome, result.error_kind), (FAILED, kind))
                self.assertEqual(result.retry_after, getattr(error, 'retry_after', None))
                self.assertEqual(result.message, self.store.records[0].last_error)
                self.assertNotIn('secret', result.message)
        self.assertEqual(len(self.client.calls), len(cases))
        self.assertEqual(self.events, [])

    def test_stop_before_resolve_preserves_record(self):
        before = replace(self.store.records[0])
        self.stop.set()
        self.assertEqual(self.run_item().outcome, INTERRUPTED)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.store.records[0], before)

    def test_pause_during_success_keeps_completed_link(self):
        resolve = self.client.resolve
        def pause(record):
            self.stop.set()
            return resolve(record)
        self.client.resolve = pause
        self.assertEqual(self.run_item().outcome, OK)
        self.assertEqual(self.store.records[0].status, 'exported')
        self.assertEqual(len(self.events), 1)

    def test_pause_during_transport_failure_is_interrupted_but_challenge_is_failed(self):
        for error, outcome in [(TransientError(), INTERRUPTED), (ChallengeRequired(), FAILED)]:
            with self.subTest(error=error):
                self.stop.clear()
                def pause(record):
                    self.stop.set()
                    raise error
                self.client.resolve = pause
                self.assertEqual(self.run_item().outcome, outcome)

    def test_database_failure_does_not_publish_event(self):
        update = self.store.update_file
        def fail(album, ident, **fields):
            if fields.get('status') == 'exported':
                raise sqlite3.OperationalError('write failed')
            update(album, ident, **fields)
        self.store.update_file = fail
        with self.assertRaises(sqlite3.OperationalError):
            self.run_item()
        self.assertEqual(self.events, [])

    def test_download_url_matches_cli_names_and_preserves_signature(self):
        rows = [record('1', 'A.mp4'), record('2', 'a.mp4'), record('3', 'a (2).mp4')]
        rows[1].link_url = 'https://cdn.test/file?' + 'token=' + 'demo%2Bvalue&ex=9999&n=old&%6E=older#part'
        url = download_url(rows[1], Config(), unique_names(rows))
        self.assertIn('token=' + 'demo%2Bvalue&ex=9999', url)
        self.assertEqual(parse_qs(urlsplit(url).query)['n'], ['a (3).mp4'])
        self.assertTrue(url.endswith('#part'))
        self.assertEqual(download_url(rows[0], Config(), {}), '')

    def test_download_url_without_names_reads_whole_album_without_writes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'state.db'
            with closing(sqlite3.connect(path)) as db, db:
                db.execute('CREATE TABLE files(album_id, file_id, name)')
                db.executemany('INSERT INTO files VALUES (?, ?, ?)',
                               [('album', '1', '片段.mp4'), ('album', '2', '片段.mp4'),
                                ('album', '3', '片段 (2).mp4'), ('other', '0', '片段.mp4')])
            before = path.read_bytes()
            row = record('2', link_url='https://cdn.test/2?token=fresh')
            url = download_url(row, Config(state_db=str(path)))
            self.assertEqual(parse_qs(urlsplit(url).query)['n'], ['片段 (3).mp4'])
            self.assertEqual(path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
