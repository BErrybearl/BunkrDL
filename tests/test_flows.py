"""Offline workflow contracts; no real downloads or desktop dialogs."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import contextlib
import json
import subprocess
import sys
import threading
import types
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bunkrdl import flows
from bunkrdl.config import Config
from bunkrdl.models import (Album, FileRecord, QueueResult, ExportResult,
                            ReconcileResult)


def record(file_id, name, kind, status='pending', **fields):
    return FileRecord(file_id, name, file_id, 'https://example.test/f/' + file_id,
                      album_id='abc', kind=kind, status=status, **fields)


class MemoryStore:
    def __init__(self, records):
        self.records = [replace(item) for item in records]
        self.updates = []

    def list_files(self, album_id, status=None, kinds=None):
        return [replace(item) for item in self.records if item.album_id == album_id
                and (status is None or item.status == status)
                and (kinds is None or item.kind in kinds)]

    def update_files(self, updates):
        for album_id, file_id, values in updates:
            self.update_file(album_id, file_id, **values)

    def update_file(self, album_id, file_id, **fields):
        row = next(item for item in self.records
                   if item.album_id == album_id and item.file_id == file_id)
        for key, value in fields.items():
            setattr(row, key, value)
        self.updates.append((file_id, dict(fields)))

    def get(self, file_id):
        return next(item for item in self.records if item.file_id == file_id)


def fake_flow_modules(store):
    specs = {
        'downloader': {'run_queue': Mock(return_value=QueueResult(done=1))},
        'export': {'export_links': Mock(return_value=ExportResult(exported=1, out_dir='out'))},
        'reconcile': {'reconcile': Mock(return_value=ReconcileResult(completed=['1']))},
    }
    modules = {}
    for name, attrs in specs.items():
        module = types.ModuleType('bunkrdl.' + name)
        module.__dict__.update(attrs)
        modules[module.__name__] = module
    return modules


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore([
            record('1', 'a.jpg', 'image'),
            record('2', 'b.mp4', 'video', 'failed'),
            record('3', 'c.zip', 'other', 'exported'),
            record('4', 'd.png', 'image', 'done'),
            record('5', 'e.mkv', 'video', 'skipped'),
            record('6', 'f.jpg', 'image', 'failed'),
        ])

    def test_four_presets(self):
        self.assertEqual(flows.select_ids(self.store, 'abc', 'images'), ['1', '3', '4', '6'])
        self.assertEqual(flows.select_ids(self.store, 'abc', 'videos'), ['2', '5'])
        self.assertEqual(flows.select_ids(self.store, 'abc', 'unfinished'),
                         ['1', '2', '3', '6'])
        self.assertEqual(flows.select_ids(self.store, 'abc', 'failed'), ['2', '6'])
        self.assertEqual(flows.select_ids(self.store, 'abc', 'images'), ['1', '3', '4', '6'])

    def test_unknown_preset_is_rejected(self):
        with self.assertRaises(ValueError):
            flows.select_ids(self.store, 'abc', 'all')


class FlowTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore([
            record('1', 'a.jpg', 'image'),
            record('2', 'b.mp4', 'video', 'failed', last_error='旧错误'),
            record('3', 'c.mp4', 'video', 'exported', link_url='https://old', link_expires=1),
            record('4', 'd.jpg', 'image', 'done'),
        ])
        self.client = Mock()
        self.config = Config(download_root='downloads')
        self.stop = threading.Event()
        self.events = []
        self.modules = fake_flow_modules(self.store)
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(sys.modules, self.modules))
        self.queue = self.modules['bunkrdl.downloader'].run_queue
        self.export = self.modules['bunkrdl.export'].export_links
        self.reconcile = self.modules['bunkrdl.reconcile'].reconcile

    def run_download(self, file_ids):
        return flows.download_files(self.client, self.store, 'abc', 'destination',
                                    self.config, file_ids,
                                    stop_event=self.stop, on_event=self.events.append)

    def test_download_files_resets_selected_and_only_downloads_them(self):
        result = self.run_download(['2', '3'])
        self.assertEqual((self.store.get('2').status, self.store.get('2').last_error),
                         ('pending', '旧错误'))
        self.assertEqual((self.store.get('3').status, self.store.get('3').link_url,
                          self.store.get('3').link_expires), ('pending', None, None))
        self.assertEqual(self.store.get('1').status, 'pending')
        self.assertEqual(self.store.get('4').status, 'done')
        call = self.queue.call_args
        self.assertEqual(call.args, (self.client, self.store, 'abc', 'destination', self.config))
        self.assertEqual(call.kwargs['only_ids'], ['2', '3'])
        self.assertIs(call.kwargs['stop_event'], self.stop)
        self.assertIs(result, self.queue.return_value)
        self.assertEqual([e['phase'] for e in self.events], ['download'])

    def test_download_files_ignores_unknown_and_done_ids(self):
        self.run_download(['1', '4', 'missing'])
        self.assertEqual(self.queue.call_args.kwargs['only_ids'], ['1', '4', 'missing'])
        self.assertEqual(self.store.get('4').status, 'done')

    def test_download_files_empty_or_already_stopped(self):
        self.assertIsInstance(self.run_download([]), QueueResult)
        self.queue.assert_not_called()
        self.stop.set()
        self.assertTrue(self.run_download(['1']).stopped)
        self.queue.assert_not_called()

    def test_make_links_only_selected_into_export_subfolder(self):
        result = flows.make_links(self.client, self.store, 'abc', 'destination', self.config,
                                  ['2', '3'], stop_event=self.stop,
                                  on_event=self.events.append)
        self.assertIs(result, self.export.return_value)
        call = self.export.call_args
        self.assertEqual(call.args[:3], (self.client, self.store, 'abc'))
        self.assertEqual(call.args[3], Path('destination/_export'))
        self.assertIs(call.args[4], self.config)
        self.assertIsNone(call.kwargs['kinds'])
        self.assertEqual(call.kwargs['file_ids'], ['2', '3'])
        self.assertIs(call.kwargs['stop_event'], self.stop)
        self.assertEqual([e['phase'] for e in self.events], ['links'])

    def test_make_links_empty_or_already_stopped(self):
        self.assertEqual(flows.make_links(self.client, self.store, 'abc', 'dest', self.config,
                                          []).exported, 0)
        self.stop.set()
        self.assertTrue(flows.make_links(self.client, self.store, 'abc', 'dest', self.config,
                                         ['2'], stop_event=self.stop).stopped)
        self.export.assert_not_called()

    def test_check_folder_marks_missing_and_mismatched_failed(self):
        self.reconcile.return_value = ReconcileResult(
            completed=['4'], missing=['1'], mismatched=['2'], unverified=['3'])
        result = flows.check_folder(self.store, 'abc', 'external')
        self.assertIs(result, self.reconcile.return_value)
        self.reconcile.assert_called_once_with(self.store, 'abc', Path('external'), requeue=False)
        self.assertEqual((self.store.get('1').status, self.store.get('1').last_error),
                         ('failed', '外部下载未完成'))
        self.assertEqual((self.store.get('2').status, self.store.get('2').last_error),
                         ('failed', '文件不完整'))
        self.assertEqual(self.store.get('3').status, 'exported')
        self.assertEqual(self.store.get('4').status, 'done')

    def test_check_folder_keeps_completed_only_result(self):
        self.reconcile.return_value = ReconcileResult(completed=['1'])
        self.assertEqual(flows.check_folder(self.store, 'abc', 'ext').completed, ['1'])
        self.assertEqual(self.store.updates, [])

    def test_hand_back_only_returns_finished_away_states(self):
        self.store.records.append(record('5', 'e.jpg', 'image', 'skipped'))
        changed = flows.hand_back(self.store, 'abc', ['1', '2', '3', '4', '5', 'unknown'])
        self.assertEqual(changed, 3)
        for file_id in ('2', '3', '5'):
            row = self.store.get(file_id)
            self.assertEqual((row.status, row.link_url, row.link_expires), ('pending', None, None))
        self.assertEqual(self.store.get('1').status, 'pending')
        self.assertEqual(self.store.get('4').status, 'done')
        self.assertEqual(flows.hand_back(self.store, 'abc', []), 0)

    def test_album_dest_uses_safe_title_and_identity(self):
        album = Album('abc', '测:试/相册', 'url')
        for title, expected in (('../../', 'abc'), ('CON', '_CON'), ('测:试/相册', '测试相册')):
            album.title = title
            dest = flows.album_dest(self.config, album)
            self.assertIsInstance(dest, str)
            self.assertEqual(Path(dest), Path('downloads') / (expected + ' [abc]'))


class DesktopAndLinksTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def test_links_all_parts_latest_directory_comments_and_expiry(self):
        old = self.root / '_export/20261025-090000'
        latest = self.root / '_export/20261025-100000'
        old.mkdir(parents=True)
        latest.mkdir()
        (old / 'part-01-links.txt').write_text('https://cdn.test/old?ex=1', encoding='utf-8')
        (latest / 'part-01-links.txt').write_text(
            '# 最早过期时间：2030-01-01T10:00:00+08:00\n\n https://cdn.test/one \n # comment\n', encoding='utf-8-sig')
        (latest / 'part-02-links.txt').write_text('https://cdn.test/two?ex=2000000000\n', encoding='utf-8')
        (latest / 'part-03-links.txt.tmp').write_text('ignored', encoding='utf-8')
        result = flows.latest_links(self.root)
        self.assertEqual(result['links'], ['https://cdn.test/one', 'https://cdn.test/two?ex=2000000000'])
        self.assertEqual(result['export_dir'], str(latest))
        self.assertEqual(result['earliest_expiry'], datetime.fromisoformat('2030-01-01T10:00:00+08:00').timestamp())

    def test_missing_empty_unknown_or_invalid_expiry(self):
        self.assertEqual(flows.latest_links(self.root), {'links': [], 'earliest_expiry': None, 'export_dir': None})
        latest = self.root / '_export/20261025-100000'
        latest.mkdir(parents=True)
        self.assertEqual(flows.latest_links(self.root)['export_dir'], str(latest))
        for expiry in ('未知', 'invalid'):
            (latest / 'part-01-links.txt').write_text(
                '# 最早过期时间：' + expiry + '\nhttps://cdn.test/one?ex=nan&ex=bad\n', encoding='utf-8')
            self.assertIsNone(flows.latest_links(self.root)['earliest_expiry'])

    def test_open_creates_directory_and_uses_platform_launcher(self):
        path = self.root / 'not yet created'
        with patch.object(flows.sys, 'platform', 'win32'), patch.object(flows.os, 'startfile', create=True) as start:
            flows.open_folder(path)
            self.assertTrue(path.is_dir())
            start.assert_called_once_with(str(path.resolve()))
        for platform, command in (('darwin', 'open'), ('linux', 'xdg-open')):
            with patch.object(flows.sys, 'platform', platform), patch.object(flows.subprocess, 'run') as run:
                flows.open_folder(path)
                run.assert_called_once_with([command, str(path.resolve())], check=True)

    def test_picker_subprocess_initial_selection_cancel_and_failure(self):
        selected = str(self.root / '中文 folder')
        with patch.object(flows.subprocess, 'run', return_value=SimpleNamespace(stdout=json.dumps(selected))) as run:
            self.assertEqual(flows.pick_folder(selected), selected)
            args = run.call_args.args[0]
            self.assertEqual(args[:2], [sys.executable, '-c'])
            self.assertIn('askdirectory', args[2])
            self.assertEqual(args[3], selected)
            for output in ('null', '""', 'not json', '{}'):
                run.return_value.stdout = output
                self.assertIsNone(flows.pick_folder())
        for error in (OSError('Tk unavailable'), subprocess.CalledProcessError(1, 'python')):
            with patch.object(flows.subprocess, 'run', side_effect=error):
                self.assertIsNone(flows.pick_folder())


class StableDirectoryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.config = Config(download_root=str(self.root))
        self.album = Album('abc', '相册', 'url')
        self.store = MemoryStore([record('1', 'a.jpg', 'image')])
        self.bound = None
        self.store.get_album_dest = lambda ident: self.bound
        self.store.set_album_dest = self.bind

    def bind(self, ident, value):
        self.bound = value

    def test_same_titles_and_title_changes_are_isolated(self):
        first = flows.album_dest(self.config, self.album, self.store)
        self.assertEqual(Path(first).name, '相册 [abc]')
        self.album.title = '新名字'
        self.assertEqual(flows.album_dest(self.config, self.album, self.store), first)
        second = flows.album_dest(self.config, Album('def', '相册', 'url'))
        self.assertNotEqual(first, second)
        self.config.download_root = 'different'
        self.assertEqual(flows.album_dest(self.config, self.album, self.store), first)

    def test_empty_or_unrelated_legacy_directory_is_not_bound(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        (legacy / 'unrelated.jpg').touch()
        self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)).name, '相册 [abc]')

    def test_unowned_legacy_file_and_partial_do_not_bind(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        for name in ('a.jpg', 'a.jpg.part'):
            with self.subTest(name=name):
                self.bound = None
                path = legacy / name
                path.write_bytes(b'kept')
                self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)).name, '相册 [abc]')
                self.assertEqual(path.read_bytes(), b'kept')
                path.unlink()
        self.bound = None
        self.store.update_file('abc', '1', status='done', local_path=str(legacy / 'renamed.jpg'))
        self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)), legacy)

    def test_done_history_binds_but_local_path_elsewhere_does_not(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        self.store.update_file('abc', '1', status='done')
        self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)), legacy)
        self.bound = None
        self.store.update_file('abc', '1', local_path=str(self.root / 'elsewhere' / 'a.jpg'))
        (legacy / 'a.jpg').write_bytes(b'foreign')
        self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)).name, '相册 [abc]')

    def test_owned_partial_binds_and_other_album_partial_does_not(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        part = legacy / 'a.jpg.hash.bunkrdl.part'
        meta = part.with_name(part.name + '.json')
        part.write_bytes(b'kept')
        for owner, expected in (('other', '相册 [abc]'), ('abc', '相册')):
            self.bound = None
            meta.write_text(json.dumps({'album_id': owner, 'file_id': '1'}), encoding='utf-8')
            self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)).name, expected)

    def test_v3_partial_without_metadata_does_not_bind(self):
        # A matching partial name alone is not evidence of ownership.
        legacy = self.root / '相册'
        legacy.mkdir()
        token = '0123456789abcdef'
        for name in ('a.jpg', 'a[1].jpg'):
            with self.subTest(name=name):
                self.bound = None
                store = MemoryStore([record('1', name, 'image')])
                store.get_album_dest = lambda ident: self.bound
                store.set_album_dest = self.bind
                partial = legacy / f'{name}.{token}.bunkrdl.part'
                partial.write_bytes(b'kept')
                try:
                    self.assertEqual(Path(flows.album_dest(self.config, self.album, store)).name, '相册 [abc]')
                    self.assertEqual(partial.read_bytes(), b'kept')
                finally:
                    partial.unlink()

    def test_partials_of_other_files_or_other_suffixes_do_not_bind(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        token = '0123456789abcdef'
        for name in (f'other.jpg.{token}.bunkrdl.part',
                     f'a.jpg.{token}.bunkrdl.tmp',
                     'notes.txt'):
            (legacy / name).write_bytes(b'x')
        self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)).name, '相册 [abc]')
        (legacy / f'a.jpg.{token}.bunkrdl.part').write_bytes(b'x')
        self.bound = None
        self.assertEqual(Path(flows.album_dest(self.config, self.album, self.store)).name, '相册 [abc]')


class FakeStop:
    """Stand-in for threading.Event that records retry waits without sleeping."""

    def __init__(self, pause_on_wait=False):
        self.waits = []
        self.flag = False
        self.pause_on_wait = pause_on_wait

    def is_set(self):
        return self.flag

    def wait(self, seconds):
        self.waits.append(seconds)
        if self.pause_on_wait:
            self.flag = True
        return self.flag

    def set(self):
        self.flag = True

    def clear(self):
        self.flag = False


class SingleDownloadTests(unittest.TestCase):
    def setUp(self):
        from bunkrdl.worktypes import WorkContext
        self.store = MemoryStore([record('1', 'a.jpg', 'image', 'exported', link_url='old', link_expires=1)])
        self.ctx = WorkContext(Config(), self.store, Mock(), threading.Event(), Mock(),
                               threading.Lock(), 'destination')

    def run_once(self, effect=None):
        with patch('bunkrdl.downloader.download_file', side_effect=effect) as download:
            result = flows.download_once(self.ctx, 'abc', '1')
            download.assert_called_once()
            self.assertIs(download.call_args.kwargs['reserve_lock'], self.ctx.reserve_lock)
            self.assertIs(download.call_args.kwargs['stop_event'], self.ctx.stop_event)
            self.assertIs(download.call_args.kwargs['on_event'], self.ctx.on_event)
        return result

    def test_success_calls_downloader_once_and_clears_old_link(self):
        self.assertEqual(self.run_once().outcome, 'ok')
        self.assertIsNone(self.store.get('1').link_url)
        self.assertIsNone(self.store.get('1').link_expires)

    def run_attempts(self, effects, stop=None):
        if stop is not None:
            self.ctx.stop_event = stop
        with patch('bunkrdl.downloader.download_file', side_effect=effects) as download:
            result = flows.download_once(self.ctx, 'abc', '1')
        return result, download

    def test_error_classification_without_any_retry(self):
        from bunkrdl.models import (BunkrError, ChallengeRequired, Maintenance, NotFound,
                                    RateLimited)
        for error, kind in ((ChallengeRequired(), 'challenge'), (Maintenance(), 'maintenance'),
                            (NotFound(), 'not_found'), (RateLimited('HTTP 429', 45), 'rate_limited'),
                            (BunkrError('文件大小不符'), 'size_mismatch'), (OSError(), 'other')):
            with self.subTest(kind=kind):
                result = self.run_once(error)
                self.assertEqual((result.outcome, result.error_kind), ('failed', kind))
                self.assertEqual(result.retry_after, getattr(error, 'retry_after', None))
                self.assertEqual(self.store.get('1').last_error, result.message)
                self.assertEqual(self.store.get('1').status, 'failed')
                self.assertTrue(any('\u4e00' <= char <= '\u9fff' for char in result.message))

    def test_transient_error_is_retried_once_after_five_seconds(self):
        import requests
        from bunkrdl.models import TransientError
        for error in (requests.Timeout('超时'), TransientError('连接失败')):
            with self.subTest(error=type(error).__name__):
                self.store.update_file('abc', '1', status='failed', last_error='旧错误',
                                       link_url='old', link_expires=1)
                stop = FakeStop()
                result, download = self.run_attempts([error, None], stop)
                self.assertEqual(result.outcome, 'ok')
                self.assertEqual(download.call_count, 2)
                self.assertEqual(stop.waits, [5.0])
                self.assertIsNone(self.store.get('1').link_url)

    def test_transient_error_twice_fails_with_stage_host_and_no_secret(self):
        from bunkrdl.models import TransientError
        from bunkrdl.requestlog import tag_error
        error = TransientError('连接超时')
        tag_error(error, '文件体', 'https://mlk-b.cdn.cr/path/file?token={{SECRET_qp8vmn2x}}')
        stop = FakeStop()
        result, download = self.run_attempts([error, error], stop)
        self.assertEqual((result.outcome, result.error_kind), ('failed', 'transient'))
        self.assertEqual(download.call_count, 2)
        self.assertEqual(stop.waits, [5.0])
        message = self.store.get('1').last_error
        self.assertIn('连接超时', message)
        self.assertIn('阶段：文件体', message)
        self.assertIn('主机 mlk-b.cdn.cr', message)
        self.assertNotIn('SECRET', message)
        self.assertNotIn('https://', message)
        self.assertEqual(message, result.message)
        self.assertEqual(self.store.get('1').status, 'failed')

    def test_rate_limited_and_challenge_are_not_retried_here(self):
        from bunkrdl.models import ChallengeRequired, RateLimited
        result, download = self.run_attempts([RateLimited('HTTP 429', 120)])
        self.assertEqual((result.outcome, result.error_kind), ('failed', 'rate_limited'))
        self.assertEqual(download.call_count, 1)
        self.assertIn('建议等待 120 秒', result.message)
        result, download = self.run_attempts([ChallengeRequired('验证')])
        self.assertEqual((result.outcome, result.error_kind), ('failed', 'challenge'))
        self.assertEqual(download.call_count, 1)

    def test_pause_during_the_retry_wait_interrupts_immediately(self):
        from bunkrdl.models import TransientError
        stop = FakeStop(pause_on_wait=True)
        result, download = self.run_attempts([TransientError('连接超时')], stop)
        self.assertEqual(result.outcome, 'interrupted')
        self.assertEqual(download.call_count, 1)
        self.assertEqual(stop.waits, [5.0])
        self.assertEqual(self.store.get('1').status, 'pending')
        self.assertIsNone(self.store.get('1').last_error)
        self.assertIsNone(self.store.get('1').link_url)

    def test_stop_before_start_and_during_download(self):
        from bunkrdl.downloader import DownloadStopped
        self.ctx.stop_event.set()
        with patch('bunkrdl.downloader.download_file') as download:
            self.assertEqual(flows.download_once(self.ctx, 'abc', '1').outcome, 'interrupted')
            download.assert_not_called()
        self.ctx.stop_event.clear()
        self.assertEqual(self.run_once(DownloadStopped()).outcome, 'interrupted')
        self.assertEqual(self.store.get('1').status, 'pending')

    def test_unknown_file_does_not_start_download(self):
        with patch('bunkrdl.downloader.download_file') as download:
            result = flows.download_once(self.ctx, 'abc', 'absent')
        self.assertEqual((result.outcome, result.error_kind), ('failed', 'not_found'))
        download.assert_not_called()

    def test_pause_while_network_request_ends_with_error_is_interrupted(self):
        from bunkrdl.models import TransientError
        def interrupted(*args, **kwargs):
            self.ctx.stop_event.set()
            raise TransientError('连接超时')
        self.assertEqual(self.run_once(interrupted).outcome, 'interrupted')
        self.assertEqual(self.store.get('1').status, 'pending')
        self.assertIsNone(self.store.get('1').last_error)

    def test_missing_destination_binds_album_under_reservation_lock(self):
        self.ctx.dest_dir = ''
        album = Album('abc', '相册', 'url')
        self.store.get_album = Mock(return_value=album)

        def bind(config, selected, store):
            self.assertTrue(self.ctx.reserve_lock.locked())
            self.assertIs(selected, album)
            self.assertIs(store, self.store)
            return 'bound-folder'

        def download(*args, **kwargs):
            self.assertFalse(self.ctx.reserve_lock.locked())
            self.assertEqual(args[3], 'bound-folder')

        with patch.object(flows, 'album_dest', side_effect=bind) as destination:
            self.assertEqual(self.run_once(download).outcome, 'ok')
        destination.assert_called_once()

    def test_interrupted_result_preserves_partial_and_ownership_metadata(self):
        from bunkrdl.downloader import DownloadStopped
        with tempfile.TemporaryDirectory() as directory:
            self.ctx.dest_dir = directory
            partial = Path(directory) / 'a.jpg.owned.bunkrdl.part'
            metadata = partial.with_name(partial.name + '.json')
            owner = json.dumps({'album_id': 'abc', 'file_id': '1'})
            partial.write_bytes(b'partial bytes')
            metadata.write_text(owner, encoding='utf-8')
            self.assertEqual(self.run_once(DownloadStopped()).outcome, 'interrupted')
            self.assertEqual(partial.read_bytes(), b'partial bytes')
            self.assertEqual(metadata.read_text(encoding='utf-8'), owner)
            self.assertEqual(self.store.get('1').status, 'pending')


class RealDownloadFlowTests(unittest.TestCase):
    def setUp(self):
        from bunkrdl.models import AlbumFile, ResolvedLink
        from bunkrdl.store import Store
        from bunkrdl.worktypes import WorkContext
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.config = Config(download_root=str(self.root))
        self.store = Store(':memory:')
        self.addCleanup(self.store.close)
        self.payload = b'\xff\xd8\xff' + b'x' * 8997
        self.album = Album('abc', '相册', 'url', [
            AlbumFile('1', 'a.jpg', '1', 'url', size=len(self.payload), kind='image')])
        self.store.upsert_album(self.album)
        self.stop = threading.Event()
        self.client = SimpleNamespace(resolve=lambda record: ResolvedLink(
            'https://cdn.test/a.jpg', 'https://example.test/', 'a.jpg'))
        self.ctx = WorkContext(self.config, self.store, self.client, self.stop,
                               None, threading.Lock())

    def response(self, body, status=200, headers=None, before_chunk=None):
        import requests
        response = requests.Response()
        response.status_code = status
        response.url = 'https://cdn.test/a.jpg'
        response.headers.update({'Content-Length': str(len(body)), 'ETag': 'v1', **(headers or {})})
        response._content = body
        response._content_consumed = True
        def chunks(chunk_size):
            for offset in range(0, len(body), 8192):
                if before_chunk:
                    before_chunk()
                yield body[offset:offset + 8192]
        response.iter_content = chunks
        return response

    def test_pause_during_real_response_preserves_challenge_failure(self):
        response = self.response(b'x' * 8192, headers={'cf-mitigated': 'challenge'},
                                 before_chunk=self.stop.set)
        with patch('bunkrdl.downloader.requests.get', return_value=response) as request:
            result = flows.download_once(self.ctx, 'abc', '1')
        request.assert_called_once()
        self.assertEqual((result.outcome, result.error_kind), ('failed', 'challenge'))
        row = self.store.list_files('abc')[0]
        self.assertEqual(row.status, 'failed')
        self.assertEqual(row.last_error, result.message)
        self.assertEqual(row.attempts, 1)
        self.assertFalse(Path(row.local_path).exists())
        self.assertEqual(list(Path(row.local_path).parent.glob('*.part')), [])

    def test_legacy_local_path_pause_and_resume_keep_original_directory(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        target = legacy / 'a.jpg'
        self.store.update_file('abc', '1', local_path=str(target))
        self.ctx.on_event = lambda event: self.stop.set() if event['type'] == 'progress' else None
        with patch('bunkrdl.downloader.requests.get', return_value=self.response(self.payload)):
            result = flows.download_once(self.ctx, 'abc', '1')
        self.assertEqual((result.outcome, result.error_kind), ('interrupted', None))
        row = self.store.list_files('abc')[0]
        self.assertEqual(row.status, 'pending')
        self.assertIsNone(row.last_error)
        self.assertEqual(Path(row.local_path), target)
        partial, = legacy.glob('*.bunkrdl.part')
        metadata = partial.with_name(partial.name + '.json')
        self.assertEqual(partial.read_bytes(), self.payload[:8192])
        self.assertEqual(json.loads(metadata.read_text(encoding='utf-8'))['album_id'], 'abc')
        self.stop.clear()
        self.ctx.on_event = None
        # Re-discover the legacy directory from actual downloader ownership JSON.
        self.store.set_album_dest('abc', '')
        self.store.update_file('abc', '1', local_path=None)
        response = self.response(self.payload[8192:], 206,
                                 {'Content-Range': 'bytes 8192-8999/9000'})
        with patch('bunkrdl.downloader.requests.get', return_value=response) as request:
            result = flows.download_once(self.ctx, 'abc', '1')
        self.assertEqual(result.outcome, 'ok')
        self.assertEqual(request.call_args.kwargs['headers']['Range'], 'bytes=8192-')
        self.assertEqual(self.store.get_album_dest('abc'), str(legacy))
        self.assertEqual(self.store.list_files('abc')[0].status, 'done')
        self.assertEqual(target.read_bytes(), self.payload)
        self.assertFalse(partial.exists())
        self.assertFalse(metadata.exists())

    def test_unowned_legacy_file_is_preserved_and_download_uses_id_directory(self):
        legacy = self.root / '相册'
        legacy.mkdir()
        foreign = legacy / 'a.jpg'
        foreign.write_bytes(b'foreign')
        with patch('bunkrdl.downloader.requests.get', return_value=self.response(self.payload)):
            result = flows.download_once(self.ctx, 'abc', '1')
        self.assertEqual(result.outcome, 'ok')
        target = self.root / '相册 [abc]' / 'a.jpg'
        self.assertEqual(Path(self.store.list_files('abc')[0].local_path), target)
        self.assertEqual(target.read_bytes(), self.payload)
        self.assertEqual(foreign.read_bytes(), b'foreign')

    def test_transient_retry_still_passes_the_shared_request_gate(self):
        from bunkrdl.models import ResolvedLink, TransientError

        gates = []

        class Throttle:
            def acquire(self, stop_event=None):
                gates.append('gate')
                return True

        class Client:
            throttle = Throttle()

            def __init__(self):
                self.calls = 0

            def resolve(self, record):
                self.throttle.acquire()
                self.calls += 1
                if self.calls == 1:
                    error = TransientError('连接超时')
                    error.stage, error.host = '解析', 'dl.bunkr.cr'
                    raise error
                return ResolvedLink('https://cdn.test/a.jpg', 'https://example.test/', 'a.jpg')

        self.ctx.client = Client()
        self.ctx.stop_event = FakeStop()
        with patch('bunkrdl.downloader.requests.get', return_value=self.response(self.payload)):
            result = flows.download_once(self.ctx, 'abc', '1')
        self.assertEqual(result.outcome, 'ok')
        self.assertEqual(self.ctx.client.calls, 2)
        # Resolve and the body of each attempt went through the same gate.
        self.assertEqual(len(gates), 3)
        self.assertEqual(self.ctx.stop_event.waits, [5.0])
        self.assertEqual(self.store.list_files('abc')[0].status, 'done')

    def broken_body_response(self, error):
        """A 200 whose body stops after the headers and part of the payload."""
        import requests
        response = requests.Response()
        response.status_code = 200
        response.url = 'https://cdn.test/a.jpg'
        response.headers.update({'Content-Length': str(len(self.payload)), 'ETag': 'v1'})
        response._content_consumed = True

        def chunks(chunk_size):
            yield self.payload[:9000]
            raise error

        response.iter_content = chunks
        return response

    def test_body_break_is_retried_once_and_can_still_succeed(self):
        import requests
        from urllib3 import exceptions as urllib3_errors
        self.ctx.stop_event = FakeStop()
        error = requests.exceptions.ChunkedEncodingError(
            'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10))
        with patch('bunkrdl.downloader.requests.get',
                   side_effect=[self.broken_body_response(error),
                                self.response(self.payload)]) as request:
            result = flows.download_once(self.ctx, 'abc', '1')
        self.assertEqual(result.outcome, 'ok')
        self.assertEqual(request.call_count, 2)
        self.assertEqual(self.ctx.stop_event.waits, [5.0])
        row = self.store.list_files('abc')[0]
        self.assertEqual(row.status, 'done')
        self.assertEqual(Path(row.local_path).read_bytes(), self.payload)
        self.assertFalse(list(Path(row.local_path).parent.glob('*.part')))

    def test_body_break_twice_fails_as_transient_with_the_body_stage(self):
        import requests
        from urllib3 import exceptions as urllib3_errors
        self.ctx.stop_event = FakeStop()
        error = requests.exceptions.ChunkedEncodingError(
            'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10))
        with patch('bunkrdl.downloader.requests.get',
                   side_effect=[self.broken_body_response(error),
                                self.broken_body_response(error)]) as request:
            result = flows.download_once(self.ctx, 'abc', '1')
        self.assertEqual((result.outcome, result.error_kind), ('failed', 'transient'))
        self.assertEqual(request.call_count, 2)
        self.assertEqual(self.ctx.stop_event.waits, [5.0])
        self.assertIn('下载中断', result.message)
        self.assertIn('可重试', result.message)
        self.assertIn('阶段：文件体', result.message)
        self.assertIn('主机 cdn.test', result.message)
        row = self.store.list_files('abc')[0]
        self.assertEqual(row.status, 'failed')
        self.assertEqual(row.last_error, result.message)
        self.assertEqual(row.attempts, 2)
        self.assertFalse(Path(row.local_path).exists())



if __name__ == '__main__':
    unittest.main()
