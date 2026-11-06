"""P1 regressions: real temporary persistence, shared fakes, no remote requests."""

import importlib
import os
import sys
import tempfile
import threading
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import Mock, patch

# Existing fixture modules also set this variable at import time. Restore the
# caller's temporary home after importing them (both unittest invocation styles).
_test_home = os.environ.setdefault(
    'BUNKRDL_HOME', os.path.join(tempfile.gettempdir(), f'bunkrdl-p1-{os.getpid()}'))
_prefix = __package__ + '.' if __package__ else ''
_limiter = importlib.import_module(_prefix + 'test_limiter')
with patch.dict(sys.modules, {'test_limiter': _limiter}):
    _queue = importlib.import_module(_prefix + 'test_jobqueue')
_web = importlib.import_module(_prefix + 'test_web')
_export = importlib.import_module(_prefix + 'test_export')
os.environ['BUNKRDL_HOME'] = _test_home

from bunkrdl import downloader, export, flows, reconcile
from bunkrdl.config import Config
from bunkrdl.jobqueue import Coordinator, PAUSE_KEY
from bunkrdl.models import Album, AlbumFile, NotFound, STATUSES
from bunkrdl.store import Store
from bunkrdl.web import make_server
from bunkrdl.worktypes import Busy, FAILED, ItemResult


class TemporaryStoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        home = patch.dict(os.environ, {'BUNKRDL_HOME': str(self.root / 'home')})
        home.start()
        self.addCleanup(home.stop)
        # A stray production request fails immediately, even if a fake regresses.
        network = patch('requests.sessions.Session.request',
                        side_effect=AssertionError('P1 tests must stay offline'))
        network.start()
        self.addCleanup(network.stop)
        self.config = Config(state_db=str(self.root / 'state.db'),
                             download_root=str(self.root / 'downloads'),
                             image_concurrency=1, video_concurrency=1,
                             image_gap=0, video_gap_min=0, video_gap_max=0,
                             web_request_interval=0, quick_retries=0)
        self.store = Store(self.config.state_db)
        self.addCleanup(lambda: self.store.close())
        self.ops = _queue.Ops()

    def add_album(self, ids=('1', '2', '3')):
        album = Album('abc', '测试相册', 'https://bunkr.cr/a/abc', [
            AlbumFile(i, i + '.jpg', i, 'https://example.test/f/' + i,
                      size=10, kind='image') for i in ids])
        self.store.upsert_album(album)
        return album

    def new_coordinator(self):
        q = Coordinator(self.config, self.config.state_db, self.store,
                        lambda throttle: Mock(throttle=throttle), ops=self.ops)

        def close():
            if not q._closed:
                q.pause_all()
            self.ops.finish_all()
            q.close(2)

        self.addCleanup(close)
        return q

    def reopen(self, q):
        q.close(2)
        self.assertFalse(q.snapshot()['active'])
        self.store.close()
        self.store = Store(self.config.state_db)
        restarted = self.new_coordinator()
        restarted.start()
        return restarted

    def records(self):
        return {r.file_id: r for r in self.store.list_files('abc')}


class PausePersistenceTests(TemporaryStoreCase):
    RESTART_REASON = '程序已重启，点“继续”处理保留的文件'

    def assert_survives_restart_and_resume(self, q, expected_kind):
        before = q.snapshot()
        self.assertTrue(before['paused'])
        self.assertEqual(before['pause_kind'], expected_kind)
        self.assertTrue(before['pause_reason'])
        self.assertGreater(before['queued'], 0)
        restarted = self.reopen(q)
        after = restarted.snapshot()
        self.assertTrue(after['paused'])
        self.assertEqual((after['pause_kind'], after['pause_reason']),
                         (before['pause_kind'], before['pause_reason']))
        self.assertEqual(after['queued'], before['queued'])
        # Keep the dispatch thread out until close, so queued work demonstrably
        # exists at the second restart and no operation races the assertion.
        with restarted._wake:
            resumed = restarted.resume_all()
            self.assertFalse(resumed['paused'])
            self.assertIsNone(resumed['pause_kind'])
            self.assertIsNone(resumed['pause_reason'])
            self.assertFalse(self.store.get_meta(PAUSE_KEY))
            restarted.close(0)
        again = self.reopen(restarted).snapshot()
        self.assertEqual((again['pause_kind'], again['pause_reason']),
                         ('restart', self.RESTART_REASON))
        self.assertTrue(again['paused'])
        self.assertGreater(again['queued'], 0)

    def test_user_pause_survives_database_reopen(self):
        self.add_album()
        q = self.new_coordinator()
        q.enqueue('abc', ['1', '2'], 'download')
        q.pause_all('用户自定义暂停原因')
        self.assertEqual(q.snapshot()['pause_reason'], '用户自定义暂停原因')
        self.assert_survives_restart_and_resume(q, 'user')

    def test_consecutive_failure_pause_survives_database_reopen(self):
        self.add_album()
        self.config.fail_pause_after = 2
        q = self.new_coordinator()
        q.start()
        q.enqueue('abc', ['1', '2', '3'], 'download')
        for count, file_id in enumerate(('1', '2'), 1):
            self.ops.wait_calls(count)
            self.ops.finish('abc', file_id, ItemResult(FAILED, 'not_found', '文件不存在'))
        with q._wake:
            self.assertTrue(q._wake.wait_for(lambda: q.snapshot()['paused'], 2))
        self.assertEqual(q.snapshot()['consecutive_failures'], 2)
        self.assert_survives_restart_and_resume(q, 'failures')

    def test_challenge_pause_survives_database_reopen(self):
        self.add_album()
        q = self.new_coordinator()
        q.start()
        q.enqueue('abc', ['1', '2'], 'download')
        self.ops.wait_calls(1)
        self.ops.finish('abc', '1', ItemResult(FAILED, 'challenge', '需要人机验证'))
        with q._wake:
            self.assertTrue(q._wake.wait_for(lambda: q.snapshot()['paused'], 2))
        self.assert_survives_restart_and_resume(q, 'challenge')

    def test_rate_limit_circuit_pause_survives_database_reopen(self):
        self.add_album()
        q = self.new_coordinator()
        q.enqueue('abc', ['1', '2'], 'link')
        for _ in range(self.config.ratelimit_pause_after):
            q.register_rate_limit(retry_after=1)
        self.assert_survives_restart_and_resume(q, 'ratelimit')

    def test_queued_restart_without_saved_pause_uses_default_reason(self):
        self.add_album()
        q = self.new_coordinator()
        q.enqueue('abc', ['1'], 'download')
        self.assertIsNone(self.store.get_meta(PAUSE_KEY))
        snapshot = self.reopen(q).snapshot()
        self.assertEqual((snapshot['pause_kind'], snapshot['pause_reason']),
                         ('restart', self.RESTART_REASON))
        self.assertTrue(snapshot['paused'])
        self.assertEqual(snapshot['queued'], 1)


class WebP1Tests(TemporaryStoreCase):
    # Use the existing loopback HTTP fixture rather than another request client.
    request = _web.RealCoordinatorWebTests.request

    def setUp(self):
        super().setUp()
        self.client = Mock()
        resolver = patch('bunkrdl.resolver.BunkrClient', return_value=self.client)
        resolver.start()
        self.addCleanup(resolver.stop)
        self.server = make_server(self.config, self.store, port=0)
        self.q = self.server.coordinator
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': .01})
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()

    def block_album_resolution(self, album):
        entered, release = threading.Event(), threading.Event()

        def resolve(url):
            entered.set()
            if not release.wait(5):
                raise AssertionError('test did not release album parsing')
            return album

        self.client.get_album.side_effect = resolve
        result = []

        def request():
            try:
                result.append(self.request('/api/albums', {'url': album.url}))
            except Exception as error:
                result.append(error)

        thread = threading.Thread(target=request, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2), 'album parsing never started')
        return release, thread, result

    def bounded_call(self, fn, release):
        finished, results = threading.Event(), []

        def run():
            try:
                results.append(fn())
            except Exception as error:
                results.append(error)
            finally:
                finished.set()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        returned = finished.wait(1)
        if not returned:
            release.set()  # Unblock the server even when testing a lock regression.
        thread.join(2)
        self.assertTrue(returned, 'scheduler operation blocked behind get_album')
        self.assertNotIsInstance(results[0], BaseException)
        return results[0]

    def test_album_parsing_keeps_snapshot_and_pause_responsive_then_persists(self):
        album = Album('new', '新相册', 'https://bunkr.cr/a/new', [
            AlbumFile('n', '新图.jpg', 'n', '/f/n', size=10, kind='image')])
        release, thread, result = self.block_album_resolution(album)
        self.bounded_call(self.q.snapshot, release)
        self.assertTrue(self.bounded_call(self.q.pause_all, release)['paused'])
        self.assertFalse(release.is_set(), 'parsing must remain blocked during lock probes')
        release.set()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [(200, {'album_id': 'new', 'title': '新相册'})])
        self.assertEqual(self.store.get_album('new').title, '新相册')
        self.assertEqual([r.file_id for r in self.store.list_files('new')], ['n'])
        self.client.session.close.assert_called_once()

    def test_album_becoming_busy_during_parsing_raises_busy_at_persist(self):
        self.add_album()
        self.q.pause_all()
        before = [asdict(r) for r in self.store.list_files('abc')]
        changed = Album('abc', '不能写入的新标题', 'https://bunkr.cr/a/abc', [
            AlbumFile('1', '不能写入的新名.mp4', '1', '/f/1', size=99, kind='video')])
        release, thread, result = self.block_album_resolution(changed)
        queued = self.bounded_call(lambda: self.q.enqueue('abc', ['1'], 'download'), release)
        self.assertEqual(queued.added, 1)
        errors = []
        original = self.q.ensure_album_idle

        def ensure_idle(ident):
            try:
                return original(ident)
            except Busy as error:
                errors.append(error)
                raise

        with patch.object(self.q, 'ensure_album_idle', side_effect=ensure_idle):
            release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], Busy)
        self.assertEqual(result[0][0], 409)
        self.assertEqual(self.store.get_album('abc').title, '测试相册')
        self.assertEqual([asdict(r) for r in self.store.list_files('abc')], before)
        self.assertEqual(len(self.store.list_work_items()), 1)
        self.client.session.close.assert_called_once()

    def test_check_rejects_queued_album_before_any_directory_scan(self):
        self.add_album()
        self.q.pause_all()
        self.q.enqueue('abc', ['1'], 'download')
        folder = self.root / 'external'
        folder.mkdir()
        before = [asdict(r) for r in self.store.list_files('abc')]
        with patch.object(flows, 'prescan_folder') as prescan, \
                patch.object(flows, 'check_folder') as check, \
                patch.object(Path, 'iterdir', side_effect=AssertionError('must not scan')) as listing:
            status, data = self.request('/api/albums/abc/check', {'folder': str(folder)})
        self.assertEqual(status, 409)
        self.assertIn('排队', data['error'])
        prescan.assert_not_called()
        check.assert_not_called()
        listing.assert_not_called()
        self.assertEqual([asdict(r) for r in self.store.list_files('abc')], before)
        self.assertIsNone(self.store.get_meta('web:abc:folder'))

    def test_handback_done_returns_updated_count_and_keeps_file(self):
        self.add_album()
        saved = self.root / 'already.jpg'
        saved.write_bytes(b'already-downloaded')
        self.store.update_file('abc', '1', status='done', local_path=str(saved),
                               link_url='https://cdn.test/old', last_error='old error')
        status, data = self.request('/api/albums/abc/handback', {'file_ids': ['1']})
        self.assertEqual((status, data), (200, {'updated': 1}))
        row = self.records()['1']
        self.assertEqual((row.status, row.local_path, row.link_url, row.last_error),
                         ('pending', None, None, None))
        self.assertEqual(saved.read_bytes(), b'already-downloaded')

    def test_handback_queued_file_is_409_and_entire_selection_is_unchanged(self):
        self.add_album()
        self.store.update_file('abc', '2', status='done', local_path=str(self.root / '2.jpg'))
        self.q.pause_all()
        self.q.enqueue('abc', ['1'], 'download')
        before = [asdict(r) for r in self.store.list_files('abc')]
        status, data = self.request('/api/albums/abc/handback', {'file_ids': ['1', '2']})
        self.assertEqual(status, 409)
        self.assertIn('排队', data['error'])
        self.assertEqual([asdict(r) for r in self.store.list_files('abc')], before)
        self.assertEqual(len(self.store.list_work_items()), 1)


class FolderAndHandbackTests(TemporaryStoreCase):
    def prepare_folder(self, name):
        folder = self.root / name
        folder.mkdir()
        payload = b'\xff\xd8\xff' + b'\x00' * 7
        album = Album('abc', '测试相册', 'https://bunkr.cr/a/abc', [
            AlbumFile('1', '原名.jpg', '1', '/f/1', size=10, cdn_name='cdn.jpg', kind='image'),
            AlbumFile('2', 'existing.jpg', '2', '/f/2', size=10, kind='image'),
            AlbumFile('3', 'missing.jpg', '3', '/f/3', size=10, kind='image'),
            AlbumFile('4', 'short.jpg', '4', '/f/4', size=10, kind='image'),
            AlbumFile('5', 'unknown.bin', '5', '/f/5', kind='other')])
        self.store.upsert_album(album)
        for file_id in ('1', '2', '3', '4', '5'):
            self.store.update_file('abc', file_id, status='exported', last_error='old error')
        (folder / 'cdn.jpg').write_bytes(payload)
        (folder / 'existing.jpg').write_bytes(payload)
        (folder / 'short.jpg').write_bytes(payload[:4])
        (folder / 'unknown.bin').write_bytes(b'\x00\x01\x02\x03')
        return folder

    def test_prescan_is_dry_run_without_database_writes_or_renames(self):
        folder = self.prepare_folder('external')
        rows = [asdict(r) for r in self.store.list_files('abc')]
        disk = {p.name: p.read_bytes() for p in folder.iterdir()}
        writes = self.store._db.total_changes
        with patch.object(Path, 'rename', side_effect=AssertionError('dry run renamed')), \
                patch.object(reconcile.os, 'link', side_effect=AssertionError('dry run linked')):
            cache = flows.prescan_folder(self.store, 'abc', folder)
        self.assertTrue(cache)
        self.assertEqual(self.store._db.total_changes, writes)
        self.assertEqual([asdict(r) for r in self.store.list_files('abc')], rows)
        self.assertEqual({p.name: p.read_bytes() for p in folder.iterdir()}, disk)

    def test_cached_check_matches_uncached_check_including_cdn_rename(self):
        def run(name, cached):
            folder = self.prepare_folder(name)
            if cached:
                cache = flows.prescan_folder(self.store, 'abc', folder)
                with patch.object(reconcile, 'content_check',
                                  side_effect=AssertionError('cached content checked twice')):
                    result = flows.check_folder(self.store, 'abc', folder, checks=cache)
            else:
                result = flows.check_folder(self.store, 'abc', folder)
            self.assertEqual(result.completed, ['1', '2'])
            self.assertEqual(result.missing, ['3'])
            self.assertEqual(result.mismatched, ['4'])
            self.assertEqual(result.unverified, ['5'])
            self.assertEqual([(Path(a).name, Path(b).name) for a, b in result.renamed],
                             [('cdn.jpg', '原名.jpg')])
            self.assertFalse((folder / 'cdn.jpg').exists())
            self.assertEqual((folder / '原名.jpg').read_bytes(), b'\xff\xd8\xff' + b'\x00' * 7)
            result_data = asdict(result)
            result_data['renamed'] = [(Path(a).name, Path(b).name) for a, b in result.renamed]
            rows = []
            for record in self.store.list_files('abc'):
                row = asdict(record)
                row.pop('updated_at')
                row['local_path'] = Path(row['local_path']).name if row['local_path'] else None
                rows.append(row)
            return result_data, rows, {p.name: p.read_bytes() for p in folder.iterdir()}

        cached = run('cached', True)
        # Use a fresh state database as well as fresh bytes for the control run.
        self.store.close()
        self.store = Store(self.root / 'uncached.db')
        self.assertEqual(cached, run('uncached', False))

    def test_handback_resets_only_eligible_statuses_and_preserves_disk(self):
        self.add_album(STATUSES)
        disk = self.root / 'saved.jpg'
        disk.write_bytes(b'keep these bytes')
        for status in STATUSES:
            self.store.update_file('abc', status, status=status, local_path=str(disk),
                                   link_url='https://cdn.test/old', link_expires=123,
                                   last_error='old error')
        before = {key: asdict(row) for key, row in self.records().items()}
        self.assertEqual(flows.hand_back(self.store, 'abc', [*STATUSES, 'done', 'absent']), 4)
        for status, record in self.records().items():
            with self.subTest(status=status):
                if status in ('done', 'exported', 'failed', 'skipped'):
                    self.assertEqual((record.status, record.local_path, record.link_url,
                                      record.link_expires, record.last_error),
                                     ('pending', None, None, None, None))
                else:
                    self.assertEqual(asdict(record), before[status])
        self.assertEqual(disk.read_bytes(), b'keep these bytes')


class CrossInterfaceRetryTests(TemporaryStoreCase):
    def test_download_body_failure_remembers_download_after_link_resolves(self):
        self.add_album(('1',))
        self.store.remember_action('abc', '1', 'link')
        client = _export.FakeClient()
        with patch.object(downloader, '_body_response', side_effect=NotFound('下载内容已删除')) as body:
            with self.assertRaises(NotFound):
                downloader.download_file(client, self.store, self.records()['1'],
                                         self.root / 'downloads', self.config)
        body.assert_called_once()
        self.assertEqual(client.calls, ['1'])
        row = self.records()['1']
        self.assertEqual((row.status, row.link_url, row.link_expires), ('failed', None, None))
        self.assertEqual(self.store.get_meta('web:abc:last_action:1'), 'download')
        q = self.new_coordinator()
        self.assertEqual(q.retry_failed('abc').added, 1)
        self.assertEqual(self.store.list_work_items()[0].action, 'download')

    def test_export_and_download_failures_remember_actions_for_web_retry(self):
        self.add_album(('link-failure', 'download-failure'))
        # Seed the opposite actions to prove each failure overwrites stale history.
        self.store.remember_action('abc', 'link-failure', 'download')
        self.store.remember_action('abc', 'download-failure', 'link')
        client = _export.FakeClient({'link-failure': [NotFound('已删除')],
                                     'download-failure': [NotFound('已删除')]})
        result = export.export_links(client, self.store, 'abc', self.root / 'export',
                                     self.config, kinds=None, pacer=_export.FakePacer(),
                                     file_ids=['link-failure'])
        self.assertEqual((result.failed, result.exported), (1, 0))
        with self.assertRaises(NotFound):
            downloader.download_file(client, self.store, self.records()['download-failure'],
                                     self.root / 'downloads', self.config)
        for file_id, action in (('link-failure', 'link'), ('download-failure', 'download')):
            with self.subTest(file_id=file_id):
                self.assertEqual(self.records()[file_id].status, 'failed')
                self.assertEqual(self.store.get_meta(f'web:abc:last_action:{file_id}'), action)
        # Reopen SQLite before switching interfaces; in-memory fake state cannot
        # make this pass accidentally, and no workers are started by retry_failed.
        self.store.close()
        self.store = Store(self.config.state_db)
        q = self.new_coordinator()
        retried = q.retry_failed('abc')
        self.assertEqual(retried.added, 2)
        self.assertEqual({w.file_id: (w.action, w.state) for w in self.store.list_work_items()},
                         {'link-failure': ('link', 'queued'),
                          'download-failure': ('download', 'queued')})


if __name__ == '__main__':
    unittest.main()
