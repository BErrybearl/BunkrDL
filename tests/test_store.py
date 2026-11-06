import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import sqlite3
from contextlib import closing
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from bunkrdl.models import Album, AlbumFile, FileRecord
from bunkrdl.store import Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'nested' / 'state.db'
        self.store = Store(self.path)
        self.album = Album('a', '相册', 'https://example/a/a', [
            AlbumFile('1', '视频.mp4', 'slug', 'https://example/f/slug',
                      size=6, kind='video', uploaded='today'),
            AlbumFile('2', '图.jpg', 'two', 'https://example/f/two', kind='image'),
        ])
        self.store.upsert_album(self.album)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_roundtrip_filters_and_summaries(self):
        self.assertEqual(self.store.get_album('a'), self.album)
        self.assertIsNone(self.store.get_album('absent'))
        self.assertIsInstance(self.store.list_files('a')[0], FileRecord)
        self.assertEqual(len(self.store.list_files('a', status='pending', kinds=('video',))), 1)
        self.assertEqual(self.store.list_files('a', kinds=[]), [])
        self.assertEqual(self.store.list_files('a', status=[]), [])
        summary = self.store.list_albums()[0]
        self.assertEqual(summary.counts['pending'], 2)
        self.assertEqual(summary.counts['done'], 0)
        self.assertIsNotNone(summary.added_at)

    def test_old_database_migration_preserves_records(self):
        self.store.update_file('a', '1', status='done', attempts=4, local_path='old/file')
        self.store.set_meta('cooldown_until', 123)
        self.store.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript('''
                ALTER TABLE albums RENAME TO old_albums;
                CREATE TABLE albums (album_id TEXT PRIMARY KEY, title TEXT NOT NULL,
                    url TEXT NOT NULL, added_at REAL NOT NULL);
                INSERT INTO albums SELECT album_id, title, url, added_at FROM old_albums;
                DROP TABLE old_albums;
                DROP TABLE work_items;
            ''')
        self.store = Store(self.path)
        row = self.store.list_files('a')[0]
        self.assertEqual((row.status, row.attempts, row.local_path), ('done', 4, 'old/file'))
        self.assertEqual(self.store.get_meta('cooldown_until'), '123')
        self.assertEqual(self.store.get_album('a'), self.album)
        self.assertIsNone(self.store.get_album_dest('a'))
        self.assertEqual(self.store.list_work_items(), [])

    def test_hidden_readd_and_stable_destination(self):
        self.store.set_album_dest('a', 'old/folder')
        self.store.set_album_hidden('a', True)
        self.assertEqual(self.store.list_albums(), [])
        self.assertEqual(len(self.store.list_albums(include_hidden=True)), 1)
        self.assertEqual(self.store.get_album('a'), self.album)
        self.store.upsert_album(self.album)
        self.assertEqual(len(self.store.list_albums()), 1)
        self.assertEqual(self.store.get_album_dest('a'), 'old/folder')

    def test_queue_persistence_deduplication_replacement_and_recovery(self):
        rows = [('a', '1', 'download', 'video'), ('a', '2', 'link', 'image')]
        added = self.store.add_work_items(rows + [rows[0]])
        self.assertEqual([w.file_id for w in added], ['1', '2'])
        self.assertEqual(self.store.add_work_items([('a', '1', 'link', 'video')]), [])
        self.store.replace_work_action(added[0].work_id, 'link')
        self.store.update_work_item(added[0].work_id, state='running')
        with self.assertRaises(ValueError):
            self.store.replace_work_action(added[0].work_id, 'download')
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.list_work_items('running')[0].action, 'link')
        self.assertEqual(self.store.list_work_items([]), [])
        self.assertEqual(self.store.reset_running_work(), 1)
        self.assertEqual(self.store.reset_running_work(), 0)
        self.assertEqual([w.work_id for w in self.store.list_work_items()], [w.work_id for w in added])
        self.store.delete_work_items([w.work_id for w in added])
        again = self.store.add_work_items(rows)
        self.assertGreater(again[0].seq, added[-1].seq)
        self.assertTrue(all(w.created_at is not None for w in again))

    def test_queue_batch_rollback_and_field_validation(self):
        with self.assertRaises(ValueError):
            self.store.add_work_items([('a', '1', 'link', 'video'), ('a', '2', 'bad', 'image')])
        self.assertEqual(self.store.list_work_items(), [])
        work = self.store.add_work_items([('a', '1', 'link', 'video')])[0]
        for values in ({'state': 'done'}, {'action': 'bad'}, {'lane': 'other'}, {'bad': 1}):
            with self.assertRaises(ValueError):
                self.store.update_work_item(work.work_id, **values)
        with self.assertRaises(KeyError):
            self.store.update_work_item(999, state='running')

    def test_concurrent_queue_insert_has_one_owner(self):
        barrier = threading.Barrier(2)
        results, errors = [], []
        def insert():
            try:
                barrier.wait(timeout=3)
                results.append(self.store.add_work_items([('a', '1', 'link', 'video')]))
            except Exception as error:
                errors.append(error)
        threads = [threading.Thread(target=insert) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(sum(map(len, results)), 1)

    def test_upsert_preserves_all_state_and_added_at(self):
        changes = dict(status='done', attempts=3, last_error='旧错误', local_path='file',
                       link_url='signed', link_expires=123)
        self.store.update_file('a', '1', **changes)
        before = self.store.list_files('a')[0]
        added = self.store.list_albums()[0].added_at
        self.album.title = '新标题'
        self.album.files[0].name = 'new.mp4'
        self.album.files[0].size = 12
        self.album.files[0].cdn_name = 'cdn.mp4'
        self.store.upsert_album(self.album)
        after = self.store.list_files('a')[0]
        for key, value in changes.items():
            self.assertEqual(getattr(after, key), value)
        self.assertEqual(after.updated_at, before.updated_at)
        self.assertEqual(after.size, 12)
        self.assertEqual(after.cdn_name, 'cdn.mp4')
        self.assertEqual(self.store.list_albums()[0].added_at, added)

    def test_reopen_recovers_only_downloading(self):
        self.store.update_file('a', '1', status='downloading', attempts=2, local_path='saved')
        self.store.update_file('a', '2', status='exported')
        self.store.close()
        self.store = Store(self.path)
        self.store.recover_interrupted()
        one, two = self.store.list_files('a')
        self.assertEqual((one.status, one.attempts, one.local_path), ('pending', 2, 'saved'))
        self.assertEqual(two.status, 'exported')

    def test_validation_and_updated_at(self):
        old = self.store.list_files('a')[0].updated_at
        self.store.update_file('a', '1', status='failed', updated_at=0)
        self.assertGreaterEqual(self.store.list_files('a')[0].updated_at, old)
        with self.assertRaises(ValueError):
            self.store.update_file('a', '1', **{'status=0; DROP TABLE files': 1})
        with self.assertRaises(ValueError):
            self.store.update_file('a', '1', status='invalid')
        with self.assertRaises(KeyError):
            self.store.update_file('a', 'missing', status='done')

    def test_second_live_connection_never_recovers_active_download(self):
        self.store.update_file('a', '1', status='downloading')
        other = Store(self.path)
        try:
            self.assertEqual(other.list_files('a')[0].status, 'downloading')
            self.assertEqual(self.store.list_files('a')[0].status, 'downloading')
            other.set_meta('next_allowed_at', '123')
            self.assertEqual(self.store.get_meta('next_allowed_at'), '123')
            self.assertIsNone(other.get_meta('absent'))
        finally:
            other.close()

    def test_purge_album_deletes_its_records_and_meta_only(self):
        self.store.upsert_album(Album('ab', '另一本', 'url', [AlbumFile('9', '别的.jpg', 's9', '/f/s9')]))
        self.store.set_album_dest('a', 'dest/a')
        self.store.set_album_dest('ab', 'dest/ab')
        self.store.set_album_hidden('a', True)
        keys = ('web:a:folder', 'web:a:last_action:1', 'web:a:last_action:2',
                'web:ab:folder', 'web:ab:last_action:9', 'web:ratelimit:until',
                'web:ratelimit:streak', 'web:lane:image:setting', 'pacer_recorded_at',
                'backoff_until')
        for key in keys:
            self.store.set_meta(key, key)
        self.store.add_work_items([('a', '1', 'download', 'video'), ('ab', '9', 'link', 'image')])
        self.assertEqual(self.store.delete_album_records('a'), 2)
        # The album row, its directory binding, hidden flag and meta are gone.
        self.assertIsNone(self.store.get_album('a'))
        self.assertEqual(self.store.list_files('a'), [])
        self.assertIsNone(self.store.get_album_dest('a'))
        self.assertEqual([s.album_id for s in self.store.list_albums(include_hidden=True)], ['ab'])
        # Nothing of the neighbouring album (or the global keys) is touched,
        # even though 'ab' starts with the purged album id 'a'.
        self.assertEqual(self.store.get_album_dest('ab'), 'dest/ab')
        self.assertEqual([r.file_id for r in self.store.list_files('ab')], ['9'])
        self.assertEqual([w.file_id for w in self.store.list_work_items()], ['9'])
        for key in keys:
            with self.subTest(key=key):
                self.assertEqual(self.store.get_meta(key), None if key.startswith('web:a:') else key)
        self.assertEqual(self.store.delete_album_records('a'), 0)
        self.assertEqual(self.store.delete_album_records('missing'), 0)

    def test_delete_files_by_ids_and_statuses(self):
        self.store.update_file('a', '1', status='done')
        self.store.update_file('a', '2', status='failed', last_error='连接超时')
        self.store.set_meta('web:a:last_action:1', 'link')
        self.store.set_meta('web:a:last_action:2', 'download')
        self.store.set_meta('web:a:folder', 'external')
        self.store.add_work_items([('a', '1', 'download', 'video'), ('a', '2', 'link', 'image')])
        self.assertEqual(self.store.match_file_ids('a', statuses=['done']), ['1'])
        self.assertEqual(self.store.match_file_ids('a', file_ids=['1', '2', 'missing']), ['1', '2'])
        self.assertEqual(self.store.match_file_ids('a', file_ids=['1'], statuses=['failed']), [])
        self.assertEqual(self.store.delete_files('a', statuses=['done']), 1)
        self.assertEqual([r.file_id for r in self.store.list_files('a')], ['2'])
        # Per-file history goes with the record, the album-level hint stays.
        self.assertIsNone(self.store.get_meta('web:a:last_action:1'))
        self.assertEqual(self.store.get_meta('web:a:last_action:2'), 'download')
        self.assertEqual(self.store.get_meta('web:a:folder'), 'external')
        self.assertEqual([w.file_id for w in self.store.list_work_items()], ['2'])
        self.assertEqual(self.store.delete_files('a', file_ids=['2'], statuses=['failed']), 1)
        self.assertEqual(self.store.list_files('a'), [])
        self.assertIsNotNone(self.store.get_album('a'))
        # Unknown albums, files and empty selections are not an error.
        self.assertEqual(self.store.delete_files('a', file_ids=['missing']), 0)
        self.assertEqual(self.store.delete_files('missing', statuses=['done']), 0)
        self.assertEqual(self.store.delete_files('a', file_ids=[]), 0)
        self.assertEqual(self.store.delete_files('a', statuses=[]), 0)
        with self.assertRaises(ValueError):
            self.store.delete_files('a')
        with self.assertRaises(ValueError):
            self.store.delete_files('a', statuses=['nope'])

    def test_readd_after_purge_behaves_like_a_new_album(self):
        self.store.set_album_dest('a', 'old/folder')
        self.store.update_file('a', '1', status='done', local_path='old/folder/视频.mp4')
        self.assertEqual(self.store.delete_album_records('a'), 2)
        self.assertIsNone(self.store.get_album_dest('a'))
        self.store.upsert_album(self.album)
        self.assertEqual(self.store.get_album('a'), self.album)
        self.assertEqual({r.status for r in self.store.list_files('a')}, {'pending'})
        # No history is left to bind a directory: flows.album_dest re-decides.
        self.assertIsNone(self.store.get_album_dest('a'))
        self.assertEqual(len(self.store.list_albums()), 1)

    def test_composite_key_and_thread_access(self):
        self.store.upsert_album(Album('b', '另一本', 'url', [self.album.files[0]]))
        errors = []

        def worker(file_id):
            try:
                for index in range(30):
                    self.store.update_file('a', file_id, attempts=index)
                    self.store.list_albums()
                    self.store.get_album('a')
            except Exception as error:
                errors.append(error)

        threads = [threading.Thread(target=worker, args=(file_id,)) for file_id in ('1', '2')]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(self.store.list_files('b')[0].attempts, 0)
        self.assertEqual(self.store.list_files('a')[0].attempts, 29)


if __name__ == '__main__':
    unittest.main()


class BatchTransactionTests(unittest.TestCase):
    def test_invalid_second_update_rolls_back_first(self):
        from bunkrdl.models import Album, AlbumFile
        store = Store(':memory:')
        try:
            store.upsert_album(Album('a', 'title', 'url', [AlbumFile('1', 'a', 's', 'url')]))
            with self.assertRaises(KeyError):
                store.update_files([('a', '1', {'status': 'exported'}),
                                    ('a', 'missing', {'status': 'exported'})])
            self.assertEqual(store.list_files('a')[0].status, 'pending')
        finally:
            store.close()

    def test_purge_rolls_back_as_one_transaction(self):
        store = Store(':memory:')
        try:
            store.upsert_album(Album('a', 'title', 'url', [AlbumFile('1', 'a', 's', 'url')]))
            store.set_meta('web:a:folder', 'external')
            with patch.object(store, '_meta_keys', side_effect=RuntimeError('boom')):
                with self.assertRaises(RuntimeError):
                    store.delete_album_records('a')
            # The failed purge left the album, its files and its meta intact.
            self.assertIsNotNone(store.get_album('a'))
            self.assertEqual(len(store.list_files('a')), 1)
            self.assertEqual(store.get_meta('web:a:folder'), 'external')
        finally:
            store.close()
