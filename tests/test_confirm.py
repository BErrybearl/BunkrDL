import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import unittest
from pathlib import Path
from bunkrdl.confirm import mark_done
from bunkrdl.models import Album, AlbumFile
from bunkrdl.store import Store


class ConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.store = Store(':memory:')
        self.addCleanup(self.store.close)
        for album in ('a', 'b'):
            self.store.upsert_album(Album(album, album, 'url', [
                AlbumFile(str(i), str(i), str(i), 'url') for i in (1, 2)]))

    def test_existing_recorded_or_explicit_path_can_be_confirmed(self):
        path = self.folder / 'checked.bin'
        path.write_bytes(b'checked')
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                self.store.update_file('a', '1', status='failed', last_error='old',
                                       local_path=None if explicit else str(path))
                self.assertEqual(mark_done(self.store, 'a', ['1'],
                                          {'1': str(path)} if explicit else None), 1)
                row = self.store.list_files('a')[0]
                self.assertEqual(row.status, 'done')
                self.assertEqual(row.local_path, str(path))
                self.assertIsNone(row.last_error)

    def test_missing_recorded_or_explicit_path_is_rejected(self):
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                path = self.folder / 'missing.bin'
                self.store.update_file('a', '1', local_path=None if explicit else str(path))
                before = self.store.list_files('a')
                with self.assertRaisesRegex(ValueError, '文件不存在'):
                    mark_done(self.store, 'a', ['1'], {'1': str(path)} if explicit else None)
                self.assertEqual(self.store.list_files('a'), before)

    def test_empty_recorded_or_explicit_path_rejects_whole_batch(self):
        good = self.folder / 'good.bin'
        good.write_bytes(b'checked')
        empty = self.folder / 'empty.bin'
        empty.touch()
        # Manual confirmation rejects empty files even when the manifest says 0.
        self.store.upsert_album(Album('a', 'a', 'url', [
            AlbumFile('2', '2', '2', 'url', size=0)]))
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                self.store.update_file('a', '1', local_path=str(good))
                self.store.update_file('a', '2', local_path=None if explicit else str(empty))
                before = self.store.list_files('a')
                with self.assertRaisesRegex(ValueError, '0 字节.*不能标为完成'):
                    mark_done(self.store, 'a', ['1', '2'],
                              {'2': str(empty)} if explicit else None)
                self.assertEqual(self.store.list_files('a'), before)
                self.assertEqual(empty.read_bytes(), b'')

    def test_other_record_ownership_rejected_for_recorded_and_explicit_paths(self):
        path = self.folder / 'owned.bin'
        path.write_bytes(b'checked')
        for owner_album in ('a', 'b'):
            for explicit in (False, True):
                with self.subTest(owner_album=owner_album, explicit=explicit):
                    for album in ('a', 'b'):
                        self.store.update_file(album, '2', local_path=None)
                    self.store.update_file(owner_album, '2', local_path=str(path))
                    self.store.update_file('a', '1', local_path=None if explicit else str(path))
                    before = self.store.list_files('a')
                    with self.assertRaisesRegex(ValueError, '路径已归属其他记录'):
                        mark_done(self.store, 'a', ['1'],
                                  {'1': str(path)} if explicit else None)
                    self.assertEqual(self.store.list_files('a'), before)

    def test_same_explicit_path_cannot_complete_two_records(self):
        path = self.folder / 'shared.bin'
        path.write_bytes(b'checked')
        before = self.store.list_files('a')
        with self.assertRaisesRegex(ValueError, '路径已归属其他记录'):
            mark_done(self.store, 'a', ['1', '2'], {'1': str(path), '2': str(path)})
        self.assertEqual(self.store.list_files('a'), before)

    def test_missing_second_file_and_other_album_owner_leave_batch_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(':memory:')
            try:
                for album in ('a', 'b'):
                    store.upsert_album(Album(album, album, 'url', [
                        AlbumFile(str(i), str(i), str(i), 'url') for i in (1, 2)]))
                path = Path(directory) / 'exists.bin'
                path.write_bytes(b'manually checked')
                with self.assertRaises(ValueError):
                    mark_done(store, 'a', ['1', '2'], {'1': str(path)})
                self.assertEqual([r.status for r in store.list_files('a')], ['pending', 'pending'])
                store.update_file('b', '1', status='skipped', local_path=str(path))
                with self.assertRaises(ValueError):
                    mark_done(store, 'a', ['1'], {'1': str(path)})
                self.assertEqual(store.list_files('a')[0].status, 'pending')
            finally:
                store.close()

    def test_hidden_album_owner_blocks_recorded_and_explicit_confirmation(self):
        path = self.folder / 'hidden-owner.bin'
        path.write_bytes(b'checked')
        self.store.update_file('a', '1', status='done', local_path=str(path))
        self.store.set_album_hidden('a', True)
        self.assertEqual([a.album_id for a in self.store.list_albums()], ['b'])
        owner = self.store.list_files('a')
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                self.store.update_file('b', '1', local_path=None if explicit else str(path))
                before = self.store.list_files('b')
                with self.assertRaisesRegex(ValueError, '路径已归属其他记录'):
                    mark_done(self.store, 'b', ['1'], {'1': str(path)} if explicit else None)
                self.assertEqual(self.store.list_files('b'), before)
                self.assertEqual(self.store.list_files('a'), owner)
                self.assertEqual(path.read_bytes(), b'checked')
