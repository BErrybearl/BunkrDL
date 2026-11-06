"""Offline checks for collision suffixes added by external downloaders."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bunkrdl.reconcile import content_check, prescan, reconcile
from tests.test_reconcile import MemoryStore, record


class ReconcileDuplicateNameTests(unittest.TestCase):
    def folder(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        return Path(temp.name).resolve()

    def test_all_suffixes_and_numbers_under_cdn_and_original_names(self):
        content = b'\x00\x00\x00\x18ftypisom'
        for stem in ('cdn-1', '原名'):
            for number in range(1, 10):
                for suffix in (f' ({number})', f'({number})', f'_{number}'):
                    with self.subTest(stem=stem, suffix=suffix):
                        folder = self.folder()
                        source = folder / f'{stem}{suffix}.mp4'
                        source.write_bytes(content)
                        store = MemoryStore([record(size=None)])
                        result = reconcile(store, 'album', folder)
                        destination = folder / '原名.mp4'
                        self.assertEqual(result.completed, ['1'])
                        self.assertEqual(result.renamed, [(str(source), str(destination))])
                        self.assertFalse(source.exists())
                        self.assertEqual(destination.read_bytes(), content)
                        self.assertEqual(store.records[0].local_path, str(destination))

    def test_invalid_content_never_completes(self):
        cases = [(b'<html>error</html>', 18),
                 (b'{"error":"expired"}', 19),
                 (b'wrong magic', None), (b'wrong size', 3)]
        for content, size in cases:
            with self.subTest(content=content):
                folder = self.folder()
                source = folder / 'cdn-1 (1).mp4'
                source.write_bytes(content)
                store = MemoryStore([record(size=size)])
                result = reconcile(store, 'album', folder)
                self.assertEqual(result.completed, [])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(result.renamed, [])
                self.assertEqual(store.records[0].status, 'pending')
                self.assertEqual(source.read_bytes(), content)

    def test_claimed_variant_cannot_be_used(self):
        folder = self.folder()
        source = folder / 'cdn-1_1.mp4'
        source.write_bytes(b'abc')
        owner = record('owner', 'owner.mp4', album_id='other', status='done',
                       local_path=str(source))
        store = MemoryStore([record(), owner])
        result = reconcile(store, 'album', folder)
        self.assertEqual(result.completed, [])
        self.assertEqual(result.mismatched, ['1'])
        self.assertEqual(result.renamed, [])
        self.assertEqual(store.records[1], owner)
        self.assertEqual(source.read_bytes(), b'abc')

    def test_shared_variant_is_first_come_first_served_in_dry_run(self):
        folder = self.folder()
        source = folder / 'shared(1).mp4'
        source.write_bytes(b'abc')
        rows = [record('1', 'first.mp4', cdn_name='shared.mp4'),
                record('2', 'second.mp4', cdn_name='shared.mp4')]
        store = MemoryStore(rows)
        checks = {}
        result = reconcile(store, 'album', folder, dry_run=True, checks=checks)
        self.assertEqual(result.mismatched, ['2'])
        self.assertEqual(result.missing, [])
        self.assertEqual({key[1] for key in checks}, {'1'})
        self.assertEqual(store.records, rows)
        self.assertEqual(source.read_bytes(), b'abc')

    def test_each_exact_candidate_takes_priority(self):
        for exact_name in ('cdn-1.mp4', '原名.mp4', 'previous.mp4'):
            with self.subTest(exact_name=exact_name):
                folder = self.folder()
                exact = folder / exact_name
                exact.write_bytes(b'abc')
                variant = folder / 'cdn-1 (1).mp4'
                variant.write_bytes(b'new')
                store = MemoryStore([record(local_path=str(folder / 'previous.mp4'))])
                result = reconcile(store, 'album', folder)
                self.assertEqual(result.completed, ['1'])
                self.assertEqual(Path(store.records[0].local_path).read_bytes(), b'abc')
                self.assertEqual(variant.read_bytes(), b'new')

    def test_unusable_exact_file_still_blocks_variants(self):
        for claimed in (False, True):
            with self.subTest(claimed=claimed):
                folder = self.folder()
                exact = folder / 'cdn-1.mp4'
                exact.write_bytes(b'abc' if claimed else b'<html>error</html>')
                variant = folder / 'cdn-1_1.mp4'
                variant.write_bytes(b'abc')
                rows = [record()]
                if claimed:
                    rows.append(record('owner', 'owner.mp4', album_id='other',
                                       status='done', local_path=str(exact)))
                store = MemoryStore(rows)
                result = reconcile(store, 'album', folder)
                self.assertEqual(result.completed, [])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(result.renamed, [])
                self.assertEqual(variant.read_bytes(), b'abc')

    def test_previous_local_path_does_not_generate_variants(self):
        folder = self.folder()
        source = folder / 'previous (1).mp4'
        source.write_bytes(b'abc')
        store = MemoryStore([record(local_path=str(folder / 'previous.mp4'))])
        result = reconcile(store, 'album', folder)
        self.assertEqual(result.completed, [])
        self.assertEqual(result.missing, ['1'])
        self.assertEqual(source.read_bytes(), b'abc')

    def test_suffix_ten_is_not_a_candidate(self):
        folder = self.folder()
        for name in ('cdn-1 (10).mp4', '原名(10).mp4', '原名_10.mp4'):
            (folder / name).write_bytes(b'abc')
        store = MemoryStore([record()])
        result = reconcile(store, 'album', folder)
        self.assertEqual(result.completed, [])
        self.assertEqual(result.missing, ['1'])

    def test_temporary_variants_and_companions_are_rejected(self):
        for suffix in ('.xltd', '.td', '.part', '.aria2', '.crdownload', '.tmp'):
            for payload in (False, True):
                with self.subTest(suffix=suffix, payload=payload):
                    folder = self.folder()
                    source = folder / 'cdn-1 (1).mp4'
                    if payload:
                        source.write_bytes(b'abc')
                    temporary = folder / (source.name + suffix)
                    temporary.write_bytes(b'abc')
                    store = MemoryStore([record()])
                    result = reconcile(store, 'album', folder)
                    self.assertEqual(result.completed, [])
                    self.assertEqual(result.mismatched, ['1'])
                    self.assertEqual(result.renamed, [])
                    self.assertTrue(temporary.exists())

    def test_variant_with_temporary_extension_is_rejected(self):
        folder = self.folder()
        source = folder / 'download (1).part'
        source.write_bytes(b'abc')
        store = MemoryStore([record(name='download.part', cdn_name=None)])
        result = reconcile(store, 'album', folder)
        self.assertEqual(result.completed, [])
        self.assertEqual(result.mismatched, ['1'])
        self.assertTrue(source.exists())

    def test_destination_uses_existing_collision_and_claim_rules(self):
        folder = self.folder()
        source = folder / 'cdn-1_1.mp4'
        source.write_bytes(b'abc')
        (folder / '原名.mp4').mkdir()
        occupied = folder / '原名 (2).mp4'
        occupied.write_bytes(b'keep')
        reserved = folder / '原名 (3).mp4'
        owner = record('owner', 'owner.mp4', album_id='other', status='done',
                       local_path=str(reserved))
        store = MemoryStore([record(), owner])
        result = reconcile(store, 'album', folder)
        destination = folder / '原名 (4).mp4'
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.renamed, [(str(source), str(destination))])
        self.assertEqual(destination.read_bytes(), b'abc')
        self.assertEqual(occupied.read_bytes(), b'keep')
        self.assertFalse(reserved.exists())

    def test_dry_run_preserves_files_and_store_and_reuses_prescan_cache(self):
        folder = self.folder()
        source = folder / 'cdn-1 (1).mp4'
        source.write_bytes(b'abc')
        rows = [record()]
        store = MemoryStore(rows)
        with patch('bunkrdl.reconcile.content_check', wraps=content_check) as check:
            checks = prescan(store, 'album', folder)
            self.assertEqual(check.call_count, 1)
            self.assertEqual(store.records, rows)
            self.assertEqual(list(folder.iterdir()), [source])
            self.assertEqual(source.read_bytes(), b'abc')
            dry_result = reconcile(store, 'album', folder, dry_run=True, checks=checks)
            self.assertEqual(dry_result.renamed, [])
            self.assertEqual(store.records, rows)
            self.assertTrue(source.exists())
            result = reconcile(store, 'album', folder, checks=checks)
            self.assertEqual(check.call_count, 1)
        self.assertEqual(result.completed, ['1'])
        self.assertEqual((folder / '原名.mp4').read_bytes(), b'abc')


if __name__ == '__main__':
    unittest.main()
