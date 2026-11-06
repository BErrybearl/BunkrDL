"""Offline filesystem checks for external-download reconciliation."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import unittest
from dataclasses import replace
from types import SimpleNamespace
from pathlib import Path

from bunkrdl.models import FileRecord
from bunkrdl.reconcile import reconcile


def record(file_id='1', name='原名.mp4', **fields):
    values = dict(album_id='album', status='exported', size=3,
                  cdn_name='cdn-' + file_id + '.mp4', link_url='old', link_expires=1)
    values.update(fields)
    return FileRecord(file_id, name, file_id, 'https://example.test/f/' + file_id,
                      kind='video', **values)


class MemoryStore:
    def __init__(self, records):
        self.records = [replace(r) for r in records]

    def list_files(self, album_id, status=None, kinds=None):
        return [replace(r) for r in self.records if r.album_id == album_id
                and (status is None or r.status == status)
                and (kinds is None or r.kind in kinds)]

    def list_albums(self, include_hidden=False):
        return [SimpleNamespace(album_id=i) for i in dict.fromkeys(r.album_id for r in self.records)]

    def update_file(self, album_id, file_id, **fields):
        row = next(r for r in self.records if r.album_id == album_id and r.file_id == file_id)
        for key, value in fields.items():
            setattr(row, key, value)


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name).resolve()

    def write(self, name, content=b'abc'):
        path = self.folder / name
        path.write_bytes(content)
        return path

    def run_reconcile(self, records, requeue=True):
        store = MemoryStore(records)
        return reconcile(store, 'album', self.folder, requeue), store

    def test_cdn_file_renamed_and_marked_done(self):
        old = self.write('cdn-1.mp4')
        result, store = self.run_reconcile([record()])
        new = self.folder / '原名.mp4'
        self.assertEqual(result.renamed, [(str(old), str(new))])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(new.read_bytes(), b'abc')
        self.assertFalse(old.exists())
        self.assertEqual(store.records[0].status, 'done')
        self.assertEqual(store.records[0].local_path, str(new))

    def test_original_file_already_present(self):
        path = self.write('原名.mp4')
        result, store = self.run_reconcile([record()])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.renamed, [])
        self.assertEqual(store.records[0].local_path, str(path))

    def test_unknown_size_and_zero_size(self):
        self.write('cdn-1.mp4', b'\x00\x00\x00\x18ftypisom')
        self.write('cdn-2.mp4', b'')
        result, _ = self.run_reconcile([record(size=None), record('2', '空.mp4', size=0)])
        self.assertEqual(result.completed, ['1', '2'])

    def test_empty_files_require_explicit_zero_size(self):
        for name in ('empty.mp4', 'empty.bin'):
            for size in (None, 0, 3):
                with self.subTest(name=name, size=size):
                    path = self.write(name, b'')
                    result, store = self.run_reconcile([
                        record(name=name, cdn_name=None, size=size)])
                    self.assertEqual(result.unverified, [])
                    self.assertEqual(result.completed, ['1'] if size == 0 else [])
                    self.assertEqual(result.mismatched, [] if size == 0 else ['1'])
                    self.assertEqual(result.requeued, [] if size == 0 else ['1'])
                    self.assertEqual(store.records[0].status, 'done' if size == 0 else 'pending')
                    self.assertEqual(path.read_bytes(), b'')
                    path.unlink()

    def test_known_size_maintenance_checked_through_first_4k(self):
        for offset in (64, 4096 - len(b'maintenance')):
            with self.subTest(offset=offset):
                content = b'\x00\x00\x00\x18ftypisom' + b'x' * (offset - 12) + b'MAINTENANCE'
                path = self.write('cdn-1.mp4', content)
                result, _ = self.run_reconcile([record(size=len(content))])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(result.completed, [])
                self.assertEqual(path.read_bytes(), content)

    def test_known_size_bom_html_rejected_for_unknown_extension(self):
        content = b'\xef\xbb\xbf \n<HTML><body>placeholder</body></html>'
        self.write('page.bin', content)
        row = replace(record(name='page.bin', cdn_name=None, size=len(content)), kind='other')
        result, _ = self.run_reconcile([row])
        self.assertEqual(result.mismatched, ['1'])
        self.assertEqual(result.completed, [])

    def test_directly_matched_temporary_file_never_completes(self):
        for suffix in ('.xltd', '.td', '.part', '.aria2', '.crdownload', '.tmp'):
            for size in (None, 3):
                with self.subTest(suffix=suffix, size=size):
                    path = self.write('download' + suffix)
                    result, _ = self.run_reconcile([
                        record(name=path.name, cdn_name=path.name, size=size)])
                    self.assertEqual(result.mismatched, ['1'])
                    self.assertEqual(result.completed, [])
                    self.assertEqual(result.unverified, [])
                    self.assertEqual(result.renamed, [])
                    self.assertEqual(path.read_bytes(), b'abc')
                    path.unlink()

    def test_known_size_other_kind_error_pages_never_complete(self):
        for content in (b'{"error":"signature expired"}', b'Error: file not found',
                        b'\xef\xbb\xbf{"message":"Access denied"}'):
            with self.subTest(content=content):
                self.write('data.bin', content)
                row = replace(record(name='data.bin', cdn_name=None, size=len(content)), kind='other')
                result, _ = self.run_reconcile([row])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(result.completed, [])

    def test_known_size_normal_json_can_complete(self):
        for content in (b'{"title":"example"}', b'{"error":false,"items":[]}',
                        b'{"title":"error analysis"}'):
            with self.subTest(content=content):
                self.write('data.json', content)
                row = replace(record(name='data.json', cdn_name=None, size=len(content)), kind='other')
                result, _ = self.run_reconcile([row])
                self.assertEqual(result.completed, ['1'])

    def test_remaining_known_video_extensions_check_magic(self):
        cases = {'.wmv': bytes.fromhex('3026b2758e66cf11a6d900aa0062ce6c') + b'data',
                 '.flv': b'FLV\x01data', '.3gp': b'xxxxftyp3gp4',
                 '.mpg': b'\x00\x00\x01\xbadata', '.mpeg': b'\x00\x00\x01\xb3data',
                 '.m2ts': b'xxxxG' + b'0' * 191 + b'G' + b'0' * 191 + b'G'}
        for ext, content in cases.items():
            with self.subTest(ext=ext):
                path = self.write('video' + ext, content)
                row = record(name=path.name, cdn_name=None, size=None)
                result, _ = self.run_reconcile([row])
                self.assertEqual(result.completed, ['1'])
                path.write_bytes(b'wrong signature')
                result, _ = self.run_reconcile([row])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(result.unverified, [])
                path.unlink()

    def test_done_path_cannot_be_claimed_by_earlier_record(self):
        path = self.write('原名.mp4')
        rows = [record('1', '原?名.mp4'),
                record('2', '原名.mp4', status='done', local_path=str(path))]
        result, store = self.run_reconcile(rows)
        self.assertEqual(result.completed, [])
        self.assertEqual(result.mismatched, ['1'])
        self.assertEqual(store.records[1], rows[1])
        self.assertEqual(path.read_bytes(), b'abc')

    def test_done_cdn_path_cannot_be_claimed_under_different_name(self):
        path = self.write('cdn-1.mp4')
        rows = [record('1', 'new.mp4'), record('2', 'owner.mp4', status='done', local_path=str(path))]
        result, _ = self.run_reconcile(rows)
        self.assertEqual(result.completed, [])
        self.assertTrue(path.exists())
        self.assertFalse((self.folder / 'new.mp4').exists())

    def test_unknown_size_rejects_empty_html_and_wrong_magic(self):
        for content in (b'', b'<!DOCTYPE html><html>maintenance', b' \n<HTML>verify',
                        b'\xef\xbb\xbf<html>error', b'GIF89aimage', b'not a video'):
            with self.subTest(content=content):
                self.write('cdn-1.mp4', content)
                result, store = self.run_reconcile([record(size=None, attempts=8)])
                self.assertEqual(result.completed, [])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(result.requeued, ['1'])
                self.assertEqual(store.records[0].attempts, 8)
                self.assertIsNone(store.records[0].link_url)
                self.assertIsNone(store.records[0].link_expires)
                self.assertEqual((self.folder / 'cdn-1.mp4').read_bytes(), content)

    def test_known_size_rejects_html_even_when_size_matches(self):
        content = b'<!DOCTYPE html>maintenance'
        self.write('cdn-1.mp4', content)
        result, _ = self.run_reconcile([record(size=len(content))])
        self.assertEqual(result.mismatched, ['1'])
        self.assertEqual(result.completed, [])

    def test_common_unknown_size_media_magic(self):
        cases = {'.jpg': b'\xff\xd8\xffdata', '.jpeg': b'\xff\xd8\xffdata',
                 '.png': b'\x89PNG\r\n\x1a\ndata', '.gif': b'GIF89adata',
                 '.webp': b'RIFFxxxxWEBPdata', '.mp4': b'\x00\x00\x00\x18ftypisom',
                 '.mov': b'\x00\x00\x00\x18ftypqt  ', '.webm': b'\x1a\x45\xdf\xa3data',
                 '.mkv': b'\x1a\x45\xdf\xa3data', '.jfif': b'\xff\xd8\xffdata'}
        for ext, content in cases.items():
            with self.subTest(ext=ext):
                source = self.write('cdn-1.mp4', content)
                result, store = self.run_reconcile([record(name='media' + ext, size=None)])
                self.assertEqual(result.completed, ['1'])
                self.assertEqual(result.unverified, [])
                Path(store.records[0].local_path).unlink()
                self.assertFalse(source.exists())
                self.write('cdn-1.mp4', b'wrong magic')
                result, _ = self.run_reconcile([record(name='media' + ext, size=None)])
                self.assertEqual(result.mismatched, ['1'])
                (self.folder / 'cdn-1.mp4').unlink()

    def test_unknown_format_reported_separately_as_unverified(self):
        self.write('cdn-1.mp4', b'opaque data')
        result, store = self.run_reconcile([record(name='archive.bin', size=None)])
        self.assertEqual(result.completed, [])
        self.assertEqual(result.unverified, ['1'])
        self.assertEqual(result.mismatched, [])
        self.assertEqual(store.records[0].status, 'exported')
        self.assertEqual(result.requeued, [])

    def test_all_album_and_status_owners_protect_source(self):
        for album in ('album', 'other'):
            for status in ('done', 'skipped', 'failed', 'pending', 'exported', 'downloading'):
                with self.subTest(album=album, status=status):
                    path = self.write('cdn-1.mp4')
                    rows = [record(), record('owner', path.name, album_id=album,
                                             status=status, local_path=str(path), cdn_name=None)]
                    result, _ = self.run_reconcile(rows)
                    self.assertNotIn('1', result.completed)
                    self.assertTrue(path.exists())
                    self.assertFalse((self.folder / '原名.mp4').exists())

    def test_missing_owned_destination_is_reserved_across_albums(self):
        self.write('cdn-1.mp4')
        reserved = self.folder / '原名.mp4'
        result, store = self.run_reconcile([record(), record('owner', 'owner.mp4',
            album_id='other', status='skipped', local_path=str(reserved))])
        self.assertEqual(result.completed, ['1'])
        self.assertFalse(reserved.exists())
        self.assertEqual(Path(store.records[0].local_path).name, '原名 (2).mp4')

    def test_unknown_size_avi_maintenance_or_html_never_completes(self):
        for data in (b'<h1>Server under maintenance</h1>',
                     b'RIFFxxxxAVI ' + b' ' * 2000 + b'MAINTENANCE'):
            with self.subTest(data=data[:30]):
                self.write('cdn-1.mp4', data)
                result, store = self.run_reconcile([record(name='movie.avi', size=None)])
                self.assertEqual(result.completed, [])
                self.assertEqual(result.mismatched, ['1'])
                self.assertNotEqual(store.records[0].status, 'done')

    def test_avi_markup_fragment_is_payload_not_a_page(self):
        # Only an HTML *prefix* marks a page; tags inside a payload do not.
        data = b'RIFFxxxxAVI ' + b' ' * 2000 + b'<div>Error</div>'
        self.write('cdn-1.mp4', data)
        result, _ = self.run_reconcile([record(name='movie.avi', size=None)])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.mismatched, [])

    def test_svg_with_matching_size_completes(self):
        content = b'<svg xmlns="http://www.w3.org/2000/svg"><rect width="1"/><text>Error</text></svg>'
        source = self.write('cdn-1.svg', content)
        result, store = self.run_reconcile(
            [replace(record(name='图.svg', cdn_name='cdn-1.svg', size=len(content)), kind='other')])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.mismatched, [])
        self.assertEqual(store.records[0].status, 'done')
        self.assertFalse(source.exists())
        self.assertEqual((self.folder / '图.svg').read_bytes(), content)

    def test_binary_with_markup_fragment_and_matching_size_completes(self):
        content = b'\x00\x01<item>payload</item>' + b'\xff' * 64
        self.write('cdn-1.mp4', content)
        result, store = self.run_reconcile([record(name='data.bin', size=len(content))])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.unverified, [])
        self.assertEqual(result.mismatched, [])
        self.assertEqual(store.records[0].status, 'done')

    def test_json_or_text_error_page_with_matching_size_never_completes(self):
        pages = (b'{"error":"signature expired"}', b'  {"error": "not found"}',
                 b'Error: file not found\n', b'Access denied',
                 b'\xef\xbb\xbf{"message":"expired"}')
        for content in pages:
            with self.subTest(content=content):
                self.write('cdn-1.mp4', content)
                result, store = self.run_reconcile([record(size=len(content))])
                self.assertEqual(result.completed, [])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(store.records[0].status, 'pending')

    def test_gif_with_matching_size_still_completes(self):
        content = (b'GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01'
                   b'\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;')
        self.write('cdn-1.mp4', content)
        result, store = self.run_reconcile([record(size=len(content))])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.mismatched, [])
        self.assertEqual(store.records[0].status, 'done')

    def test_html_prefix_with_matching_size_never_completes(self):
        for content in (b'<!doctype html><html><body>error', b' \r\n<HTML><body>error',
                        b'\t<!DOCTYPE HTML><p>held'):
            with self.subTest(content=content):
                self.write('cdn-1.mp4', content)
                result, store = self.run_reconcile([record(size=len(content))])
                self.assertEqual(result.completed, [])
                self.assertEqual(result.mismatched, ['1'])
                self.assertEqual(store.records[0].status, 'pending')

    def test_unverified_preserves_each_eligible_state_and_can_be_rechecked(self):
        for status in ('pending', 'failed', 'exported'):
            path = self.write('unknown.bin', b'opaque')
            row = record(name=path.name, cdn_name=None, size=None, status=status, attempts=8)
            result, store = self.run_reconcile([row])
            self.assertEqual((result.completed, result.requeued, result.unverified), ([], [], ['1']))
            self.assertEqual(store.records[0].status, status)
            self.assertEqual(store.records[0].last_error, '内容无法确认，请打开检查后手动标为完成')
            self.assertEqual(store.records[0].attempts, 8)
            self.assertEqual(store.records[0].link_url, row.link_url)
            self.assertEqual(store.records[0].link_expires, row.link_expires)
            self.assertEqual(reconcile(store, 'album', self.folder).unverified, ['1'])
            path.unlink()

    def test_additional_media_signatures_and_reject_wrong_magic(self):
        cases = {'.avi': b'RIFFxxxxAVI data', '.bmp': b'BMdata', '.m4v': b'xxxxftypM4V ',
                 '.ts': b'G' + b'0' * 187 + b'G' + b'0' * 187 + b'G',
                 '.heic': b'xxxxftypheic', '.avif': b'xxxxftypavif',
                 '.tiff': b'II\x2a\x00data', '.tif': b'MM\x00\x2adata'}
        for ext, data in cases.items():
            with self.subTest(ext=ext):
                path = self.write('media' + ext, data)
                result, _ = self.run_reconcile([record(name=path.name, cdn_name=None, size=None)])
                self.assertEqual(result.completed, ['1'])
                path.write_bytes(b'wrong signature')
                result, _ = self.run_reconcile([record(name=path.name, cdn_name=None, size=None)])
                self.assertEqual(result.mismatched, ['1'])
                path.unlink()

    def test_requeue_keeps_accumulated_attempts(self):
        result, store = self.run_reconcile([record(attempts=99)])
        self.assertEqual(result.requeued, ['1'])
        self.assertEqual(store.records[0].attempts, 99)
        self.assertIsNone(store.records[0].link_url)
        self.assertIsNone(store.records[0].link_expires)

    def test_rename_conflict_keeps_existing_files(self):
        self.write('cdn-1.mp4', b'new')
        self.write('原名.mp4', b'existing')
        self.write('原名 (2).mp4', b'existing two')
        result, store = self.run_reconcile([record()])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual((self.folder / '原名.mp4').read_bytes(), b'existing')
        self.assertEqual((self.folder / '原名 (2).mp4').read_bytes(), b'existing two')
        self.assertEqual((self.folder / '原名 (3).mp4').read_bytes(), b'new')
        self.assertEqual(Path(store.records[0].local_path).name, '原名 (3).mp4')
        # A later reconciliation recognizes a collision suffix via local_path.
        store.update_file('album', '1', status='exported')
        again = reconcile(store, 'album', self.folder)
        self.assertEqual(again.completed, ['1'])
        self.assertEqual(again.renamed, [])

    def test_album_duplicate_names_match_export_including_ineligible_records(self):
        rows = [record(status='done'), record('2', '原?名.mp4'),
                record('3', '原名 (2).mp4')]
        self.write('原名.mp4', b'old')
        self.write('cdn-2.mp4')
        self.write('cdn-3.mp4')
        result, store = self.run_reconcile(rows)
        self.assertEqual(result.completed, ['2', '3'])
        self.assertEqual(Path(store.records[1].local_path).name, '原名 (3).mp4')
        self.assertEqual(Path(store.records[2].local_path).name, '原名 (2).mp4')
        self.assertEqual((self.folder / '原名.mp4').read_bytes(), b'old')

    def test_wrong_sizes_and_missing_requeued_without_deletion(self):
        self.write('cdn-1.mp4', b'x')
        self.write('二.mp4', b'xx')
        rows = [record(), record('2', '二.mp4', status='failed'),
                record('3', '三.mp4', status='pending')]
        before = {p.name: p.read_bytes() for p in self.folder.iterdir()}
        result, store = self.run_reconcile(rows)
        self.assertEqual(result.mismatched, ['1', '2'])
        self.assertEqual(result.missing, ['3'])
        self.assertEqual(result.requeued, ['1', '2', '3'])
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.folder.iterdir()})
        for row in store.records:
            self.assertEqual(row.status, 'pending')
            self.assertIsNone(row.link_url)
            self.assertIsNone(row.link_expires)

    def test_all_temporary_suffixes_under_both_names(self):
        suffixes = ('.xltd', '.td', '.part', '.aria2', '.crdownload', '.tmp')
        for prefix in ('cdn-1.mp4', '原名.mp4'):
            for suffix in suffixes:
                with self.subTest(prefix=prefix, suffix=suffix):
                    path = self.write(prefix + suffix)
                    result, _ = self.run_reconcile([record()])
                    self.assertEqual(result.mismatched, ['1'])
                    self.assertEqual(result.requeued, ['1'])
                    self.assertEqual(path.read_bytes(), b'abc')
                    path.unlink()

    def test_aria2_companion_blocks_even_full_sized_payload(self):
        payload = self.write('cdn-1.mp4')
        control = self.write('cdn-1.mp4.aria2', b'progress')
        result, _ = self.run_reconcile([record()])
        self.assertEqual(result.mismatched, ['1'])
        self.assertEqual(result.completed, [])
        self.assertTrue(payload.exists())
        self.assertTrue(control.exists())

    def test_good_original_can_complete_when_cdn_is_partial(self):
        self.write('cdn-1.mp4', b'bad-size')
        self.write('原名.mp4')
        result, _ = self.run_reconcile([record()])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual((self.folder / 'cdn-1.mp4').read_bytes(), b'bad-size')

    def test_no_requeue_preserves_states_and_signatures(self):
        self.write('cdn-1.mp4', b'bad size')
        rows = [record(), record('2', '二.mp4', status='failed'),
                record('3', '三.mp4', status='pending')]
        result, store = self.run_reconcile(rows, requeue=False)
        self.assertEqual(result.mismatched, ['1'])
        self.assertEqual(result.missing, ['2', '3'])
        self.assertEqual(result.requeued, [])
        self.assertEqual(store.records, rows)

    def test_no_requeue_still_confirms_complete_files(self):
        self.write('cdn-1.mp4')
        result, store = self.run_reconcile([record()], requeue=False)
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(store.records[0].status, 'done')

    def test_nonrecursive_and_ignored_statuses_and_album(self):
        subfolder = self.folder / 'nested'
        subfolder.mkdir()
        (subfolder / 'cdn-1.mp4').write_bytes(b'abc')
        rows = [record(), record('2', status='done'), record('3', status='skipped'),
                record('4', status='downloading'), record('5', album_id='other')]
        result, store = self.run_reconcile(rows)
        self.assertEqual(result.missing, ['1'])
        self.assertEqual(store.records[1:], rows[1:])
        self.assertEqual((subfolder / 'cdn-1.mp4').read_bytes(), b'abc')

    def test_directory_collision_is_preserved(self):
        (self.folder / '原名.mp4').mkdir()
        self.write('cdn-1.mp4')
        result, store = self.run_reconcile([record()])
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(Path(store.records[0].local_path).name, '原名 (2).mp4')
        self.assertTrue((self.folder / '原名.mp4').is_dir())

    def test_untrusted_cdn_path_cannot_escape_folder(self):
        self.write('unrelated.mp4')
        result, _ = self.run_reconcile([record(cdn_name='../unrelated.mp4')])
        self.assertEqual(result.missing, ['1'])
        self.assertEqual((self.folder / 'unrelated.mp4').read_bytes(), b'abc')

    def test_same_local_file_is_not_counted_twice(self):
        self.write('cdn-1.mp4')
        rows = [record(), record('2', '原名.mp4', cdn_name='原名.mp4')]
        result, _ = self.run_reconcile(rows)
        self.assertEqual(result.completed, ['1'])
        self.assertEqual(result.mismatched, ['2'])

    def test_nonexistent_folder_raises_without_requeue(self):
        store = MemoryStore([record()])
        with self.assertRaises(FileNotFoundError):
            reconcile(store, 'album', self.folder / 'absent')
        self.assertEqual(store.records[0].status, 'exported')

    def test_hidden_album_owner_blocks_external_folder_claim(self):
        from bunkrdl.models import Album, AlbumFile
        from bunkrdl.store import Store
        from bunkrdl.flows import check_folder
        store = Store(':memory:')
        self.addCleanup(store.close)
        content = b'\xff\xd8\xffimage'
        path = self.write('shared.jpg', content)
        for ident in ('a', 'b'):
            store.upsert_album(Album(ident, ident, 'url', [
                AlbumFile('1', 'shared.jpg', '1', 'url', size=len(content), kind='image')]))
        store.update_file('a', '1', status='done', local_path=str(path))
        store.set_album_hidden('a', True)
        self.assertEqual([a.album_id for a in store.list_albums()], ['b'])
        owner = store.list_files('a')
        for check in (lambda: reconcile(store, 'b', self.folder),
                      lambda: check_folder(store, 'b', self.folder)):
            result = check()
            self.assertEqual(result.completed, [])
            self.assertEqual(result.mismatched, ['1'])
            self.assertEqual(result.renamed, [])
            self.assertIsNone(store.list_files('b')[0].local_path)
            self.assertEqual(store.list_files('a'), owner)
            self.assertEqual(path.read_bytes(), content)


if __name__ == '__main__':
    unittest.main()
