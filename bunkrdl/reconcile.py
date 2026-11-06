"""Verify external downloads and restore their original names."""
from __future__ import annotations

import json
import os
from pathlib import Path

from .names import _numbered, unique_names
from .models import ReconcileResult


_TEMP_SUFFIXES = ('.xltd', '.td', '.part', '.aria2', '.crdownload', '.tmp')
# A page starts like HTML, matching downloader.py; markup inside a payload does not.
_HTML_STARTS = (b'<!doctype html', b'<html')
_MEDIA_KINDS = ('image', 'video')
# Words that only appear in a served error page, never in an image or a video.
_ERROR_PAGE_WORDS = (b'error', b'not found', b'forbidden', b'denied', b'unauthorized',
                     b'expired', b'bad gateway', b'too many requests', b'cloudflare')


def _looks_like_text(head):
    """True when the payload starts with printable text rather than media bytes."""
    sample = head[:64]
    return bool(sample) and all(byte in (9, 10, 13) or 32 <= byte < 127 for byte in sample)


def _other_error_page(body):
    """Recognize explicit error responses without rejecting normal JSON or SVG."""
    if body[:1] == b'{':
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError):
            return False
        error = data.get('error')
        if error:
            return True
        message = data.get('message')
        return isinstance(message, str) and any(
            word.decode('ascii') in message for word in _ERROR_PAGE_WORDS)
    return body.startswith(_ERROR_PAGE_WORDS + (b'access denied',))


_MAGIC_CHECKS = {
    '.avi': lambda head: head.startswith(b'RIFF') and head[8:12] == b'AVI ',
    '.bmp': lambda head: head.startswith(b'BM'),
    '.ts': lambda head: len(head) >= 377 and all(head[i] == 0x47 for i in (0, 188, 376)),
    '.m2ts': lambda head: len(head) >= 389 and all(head[i] == 0x47 for i in (4, 196, 388)),
    '.wmv': lambda head: head.startswith(bytes.fromhex('3026b2758e66cf11a6d900aa0062ce6c')),
    '.flv': lambda head: head.startswith(b'FLV'),
    '.mpg': lambda head: head.startswith((b'\x00\x00\x01\xba', b'\x00\x00\x01\xb3')),
    '.mpeg': lambda head: head.startswith((b'\x00\x00\x01\xba', b'\x00\x00\x01\xb3')),
    '.tif': lambda head: head.startswith((b'II\x2a\x00', b'MM\x00\x2a')),
    '.tiff': lambda head: head.startswith((b'II\x2a\x00', b'MM\x00\x2a')),
    '.heic': lambda head: head[4:8] == b'ftyp' and b'heic' in head[8:32],
    '.avif': lambda head: head[4:8] == b'ftyp' and b'avif' in head[8:32],
    '.jpg': lambda head: head.startswith(b'\xff\xd8\xff'),
    '.jpeg': lambda head: head.startswith(b'\xff\xd8\xff'),
    '.jfif': lambda head: head.startswith(b'\xff\xd8\xff'),
    '.png': lambda head: head.startswith(b'\x89PNG\r\n\x1a\n'),
    '.gif': lambda head: head.startswith((b'GIF87a', b'GIF89a')),
    '.webp': lambda head: head.startswith(b'RIFF') and head[8:12] == b'WEBP',
    '.mp4': lambda head: head[4:8] == b'ftyp',
    '.mov': lambda head: head[4:8] == b'ftyp',
    '.m4v': lambda head: head[4:8] == b'ftyp',
    '.3gp': lambda head: head[4:8] == b'ftyp',
    '.webm': lambda head: head.startswith(b'\x1a\x45\xdf\xa3'),
    '.mkv': lambda head: head.startswith(b'\x1a\x45\xdf\xa3'),
}


def content_check(path, record):
    """Return (acceptable, unverified), inspecting the first 4 KiB."""
    size = path.stat().st_size
    if record.size is not None and size != record.size:
        return False, False
    with path.open('rb') as stream:
        head = stream.read(4096)
    lower = head.lower()
    if b'maintenance' in lower:
        return False, False
    stripped = lower.lstrip().removeprefix(b'\xef\xbb\xbf').lstrip()
    # Sizes can match an error page, so these run before the known-size pass.
    if stripped.startswith(_HTML_STARTS):
        return False, False
    ext = Path(record.name).suffix.lower()
    magic = _MAGIC_CHECKS.get(ext)
    if magic is not None and magic(head):
        # A real media signature wins: payloads may legitimately contain markup
        # or the word "error" after the header.
        return True, False
    if record.kind in _MEDIA_KINDS:
        # A JSON body is an API error; printable text mentioning a failure is an
        # error page.  Both can happen to be exactly the size we expect.
        if stripped[:1] in (b'{', b'['):
            return False, False
        if _looks_like_text(stripped) and any(word in lower for word in _ERROR_PAGE_WORDS):
            return False, False
    elif record.size is not None and _other_error_page(stripped):
        return False, False
    if record.size is not None:
        return True, False
    if not size:
        return False, False
    if magic is not None:
        return False, False
    return True, True


def prescan(store, album_id, folder) -> dict:
    """Read-only pass over the folder: the slow content checks, keyed for reuse.

    Runs outside the scheduler lock. ``reconcile(..., checks=cache)`` then only
    renames and writes records, so the lock covers fast work only.
    """
    cache = {}
    reconcile(store, album_id, folder, requeue=False, dry_run=True, checks=cache)
    return cache


def reconcile(store, album_id, folder, requeue=True, *, dry_run=False, checks=None) -> ReconcileResult:
    root = Path(folder).resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(str(root))
    records = store.list_files(album_id)
    names = unique_names(records)
    # Never follow links or inspect nested directories.
    entries = {p.name.casefold(): p for p in root.iterdir() if p.is_file() and not p.is_symlink()}
    occupied = {p.name.casefold() for p in root.iterdir()}
    reserved = {name.casefold() for name in names.values()}
    def path_key(path):
        return str(Path(path).resolve()).casefold()

    claimed = {}
    for album in store.list_albums(include_hidden=True):
        for record in store.list_files(album.album_id):
            if record.local_path:
                claimed.setdefault(path_key(record.local_path), set()).add((record.album_id, record.file_id))

    def available(path, record):
        return not (claimed.get(path_key(path), set()) - {(record.album_id, record.file_id)})
    result = ReconcileResult()
    checks = checks if checks is not None else {}

    def checked(path, record):
        try:
            st = path.stat()
            key = (str(path), record.file_id, st.st_size, st.st_mtime_ns)
        except OSError:
            return content_check(path, record)
        if key not in checks:
            checks[key] = content_check(path, record)
        return checks[key]

    def complete(path, record):
        return (path is not None and available(path, record)
                and not path.name.casefold().endswith(_TEMP_SUFFIXES)
                and not any(path.name.casefold() + suffix in entries for suffix in _TEMP_SUFFIXES)
                and checked(path, record)[0])

    for record in records:
        if record.status not in ('exported', 'failed', 'pending'):
            continue
        name = names[record.file_id]
        candidates = []
        # Treat CDN names as basenames, never as paths supplied by the server.
        if record.cdn_name and '/' not in record.cdn_name and '\\' not in record.cdn_name:
            candidates.append(record.cdn_name)
        candidates.append(name)
        base_candidates = list(dict.fromkeys(candidates))
        if record.local_path:
            previous = Path(record.local_path)
            if previous.parent == root:
                candidates.append(previous.name)
        paths = [entries.get(candidate.casefold()) for candidate in candidates]
        # Downloader collision suffixes are a fallback only when every exact
        # name is absent, including the previously recorded local path.
        duplicate_match = not any(path is not None for path in paths)
        if duplicate_match:
            for candidate in base_candidates:
                base = Path(candidate)
                for number in range(1, 10):
                    candidates.extend(f'{base.stem}{suffix}{base.suffix}' for suffix in
                                      (f' ({number})', f'({number})', f'_{number}'))
            paths = [entries.get(candidate.casefold()) for candidate in candidates]
        found = next((path for path in paths if complete(path, record)), None)
        if found is not None:
            unverified = checked(found, record)[1]
            destination = found
            if duplicate_match or (record.cdn_name and found.name.casefold() == record.cdn_name.casefold()):
                destination = root / name
                if destination.name.casefold() != found.name.casefold() and not dry_run:
                    number = 2
                    while destination.name.casefold() in occupied or not available(destination, record) or (
                            destination.name.casefold() in reserved and destination.name != name):
                        destination = root / _numbered(name, number)
                        number += 1
                    # POSIX rename replaces existing files; link/unlink avoids that race.
                    if os.name == 'nt':
                        found.rename(destination)
                    else:
                        os.link(found, destination)
                        found.unlink()
                    result.renamed.append((str(found), str(destination)))
                    entries.pop(found.name.casefold())
                    entries[destination.name.casefold()] = destination
                    occupied.discard(found.name.casefold())
                    occupied.add(destination.name.casefold())
            claimed.setdefault(path_key(found if dry_run else destination), set()).add(
                (record.album_id, record.file_id))
            if dry_run:
                continue
            if unverified:
                store.update_file(album_id, record.file_id, local_path=str(destination),
                                  last_error='内容无法确认，请打开检查后手动标为完成')
                result.unverified.append(record.file_id)
                continue
            store.update_file(album_id, record.file_id, status='done',
                              local_path=str(destination), last_error=None)
            result.completed.append(record.file_id)
            continue
        present = any(path is not None for path in paths) or any(
            candidate.casefold() + suffix in entries
            for candidate in candidates for suffix in _TEMP_SUFFIXES)
        category = result.mismatched if present else result.missing
        category.append(record.file_id)
        if requeue and not dry_run:
            store.update_file(album_id, record.file_id, status='pending', link_url=None,
                              link_expires=None, last_error='文件不完整或大小不符' if present else '文件缺失')
            result.requeued.append(record.file_id)
    return result
