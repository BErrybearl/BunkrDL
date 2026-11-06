"""File-level workflows shared by the web UI and the CLI."""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .models import ExportResult, QueueResult, ReconcileResult
from .names import safe_name
from .worktypes import WorkContext, ItemResult, OK, FAILED, INTERRUPTED

PRESETS = ("images", "videos", "unfinished", "failed")
IMAGE_KINDS = ("image", "other")
# A transient network error (timeout/connection) is retried once, in place.
TRANSIENT_RETRY_DELAY = 5.0


def _phase(on_event, phase, **fields):
    if on_event:
        on_event({"type": "phase", "phase": phase, **fields})


def _stopped(stop_event):
    return stop_event is not None and stop_event.is_set()


def _wait_before_retry(stop_event, seconds):
    """Pausable wait between attempts; True when the user paused during it."""
    if stop_event is None:
        time.sleep(seconds)
        return False
    return bool(stop_event.wait(seconds))


def select_ids(store, album_id, preset: str) -> list[str]:
    """File ids for a quick selection preset, in album order."""
    if preset not in PRESETS:
        raise ValueError("未知的选择方式")
    selected = []
    for record in store.list_files(album_id):
        if preset == "images":
            match = record.kind in IMAGE_KINDS
        elif preset == "videos":
            match = record.kind == "video"
        elif preset == "unfinished":
            match = record.status not in ("done", "skipped")
        else:
            match = record.status == "failed"
        if match:
            selected.append(record.file_id)
    return selected


def download_files(client, store, album_id, dest_dir, config, file_ids,
                   stop_event=None, on_event=None) -> QueueResult:
    """Download exactly the selected files with this tool."""
    from .downloader import run_queue

    wanted = list(dict.fromkeys(file_ids or ()))
    if not wanted:
        return QueueResult()
    records = {r.file_id: r for r in store.list_files(album_id)}
    updates = []
    for file_id in wanted:
        record = records.get(file_id)
        if record is None or record.status not in ("pending", "failed", "exported"):
            continue
        # Failed/exported files go back to the queue; the old reason is kept
        # until the next attempt overwrites or clears it.
        if record.status != "pending" or record.link_url or record.link_expires is not None:
            updates.append((album_id, file_id, {"status": "pending", "link_url": None,
                                                "link_expires": None}))
    if updates:
        store.update_files(updates)
    if _stopped(stop_event):
        return QueueResult(stopped=True)
    runnable = [file_id for file_id in wanted
                if records.get(file_id) is not None
                and records[file_id].status not in ("done", "skipped", "downloading")]
    _phase(on_event, "download", count=len(runnable))
    return run_queue(client, store, album_id, dest_dir, config,
                     stop_event=stop_event, on_event=on_event, only_ids=wanted)


def make_links(client, store, album_id, dest_dir, config, file_ids,
               stop_event=None, on_event=None) -> ExportResult:
    """Write fresh links for exactly the selected unfinished files."""
    from .export import export_links

    wanted = list(dict.fromkeys(file_ids or ()))
    if not wanted:
        return ExportResult(out_dir="")
    if _stopped(stop_event):
        return ExportResult(stopped=True)
    _phase(on_event, "links", count=len(wanted))
    return export_links(client, store, album_id, Path(dest_dir) / "_export", config,
                        kinds=None, stop_event=stop_event, on_event=on_event,
                        file_ids=wanted)


def check_folder(store, album_id, folder) -> ReconcileResult:
    """Check an external download folder without any network request."""
    from .reconcile import reconcile

    checked = reconcile(store, album_id, Path(folder).expanduser(), requeue=False)
    updates = [(album_id, file_id, {"status": "failed", "last_error": "外部下载未完成"})
               for file_id in checked.missing]
    updates += [(album_id, file_id, {"status": "failed", "last_error": "文件不完整"})
                for file_id in checked.mismatched]
    if updates:
        store.update_files(updates)
    return checked


def hand_back(store, album_id, file_ids) -> int:
    """Return exported/failed/skipped files to the pending queue."""
    wanted = set(file_ids or ())
    updates = [(album_id, record.file_id,
                {"status": "pending", "link_url": None, "link_expires": None})
               for record in store.list_files(album_id)
               if record.file_id in wanted and record.status in ("exported", "failed", "skipped")]
    if updates:
        store.update_files(updates)
    return len(updates)


def latest_links(dest_dir) -> dict:
    root = Path(dest_dir).expanduser() / "_export"
    result = {"links": [], "earliest_expiry": None, "export_dir": None}
    if not root.is_dir():
        return result
    # The exporter reserves sortable YYYYMMDD-HHMMSS directory names atomically.
    directories = sorted(p for p in root.iterdir() if p.is_dir())
    if not directories:
        return result
    latest = directories[-1]
    result["export_dir"] = str(latest)
    expirations = []
    for part in sorted(latest.glob("part-*-links.txt")):
        for line in part.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                if line.startswith("# 最早过期时间："):
                    try:
                        stamp = datetime.fromisoformat(line.partition("：")[2].strip()).timestamp()
                        if math.isfinite(stamp):
                            expirations.append(stamp)
                    except (ValueError, OverflowError, OSError):
                        pass
                continue
            result["links"].append(line)
            for value in parse_qs(urlsplit(line).query).get("ex", []):
                try:
                    stamp = float(value)
                    if math.isfinite(stamp):
                        expirations.append(stamp)
                except ValueError:
                    pass
    result["earliest_expiry"] = min(expirations) if expirations else None
    return result


def open_folder(path) -> None:
    folder = Path(path).expanduser().resolve()
    folder.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        os.startfile(str(folder))
    else:
        subprocess.run(["open" if sys.platform == "darwin" else "xdg-open", str(folder)], check=True)


_PICK_FOLDER = '''import json, sys, tkinter as tk
from tkinter import filedialog
root = tk.Tk()
root.withdraw()
try:
    path = filedialog.askdirectory(initialdir=sys.argv[1] or None, title="选择外部下载文件夹", parent=root)
    print(json.dumps(path or None))
finally:
    root.destroy()
'''


def pick_folder(initial=None) -> str | None:
    try:
        completed = subprocess.run(
            [sys.executable, "-c", _PICK_FOLDER, str(initial or "")],
            capture_output=True, text=True, encoding="utf-8", check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if sys.platform == "win32" else 0)
        path = json.loads(completed.stdout)
        return path if isinstance(path, str) and path.strip() else None
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1909:
            raise
        return None
    except (ValueError, subprocess.SubprocessError):
        return None


def album_dest(config, album, store=None) -> str:
    if store is not None:
        bound = store.get_album_dest(album.album_id)
        if bound:
            return bound
    name = safe_name(album.title)
    if name == safe_name(""):
        name = safe_name(album.album_id)
    root = Path(config.download_root).expanduser()
    legacy = root / name[:180].rstrip('. ')
    destination = root / (name + ' [' + safe_name(album.album_id) + ']')
    if store is not None and legacy.is_dir():
        records = store.list_files(album.album_id)
        for record in records:
            # Only this album's persisted history can establish a legacy folder.
            if record.status == 'done' and not record.local_path:
                destination = legacy
                break
            if (record.local_path
                    and Path(record.local_path).expanduser().resolve().parent == legacy.resolve()):
                destination = legacy
                break
        if destination != legacy:
            known_ids = {r.file_id for r in records}
            for meta in legacy.glob('*.bunkrdl.part.json'):
                try:
                    owner = json.loads(meta.read_text(encoding='utf-8'))
                except (ValueError, UnicodeError):
                    continue
                if (isinstance(owner, dict) and owner.get('album_id') == album.album_id
                        and owner.get('file_id') in known_ids
                        and meta.with_suffix('').is_file()):
                    destination = legacy
                    break
    if store is not None:
        destination = destination.resolve()
        store.set_album_dest(album.album_id, str(destination))
    return str(destination)


def download_once(ctx: WorkContext, album_id, file_id) -> ItemResult:
    """Run one download attempt; a transient network error gets one retry here.

    Rate limits are handled by the scheduler's lane cooldown, and challenges or
    missing files are never retried. Every attempt still goes through the shared
    request gate, because the downloader asks the client for it.
    """
    from .downloader import DownloadStopped, download_file, _detail
    from .messages import describe_error, error_context, error_kind
    from .models import BunkrError, ChallengeRequired

    record = next((r for r in ctx.store.list_files(album_id) if r.file_id == file_id), None)
    if record is None:
        return ItemResult(FAILED, error_kind='not_found', message=describe_error('not_found'))
    if _stopped(ctx.stop_event):
        ctx.store.update_file(album_id, file_id, status='pending', last_error=None)
        return ItemResult(INTERRUPTED)
    retried = False
    while True:
        # Refresh after a failure so attempts/status from the last try are seen.
        record = next((r for r in ctx.store.list_files(album_id) if r.file_id == file_id), None)
        if record is None:
            return ItemResult(FAILED, error_kind='not_found', message=describe_error('not_found'))
        try:
            destination = ctx.dest_dir
            if not destination:
                album = ctx.store.get_album(album_id)
                if album is None:
                    raise BunkrError('相册不存在')
                with ctx.reserve_lock:
                    destination = album_dest(ctx.config, album, ctx.store)
            # Old generated links must never be reused for a new download attempt.
            ctx.store.update_file(album_id, file_id, link_url=None, link_expires=None)
            download_file(ctx.client, ctx.store, record, destination, ctx.config,
                          on_event=ctx.on_event, stop_event=ctx.stop_event,
                          reserve_lock=ctx.reserve_lock)
            return ItemResult(OK)
        except DownloadStopped:
            ctx.store.update_file(album_id, file_id, status='pending', last_error=None)
            return ItemResult(INTERRUPTED)
        except Exception as error:
            if getattr(error, 'winerror', None) == 1909:
                raise
            if _stopped(ctx.stop_event) and not isinstance(error, ChallengeRequired):
                ctx.store.update_file(album_id, file_id, status='pending', last_error=None)
                return ItemResult(INTERRUPTED)
            kind = error_kind(error)
            if kind == 'transient' and not retried:
                retried = True
                # Do not leave the row showing a failure while it is retried.
                ctx.store.update_file(album_id, file_id, status='pending', last_error=None,
                                      link_url=None, link_expires=None)
                if _wait_before_retry(ctx.stop_event, TRANSIENT_RETRY_DELAY):
                    ctx.store.update_file(album_id, file_id, status='pending', last_error=None)
                    return ItemResult(INTERRUPTED)
                continue
            message = describe_error(kind, _detail(error), **error_context(error))
            ctx.store.update_file(album_id, file_id, status='failed', last_error=message,
                                  link_url=None, link_expires=None)
            return ItemResult(FAILED, error_kind=kind, message=message,
                              retry_after=getattr(error, 'retry_after', None))
