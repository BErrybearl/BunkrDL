"""Shared data structures and exceptions. Changes here must go through the coordinator."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# File status values (DESIGN 5.2)
PENDING = "pending"
DOWNLOADING = "downloading"
DONE = "done"
FAILED = "failed"
EXPORTED = "exported"
SKIPPED = "skipped"
STATUSES = (PENDING, DOWNLOADING, DONE, FAILED, EXPORTED, SKIPPED)

# File kinds
IMAGE = "image"
VIDEO = "video"
OTHER = "other"
KINDS = (IMAGE, VIDEO, OTHER)

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".avif", ".heic", ".jfif", ".tif", ".tiff"}
VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".avi", ".wmv", ".flv", ".ts", ".m2ts", ".mpg", ".mpeg", ".3gp"}


def kind_of(name: str) -> str:
    """Classify a file by extension."""
    dot = name.rfind(".")
    ext = name[dot:].lower() if dot >= 0 else ""
    if ext in IMAGE_EXTS:
        return IMAGE
    if ext in VIDEO_EXTS:
        return VIDEO
    return OTHER


@dataclass
class AlbumFile:
    file_id: str            # Bunkr file id (window.albumFiles id / data-file-id)
    name: str               # original file name as shown on the album page
    slug: str               # path part of the file page, /f/<slug>
    page_url: str           # absolute file page URL
    size: Optional[int] = None   # bytes, if the album page lists it
    cdn_name: Optional[str] = None  # storage name on the CDN (uuid-style), if known
    kind: str = OTHER
    uploaded: Optional[str] = None  # timestamp text from the album page


@dataclass
class Album:
    album_id: str
    title: str
    url: str
    files: list[AlbumFile] = field(default_factory=list)


@dataclass
class ResolvedLink:
    url: str                # signed direct CDN URL, ready to GET
    referer: str            # Referer header required by the CDN
    cdn_name: str           # basename of the CDN path
    expires_at: Optional[float] = None  # unix time; None if unknown
    headers: dict[str, str] = field(default_factory=dict)  # full header set to send on download


@dataclass
class FileRecord(AlbumFile):
    """AlbumFile plus persisted state (store.py)."""
    album_id: str = ""
    status: str = PENDING
    attempts: int = 0
    last_error: Optional[str] = None
    local_path: Optional[str] = None
    link_url: Optional[str] = None
    link_expires: Optional[float] = None
    updated_at: Optional[float] = None


@dataclass
class AlbumSummary:
    album_id: str
    title: str
    url: str
    counts: dict[str, int] = field(default_factory=dict)  # status -> count
    added_at: Optional[float] = None


@dataclass
class QueueResult:
    done: int = 0
    failed: int = 0
    skipped: int = 0
    stopped: bool = False       # stop_event was set
    aborted_reason: Optional[str] = None  # e.g. challenge required on all domains


@dataclass
class ExportResult:
    exported: int = 0
    failed: int = 0
    out_dir: str = ""
    files: list[str] = field(default_factory=list)  # all volume paths and index.txt
    stopped: bool = False
    aborted_reason: Optional[str] = None


@dataclass
class ReconcileResult:
    renamed: list[tuple[str, str]] = field(default_factory=list)  # (old path, new path)
    completed: list[str] = field(default_factory=list)   # file_ids confirmed complete
    missing: list[str] = field(default_factory=list)     # file_ids not found
    mismatched: list[str] = field(default_factory=list)  # file_ids found with wrong size
    requeued: list[str] = field(default_factory=list)    # file_ids set back to pending
    unverified: list[str] = field(default_factory=list)  # file_ids whose content could not be confirmed


class BunkrError(Exception):
    """Base error for Bunkr access."""


class RateLimited(BunkrError):
    def __init__(self, message: str = "rate limited", retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class Maintenance(BunkrError):
    """CDN server under maintenance / placeholder video returned."""


class NotFound(BunkrError):
    """Album or file deleted / not found."""


class ChallengeRequired(BunkrError):
    """All configured domains require a DDoS-Guard / Cloudflare challenge."""


class TransientError(BunkrError):
    """Timeout / connection error; retry with backoff."""


class JobBusy(Exception):
    """Another process or job already holds the job lock for this state db."""
