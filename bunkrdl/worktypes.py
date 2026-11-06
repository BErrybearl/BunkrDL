"""Shared types for the v3 web scheduler. Changes here must go through the coordinator."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

# Work actions
DOWNLOAD = "download"
LINK = "link"
ACTIONS = (DOWNLOAD, LINK)

# Work item states (persisted in the work_items table)
QUEUED = "queued"
RUNNING = "running"
WORK_STATES = (QUEUED, RUNNING)  # finished items are deleted from the table

# Scheduler lanes: images (and other small files) and videos are limited separately
LANE_IMAGE = "image"
LANE_VIDEO = "video"


def lane_of(kind: str) -> str:
    """Map a file kind to a scheduler lane; 'other' goes to the image lane."""
    return LANE_VIDEO if kind == "video" else LANE_IMAGE


# ItemResult outcomes
OK = "ok"
FAILED = "failed"
INTERRUPTED = "interrupted"


@dataclass
class WorkItem:
    work_id: int
    album_id: str
    file_id: str
    action: str           # DOWNLOAD | LINK
    seq: int              # FIFO order
    state: str = QUEUED
    lane: str = LANE_IMAGE
    created_at: Optional[float] = None


@dataclass
class ItemResult:
    outcome: str                      # OK | FAILED | INTERRUPTED
    error_kind: Optional[str] = None  # messages.ERROR_KINDS when FAILED
    message: Optional[str] = None     # Chinese reason (messages.describe_error)
    retry_after: Optional[float] = None


@dataclass
class EnqueueResult:
    added: int = 0
    already: int = 0          # same action already queued/running
    replaced: int = 0         # queued opposite action replaced
    busy: int = 0             # running with the opposite action
    not_applicable: int = 0   # status does not allow this action (e.g. done, skipped for LINK)
    work_ids: list[int] = field(default_factory=list)


@dataclass
class WorkContext:
    """Everything a single-file operation needs. Built by the coordinator per slot."""
    config: Any
    store: Any
    client: Any                                   # BunkrClient for this slot
    stop_event: threading.Event
    on_event: Callable[[dict], None]              # progress events: dict with type/bytes/total/...
    reserve_lock: threading.Lock                  # guards target-path reservation across slots
    dest_dir: str = ""                            # album download directory


class Busy(Exception):
    """The requested change touches a file that is being processed."""
