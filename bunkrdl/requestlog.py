"""Request-level log for every Bunkr request, plus error context metadata.

One line per request goes to ``web.log``: ISO time, stage, method, host, status
code or exception type, Retry-After, received byte count (when known) and
elapsed milliseconds. Paths, query strings, tokens, cookies and full URLs are
never written; the Retry-After header is normalized to integer seconds (or
``invalid`` / ``none``) instead of being copied. A body read that times out or
breaks off after the headers adds one error line carrying the same request
number, so the two lines can be matched. Logging failures are swallowed and the
handler keeps them quiet, so a download never depends on the log file and a
failed write never prints a record to stderr.
"""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import urlsplit

# Request stages, shared with the resolver, the downloader and error messages.
STAGE_RESOLVE = "解析"
STAGE_SIGN = "签名"
STAGE_BODY = "文件体"
STAGE_PAGE = "相册页"
STAGES = (STAGE_RESOLVE, STAGE_SIGN, STAGE_BODY, STAGE_PAGE)

LOG_NAME = "web.log"
MAX_BYTES = 1024 * 1024
BACKUP_COUNT = 3

_lock = threading.Lock()
_loggers: dict[str, logging.Logger] = {}


class _QuietRotatingFileHandler(RotatingFileHandler):
    """A log handler that never reports its own failures.

    ``logging.Handler.handleError`` prints the traceback *and the record* to
    stderr, which would leak the very request line the log is supposed to keep
    to itself. A failed write or rotation must stay silent instead.
    """

    def handleError(self, record):  # noqa: N802 - logging's own hook name
        pass


def log_path(config=None) -> Path:
    """``<BUNKRDL_HOME>/web.log``, else next to the configured state database."""
    home = os.environ.get("BUNKRDL_HOME")
    if home:
        return Path(home).expanduser() / LOG_NAME
    state = getattr(config, "state_db", None)
    if isinstance(state, str) and state and state != ":memory:":
        return Path(state).expanduser().parent / LOG_NAME
    from .config import default_home
    return default_home() / LOG_NAME


def _logger(path: Path) -> logging.Logger:
    key = str(path)
    with _lock:
        logger = _loggers.get(key)
        if logger is None:
            logger = logging.getLogger(f"bunkrdl.web:{key}")
            logger.setLevel(logging.INFO)
            logger.propagate = False
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                handler = _QuietRotatingFileHandler(path, maxBytes=MAX_BYTES,
                                                    backupCount=BACKUP_COUNT, encoding="utf-8")
                handler.setFormatter(logging.Formatter("%(message)s"))
            except OSError:
                handler = logging.NullHandler()
            logger.addHandler(handler)
            _loggers[key] = logger
        return logger


def reset() -> None:
    """Close cached handlers; tests use this before removing a temporary home."""
    with _lock:
        for logger in _loggers.values():
            for handler in list(logger.handlers):
                try:
                    handler.close()
                except Exception:
                    pass
                logger.removeHandler(handler)
        _loggers.clear()


def host_of(url) -> str:
    """Host with a non-default port; never userinfo, path or query."""
    if not isinstance(url, str) or not url:
        return ""
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        port = parts.port
    except (TypeError, ValueError):
        return ""
    if not host:
        return ""
    if port and port not in (80, 443):
        return f"{host}:{port}"
    return host


def tag_error(error, stage=None, url=None, host=None) -> None:
    """Attach a stage/host to an exception once, for the Chinese error message."""
    try:
        if stage in STAGES and not getattr(error, "stage", None):
            error.stage = stage
        if host is None:
            host = host_of(url) if url else None
        if host and not getattr(error, "host", None):
            error.host = host
    except Exception:
        pass


def _retry_after(value) -> str:
    """Normalized Retry-After: seconds as an integer, ``invalid`` or ``none``.

    The raw header is never written: it can carry arbitrary text (and with it
    cookies, tokens or newlines). A number is logged as parsed seconds, an HTTP
    date as the seconds it resolves to, anything else only as ``invalid``.
    """
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "invalid"
    text = str(value).strip()
    if not text:
        return "none"
    try:
        number = float(text)
    except (TypeError, ValueError, OverflowError):
        number = None
    if number is not None:
        if not math.isfinite(number) or number < 0:
            return "invalid"
        return str(int(number))
    try:
        date = parsedate_to_datetime(text)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        seconds = (date - datetime.now(timezone.utc)).total_seconds()
    except (TypeError, ValueError, OverflowError):
        return "invalid"
    if not math.isfinite(seconds):
        return "invalid"
    return str(max(0, int(seconds)))


def log_request(stage, method, url, *, status=None, error=None, retry_after=None,
                bytes=None, elapsed_ms=None, started=None, seq=None, config=None) -> None:
    """Append one request line; any failure here is swallowed.

    ``seq`` links the line with the later line of the same request (a response
    header log and the body-read failure that followed it).
    """
    try:
        if elapsed_ms is None and started is not None:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
        fields = [datetime.now().astimezone().isoformat(timespec="milliseconds"),
                  stage if stage in STAGES else "-",
                  str(method or "GET").upper(),
                  host_of(url) or "-"]
        if status is not None:
            fields.append(f"status={int(status)}")
        elif error is not None:
            fields.append(f"error={type(error).__name__}")
        else:
            fields.append("status=-")
        fields.append("retry_after=" + _retry_after(retry_after))
        fields.append("bytes=" + ("-" if bytes is None else str(int(bytes))))
        fields.append("elapsed_ms=" + ("-" if elapsed_ms is None else f"{elapsed_ms:.1f}"))
        if seq is not None:
            fields.append(f"seq={int(seq)}")
        _logger(log_path(config)).info(" ".join(fields))
    except Exception:
        pass
