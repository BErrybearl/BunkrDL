"""User-facing Chinese wording for errors and waits, shared by CLI and web."""
from __future__ import annotations

import http.client
import math
import re
import socket
import time
from typing import Optional

import requests

from .models import ChallengeRequired, Maintenance, NotFound, RateLimited, TransientError
from .requestlog import STAGES

try:  # requests always ships urllib3; a moved import must not break the wording.
    from urllib3 import exceptions as _urllib3_errors
except Exception:  # pragma: no cover - only when urllib3 is absent
    _urllib3_errors = None

# error_kind values carried on "failed" events
ERROR_KINDS = ("maintenance", "not_found", "challenge", "rate_limited", "transient", "size_mismatch", "other")

_ERROR_TEXT = {
    "maintenance": "服务器维护中，稍后点“重试失败的”",
    "not_found": "文件已被删除，可跳过",
    "challenge": "网站要求人机验证，已停止。换个网络或过一阵再试",
    "rate_limited": "多次被限速，稍后重试",
    "transient": "网络不稳定，稍后重试",
    "size_mismatch": "文件不完整，稍后重试",
    "other": "下载失败",
}

# reason values carried on wait-type events
_WAIT_TEXT = {
    "interval": "防止被限速，稍等再下载下一个",
    "batch": "已连续下载一批，休息一下",
    "backoff": "被限速或网络出错，稍后自动重试",
    "cooldown": "连续被限速，自动暂停一段时间",
}


def _exception_types(module, *names) -> tuple:
    """The classes that exist in an optional module, for isinstance checks."""
    if module is None:
        return ()
    return tuple(value for value in (getattr(module, name, None) for name in names)
                 if isinstance(value, type))


# The file body broke off after the headers arrived. urllib3 raises these
# directly, requests re-raises some of them as ConnectionError / ChunkedEncodingError,
# and the socket layer surfaces the rest as plain OSErrors. A local write failure
# never looks like one of these, so a disk or permission error stays 'other'.
BODY_BREAK_TYPES = (
    requests.exceptions.ChunkedEncodingError,
    http.client.IncompleteRead,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
    socket.timeout,
    TimeoutError,
) + _exception_types(_urllib3_errors, "ProtocolError", "ReadTimeoutError", "IncompleteRead",
                     "InvalidChunkLength")

# Network failures that can happen before any body byte is read; they are
# transient too, but not evidence that a body was cut off.
CONNECT_ERROR_TYPES = (requests.exceptions.ConnectionError,) + _exception_types(
    _urllib3_errors, "NewConnectionError", "ConnectTimeoutError")

# requests wraps a body decode failure; only a cut-off stream counts as a break.
_TRUNCATED = re.compile(r"truncat|incomplete|eof|broken|reset|aborted|timed out|closed",
                        re.IGNORECASE)


def _chain(error: BaseException, limit: int = 10):
    """The error plus the exceptions it wraps, via args/__cause__/__context__."""
    queue, seen = [error], set()
    while queue and len(seen) < limit:
        item = queue.pop(0)
        if not isinstance(item, BaseException) or id(item) in seen:
            continue
        seen.add(id(item))
        yield item
        queue.extend(item.args)
        queue.extend((item.__cause__, item.__context__))


def body_read_broken(exc: BaseException) -> bool:
    """True when the failure happened while the response body was being read.

    Covers a body cut off mid-transfer: urllib3's ProtocolError /
    ReadTimeoutError / IncompleteRead, requests' ChunkedEncodingError and the
    ConnectionError it wraps them in, socket resets and broken pipes, plus a
    content-decode failure whose cause says the stream ended early. A 4xx/5xx
    response, a content check or a disk error is never included.
    """
    if not isinstance(exc, BaseException):
        return False
    chain = list(_chain(exc))
    if any(isinstance(item, requests.exceptions.HTTPError) for item in chain):
        # A server status answer carried inside a wrapper is not a broken stream.
        return False
    if any(isinstance(item, BODY_BREAK_TYPES) for item in chain):
        return True
    if any(isinstance(item, requests.exceptions.ContentDecodingError) for item in chain):
        return any(_TRUNCATED.search(str(item)) for item in chain)
    return False


def error_kind(exc: BaseException) -> str:
    """Map an exception to an error_kind value."""
    if isinstance(exc, ChallengeRequired):
        return "challenge"
    if isinstance(exc, Maintenance):
        return "maintenance"
    if isinstance(exc, NotFound):
        return "not_found"
    if isinstance(exc, RateLimited):
        return "rate_limited"
    if isinstance(exc, (TransientError, requests.Timeout) + CONNECT_ERROR_TYPES):
        return "transient"
    if body_read_broken(exc):
        return "transient"
    text = str(exc).lower()
    if "size" in text or "大小" in text or "incomplete" in text:
        return "size_mismatch"
    return "other"


_URL = re.compile(r'https?://\S+')


def _short(detail: Optional[str]) -> str:
    """First line of a detail string, without URLs that could carry tokens."""
    text = (detail or "").strip()
    if not text:
        return ""
    text = _URL.sub("[地址已省略]", text).splitlines()[0]
    return text[:120]


# A host label from requestlog.host_of; anything else is dropped.
_HOST = re.compile(r"[A-Za-z0-9._:\[\]-]{1,100}")


def _label_host(host) -> Optional[str]:
    """Accept only a bare host[:port]; never userinfo, path or query."""
    if not isinstance(host, str):
        return None
    host = host.strip()
    if not host or not _HOST.fullmatch(host):
        return None
    return host


def _retry_seconds(retry_after) -> Optional[int]:
    if not isinstance(retry_after, (int, float)) or isinstance(retry_after, bool):
        return None
    try:
        value = float(retry_after)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return max(1, int(round(value)))


def _context(stage, host, retry_after) -> str:
    """'· 阶段：文件体 · 主机 cdn.example · 建议等待 120 秒', sanitized."""
    parts = []
    if stage in STAGES:
        parts.append("阶段：" + stage)
    label = _label_host(host)
    if label:
        parts.append("主机 " + label)
    seconds = _retry_seconds(retry_after)
    if seconds is not None:
        parts.append(f"建议等待 {seconds} 秒")
    return "· " + " · ".join(parts) if parts else ""


def error_context(exc) -> dict:
    """Sanitized stage/host/Retry-After carried by an exception, for describe_error."""
    if exc is None:
        return {}
    context = {}
    stage = getattr(exc, "stage", None)
    if stage in STAGES:
        context["stage"] = stage
    host = _label_host(getattr(exc, "host", None))
    if host:
        context["host"] = host
    seconds = _retry_seconds(getattr(exc, "retry_after", None))
    if seconds is not None:
        context["retry_after"] = seconds
    return context


def describe_error(kind: Optional[str], detail: Optional[str] = None, *,
                   stage=None, host=None, retry_after=None) -> str:
    """Chinese explanation and suggestion for an error_kind.

    detail is the short technical reason; it refines transient/rate-limited
    wording and is appended for uncategorized failures. stage/host/retry_after
    add the sanitized request context, never a URL or token.
    """
    kind = kind or "other"
    text = _short(detail)
    if kind == "transient":
        if "连接超时" in text:
            base = "连接超时（可能是代理或网络问题）"
        elif "连接失败" in text:
            base = "连接失败（可能是代理或网络问题）"
        elif "下载中断" in text:
            base = "下载中断（可能是网络问题，可重试）"
        else:
            base = _ERROR_TEXT["transient"]
    elif kind == "rate_limited":
        if "429" in text:
            base = "被网站限速（429）"
        elif "503" in text:
            base = "服务器繁忙（503）"
        else:
            base = _ERROR_TEXT["rate_limited"]
    else:
        base = _ERROR_TEXT.get(kind, _ERROR_TEXT["other"])
        if kind == "other" and text:
            base = f"{base}：{text}"
    return base + _context(stage, host, retry_after)


def describe_wait(reason: Optional[str], until: Optional[float], now: Optional[float] = None) -> str:
    """Chinese wait message, e.g. '防止被限速，稍等再下载下一个（4 秒后，21:43:05 继续）'."""
    base = _WAIT_TEXT.get(reason or "interval", _WAIT_TEXT["interval"])
    if until is None:
        return base
    now = time.time() if now is None else now
    secs = max(0, int(round(until - now)))
    clock = time.strftime("%H:%M:%S", time.localtime(until))
    if reason == "interval":
        return f"{base}（{secs} 秒后）"
    return f"{base}，预计 {clock} 继续，不用操作"
