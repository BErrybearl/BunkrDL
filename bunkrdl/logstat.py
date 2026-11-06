"""Summarize the request log written by :mod:`bunkrdl.requestlog`.

    python -m bunkrdl.logstat [日志路径] [--since 2h] [--json]

The default path is ``~/.bunkrdl/web.log``; the rotated files
``.1``/``.2``/``.3`` are read as well, because the logger keeps four files.
Only the log is read: nothing is downloaded and no network access happens.
Lines that do not match the logged format are counted and ignored.
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .requestlog import STAGES, log_path

WINDOW_SECONDS = 10.0
ROTATED_FILES = 3
UNKNOWN_STAGE = "未标注"

# time stage method host status=/error= retry_after= bytes= elapsed_ms=
# retry_after is non-greedy so an HTTP-date value with spaces still parses.
_LINE = re.compile(
    r"^(?P<time>\S+)\s+(?P<stage>\S+)\s+(?P<method>\S+)\s+(?P<host>\S+)\s+"
    r"(?:(?P<status>status=(?:-|-?\d+))|(?P<error>error=\S+))\s+"
    r"retry_after=(?P<retry>.*?)\s+bytes=(?P<bytes>\d+|-)\s+"
    r"elapsed_ms=(?P<elapsed>\d+(?:\.\d+)?|-)$"
)
_DURATION = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$", re.IGNORECASE)
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


@dataclass(frozen=True)
class Request:
    """One parsed log line."""

    when: datetime
    stage: str
    method: str
    host: str
    status: int | None
    error: str | None
    retry_after: str
    bytes: int | None
    elapsed_ms: float | None

    @property
    def rate_limited(self) -> bool:
        return self.status == 429

    @property
    def stage_label(self) -> str:
        return self.stage if self.stage in STAGES else UNKNOWN_STAGE


@dataclass
class Loaded:
    """Parsed records plus what was skipped."""

    records: list[Request]
    ignored: int
    files: list[Path]

    @property
    def empty(self) -> bool:
        return not self.files


def _positive_int(text):
    value = int(text)
    return value if value >= 0 else None


def _optional_number(text, kind):
    if text == "-":
        return True, None
    try:
        value = kind(text)
    except (TypeError, ValueError):
        return False, None
    if not math.isfinite(value) or value < 0:
        return False, None
    return True, value


def parse_line(text):
    """Parse one logged line; ``None`` means the format does not match."""
    if not isinstance(text, str):
        return None
    match = _LINE.match(text.strip())
    if match is None:
        return None
    try:
        when = datetime.fromisoformat(match["time"])
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.astimezone()

    stage = match["stage"]
    if stage != "-" and stage not in STAGES:
        return None
    method, host = match["method"], match["host"]
    if not method or not host:
        return None

    status = None
    if match["status"] is not None:
        if match["status"] != "status=-":
            status = _positive_int(match["status"][len("status="):])
            if status is None:
                return None
    error = None
    if match["error"] is not None:
        error = match["error"][len("error="):]
        if not error:
            return None

    retry_after = match["retry"]
    if not retry_after:
        return None
    ok, size = _optional_number(match["bytes"], int)
    if not ok:
        return None
    ok, elapsed = _optional_number(match["elapsed"], float)
    if not ok:
        return None
    return Request(when, stage, method, host, status, error, retry_after, size, elapsed)


def candidate_files(path) -> list[Path]:
    """The log file plus its rotated siblings ``.1``/``.2``/``.3``."""
    path = Path(path).expanduser()
    return [path] + [path.with_name(f"{path.name}.{index}")
                     for index in range(1, ROTATED_FILES + 1)]


def load(path, since=None) -> Loaded:
    """Read every existing candidate file; keep only records at/after ``since``."""
    records, ignored, files = [], 0, []
    for item in candidate_files(path):
        try:
            text = item.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        files.append(item)
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            record = parse_line(line)
            if record is None:
                ignored += 1
            elif since is None or record.when >= since:
                records.append(record)
    records.sort(key=lambda item: item.when)
    return Loaded(records, ignored, files)


def parse_since(text, now=None) -> datetime:
    """``2h``/``30m``/``90s``/``3d`` relative to now, or an ISO time."""
    text = (text or "").strip()
    if not text:
        raise ValueError("--since 需要一个时间，例如 2h 或 2026-11-03T12:00:00")
    match = _DURATION.match(text)
    if match is not None:
        amount = float(match.group(1))
        if amount <= 0:
            raise ValueError(f"--since 的时长必须大于 0：{text}")
        seconds = amount * _UNITS[match.group(2).lower()]
        base = now if now is not None else datetime.now().astimezone()
        return base - timedelta(seconds=seconds)
    try:
        when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(
            f"无法识别的时间：{text}（可用 2h、30m、90s、3d 或 2026-11-03T12:00:00）"
        ) from None
    if when.tzinfo is None:
        when = when.astimezone()
    return when


def _share(count, total) -> float:
    return count / total if total else 0.0


def _pct(count, total) -> str:
    return f"{count / total * 100:.1f}%" if total else "0.0%"


def _retry_label(value) -> str:
    text = (value or "").strip()
    if not text or text.lower() in {"none", "-"}:
        return "未提供"
    return text


def _is_timeout(name) -> bool:
    return bool(name) and "timeout" in name.lower()


def advice_for(stats) -> str:
    """One sentence telling the user what to change next."""
    if not stats["requests"]:
        if stats["since"]:
            return "这个时间范围内没有请求记录，可以放宽 --since 再看。"
        return "日志里还没有请求记录。"
    limited = stats["rate_limited"]
    if limited["count"]:
        stage = UNKNOWN_STAGE
        if limited["stages"]:
            stage = max(limited["stages"].items(), key=lambda item: (item[1], item[0]))[0]
        window = limited["pre_window"]
        average = window["average"] or 0.0
        if average >= 5:
            return (f"429 集中在{stage}阶段，每次前 10 秒平均 {average:.1f} 个请求，"
                    "像是并发过高；建议把同时处理数降到 1，并把请求间隔保持在 1 秒以上。")
        return (f"429 出现在{stage}阶段，每次前 10 秒平均 {average:.1f} 个请求，"
                "更像累计请求率触发；建议把请求间隔提高到 1.5 秒以上，冷却结束后再继续。")
    if stats["timeouts"]["count"]:
        return (f"没有出现 429，但有 {stats['timeouts']['count']} 次超时，"
                "建议先检查代理或网络，再继续下载。")
    return "没有出现 429 或超时，当前请求节奏可以保持。"


def analyze(records, *, files=(), ignored=0, since=None) -> dict:
    """Aggregate parsed records into a JSON-ready summary."""
    records = sorted(records, key=lambda item: item.when)
    total = len(records)
    start = records[0].when if records else None
    end = records[-1].when if records else None
    span = (end - start).total_seconds() if start and end else 0.0

    stages = {stage: 0 for stage in STAGES}
    hosts, statuses, errors = Counter(), Counter(), Counter()
    for record in records:
        stages[record.stage_label] = stages.get(record.stage_label, 0) + 1
        hosts[record.host] += 1
        statuses["unknown" if record.status is None else str(record.status)] += 1
        if record.error:
            errors[record.error] += 1

    limited = [record for record in records if record.rate_limited]
    limited_stages, retry_after = Counter(), Counter()
    for record in limited:
        limited_stages[record.stage_label] += 1
        retry_after[_retry_label(record.retry_after)] += 1
    times = [record.when for record in records]
    samples = []
    for record in limited:
        left = bisect.bisect_left(times, record.when - timedelta(seconds=WINDOW_SECONDS))
        right = bisect.bisect_right(times, record.when)
        samples.append(max(0, right - left - 1))

    minutes = Counter(record.when.replace(second=0, microsecond=0) for record in records)
    peak = None
    if minutes:
        minute, count = min(minutes.items(), key=lambda item: (-item[1], item[0]))
        peak = {"minute": minute.isoformat(), "count": count}
    span_minutes = max(span, 60.0) / 60.0 if total else 0.0

    elapsed_values = [(record.elapsed_ms, record.when) for record in records
                      if record.elapsed_ms is not None]
    elapsed_average = sum(value for value, _ in elapsed_values) / len(elapsed_values) \
        if elapsed_values else None
    elapsed_max = max((value for value, _ in elapsed_values), default=None)
    elapsed_max_at = None
    if elapsed_max is not None:
        elapsed_max_at = next(when for value, when in elapsed_values if value == elapsed_max)

    timeout_records = [record for record in records if _is_timeout(record.error)]
    timeout_points = Counter(record.when.replace(microsecond=0) for record in timeout_records)

    stats = {
        "log_files": [str(item) for item in files],
        "ignored_lines": ignored,
        "since": since.isoformat() if since else None,
        "requests": total,
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "span_seconds": span,
        "stages": dict(stages),
        "hosts": dict(hosts),
        "statuses": dict(statuses),
        "errors": dict(errors),
        "rate_limited": {
            "count": len(limited),
            "share": _share(len(limited), total),
            "stages": dict(limited_stages),
            "pre_window": {
                "window_seconds": WINDOW_SECONDS,
                "samples": samples,
                "average": sum(samples) / len(samples) if samples else None,
                "max": max(samples) if samples else None,
                "min": min(samples) if samples else None,
            },
            "retry_after": dict(retry_after),
        },
        "rate_per_minute": {
            "peak": peak,
            "average": total / span_minutes if span_minutes else 0.0,
            "active_minutes": len(minutes),
        },
        "elapsed_ms": {
            "count": len(elapsed_values),
            "average": elapsed_average,
            "max": elapsed_max,
            "max_at": elapsed_max_at.isoformat() if elapsed_max_at else None,
        },
        "timeouts": {
            "count": len(timeout_records),
            "points": [{"time": when.isoformat(), "count": count}
                       for when, count in sorted(timeout_points.items())],
        },
    }
    stats["advice"] = advice_for(stats)
    return stats


def _stamp(value, fmt="%Y-%m-%d %H:%M:%S") -> str:
    if not value:
        return "无"
    try:
        return datetime.fromisoformat(value).astimezone().strftime(fmt)
    except ValueError:
        return str(value)


def _duration(seconds) -> str:
    seconds = max(0.0, float(seconds))
    if seconds < 60:
        return f"{seconds:.0f} 秒"
    if seconds < 3600:
        return f"{int(seconds // 60)} 分 {int(seconds % 60)} 秒"
    return f"{int(seconds // 3600)} 小时 {int(seconds % 3600 // 60)} 分"


def _counter(counter, total, *, limit=None, labels=None) -> str:
    items = [(key, count) for key, count in counter.items() if count]
    items.sort(key=lambda item: (-item[1], str(item[0])))
    if not items:
        return "无"
    shown = items if limit is None else items[:limit]
    parts = []
    for key, count in shown:
        name = (labels or {}).get(str(key), str(key))
        parts.append(f"{name} {count}（{_pct(count, total)}）")
    if limit is not None and len(items) > limit:
        parts.append(f"…共 {len(items)} 项")
    return "｜".join(parts)


def render(stats) -> str:
    """Chinese report for a terminal."""
    lines = ["Bunkr 请求日志分析"]
    files = stats["log_files"]
    if files:
        extra = len(files) - 1
        lines.append("日志文件：" + files[0] + (f"（另有 {extra} 个轮转文件）" if extra else ""))
    if stats["since"]:
        lines.append("统计起始：" + _stamp(stats["since"]))
    if stats["ignored_lines"]:
        lines.append(f"忽略行数：{stats['ignored_lines']}（格式不匹配，已跳过）")
    lines.append(f"总请求数：{stats['requests']}")
    if stats["requests"]:
        lines.append("时间范围：" + _stamp(stats["start"]) + " → " + _stamp(stats["end"])
                     + f"（约 {_duration(stats['span_seconds'])}）")
        lines.append("按阶段：" + _counter(stats["stages"], stats["requests"]))
        lines.append("按主机：" + _counter(stats["hosts"], stats["requests"], limit=8))
        lines.append("状态码：" + _counter(stats["statuses"], stats["requests"],
                                          labels={"unknown": "未知"}))
        limited = stats["rate_limited"]
        if limited["count"]:
            lines.append(f"429：{limited['count']} 次（占全部请求 "
                         f"{_pct(limited['count'], stats['requests'])}）｜阶段 "
                         + _counter(limited["stages"], limited["count"]))
            window = limited["pre_window"]
            lines.append(
                f"429 前 10 秒请求数：平均 {window['average']:.1f} 个，"
                f"最多 {window['max']} 个，最少 {window['min']} 个"
                f"（样本 {len(window['samples'])} 次）")
            lines.append("Retry-After：" + _counter(limited["retry_after"], limited["count"]))
        else:
            lines.append("429：0 次")
        if stats["errors"]:
            lines.append("异常类型：" + _counter(stats["errors"], stats["requests"]))
        rate = stats["rate_per_minute"]
        if rate["peak"]:
            peak = rate["peak"]
            lines.append(f"请求速率：峰值 {_stamp(peak['minute'], '%m-%d %H:%M')}"
                         f"（{peak['count']} 个/分钟），平均 {rate['average']:.1f} 个/分钟"
                         f"（有请求的分钟 {rate['active_minutes']} 个）")
        elapsed = stats["elapsed_ms"]
        if elapsed["count"]:
            text = f"耗时：平均 {elapsed['average']:.1f} 毫秒，最大 {elapsed['max']:.1f} 毫秒"
            if elapsed["max_at"]:
                text += f"（{_stamp(elapsed['max_at'])}）"
            lines.append(text)
        else:
            lines.append("耗时：日志里没有记录")
        timeouts = stats["timeouts"]
        if timeouts["count"]:
            points = timeouts["points"]
            shown = [f"{_stamp(point['time'], '%m-%d %H:%M:%S')}"
                     + (f" ×{point['count']}" if point["count"] > 1 else "")
                     for point in points[:8]]
            more = "" if len(points) <= 8 else f" 等 {len(points)} 个时间点"
            lines.append(f"超时：{timeouts['count']} 次，" + "、".join(shown) + more)
        else:
            lines.append("超时：0 次")
    lines.append("建议：" + stats["advice"])
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bunkrdl.logstat",
        description="汇总 Bunkr 请求日志：请求数、阶段、状态码、限速与耗时。")
    parser.add_argument("path", nargs="?",
                        help="日志路径，默认 ~/.bunkrdl/web.log（同时读 .1/.2/.3）")
    parser.add_argument("--since", help="只看这段时间之后：2h、30m、90s、3d 或 ISO 时间")
    parser.add_argument("--json", action="store_true", help="输出机器可读的 JSON")
    args = parser.parse_args(argv)

    since = None
    if args.since:
        try:
            since = parse_since(args.since)
        except ValueError as error:
            parser.error(str(error))
    path = Path(args.path).expanduser() if args.path else log_path()
    loaded = load(path, since)
    if loaded.empty:
        message = (f"没有找到请求日志：{path}"
                   "（运行一次下载后会自动生成，同时会读取 .1/.2/.3 轮转文件）")
        if args.json:
            print(json.dumps({"log_files": [], "requests": 0, "empty": True,
                              "message": message}, ensure_ascii=False, indent=2))
        else:
            print(message)
        return 0
    stats = analyze(loaded.records, files=loaded.files, ignored=loaded.ignored, since=since)
    if args.json:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        print(render(stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
