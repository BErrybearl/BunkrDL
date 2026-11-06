"""Chinese command-line entry points."""
from __future__ import annotations

import argparse
import re
import signal
import sys
import threading
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit

from .guard import JobLock
from .config import load_config
from .names import unique_names
from . import flows, messages
from .models import Album, BunkrError, ChallengeRequired, JobBusy, KINDS


STATUS_LABELS = {"pending": "待下载", "downloading": "本工具下载中", "done": "已完成",
                 "failed": "失败", "exported": "链接已生成", "skipped": "已跳过"}
KIND_LABELS = {"image": "图片", "video": "视频", "other": "其他"}
LOCK_BUSY_TEXT = "另一个任务正在运行，请等它结束或在网页上暂停它"
_CJK = re.compile(r"[\u4e00-\u9fff]")
_EXC_PREFIX = re.compile(r"^[A-Za-z_][\w.]*:\s*")


class AlbumNotAdded(Exception):
    """The album must be added before checking or handing files back."""


class ChineseParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, "参数错误：命令或参数无效，请使用 --help 查看用法。\n")


def album_id(value):
    match = re.fullmatch(r"/(a|f|v|i|d)/([A-Za-z0-9_-]+)/?", urlsplit(value).path)
    if match:
        return match.group(2) if match.group(1) == 'a' else 'file-' + match.group(2)
    if re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return value
    raise ValueError("请输入相册 id、相册链接或单文件链接")


def album_url(value, config):
    ident = album_id(value)
    if urlsplit(value).scheme in ("http", "https"):
        return value
    domain = config.domains[0].rstrip("/")
    if not domain.startswith(("http://", "https://")):
        domain = "https://" + domain
    return f"{domain}/a/{ident}"


def album_directory(config, album, store=None):
    """Compatibility wrapper for older CLI/web callers."""
    # The store keeps the directory stable when an album title changes.
    if store is None:
        return Path(flows.album_dest(config, album))
    return Path(flows.album_dest(config, album, store))


def parse_kinds(value):
    kinds = value.split(",") if isinstance(value, str) else value
    if not isinstance(kinds, (list, tuple)) or not kinds or any(k not in KINDS for k in kinds):
        raise ValueError("类型须为 image、video、other，以逗号分隔")
    return tuple(dict.fromkeys(kinds))


def positive_int(value):
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("须为正整数") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("须为正整数")
    return number


def fetch_album(client, value, config):
    url = album_url(value, config)
    if re.fullmatch(r"/(?:f|v|i|d)/[^/]+/?", urlsplit(url).path):
        file = client.get_file(url)
        return Album(album_id(value), file.name, url, [file])
    return client.get_album(url)


def ensure_album(store, client, value, config):
    album = store.get_album(album_id(value))
    if album is None:
        album = fetch_album(client, value, config)
        store.upsert_album(album)
    return album


def stored_album(store, value):
    """Check and hand-back work on files already known locally; never fetch."""
    album = store.get_album(album_id(value))
    if album is None:
        raise AlbumNotAdded("相册还没有添加，请先运行 python -m bunkrdl get 添加并下载")
    return album


def _chinese_message(event, name=None, default_kind="other"):
    """Prefer the Chinese reason carried by the event; never show an untranslated exception."""
    message = event.get("message")
    if isinstance(message, str) and _CJK.search(message):
        text = _EXC_PREFIX.sub("", message.strip())
        if name and text.startswith(name + "："):
            text = text[len(name) + 1:]
        return text
    return messages.describe_error(event.get("error_kind") or default_kind, event.get("detail"))


def _format_time(timestamp):
    if timestamp is None:
        return "未知"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(timestamp)))
    except (TypeError, ValueError, OverflowError, OSError):
        return "未知"


class EventPrinter:
    """One renderer per command; recent byte samples never cross file boundaries."""
    def __init__(self, stream=None, clock=None):
        self.stream = sys.stdout if stream is None else stream
        self.clock = clock or time.monotonic
        self.tty = self.stream.isatty()
        self.samples = deque()
        self.file_id = None
        self.name = ""
        self.index = self.count = 0
        self.width = 0

    def finish(self):
        if self.width:
            print(file=self.stream, flush=True)
            self.width = 0

    def __call__(self, event):
        kind = event.get("type")
        if kind == "phase":
            self.samples.clear()
            self.index = self.count = 0
            text = {"links": "正在生成下载链接", "download": "正在下载",
                    "check": "正在检查外部下载文件夹"}.get(event.get("phase"))
            if event.get("phase") == "refill":
                text = f"需要补下 {event.get('count', 0)} 个文件"
        elif kind in ("start", "progress", "done"):
            ident = event.get("file_id")
            if ident != self.file_id or kind == "start":
                self.samples.clear()
                self.file_id = ident
                self.name = ""
            self.name = event.get("name") or self.name or ident or ""
            self.index = event.get("index", self.index)
            self.count = event.get("count", self.count)
            prefix = f"[{self.index}/{self.count}] {self.name}"
            if kind == "progress":
                now = self.clock()
                size = event.get("bytes", event.get("downloaded", 0))
                if self.samples and size < self.samples[-1][1]:
                    self.samples.clear()
                self.samples.append((now, size))
                while len(self.samples) > 2 and self.samples[1][0] <= now - 5:
                    self.samples.popleft()
                first_time, first_size = self.samples[0]
                speed = (size - first_size) / (now - first_time) if now > first_time else 0
                total = event.get("total")
                percent = f"{min(100, size / total * 100):.0f}%" if total else "大小未知"
                text = f"{prefix} {percent} {size}/{total if total is not None else '未知'} 字节 {speed / 1024:.1f} KiB/s"
            else:
                text = f"{prefix} {'完成' if kind == 'done' else '开始'}"
        elif kind == "retry":
            text = _chinese_message(event, default_kind="transient")
            seconds = event.get("seconds")
            if seconds is not None:
                try:
                    text += f"（{max(0, int(round(float(seconds))))} 秒后重试）"
                except (TypeError, ValueError, OverflowError):
                    pass
        elif kind in ("wait", "cooldown", "pause"):
            reason = event.get("reason") or {"cooldown": "cooldown", "pause": "batch"}.get(kind, "interval")
            until = event.get("until")
            if until is None and "seconds" in event:
                until = time.time() + event["seconds"]
            text = messages.describe_wait(reason, until)
            if until is not None and reason == "interval":
                text += "，预计 " + time.strftime("%H:%M:%S", time.localtime(until)) + " 继续"
        elif kind == "failed":
            name = event.get("name") or event.get("file_id") or ""
            reason = _chinese_message(event, name)
            text = f"{name}：{reason}" if name else reason
        elif kind in ("challenge", "aborted"):
            text = messages.describe_error("challenge")
        elif kind == "stopped":
            text = "已停止，可再次运行继续"
        elif kind == "exported":
            text = "链接已生成"
        elif kind == "skipped":
            text = "已跳过"
        elif kind == "invalid_time":
            text = event.get("message") if _CJK.search(str(event.get("message") or "")) else "忽略无效等待时间"
        else:
            text = None
        if not text:
            return
        if self.tty and kind == "progress":
            print("\r" + text.ljust(self.width), end="", file=self.stream, flush=True)
            self.width = len(text)
        else:
            self.finish()
            print(text, file=self.stream, flush=True)


def call_export(*args, stop_event=None, pacer=None, **kwargs):
    """Forward cancellation and an optional shared pacer to the export interface."""
    from .export import export_links
    return export_links(*args, stop_event=stop_event, pacer=pacer, **kwargs)


def _intersect(base, allowed):
    allowed = set(allowed)
    return [ident for ident in base if ident in allowed]


def _status_ids(store, album_id_value, status):
    return {record.file_id for record in store.list_files(album_id_value, status=status)}


def _print_download_urls(store, album_id_value, file_ids, config):
    """Print one copyable download URL per selected file, in selection order."""
    from .export import download_url
    records = {record.file_id: record for record in store.list_files(album_id_value)}
    names = unique_names(records.values())
    count = 0
    for ident in file_ids:
        record = records.get(ident)
        if record is None:
            continue
        try:
            url = download_url(record, config, names)
        except (BunkrError, KeyError, OSError, TypeError, ValueError):
            # A file whose link was not generated has no URL to copy; keep the rest.
            continue
        if url:
            print(url)
            count += 1
    return count


def _print_names(store, album_id_value, label, ids):
    if not ids:
        return
    names = {record.file_id: record.name for record in store.list_files(album_id_value)}
    print(f"{label}：{'、'.join(names.get(ident, ident) for ident in ids)}")


def _run_download(store, client, album, dest_dir, config, file_ids, stop, progress, label="下载结束"):
    result = flows.download_files(client, store, album.album_id, dest_dir, config, file_ids,
                                  stop_event=stop, on_event=progress)
    progress.finish()
    if result.aborted_reason:
        print(messages.describe_error("challenge"), file=sys.stderr)
        return 3
    print(f"{label}：完成 {result.done}，失败 {result.failed}，跳过 {result.skipped}")
    if result.stopped or stop.is_set():
        if not stop.is_set():
            print("已停止，可再次运行继续")
        return 0
    return int(bool(result.failed))


def main(argv=None) -> int:
    parser = ChineseParser(description="Bunkr 相册下载：get 下载文件，links 生成下载链接，check 检查外部下载文件夹")
    parser.add_argument("--config", metavar="TOML", help="配置文件")
    parser.add_argument("--db", metavar="路径", help="状态库路径")
    commands = parser.add_subparsers(dest="command", required=True, title="命令")

    sub = commands.add_parser("get", help="添加相册，下载选中的文件（默认全部未完成）")
    sub.add_argument("album", metavar="链接")
    group = sub.add_mutually_exclusive_group()
    group.add_argument("--images", action="store_true", help="只选图片和其他文件")
    group.add_argument("--videos", action="store_true", help="只选视频")

    sub = commands.add_parser("links", help="为选中的文件生成下载链接")
    sub.add_argument("album", metavar="链接")
    group = sub.add_mutually_exclusive_group()
    group.add_argument("--videos", action="store_true", help="只选视频（默认）")
    group.add_argument("--all", action="store_true", help="图片和视频都选")
    sub.add_argument("--limit", type=positive_int, metavar="N", help="只选前 N 个文件")
    sub.add_argument("--print", action="store_true", dest="print_links",
                     help="逐行打印每个文件的下载链接")

    sub = commands.add_parser("check", help="检查外部下载文件夹", description="检查外部下载文件夹")
    sub.add_argument("album", metavar="链接")
    sub.add_argument("folder", metavar="外部下载文件夹")
    sub.add_argument("--redownload", action="store_true", help="检查后由本工具补下没下完和不完整的文件")

    sub = commands.add_parser("handback", help="把选中的文件交回本工具下载")
    sub.add_argument("album", metavar="链接")
    sub.add_argument("file_ids", nargs="+", metavar="文件id")

    commands.add_parser("web", help="启动本地网页").add_argument("--port", type=int, help="端口")
    commands.add_parser("app", help="打开桌面窗口")
    commands.add_parser("list", help="查看相册或文件").add_argument("album", nargs="?", metavar="相册")
    sub = commands.add_parser("mark-done", help="手动确认本地文件完成")
    sub.add_argument("album", metavar="相册")
    sub.add_argument("file_ids", nargs="+", metavar="文件id")
    sub.add_argument("--path", action="append", default=[], metavar="文件id=路径", help="可重复；单个文件也可直接给路径")

    commands.add_parser("add", help="高级：添加相册或单文件链接").add_argument("album", metavar="链接或相册id")
    for name, option, default in (("download", "--dest", None), ("export", "--out", "video")):
        sub = commands.add_parser(name, help="高级：下载全部文件" if name == "download" else "高级：生成下载链接")
        sub.add_argument("album", metavar="相册")
        sub.add_argument("--kinds", default=default, metavar="类型")
        sub.add_argument(option, metavar="目录")
    sub = commands.add_parser("reconcile", help="高级：检查外部下载文件夹")
    sub.add_argument("album", metavar="相册")
    sub.add_argument("folder", metavar="目录")
    sub.add_argument("--no-requeue", action="store_true", help="不重新排队")

    try:
        args = parser.parse_args(argv)
        if getattr(args, "album", None):
            album_id(args.album)
        kinds = parse_kinds(args.kinds) if getattr(args, "kinds", None) is not None else None
        config = load_config(args.config)
        if args.db:
            config.state_db = args.db
    except SystemExit as exc:
        return int(exc.code)
    except (ValueError, OSError) as exc:
        print(f"参数错误：{exc}", file=sys.stderr)
        return 2
    if args.command == "app":
        from .launcher import run
        return run(config)
    if args.command == "web":
        from .web import serve
        try:
            serve(config, port=args.port)
            return 0
        except ValueError as exc:
            print(f"参数错误：{exc}", file=sys.stderr)
            return 2
        except OSError as exc:
            print(f"启动失败：{exc}", file=sys.stderr)
            return 1

    store = None
    job_lock = None
    stop = threading.Event()
    previous = None
    progress = EventPrinter()
    announced = False

    def interrupt(signum, frame):
        nonlocal announced
        stop.set()
        progress.finish()
        if not announced:
            announced = True
            print("已停止，可再次运行继续", flush=True)

    try:
        from .store import Store
        store = Store(config.state_db)
        if threading.current_thread() is threading.main_thread():
            previous = signal.signal(signal.SIGINT, interrupt)
        if args.command == "list":
            if args.album:
                for item in store.list_files(album_id(args.album)):
                    print(f"{item.file_id}\t{item.name}\t{KIND_LABELS.get(item.kind, item.kind)}\t{item.size if item.size is not None else '未知'}\t{STATUS_LABELS.get(item.status, item.status)}\t尝试 {item.attempts}\t{item.last_error or ''}")
            else:
                for item in store.list_albums():
                    counts = "，".join(f"{STATUS_LABELS.get(k, k)} {v}" for k, v in item.counts.items() if v)
                    print(f"{item.album_id}\t{item.title}\t{counts or '没有文件'}")
            return 0

        job_lock = JobLock(config.state_db)
        try:
            job_lock.__enter__()
        except JobBusy:
            job_lock = None
            print(LOCK_BUSY_TEXT, file=sys.stderr)
            return 4
        store.recover_interrupted()

        if args.command == "mark-done":
            from .confirm import mark_done
            paths = {}
            for value in args.path:
                if len(args.file_ids) == 1 and not value.startswith(args.file_ids[0] + '='):
                    paths[args.file_ids[0]] = value
                else:
                    key, sep, path = value.partition('=')
                    if not sep:
                        print('参数错误：--path 须为 文件id=路径', file=sys.stderr)
                        return 2
                    paths[key] = path
            count = mark_done(store, album_id(args.album), args.file_ids, paths)
            print(f"已标为完成：{count} 个文件")
            return 0

        if args.command == "reconcile":
            from .reconcile import reconcile
            result = reconcile(store, album_id(args.album), Path(args.folder).expanduser(),
                               requeue=not args.no_requeue)
            print(f"检查完成：已完成 {len(result.completed)}，没下完 {len(result.missing)}，"
                  f"不完整 {len(result.mismatched)}，需要你确认 {len(result.unverified)}")
            names = {r.file_id: r.name for r in store.list_files(album_id(args.album))}
            for label, ids in (("已完成", result.completed), ("没下完", result.missing),
                               ("不完整", result.mismatched), ("需要你确认", result.unverified)):
                if ids:
                    print(f"{label}：{'、'.join(names.get(i, i) for i in ids)}")
            return int(bool(result.missing or result.mismatched))

        from .resolver import BunkrClient

        if args.command == "add":
            client = BunkrClient(config)
            album = fetch_album(client, args.album, config)
            store.upsert_album(album)
            print(f"已添加：{album.title}（{len(album.files)} 个文件）")
            return 0

        if args.command == "handback":
            album = stored_album(store, args.album)
            count = flows.hand_back(store, album.album_id, args.file_ids)
            print(f"已交回本工具下载：{count} 个文件")
            return 0

        if args.command == "check":
            album = stored_album(store, args.album)
            folder = Path(args.folder).expanduser()
            if not folder.is_dir():
                print(f"参数错误：文件夹不存在：{args.folder}", file=sys.stderr)
                return 2
            progress({"type": "phase", "phase": "check"})
            result = flows.check_folder(store, album.album_id, folder)
            progress.finish()
            print(f"检查完成：已完成 {len(result.completed)}，没下完 {len(result.missing)}，"
                  f"不完整 {len(result.mismatched)}，需要你确认 {len(result.unverified)}")
            _print_names(store, album.album_id, "已完成", result.completed)
            _print_names(store, album.album_id, "没下完", result.missing)
            _print_names(store, album.album_id, "不完整", result.mismatched)
            _print_names(store, album.album_id, "需要你确认", result.unverified)
            if result.unverified:
                print("需要你确认的文件请打开检查，没问题就在文件列表里标为完成")
            need = list(dict.fromkeys([*result.missing, *result.mismatched]))
            if args.redownload:
                if not need:
                    print("没有需要补下的文件")
                    return 0
                if stop.is_set():
                    return 0
                progress({"type": "phase", "phase": "refill", "count": len(need)})
                return _run_download(store, BunkrClient(config), album,
                                     flows.album_dest(config, album, store),
                                     config, need, stop, progress, label="补下结束")
            if need:
                print(f'运行 python -m bunkrdl check "{args.album}" "{args.folder}" --redownload 可由本工具补下')
            elif not result.unverified:
                print("全部已下好")
            return int(bool(need))

        client = BunkrClient(config)
        album = ensure_album(store, client, args.album, config)
        if stop.is_set():
            return 0
        dest_dir = flows.album_dest(config, album, store)

        if args.command == "get":
            if args.images:
                selected = flows.select_ids(store, album.album_id, "images")
                file_ids = _intersect(selected, flows.select_ids(store, album.album_id, "unfinished"))
            elif args.videos:
                selected = flows.select_ids(store, album.album_id, "videos")
                file_ids = _intersect(selected, flows.select_ids(store, album.album_id, "unfinished"))
            else:
                file_ids = flows.select_ids(store, album.album_id, "unfinished")
            if not file_ids:
                print("没有需要下载的文件")
                return 0
            file_ids = list(file_ids)
            print(f"准备下载 {len(file_ids)} 个文件")
            return _run_download(store, client, album, dest_dir, config, file_ids, stop, progress)

        if args.command == "links":
            if args.all:
                pool = flows.select_ids(store, album.album_id, "unfinished")
            else:
                videos = flows.select_ids(store, album.album_id, "videos")
                pool = _intersect(flows.select_ids(store, album.album_id, "unfinished"), videos)
            exported = _status_ids(store, album.album_id, "exported")
            file_ids = [ident for ident in pool if ident not in exported]
            if args.limit:
                file_ids = file_ids[:args.limit]
            if not file_ids:
                print("没有需要生成链接的文件")
                return 0
            result = flows.make_links(client, store, album.album_id, dest_dir, config, file_ids,
                                      stop_event=stop, on_event=progress)
            progress.finish()
            if result.aborted_reason:
                print(messages.describe_error("challenge"), file=sys.stderr)
                return 3
            if result.exported:
                print(f"链接已生成：{result.exported} 个文件")
            if result.failed:
                print(f"生成链接失败：{result.failed} 个文件")
            if result.exported:
                links = flows.latest_links(dest_dir)
                # Each run writes a fresh timestamped directory; fall back to the result.
                export_dir = links.get("export_dir") or result.out_dir or Path(dest_dir) / "_export"
                print(f"链接文件所在目录：{export_dir}")
                print(f"链接数：{result.exported}")
                print(f"最早过期时间：{_format_time(links.get('earliest_expiry'))}")
                print("用迅雷、IDM 等下载工具新建任务，粘贴 part-01-links.txt 里的链接；下完后运行："
                      f'python -m bunkrdl check "{args.album}" "<外部下载文件夹>"')
                if args.print_links:
                    _print_download_urls(store, album.album_id, file_ids, config)
            if result.stopped or stop.is_set():
                if not stop.is_set():
                    print("已停止，可再次运行继续")
                return 0
            return int(bool(result.failed))

        if args.command == "download":
            from .downloader import run_queue
            result = run_queue(client, store, album.album_id,
                               Path(args.dest).expanduser() if args.dest else album_directory(config, album, store),
                               config, kinds=kinds, stop_event=stop, on_event=progress)
            progress.finish()
            if result.aborted_reason:
                print(messages.describe_error("challenge"), file=sys.stderr)
                return 3
            print(f"下载结束：完成 {result.done}，失败 {result.failed}，跳过 {result.skipped}")
            if result.stopped or stop.is_set():
                if not stop.is_set():
                    print("已停止，可再次运行继续")
                return 0
            return int(bool(result.failed))

        def event(value):
            if stop.is_set():
                raise KeyboardInterrupt
            progress(value)

        result = call_export(client, store, album.album_id,
                             Path(args.out).expanduser() if args.out else album_directory(config, album, store) / "_export",
                             config, kinds=kinds, on_event=event, stop_event=stop)
        progress.finish()
        if result.aborted_reason:
            print(messages.describe_error("challenge"), file=sys.stderr)
            return 3
        print(f"链接已生成：{result.out_dir}，失败 {result.failed}")
        if result.stopped or stop.is_set():
            if not stop.is_set():
                print("已停止，可再次运行继续")
            return 0
        return int(bool(result.failed))
    except JobBusy:
        print(LOCK_BUSY_TEXT, file=sys.stderr)
        return 4
    except ChallengeRequired:
        progress.finish()
        print(messages.describe_error("challenge"), file=sys.stderr)
        return 3
    except AlbumNotAdded as exc:
        print(f"参数错误：{exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        interrupt(None, None)
        return 0
    except BunkrError as exc:
        progress.finish()
        print(messages.describe_error(messages.error_kind(exc)), file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        if getattr(exc, "winerror", None) == 1909:
            raise
        progress.finish()
        print(f"操作失败：{exc}", file=sys.stderr)
        return 1
    finally:
        progress.finish()
        if previous is not None:
            signal.signal(signal.SIGINT, previous)
        if store is not None:
            store.close()
        if job_lock is not None:
            job_lock.__exit__(None, None, None)
