"""Loopback-only single-page UI and coordinator-backed JSON API."""
from __future__ import annotations

import importlib
import json
import math
import re
import threading
import time
from dataclasses import asdict, is_dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from . import messages
from .models import Album, BunkrError, ChallengeRequired, JobBusy, RateLimited, STATUSES
from .names import unique_names
from .worktypes import Busy

_BODY_LIMIT = 65536
_LINK_PATH = re.compile(r"/(a|f|v|i|d)/([A-Za-z0-9_-]+)/?")


def _album_target(value, config):
    parts = urlsplit(value)
    match = _LINK_PATH.fullmatch(parts.path)
    if match:
        kind, slug = match.groups()
        ident = slug if kind == "a" else "file-" + slug
    elif re.fullmatch(r"[A-Za-z0-9_-]+", value):
        ident = value
    else:
        raise ValueError("请输入相册链接、单文件链接或相册编号")
    if parts.scheme in ("http", "https") and parts.netloc:
        return ident, value
    if parts.scheme:
        raise ValueError("链接须使用 http 或 https")
    domain = config.domains[0].rstrip("/")
    if not domain.startswith(("http://", "https://")):
        domain = "https://" + domain
    path = f"/f/{ident[5:]}" if ident.startswith("file-") else f"/a/{ident}"
    return ident, domain + path


def _jsonable(value):
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError("无法转换操作结果")


def _file_error(text):
    if not text or re.search(r"[\u3400-\u9fff]", text):
        return text
    lowered = text.lower()
    kind = next((kind for words, kind in (
        (("maintenance",), "maintenance"), (("not found", "404"), "not_found"),
        (("challenge",), "challenge"), (("429", "503", "rate limit"), "rate_limited"),
        (("timeout", "connection", "transient"), "transient"),
        (("size", "incomplete"), "size_mismatch"))
        if any(word in lowered for word in words)), "other")
    return messages.describe_error(kind, text)


def make_server(config, store=None, host=None, port=None):
    host = config.web_host if host is None else host
    port = config.web_port if port is None else port
    if host not in ("127.0.0.1", "localhost"):
        raise ValueError("只允许绑定回环地址 127.0.0.1")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError("端口须为 0 至 65535 的整数")
    owned = store is None
    if owned:
        from .store import Store
        store = Store(config.state_db)
    coordinator = None
    try:
        flows = importlib.import_module(".flows", __package__)
        export = importlib.import_module(".export", __package__)
        Coordinator = importlib.import_module(".jobqueue", __package__).Coordinator
        from .resolver import BunkrClient
        coordinator = Coordinator(
            config, config.state_db, store,
            client_factory=lambda throttle, fail_fast=True: BunkrClient(
                config, throttle=throttle, fail_fast=fail_fast))
    except BaseException:
        if owned:
            store.close()
        raise

    class Handler(BaseHTTPRequestHandler):
        timeout = 30
        protocol_version = "HTTP/1.0"

        def log_message(self, format, *args):
            pass

        def send_data(self, status, data, content_type="application/json; charset=utf-8"):
            body = (data if isinstance(data, str) else json.dumps(
                data, ensure_ascii=False, default=_jsonable, allow_nan=False)).encode("utf-8")
            self.close_connection = True
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; "
                                 "style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                pass

        def safe_host(self):
            values = self.headers.get_all("Host", [])
            if len(values) != 1 or not re.fullmatch(
                    r"(?:127\.0\.0\.1|localhost)(?::[0-9]{1,5})?", values[0], re.I):
                self.send_data(403, {"error": "只接受本机 Host 请求"})
                return False
            return True

        def fail(self, exc):
            if getattr(exc, "winerror", None) == 1909:
                raise exc
            if isinstance(exc, Busy):
                self.send_data(409, {"error": str(exc) or "文件正在排队或处理中，请稍后再操作"})
            elif isinstance(exc, JobBusy):
                self.send_data(409, {"error": "另一个任务正在运行，请稍后再操作"})
            elif isinstance(exc, (ValueError, TypeError, UnicodeError, KeyError)):
                self.send_data(400, {"error": f"参数错误：{exc}"})
            elif isinstance(exc, BunkrError):
                self.send_data(502, {"error": messages.describe_error(messages.error_kind(exc), str(exc))})
            else:
                self.send_data(500, {"error": "操作失败，请检查路径、权限和程序状态"})

        def since(self):
            values = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get("since", ["0"])
            if len(values) != 1 or not re.fullmatch(r"[0-9]+", values[0]):
                raise ValueError("since 须为非负整数且只能给一次")
            return int(values[0])

        def file_ids(self, body, ident):
            ids = body.get("file_ids")
            if not isinstance(ids, list) or not ids or any(not isinstance(i, str) or not i for i in ids):
                raise ValueError("file_ids 须为非空文件 id 数组")
            if set(ids) - {r.file_id for r in store.list_files(ident)}:
                raise ValueError("包含不属于此相册的文件")
            return list(dict.fromkeys(ids))

        def do_GET(self):
            if not self.safe_host():
                return
            try:
                path = unquote(urlsplit(self.path).path)
                if path in ("/", "/index.html"):
                    self.send_data(200, PAGE, "text/html; charset=utf-8")
                elif path == "/api/state":
                    since = self.since()
                    snap = coordinator.snapshot()
                    batch = coordinator.events(since)
                    albums = []
                    for summary in store.list_albums():
                        counts = summary.counts
                        total = sum(counts.values())
                        active = any(a["album_id"] == summary.album_id for a in snap["active"])
                        queued = snap["queued_by_album"].get(summary.album_id, 0)
                        status = ("进行中" if active else "已暂停" if snap["paused"] and queued else
                                  "排队中" if queued else "有失败" if counts.get("failed", 0) else
                                  "已完成" if total and counts.get("done", 0) == total else "未完成")
                        albums.append({"id": summary.album_id, "album_id": summary.album_id,
                                       "title": summary.title, "total": total, "status": status,
                                       "percent": round(counts.get("done", 0) * 100 / total) if total else 0,
                                       "failed": counts.get("failed", 0), "queued": queued})
                    self.send_data(200, {"snapshot": snap, "events": batch["events"],
                                         "next": batch["next"], "albums": albums})
                else:
                    match = re.fullmatch(r"/api/albums/([A-Za-z0-9_-]+)", path)
                    if match:
                        self.album_detail(match[1])
                    else:
                        self.send_data(404, {"error": "地址不存在"})
            except Exception as exc:
                self.fail(exc)

        def album_detail(self, ident):
            album = store.get_album(ident)
            if album is None:
                self.send_data(404, {"error": "相册不存在"})
                return
            records = store.list_files(ident)
            names = unique_names(records)
            work = {w.file_id: w for w in store.list_work_items() if w.album_id == ident}
            active = {a["file_id"]: a for a in coordinator.snapshot()["active"] if a["album_id"] == ident}
            files = []
            for record in records:
                item = asdict(record)
                valid = (record.status == "exported" and bool(record.link_url)
                         and record.link_expires is not None and math.isfinite(record.link_expires)
                         and record.link_expires > time.time())
                item.update(download_url=export.download_url(record, config, names) if valid else None,
                            last_error=_file_error(record.last_error),
                            queued=record.file_id in work and work[record.file_id].state == "queued",
                            active=record.file_id in active, progress=active.get(record.file_id))
                item.pop("link_url", None)
                files.append(item)
            counts = {s: sum(r.status == s for r in records) for s in STATUSES}
            self.send_data(200, {"album_id": ident, "title": album.title, "url": album.url,
                                 "files": files, "counts": counts, "total": len(records),
                                 "percent": round(counts["done"] * 100 / len(records)) if records else 0,
                                 "folder": store.get_meta(f"web:{ident}:folder") or ""})

        def do_POST(self):
            try:
                lengths = self.headers.get_all("Content-Length", [])
                if len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]):
                    raise ValueError("请求正文长度无效")
                length = int(lengths[0])
                # Drain before rejecting Host, media type or size; never retain large bodies.
                chunks, remaining = [], length
                while remaining:
                    chunk = self.rfile.read(min(remaining, 65536))
                    if not chunk:
                        raise ValueError("请求正文长度无效")
                    if length <= _BODY_LIMIT:
                        chunks.append(chunk)
                    remaining -= len(chunk)
                if not self.safe_host():
                    return
                if self.headers.get("Transfer-Encoding"):
                    raise ValueError("不支持分块请求正文")
                if length > _BODY_LIMIT:
                    self.send_data(413, {"error": "请求正文过大"})
                    return
                if self.headers.get_content_type() != "application/json":
                    self.send_data(415, {"error": "请求须使用 application/json"})
                    return
                body = json.loads(b"".join(chunks))
                if not isinstance(body, dict):
                    raise ValueError("JSON 正文须为对象")
                self.dispatch(body)
            except Exception as exc:
                self.fail(exc)

        def dispatch(self, body):
            path = unquote(urlsplit(self.path).path)
            if path == "/api/albums":
                value = body.get("url")
                if not isinstance(value, str) or not value.strip():
                    raise ValueError("请输入相册链接")
                ident, url = _album_target(value.strip(), config)
                remaining = coordinator.cooldown_remaining()
                if remaining:
                    # One shared cooldown for the exit IP: do not even send the
                    # album request while it lasts.
                    seconds = max(1, math.ceil(remaining))
                    self.send_data(429, {"error": "网站正在限速，建议等待 "
                                                  f"{seconds} 秒后再添加相册",
                                         "retry_after": seconds})
                    return

                def resolve():
                    # Network parsing runs outside the scheduler lock.
                    client = coordinator.new_client()
                    try:
                        if _LINK_PATH.fullmatch(urlsplit(url).path).group(1) != "a":
                            item = client.get_file(url)
                            return Album(ident, item.name, url, [item])
                        return client.get_album(url)
                    finally:
                        client.session.close()

                def persist(album):
                    # Re-adding an album must not overwrite records that the
                    # scheduler is still using; check by album id before persist.
                    coordinator.ensure_album_idle(album.album_id)
                    store.upsert_album(album)
                    store.set_album_hidden(album.album_id, False)
                    flows.album_dest(config, album, store=store)
                    return {"album_id": album.album_id, "title": album.title}
                try:
                    album = resolve()
                    result = coordinator.mutate(lambda: persist(album))
                except ChallengeRequired:
                    # Parsing happens off the work queue; still pause everything
                    # through the scheduler so the UI shows the challenge reason.
                    coordinator.pause_for_challenge()
                    raise
                except RateLimited as exc:
                    # A rate limit found while parsing still belongs to the shared
                    # gate: same backoff, same dispatch stop, same pause rule.
                    coordinator.register_rate_limit(getattr(exc, "retry_after", None))
                    seconds = max(1, math.ceil(coordinator.cooldown_remaining()))
                    self.send_data(429, {"error": "被网站限速，已暂停派发新文件；建议等待 "
                                                  f"{seconds} 秒后再添加相册",
                                         "retry_after": seconds})
                    return
                self.send_data(200, result)
                return
            if path == "/api/pick-folder":
                initial = body.get("initial")
                if initial is not None and not isinstance(initial, str):
                    raise ValueError("初始路径须为字符串")
                self.send_data(200, {"path": flows.pick_folder(initial)})
                return
            if path in ("/api/queue/pause", "/api/queue/resume"):
                self.send_data(200, coordinator.pause_all() if path.endswith("pause") else coordinator.resume_all())
                return
            if path == "/api/queue/concurrency":
                lane, value = body.get("lane"), body.get("value")
                if lane not in ("image", "video") or type(value) is not int or not 1 <= value <= (6 if lane == "image" else 3):
                    raise ValueError("图片同时下载数须为 1–6，视频须为 1–3")
                self.send_data(200, coordinator.set_concurrency(lane, value))
                return
            # ---- 清除记录：只删除状态库里的记录，磁盘上已下载的文件一律不动 ----
            match = re.fullmatch(r"/api/albums/([A-Za-z0-9_-]+)/(purge|files/delete)", path)
            if match:
                ident, op = match.groups()
                if store.get_album(ident) is None:
                    self.send_data(404, {"error": "相册不存在"})
                    return
                if op == "purge":
                    deleted = coordinator.purge_album(ident)
                else:
                    scope = {}
                    if "file_ids" in body:
                        scope["file_ids"] = self.file_ids(body, ident)
                    if "statuses" in body:
                        statuses = body["statuses"]
                        if (not isinstance(statuses, list) or not statuses
                                or any(not isinstance(s, str) or s not in STATUSES for s in statuses)):
                            raise ValueError("statuses 须为非空状态数组")
                        scope["statuses"] = list(dict.fromkeys(statuses))
                    if not scope:
                        raise ValueError("请指定要清除的文件 id 或状态")
                    deleted = coordinator.purge_files(ident, **scope)
                self.send_data(200, {"deleted": deleted,
                                     "message": f"已清除 {deleted} 条文件记录；磁盘上已下载的文件未删除"})
                return
            match = re.fullmatch(r"/api/albums/([A-Za-z0-9_-]+)/([a-z-]+)", path)
            if not match:
                self.send_data(404, {"error": "地址不存在"})
                return
            ident, op = match.groups()
            album = store.get_album(ident)
            if album is None:
                self.send_data(404, {"error": "相册不存在"})
                return
            if op == "enqueue":
                ids = self.file_ids(body, ident)
                if body.get("action") not in ("download", "link"):
                    raise ValueError("action 须为 download 或 link")
                self.send_data(200, coordinator.enqueue(ident, ids, body["action"]))
            elif op == "cancel-queue":
                ids = self.file_ids(body, ident)
                cancelled = coordinator.cancel_queued(ident, ids)
                self.send_data(200, {"cancelled": cancelled,
                                     "message": f"已取消 {cancelled} 个排队中的文件"})
            elif op == "retry-failed":
                self.send_data(200, coordinator.retry_failed(ident))
            elif op == "remove":
                def remove():
                    cancelled = coordinator.cancel_album(ident)
                    store.set_album_hidden(ident, True)
                    return cancelled
                cancelled = coordinator.mutate(remove)
                self.send_data(200, {"cancelled": cancelled, "message": "已从列表移除，保留文件与记录"})
            elif op == "open-folder":
                coordinator.mutate(lambda: flows.open_folder(flows.album_dest(config, album, store=store)))
                self.send_data(200, {"message": "已打开下载文件夹"})
            elif op == "check":
                folder = body.get("folder")
                if not isinstance(folder, str) or not folder.strip():
                    raise ValueError("请选择或输入外部下载文件夹")
                folder = folder.strip()

                def check():
                    result = flows.check_folder(store, ident, folder, checks=cache)
                    store.set_meta(f"web:{ident}:folder", folder)
                    data = asdict(result)
                    data["failed_ids"] = list(dict.fromkeys(result.missing + result.mismatched))
                    return data
                keys = [(ident, r.file_id) for r in store.list_files(ident)]
                # Directory listing and content checks run outside the lock;
                # inside it only renames and record writes remain.
                coordinator.ensure_files_idle(keys)
                cache = flows.prescan_folder(store, ident, folder)
                self.send_data(200, coordinator.mutate(check, file_keys=keys))
            elif op == "handback":
                ids = self.file_ids(body, ident)
                updated = coordinator.mutate(lambda: flows.hand_back(store, ident, ids),
                                             file_keys=[(ident, i) for i in ids])
                self.send_data(200, {"updated": updated})
            elif op in ("skip", "mark-done"):
                ids = self.file_ids(body, ident)

                def change():
                    if op == "mark-done":
                        from .confirm import mark_done
                        return mark_done(store, ident, ids, body.get("paths"))
                    store.update_files([(ident, i, {"status": "skipped", "last_error": None}) for i in ids])
                    return len(ids)
                updated = coordinator.mutate(change, file_keys=[(ident, i) for i in ids])
                self.send_data(200, {"updated": updated})
            else:
                self.send_data(404, {"error": "地址不存在"})

    class Server(ThreadingHTTPServer):
        daemon_threads = False
        block_on_close = True

        def __init__(self, *args, **kwargs):
            self._close_lock = threading.Lock()
            self._closed = False
            super().__init__(*args, **kwargs)

        def server_close(self):
            with self._close_lock:
                if self._closed:
                    return
                self._closed = True
                # Join HTTP mutations before closing workers and the database.
                super().server_close()
                coordinator.close()
                if owned:
                    store.close()

    server = None
    try:
        server = Server(("127.0.0.1", port), Handler)
        server.coordinator = coordinator
        server.store = store
        coordinator.start()
        return server
    except BaseException:
        if server is not None:
            server.server_close()
        else:
            coordinator.close()
            if owned:
                store.close()
        raise


def serve(config, store=None, host=None, port=None):
    server = make_server(config, store, host, port)
    print(f"本地页面：http://127.0.0.1:{server.server_port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("已停止，可再次运行继续", flush=True)
    finally:
        server.server_close()


PAGE = r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bunkr 相册下载</title><style>
/* 基础与主题：浅灰背景、白底卡片、蓝色主操作 */
*{box-sizing:border-box}
html,body{height:100%}
body{margin:0;background:#f4f6fa;color:#203047;font:14px/1.6 system-ui,"Microsoft YaHei",sans-serif;display:flex;flex-direction:column;min-height:0}
header,#banner,#message{flex:none}
button,input,select,textarea{font:inherit;border:1px solid #c7cfdb;border-radius:6px;padding:6px 10px;background:white;color:inherit}
button,summary{cursor:pointer}
button:hover{background:#edf2fc}
button.primary{background:#235dd0;color:white}
button:disabled{opacity:.5;cursor:default}

/* 顶栏：摘要、进行中卡片、冷却与暂停 */
header{position:sticky;top:0;z-index:2;padding:12px 22px;background:white;border-bottom:1px solid #dce2eb}
.line{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
h1{font-size:19px;margin:0 16px 0 0}
h2{font-size:20px;margin:0}
p{margin:10px 0}
.muted{color:#68768a}
.grow{flex:1}
.summary{display:flex;gap:14px;align-items:baseline;flex-wrap:wrap;font-size:13px;color:#4a5a70}
.summary span{white-space:nowrap}
#active-panel{margin-top:8px}
.active-head{display:flex;gap:8px;align-items:baseline;font-size:13px}
.cards{display:flex;gap:8px;margin-top:6px;overflow-x:auto;padding-bottom:2px}
.active-card{flex:0 0 216px;min-width:0;border:1px solid #dce2eb;border-radius:8px;padding:8px 10px;background:#f8fafd;cursor:pointer;text-align:left}
.active-card:hover{background:#edf2fc}
.active-card progress{height:7px;margin:4px 0 2px}
.card-title{display:flex;gap:8px;align-items:center;justify-content:space-between}
.card-title strong{font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.lane{flex:none;padding:0 6px;border-radius:9px;background:#e4ebf7;color:#2c4a7c;font-size:12px}
.card-more{flex:none;align-self:center;color:#68768a;font-size:13px;padding:0 4px}
#cooldown{display:flex;gap:10px;align-items:center;margin-top:8px;min-height:22px;font-size:13px}
#cooldown-text{font-weight:600;white-space:nowrap}
#cooldown-note{flex:1;min-width:0}
#cooldown-bar{flex:0 1 240px;width:240px;height:7px}
#banner{padding:10px 22px}
#banner.manual{background:#eef2f7;color:#3a4658}
#banner.auto,#banner.failure{background:#ffe6e6;color:#9c2323}
#pause.manual{background:#eef2f7;border-color:#c7cfdb;color:#203047}
#pause.auto{background:#c62828;border-color:#c62828;color:#fff}
#message{padding:0 22px}

/* 主体：左栏相册，右栏文件表 */
main{flex:1;min-height:0;display:grid;grid-template-columns:280px minmax(0,1fr);overflow:hidden}
aside{padding:18px;border-right:1px solid #dce2eb;min-height:0;overflow:auto}
aside form{display:flex;gap:4px}
aside input{min-width:0;width:100%}
#albums{margin-top:18px}
.album{padding:12px 0;border-bottom:1px solid #dce2eb}
.album.current{border-left:3px solid #235dd0;padding-left:8px}
.album button.title{border:0;background:transparent;text-align:left;overflow-wrap:anywhere;width:100%;padding:0;font-weight:600}

/* 相册更多菜单 */
details{position:relative}
details.menu[open]{background:white;border:1px solid #dce2eb;border-radius:6px;padding:6px}
.menu button{display:block;margin:4px;width:95%;white-space:nowrap}

/* 文件表 */
article{padding:22px;min-width:0;display:flex;flex-direction:column;min-height:0;overflow:hidden}
#detail{display:flex;flex-direction:column;min-height:0;flex:1}
#detail>section:first-child{flex:none;max-height:42vh;overflow:auto}
section{margin-bottom:18px}
#files-section{display:flex;flex-direction:column;min-height:0;flex:1;margin-bottom:0}
.scroll{flex:1 1 auto;min-height:120px;overflow:auto;background:white;border:1px solid #dce2eb;border-radius:8px}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:10px;border-bottom:1px solid #e6eaf0;vertical-align:top;overflow-wrap:anywhere}
th{background:#edf1f8;position:sticky;top:0;z-index:1}
td.name{min-width:160px;max-width:360px;overflow-wrap:anywhere}
td.progress{min-width:150px}
td.state{min-width:200px}
button.sort{border:0;background:transparent;padding:0;font-weight:600;white-space:nowrap}
button.sort:hover{background:transparent;color:#235dd0}
.err{margin-top:4px}
.err summary{color:#8a1f1f;font-size:12px}
.err div{max-height:96px;overflow:auto;white-space:pre-wrap;margin:4px 0}
input[type=checkbox]{width:17px;height:17px}
progress{width:100%;height:9px;accent-color:#235dd0}
.error{color:#ac2525}
.small{font-size:12px}

/* 批量操作栏与外部检查目录 */
.selection{background:#e9effc;padding:10px;border-radius:7px;margin:12px 0;flex:none}
#folder{min-width:150px;flex:1}
#copy-fallback{width:100%;height:130px;margin-top:10px}

/* 顶栏进行中与筛选 */
#active-list{padding:10px 0}
#search{flex:0 1 220px;min-width:120px}
#filter{margin-left:auto}

/* 行状态底色与徽标 */
.focus{background:#fff3ca}
tr.queued td{background:#fff8e1}
tr.running td{background:#eaf3ff}
.badge{display:inline-block;padding:1px 6px;border-radius:9px;background:#f0b429;color:#3d2b00;font-size:12px;margin-right:6px}
.badge.run{background:#235dd0;color:#fff}
[hidden]{display:none!important}

/* 空状态与出错 */
#empty{padding:40px 2px;max-width:620px}
#empty strong{display:block;font-size:16px;margin-bottom:6px}
#empty p{margin:0;color:#68768a}
#empty .line{margin-top:14px}
td.empty-cell{text-align:center;color:#68768a;padding:34px 12px}
#add button{width:96px;padding:6px 0;white-space:nowrap}

/* 窄窗口：单栏，相册列表限高 */
@media(max-width:900px){html,body{height:auto}body{display:block}main{display:block;overflow:visible}aside{border-right:0;border-bottom:1px solid #dce2eb;max-height:260px}header,article{padding:12px}article{display:block;overflow:visible}#detail{display:block}#detail>section:first-child{max-height:none;overflow:visible}#files-section{display:block}.line{gap:7px}#albums{max-height:220px;overflow:auto}.scroll{max-height:52vh;min-height:160px}}
</style></head><body>
<header>
<div class="line"><h1>Bunkr 相册下载</h1>
<div class="summary"><span id="active-count">下载中 0</span><span id="queued">等待 0</span><span id="failed-count">失败 0</span><span id="speed">总速度 0 B/秒</span></div>
<span class="grow"></span><label>图片同时下载数 <select id="image-concurrency" aria-label="图片同时下载数"></select></label><label>视频同时下载数 <select id="video-concurrency" aria-label="视频同时下载数"></select></label><button id="pause">暂停全部</button></div>
<div id="active-panel"><div class="active-head"><strong>进行中</strong><span id="active-total" class="muted small"></span></div><div id="active-list" class="cards"></div></div>
<div id="cooldown"><span id="cooldown-text">正常</span><span id="cooldown-note" class="small muted"></span><progress id="cooldown-bar" max="100" value="0" hidden aria-label="冷却剩余时间"></progress></div>
</header>
<div id="banner" role="alert" hidden><span id="banner-text"></span> <button id="show-failed">查看失败的</button></div><p id="message" role="status" aria-live="polite"></p>
<main><aside><form id="add"><input id="url" aria-label="相册链接" placeholder="粘贴相册链接" required><button id="add-submit" type="submit">添加</button></form><div id="albums"></div></aside>
<article><div id="empty" role="status" aria-live="polite"><strong id="empty-title">还没有选择相册</strong><p id="empty-hint">在左侧粘贴相册链接或单文件链接，点“添加”开始；已有相册点标题即可打开。打开相册后可用“检查外部下载文件夹”，把已经下过或下了一半的文件登记进来。</p><div id="empty-actions" class="line"><button id="empty-paste" type="button">去粘贴链接</button><button id="empty-retry" type="button" hidden>重试</button></div></div><div id="detail" hidden>
<section><h2 id="title"></h2><p id="overview" class="muted"></p><progress id="album-progress" max="100" value="0" aria-label="相册完成进度"></progress><div class="line"><button id="retry" hidden>重试失败的 (0)</button><button id="check-toggle">检查外部下载文件夹</button><button id="open">打开下载文件夹</button></div><div id="check-box" hidden><p id="check-note" class="small muted">外部下载文件夹（仅用于检查，不是本相册的保存位置）</p><div class="line"><input id="folder" aria-label="外部下载文件夹" placeholder="选择或输入外部下载文件夹"><button id="pick">选择文件夹</button><button id="check">开始检查</button></div><p id="check-result" class="muted"></p></div></section>
<section id="files-section"><div class="line"><strong id="file-count">文件</strong><input id="search" type="search" placeholder="搜索文件名" aria-label="搜索文件名"><select id="filter" aria-label="筛选文件"><option value="all">全部</option><option value="image">图片</option><option value="video">视频</option><option value="unfinished">未完成</option><option value="failed">失败</option><option value="exported">链接已生成</option></select></div>
<div id="selection" class="line selection" hidden><span id="selected-count"></span><button id="cancel-queue" class="primary" hidden>取消排队</button><button id="download" class="primary">下载</button><button id="links">获取下载链接</button><button id="copy">复制链接</button><details class="menu"><summary>更多</summary><button id="skip">跳过</button><button id="mark-done">标为完成</button><button id="handback">改回待下载</button><button id="purge-selected">清除选中的记录…</button></details><span id="selection-note" class="small muted"></span></div>
<textarea id="copy-fallback" readonly hidden aria-label="请全选并复制下载链接"></textarea>
<div id="files-scroll" class="scroll"><table><thead><tr><th><input id="select-visible" type="checkbox" aria-label="全选当前筛选"></th><th><button id="sort-name" class="sort" type="button">名称</button></th><th>类型</th><th><button id="sort-size" class="sort" type="button">大小</button></th><th>进度</th><th><button id="sort-status" class="sort" type="button">状态</button></th></tr></thead><tbody id="files"></tbody></table></div></section></div></article></main>
<script>
'use strict';
const $=id=>document.getElementById(id), STATUS={pending:'待下载',downloading:'下载中',done:'已完成',failed:'失败',exported:'链接已生成',skipped:'已跳过'}, KIND={image:'图片',video:'视频',other:'其他'};
let albumId=null,album=null,albums=[],snapshot=null,chosen=new Set(),filter='all',search='',sortKey='',sortDir=1,since=0,revision=0,folderTouched=false,focusFile=null,latestFailure=null,failureUntil=0,localPausing=false,polling=false,stateRequest=0,albumRequest=0,detailAt=0,paneState='',paneDetail='',adding=false,retrying=false;
function markDirty(){detailAt=0}
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n}
function button(text,fn){const n=el('button',text);n.type='button';n.onclick=()=>run(fn);return n}
function bytes(n){if(n===null||n===undefined)return '未知';const units=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<4){n/=1024;i++}return (i?n.toFixed(1):Math.round(n))+' '+units[i]}
function pct(n,total){return total?Math.min(100,Math.max(0,Math.round(n*100/total))):0}
function notify(text){$('message').textContent=text||''}
async function run(fn){try{return await fn()}catch(e){notify(e.message||'操作失败')}}
async function api(path,body){const response=await fetch('/api/'+path,body===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const data=await response.json();if(!response.ok){const err=Error(data.error||'请求失败');err.status=response.status;throw err}return data}
function albumPath(id,op=''){return 'albums/'+encodeURIComponent(id)+(op?'/'+op:'')}
function validLink(f){return f.download_url&&f.link_expires>Date.now()/1000}
function visibleFiles(){if(!album)return [];const q=search.trim().toLowerCase();let list=album.files.filter(f=>filter==='all'||filter==='image'&&f.kind!=='video'||filter==='video'&&f.kind==='video'||filter==='unfinished'&&!['done','skipped'].includes(f.status)||filter==='failed'&&f.status==='failed'||filter==='exported'&&f.status==='exported');if(q)list=list.filter(f=>(f.name||'').toLowerCase().includes(q));if(sortKey){const rank={pending:0,downloading:1,failed:2,exported:3,done:4,skipped:5},val=f=>sortKey==='name'?(f.name||''):sortKey==='size'?(f.size||0):(rank[f.status]!==undefined?rank[f.status]:9);list=list.slice().sort((a,b)=>{const x=val(a),y=val(b),c=typeof x==='string'?x.localeCompare(y):x-y;return (c||(a.name||'').localeCompare(b.name||''))*sortDir})}return list}
function setFilter(value){filter=value;$('filter').value=value;chosen.clear();focusFile=null;renderFiles()}
let albumCache=new Map(),albumEmpty=null;
function renderAlbums(){const box=$('albums'),top=box.scrollTop||0;
const ids=new Set(albums.map(a=>a.id));for(const id of [...albumCache.keys()])if(!ids.has(id))albumCache.delete(id);
if(!albums.length){if(!albumEmpty)albumEmpty=el('p','还没有相册','muted');if(box.children[0]!==albumEmpty)box.replaceChildren(albumEmpty);box.scrollTop=top;return}
const nodes=[];
for(const a of albums){let e=albumCache.get(a.id);
if(!e){const row=el('div'),title=button(a.title,()=>selectAlbum(e.a.id)),meta=el('div',undefined,'small muted'),menu=el('details',undefined,'menu');
menu.append(el('summary','更多'),button('打开文件夹',()=>api(albumPath(e.a.id,'open-folder'),{})),button('从列表移除',async()=>{await api(albumPath(e.a.id,'remove'),{});if(albumId===e.a.id){revision++;albumId=null;album=null;chosen.clear();clearFileRows();setPane('guide');history.replaceState(null,'',location.pathname)}markDirty();await refresh()}),button('清除这个相册的记录…',()=>purgeAlbum(e.a.id,e.a.title)));
row.append(title,meta,menu);e={row,title,meta,a};albumCache.set(a.id,e)}
e.a=a;e.title.textContent=a.title;e.meta.textContent=a.percent+'% · '+a.status+' · 失败 '+a.failed+' · 排队 '+a.queued;
e.row.className='album'+(a.id===albumId?' current':'');nodes.push(e.row)}
const kids=Array.from(box.children);
if(kids.length!==nodes.length||kids.some((n,i)=>n!==nodes[i]))box.replaceChildren(...nodes);
box.scrollTop=top}
let cooldownWatch={image:{until:0,start:0},video:{until:0,start:0}};
function failureCount(){return (albums||[]).reduce((n,a)=>n+(a.failed||0),0)}
function fmtClock(seconds){const d=new Date(seconds*1000);return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')}
function renderActiveCards(active){const box=$('active-list');box.replaceChildren();$('active-total').textContent=active.length?active.length+' 个文件':'';for(const a of active.slice(0,3)){const card=el('div',undefined,'active-card');card.tabIndex=0;card.setAttribute('role','button');const title=el('div',undefined,'card-title'),name=el('strong',a.name||a.file_id);name.title=a.name||'';title.append(name,el('span',a.lane==='video'?'视频':'图片','lane'));card.append(title);if(a.total&&a.bytes){const bar=el('progress');bar.max=100;bar.value=pct(a.bytes,a.total);card.append(bar,el('div',pct(a.bytes,a.total)+'% · '+bytes(a.speed||0)+'/秒','small muted'))}else card.append(el('div',(a.action==='link'?'正在获取下载链接':'正在连接…')+' · '+bytes(a.speed||0)+'/秒','small muted'));card.onclick=()=>selectAlbum(a.album_id,a.file_id);box.append(card)}const rest=active.length-3;if(rest>0)box.append(el('span','+'+rest+' 个','card-more'));if(!active.length)box.append(el('span','暂无进行中的文件','muted'))}
function renderSummary(){const active=snapshot.active||[];$('active-count').textContent='下载中 '+active.length;$('queued').textContent='等待 '+(snapshot.queued||0);$('failed-count').textContent='失败 '+failureCount();$('speed').textContent='总速度 '+bytes(snapshot.speed||0)+'/秒';renderActiveCards(active)}
function renderGate(){const now=Date.now()/1000,cooling=[];for(const lane of ['image','video']){const v=snapshot.lanes[lane];if(document.activeElement!==$(lane+'-concurrency'))$(lane+'-concurrency').value=v.setting;const until=v.cooldown_until||0;if(until>now){const w=cooldownWatch[lane];if(w.until!==until){w.until=until;w.start=now}cooling.push({lane,until,left:Math.ceil(until-now),total:Math.max(1,until-w.start)})}else cooldownWatch[lane]={until:0,start:0}}
const note=[];for(const lane of ['image','video']){const v=snapshot.lanes[lane];note.push((lane==='image'?'图片':'视频')+' 设置 '+v.setting+' · 当前限为 '+v.effective)}
if(!cooling.length){$('cooldown-text').textContent='正常';$('cooldown-note').textContent=note.join(' · ');$('cooldown-bar').hidden=true;return}
const end=Math.max(...cooling.map(c=>c.until)),left=Math.max(...cooling.map(c=>c.left)),labels=cooling.map(c=>c.lane==='image'?'图片':'视频').join('、');
$('cooldown-text').textContent='冷却至 '+fmtClock(end)+'（剩 '+left+' 秒）';$('cooldown-note').textContent=labels+'限速 · '+note.join(' · ');
const bar=$('cooldown-bar');bar.hidden=false;bar.max=100;bar.value=Math.min(100,Math.max(0,Math.round(100*(1-left/Math.max(...cooling.map(c=>c.total))))))}
function pauseIsAuto(){return ['failures','challenge','ratelimit'].includes(snapshot.pause_kind)}
function renderPauseButton(){const active=snapshot.active||[],waiting=localPausing||snapshot.stopping||snapshot.paused&&active.length>0;$('pause').textContent=waiting?'正在暂停…':snapshot.paused?'继续':'暂停全部';$('pause').disabled=!!waiting;$('pause').className=snapshot.paused?(pauseIsAuto()?'auto':'manual'):''}
function renderBanner(){const active=snapshot.active||[],waiting=localPausing||snapshot.stopping||snapshot.paused&&active.length>0,persistent=snapshot.paused,transient=latestFailure&&Date.now()<failureUntil,hasFailures=failureCount()>0;
$('banner').hidden=!persistent&&!transient&&!hasFailures;$('show-failed').hidden=!hasFailures;
$('banner').className=persistent?(pauseIsAuto()?'auto':'manual'):(transient||hasFailures?'failure':'');
$('banner-text').textContent=persistent?((waiting?'正在暂停… ':'已暂停：')+((snapshot.pause_kind==='user'?'':snapshot.pause_reason)||{user:'手动暂停',restart:'重启后等待继续',failures:'连续失败',challenge:'网站要求人机验证',ratelimit:'被网站连续限速，已暂停'}[snapshot.pause_kind]||'等待继续')):transient?((latestFailure.action==='link'?'生成下载链接失败，继续处理下一个':'下载失败，继续处理下一个')+'：'+(latestFailure.message||'请稍后重试')):hasFailures?('有 '+failureCount()+' 个文件失败'):''}
function renderTop(){if(!snapshot)return;renderSummary();renderGate();renderPauseButton();renderBanner()}
const rowCache=new Map();let rowOrderKey='',emptyRow=null;
// 手工清空 tbody 时必须同时清掉排序键，否则同相册重选会因键相等而跳过重建、表格空白。
function clearFileRows(){const body=$('files');body.replaceChildren();rowOrderKey=''}
function firstLine(text){return String(text||'').split('\n')[0].trim()}
function buildRow(f){
const row=el('tr');row.dataset.fileId=f.file_id;
const cell=el('td'),check=el('input');check.type='checkbox';
check.onchange=()=>{const id=row.dataset.fileId;if(check.checked)chosen.add(id);else chosen.delete(id);renderSelection()};
cell.append(check);
const name=el('td',undefined,'name'),kind=el('td'),size=el('td');
const progress=el('td',undefined,'progress'),bar=el('progress');bar.max=100;bar.hidden=true;
const pline=el('div',undefined,'small muted');progress.append(bar,pline);
const state=el('td',undefined,'state'),head=el('div'),badge=el('span',undefined,'badge'),label=el('span');
head.append(badge,label);
const detail=el('div',undefined,'small error');detail.hidden=true;
const err=el('details',undefined,'err');err.hidden=true;const errText=el('div',undefined,'small');
err.append(el('summary','查看完整错误'),errText,button('复制错误',()=>copyText(row._err||'','已复制错误信息')));
const retry=button('重试本条',()=>retryOne(row._id)),copy=button('复制',()=>{if(row._file)copyLinks([row._file])});
retry.hidden=true;copy.hidden=true;
state.append(head,detail,err,retry,copy);
row.append(cell,name,kind,size,progress,state);
row._file=f;row._id=f.file_id;
return {row,check,name,kind,size,bar,pline,badge,label,detail,err,errText,retry,copy,snap:null,file:f};
}
function rowSnap(f,p){const exp=f.status==='exported'&&validLink(f);return [f.name||'',f.kind||'',f.size,f.status||'',!!f.queued,!!f.active,f.file_id===focusFile,chosen.has(f.file_id),p?p.bytes||0:0,p?p.total||0:0,p?p.action||'':0,p?p.speed||0:0,exp?Math.ceil((f.link_expires-Date.now()/1000)/60):0]}
function updateRow(e,f,p){
const s=rowSnap(f,p),o=e.snap;e.snap=s;e.file=f;e.row._file=f;e.row._id=f.file_id;
const ch=i=>!o||o[i]!==s[i];
if(ch(0)){e.name.textContent=f.name||'';e.check.setAttribute('aria-label','选择 '+f.name)}
if(ch(1))e.kind.textContent=KIND[f.kind]||'其他';
if(ch(2))e.size.textContent=bytes(f.size);
if(ch(4)||ch(5)||ch(6)||ch(7)){
 const cls=f.active?'running':f.queued?'queued':'';
 const foc=f.file_id===focusFile;e.row.className=cls?(foc?cls+' focus':cls):(foc?'focus':'');
 e.check.checked=chosen.has(f.file_id);e.check.disabled=!!f.active;
 e.check.title=f.active?'正在处理，请稍后或使用“暂停全部”':(f.queued?'排队中：勾选后可取消排队':'');
}
if(ch(8)||ch(9)||ch(10)||ch(11)){
 if(p&&p.total){e.bar.hidden=false;e.bar.value=pct(p.bytes||0,p.total);e.pline.textContent=pct(p.bytes||0,p.total)+'%'+(p.speed?' · '+bytes(p.speed)+'/秒':'')}
 else if(p){e.bar.hidden=true;e.pline.textContent=(p.action==='link'?'正在获取下载链接':'正在连接…')+(p.speed?' · '+bytes(p.speed)+'/秒':'')}
 else{e.bar.hidden=true;e.pline.textContent=f.status==='done'?'完成':'—'}
}
if(ch(3)||ch(4)||ch(5)||ch(7)||ch(12)){
 const active=!!p||!!f.active,failed=f.status==='failed';
 let text=STATUS[f.status]||f.status||'';
 if(active)text=(p&&p.action==='link')?'正在获取下载链接':'下载中';
 else if(f.queued)text='等待处理，可勾选后取消排队';
 else if(failed)text=(f.last_error||'').includes('无法确认')?'内容待确认':'失败';
 else if(f.status==='exported')text=validLink(f)?'链接已生成 · 剩余 '+Math.ceil((f.link_expires-Date.now()/1000)/60)+' 分钟':'下载链接已过期，请重新获取';
 e.badge.hidden=!(active||f.queued);e.badge.textContent=active?'处理中':(f.queued?'排队中':'');
 e.badge.className='badge'+(active?' run':'');
 e.label.textContent=text;e.label.className=failed?'error':'';
 e.detail.hidden=!(failed&&f.last_error);
 if(failed&&f.last_error)e.detail.textContent=firstLine(f.last_error);
 e.err.hidden=!(failed&&f.last_error);
 if(failed&&f.last_error){e.errText.textContent=f.last_error;e.row._err=f.last_error}
 const canRetry=failed&&!!f.last_error,canCopy=f.status==='exported'&&validLink(f);
 e.retry.hidden=!canRetry;e.copy.hidden=!canCopy;
}
return e;
}
async function copyText(text,ok){try{await navigator.clipboard.writeText(text);notify(ok||'已复制')}catch(e){const area=$('copy-fallback');area.hidden=false;area.value=text;area.focus();area.select();notify('无法自动复制，请全选文本框中的内容后复制')}}
async function retryOne(fileId){if(!albumId)return;const id=albumId,token=revision;const data=await api(albumPath(id,'enqueue'),{file_ids:[fileId],action:'download'});if(albumId!==id||revision!==token)return;notify('已提交重试');markDirty();await refresh()}
function renderFiles(){
if(!album)return;
const list=visibleFiles(),body=$('files'),scroller=$('files-scroll'),top=scroller?scroller.scrollTop:0,keep=document.activeElement;
const ids=new Set((album.files||[]).map(f=>f.file_id));for(const id of [...rowCache.keys()])if(!ids.has(id))rowCache.delete(id);
const act=new Map(((snapshot&&snapshot.active)||[]).filter(a=>a.album_id===albumId).map(a=>[a.file_id,a]));
const nodes=[];for(const f of list){let e=rowCache.get(f.file_id);if(!e){e=buildRow(f);rowCache.set(f.file_id,e)}updateRow(e,f,act.get(f.file_id)||f.progress||null);nodes.push(e.row)}
const key=list.map(f=>f.file_id).join('\u0001');
if(!list.length){
 if(!emptyRow){emptyRow=el('tr');const cell=el('td','没有符合条件的文件','empty-cell');cell.colSpan=6;emptyRow.dataset.empty='1';emptyRow.append(cell)}
 if(rowOrderKey!==''||body.children[0]!==emptyRow)body.replaceChildren(emptyRow);
 rowOrderKey='';
}else if(key!==rowOrderKey){rowOrderKey=key;body.replaceChildren(...nodes)}
if(scroller)scroller.scrollTop=top;
if(keep&&keep.focus&&document.activeElement!==keep)keep.focus();
$('file-count').textContent='文件 '+album.files.length+' 个 · 当前筛选 '+list.length+' 个';
updateSortMarks();renderSelection();
}
function renderSelection(){const list=visibleFiles(),checkable=list.filter(f=>!f.active),n=checkable.filter(f=>chosen.has(f.file_id)).length,picked=(album?.files||[]).filter(f=>chosen.has(f.file_id));$('select-visible').checked=!!checkable.length&&n===checkable.length;$('select-visible').indeterminate=n>0&&n<checkable.length;$('select-visible').disabled=!checkable.length;$('selection').hidden=!chosen.size;$('selected-count').textContent='已选 '+chosen.size+' 个';const queued=picked.filter(f=>f.queued),active=picked.filter(f=>f.active),blocked=queued.length>0||active.length>0,startable=picked.filter(f=>!f.queued&&!f.active&&['pending','failed','exported'].includes(f.status));$('cancel-queue').hidden=!queued.length;$('cancel-queue').textContent='取消排队 ('+queued.length+')';$('download').disabled=blocked||!startable.length;$('links').disabled=blocked||!startable.length;$('skip').disabled=blocked||!picked.length;$('mark-done').disabled=blocked||!picked.some(f=>f.local_path);$('purge-selected').disabled=blocked||!picked.length;$('copy').disabled=!picked.some(validLink);$('selection-note').textContent=active.length?'正在处理，请稍后或使用“暂停全部”':queued.length?'选中的文件在排队：取消排队后可改状态；正在处理的不能单独取消':picked.length&&!startable.length&&picked.every(f=>['done','skipped'].includes(f.status))?'选中的文件已完成或已跳过':''}
function errorText(e){const m=((e&&e.message)||'').trim();if(!m||/failed to fetch|networkerror|load failed|fetch failed/i.test(m))return '无法连接到本机服务，请确认程序仍在运行';return m}
function setPane(state,detail){
if(state===paneState&&(detail||'')===paneDetail)return;
paneState=state;paneDetail=detail||'';
if(state==='detail'){$('empty').hidden=true;$('detail').hidden=false;return}
$('empty').hidden=false;$('detail').hidden=true;
const title=$('empty-title'),hint=$('empty-hint'),actions=$('empty-actions'),retry=$('empty-retry'),paste=$('empty-paste');
actions.hidden=state==='loading';retry.hidden=state!=='error';paste.hidden=state==='error';
if(state==='loading'){title.textContent='正在读取相册…';hint.textContent='正在向本机服务获取文件列表';}
else if(state==='error'){title.textContent='相册读取失败';hint.textContent=detail||'暂时无法读取这个相册，请稍后重试';}
else{title.textContent='还没有选择相册';hint.textContent='在左侧粘贴相册链接或单文件链接，点“添加”开始；已有相册点标题即可打开。打开相册后可用“检查外部下载文件夹”，把已经下过或下了一半的文件登记进来。';}
}
async function retryAlbum(){
if(retrying||!albumId)return;
retrying=true;const btn=$('empty-retry');btn.disabled=true;btn.textContent='正在重试…';setPane('loading');
try{await loadAlbum()}finally{retrying=false;btn.disabled=false;btn.textContent='重试';if(!albumId)setPane('guide')}
}
function renderDetail(){if(!album)return;setPane('detail');$('title').textContent=album.title;$('overview').textContent='共 '+album.total+' 个文件 · 已完成 '+album.counts.done+' · 失败 '+album.counts.failed+' · '+album.percent+'%';$('album-progress').value=album.percent;$('retry').hidden=!album.counts.failed;$('retry').textContent='重试失败的 ('+album.counts.failed+')';renderFiles()}
async function loadAlbum(){if(!albumId)return;const id=albumId,token=revision,request=++albumRequest;let data;
try{data=await api(albumPath(id))}
catch(e){if(albumId===id&&token===revision&&request===albumRequest){if(album)notify('读取相册详情失败：'+errorText(e));else setPane('error',errorText(e))}return}
if(albumId!==id||token!==revision||request!==albumRequest||!data||data.album_id!==id)return;
album=data;const selectable=new Set(data.files.filter(f=>!f.active).map(f=>f.file_id));chosen=new Set([...chosen].filter(id=>selectable.has(id)));renderDetail()}
async function selectAlbum(id,file=null){revision++;albumId=id;album=null;chosen.clear();filter='all';$('filter').value='all';search='';$('search').value='';sortKey='';sortDir=1;folderTouched=false;focusFile=file;$('copy-fallback').hidden=true;$('check-box').hidden=true;$('check-result').textContent='';clearFileRows();setPane('loading');history.replaceState(null,'','#a='+encodeURIComponent(id));renderAlbums();await loadAlbum();if(albumId===id&&file){const row=Array.from($('files').children).find(r=>r.dataset.fileId===file);row?.scrollIntoView({block:'center'})}}
async function refresh(){const request=++stateRequest,data=await api('state?since='+since);if(request!==stateRequest)return;snapshot=data.snapshot;since=data.next;albums=data.albums;for(const event of data.events||[])if(event.type==='failed'&&Date.now()/1000-(event.time||0)<6){latestFailure=event;failureUntil=(event.time+6)*1000}if(localPausing&&!(snapshot.active||[]).length)localPausing=false;renderTop();renderAlbums();
if(albumId){const hot=!!((snapshot.active||[]).length||snapshot.queued||snapshot.stopping||snapshot.paused)||!!(album&&(album.files||[]).some(f=>!['done','skipped'].includes(f.status)));const wait=hot?900:10000;if(!album||Date.now()-detailAt>=wait){detailAt=Date.now();await loadAlbum()}}}
async function selectedAction(op,action){if(!albumId||!chosen.size)return;const id=albumId,token=revision,ids=[...chosen];const data=await api(albumPath(id,op),{file_ids:ids,...(action?{action}:{})});if(albumId!==id||revision!==token)return;if(op==='enqueue')notify('新增 '+data.added+' 个 · 已在队列 '+data.already+' 个 · 替换 '+data.replaced+' 个 · 正在处理 '+data.busy+' 个 · 不适用 '+data.not_applicable+' 个');else{chosen.clear();notify('已更新 '+data.updated+' 个文件')}markDirty();await refresh()}
async function cancelQueued(){if(!albumId||!chosen.size)return;const id=albumId,token=revision,ids=(album?.files||[]).filter(f=>chosen.has(f.file_id)&&f.queued).map(f=>f.file_id);if(!ids.length)return;const data=await api(albumPath(id,'cancel-queue'),{file_ids:ids});if(albumId!==id||revision!==token)return;chosen.clear();notify(data.message||('已取消 '+data.cancelled+' 个排队中的文件'));markDirty();await refresh()}
async function copyLinks(files){const id=albumId,token=revision,text=files.filter(validLink).map(f=>f.download_url).join('\n');if(!text){notify('选中的文件没有有效下载链接');return}try{await navigator.clipboard.writeText(text);if(albumId!==id||revision!==token)return;$('copy-fallback').hidden=true;notify('已复制下载链接')}catch(e){if(albumId!==id||revision!==token)return;const area=$('copy-fallback');area.hidden=false;area.value=text;area.focus();area.select();notify('无法自动复制，请全选文本框中的下载链接后复制')}}
async function pauseResume(resume=false){if(!resume&&!snapshot?.paused){localPausing=true;renderTop()}try{await api('queue/'+(resume||snapshot?.paused?'resume':'pause'),{});markDirty();await refresh()}catch(e){localPausing=false;renderTop();throw e}}
for(const [lane,max] of [['image',6],['video',3]]){const select=$(lane+'-concurrency');for(let i=1;i<=max;i++){const option=el('option',String(i));option.value=i;select.append(option)}select.value=lane==='image'?2:1;select.onchange=()=>run(async()=>{try{await api('queue/concurrency',{lane,value:Number(select.value)})}finally{markDirty();await refresh()}})}
$('pause').onclick=()=>run(()=>pauseResume());$('empty-paste').onclick=()=>{$('url').focus()};$('empty-retry').onclick=()=>run(retryAlbum);$('show-failed').onclick=()=>run(async()=>{const last=(snapshot?.recent_failures||[]).slice(-1)[0];if(last){await selectAlbum(last.album_id);if(albumId===last.album_id)setFilter('failed');return}if(album&&album.album_id===albumId&&album.counts.failed>0){setFilter('failed');return}const target=albums.find(a=>a.failed>0);if(!target)return;await selectAlbum(target.id);if(albumId===target.id)setFilter('failed')});
$('add').onsubmit=event=>{event.preventDefault();if(adding)return;const value=$('url').value.trim();if(!value){notify('请先粘贴相册链接或单文件链接');$('url').focus();return}
adding=true;const submit=$('add-submit');submit.disabled=true;submit.textContent='正在解析…';
return run(async()=>{try{const data=await api('albums',{url:value});if($('url').value.trim()===value)$('url').value='';await selectAlbum(data.album_id);await refresh()}finally{adding=false;submit.disabled=false;submit.textContent='添加'}})};
$('filter').onchange=()=>setFilter($('filter').value);
$('search').oninput=()=>{search=$('search').value.trim();focusFile=null;renderFiles()};
function updateSortMarks(){for(const [key,id,label] of [['name','sort-name','名称'],['size','sort-size','大小'],['status','sort-status','状态']])$(id).textContent=label+(sortKey===key?(sortDir>0?' ↑':' ↓'):'')}
function setSort(key){if(sortKey===key)sortDir=-sortDir;else{sortKey=key;sortDir=1}focusFile=null;renderFiles()}
$('sort-name').onclick=()=>setSort('name');$('sort-size').onclick=()=>setSort('size');$('sort-status').onclick=()=>setSort('status');$('select-visible').onchange=event=>{for(const f of visibleFiles())if(!f.active){if(event.target.checked)chosen.add(f.file_id);else chosen.delete(f.file_id)}renderFiles()};
$('cancel-queue').onclick=()=>run(cancelQueued);
// 清除记录：只删状态库里的记录，磁盘上已下载的文件一律不动。
function askClear(text){return window.confirm(text+'\n只清除记录，不会删除已下载的文件。')}
async function purgeAlbum(id,title){const detail=await api(albumPath(id));if(!askClear('清除相册「'+title+'」的全部记录？\n将清除 '+detail.total+' 个文件的记录，其中已完成 '+detail.counts.done+' 个。'))return;const data=await api(albumPath(id,'purge'),{});if(albumId===id){revision++;albumId=null;album=null;chosen.clear();focusFile=null;clearFileRows();setPane('guide');history.replaceState(null,'',location.pathname)}notify(data.message||('已清除 '+data.deleted+' 条记录'));markDirty();await refresh()}
async function purgeSelected(){if(!albumId||!chosen.size)return;const id=albumId,token=revision,picked=(album?.files||[]).filter(f=>chosen.has(f.file_id));if(!picked.length)return;if(!askClear('清除选中的 '+picked.length+' 条文件记录？\n其中已完成 '+picked.filter(f=>f.status==='done').length+' 个。'))return;const data=await api(albumPath(id,'files/delete'),{file_ids:picked.map(f=>f.file_id)});if(albumId!==id||revision!==token)return;chosen.clear();notify(data.message||('已清除 '+data.deleted+' 条记录'));markDirty();await refresh()}
$('purge-selected').onclick=()=>run(purgeSelected);$('download').onclick=()=>run(()=>selectedAction('enqueue','download'));$('links').onclick=()=>run(()=>selectedAction('enqueue','link'));$('skip').onclick=()=>run(()=>selectedAction('skip'));$('mark-done').onclick=()=>run(()=>selectedAction('mark-done'));$('handback').onclick=()=>run(()=>selectedAction('handback'));$('copy').onclick=()=>run(()=>copyLinks((album?.files||[]).filter(f=>chosen.has(f.file_id))));
$('retry').onclick=()=>run(async()=>{const id=albumId;await api(albumPath(id,'retry-failed'),{});if(albumId===id)markDirty();await refresh()});$('open').onclick=()=>run(()=>api(albumPath(albumId,'open-folder'),{}));$('check-toggle').onclick=()=>{$('check-box').hidden=!$('check-box').hidden};$('folder').oninput=()=>{folderTouched=true};
$('pick').onclick=()=>run(async()=>{const id=albumId,token=revision,data=await api('pick-folder',{initial:$('folder').value||null});if(albumId!==id||revision!==token)return;if(data.path){$('folder').value=data.path;folderTouched=true}else notify('没有选择文件夹，也可以手动输入路径')});
$('check').onclick=()=>run(async()=>{const id=albumId,token=revision,folder=$('folder').value.trim();if(!folder){notify('请选择或输入外部下载文件夹');return}const data=await api(albumPath(id,'check'),{folder});if(albumId!==id||revision!==token)return;folderTouched=false;await loadAlbum();if(albumId!==id||revision!==token)return;setFilter('failed');chosen=new Set(data.failed_ids||[]);renderFiles();const text='已完成 '+data.completed.length+' · 缺失 '+data.missing.length+' · 不完整 '+data.mismatched.length+' · 内容待确认 '+data.unverified.length+(chosen.size?'；已选中需要补下的文件，点“下载”即可补下':'');$('check-result').textContent=text;notify(text)});
async function tick(){if(polling)return;polling=true;try{await refresh()}catch(e){notify(e.message)}finally{polling=false;setTimeout(tick,snapshot&&((snapshot.active||[]).length||snapshot.paused||snapshot.queued||snapshot.stopping)?1000:3000)}}
const match=location.hash.match(/^#a=([A-Za-z0-9_-]+)$/);if(match)albumId=match[1];setPane(albumId?'loading':'guide');tick();
</script></body></html>'''
