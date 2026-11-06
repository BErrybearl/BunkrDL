"""Native window shell. Downloading and scheduling stay in the web service."""
from __future__ import annotations

import errno
import html
import importlib
import json
import os
import secrets
import socket
import sys
import threading
from pathlib import Path

from .guard import JobLock
from .models import JobBusy

TITLE = 'Bunkr 下载'
INSTALL_MESSAGE = '桌面窗口需要 pywebview，请运行：pip install pywebview'


def load_webview():
    try:
        return importlib.import_module('webview')
    except ImportError:
        raise RuntimeError(INSTALL_MESSAGE) from None


def notify(message):
    """Also works under pythonw, where stderr is None."""
    if sys.stderr is not None:
        print(message, file=sys.stderr)
    if os.name == 'nt':
        import ctypes
        ctypes.windll.user32.MessageBoxW(None, message, TITLE, 0x10)


def startup_message(error):
    if isinstance(error, JobBusy):
        return '命令行或其他网页任务正在运行，请先暂停或结束该任务，再重新打开桌面窗口。'
    return f'无法启动桌面窗口，请检查状态库路径、访问权限和配置。原因：{error}'


def startup_check(config):
    # Do not recover or mutate state until we know the task lock is available.
    from .store import Store
    with JobLock(config.state_db):
        store = Store(config.state_db)
        store.close()


def bind_server(config, factory=None):
    """Bind rather than probe: no check/bind race and no unrelated errors hidden."""
    if factory is None:
        from .web import make_server
        factory = make_server
    port = config.web_port
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError('桌面端口须为 1 至 65535 的整数')
    while port <= 65535:
        try:
            return factory(config, host='127.0.0.1', port=port)
        except OSError as error:
            if error.errno != errno.EADDRINUSE and getattr(error, 'winerror', None) != 10048:
                raise
            port += 1
    raise OSError('没有可用的本机端口')


class AppInstance:
    """Separate from the shared task lock; one desktop per state database."""
    def __init__(self, db_path):
        self.lock = JobLock(str(Path(db_path).expanduser().resolve()) + '.desktop')
        self.endpoint = Path(str(self.lock.path) + '.json')
        self.socket = None
        self.thread = None
        self.stop = threading.Event()

    def acquire(self):
        self.lock.__enter__()

    def listen(self, focus):
        token = secrets.token_hex(24)
        listener = socket.socket()
        self.socket = listener
        listener.bind(('127.0.0.1', 0))
        listener.listen(4)
        listener.settimeout(.2)
        self.endpoint.write_text(json.dumps({'port': listener.getsockname()[1], 'token': token}), encoding='utf-8')

        def run():
            while not self.stop.is_set():
                try:
                    conn, _ = listener.accept()
                    with conn:
                        conn.settimeout(1)
                        if conn.recv(128).decode('ascii') == token:
                            focus()
                            conn.sendall(b'ok')
                except Exception:
                    # A failed foreground request must not stop the owner.
                    if self.stop.is_set():
                        return
        self.thread = threading.Thread(target=run, daemon=True, name='bunkrdl-focus')
        self.thread.start()

    def activate(self):
        try:
            data = json.loads(self.endpoint.read_text(encoding='utf-8'))
            with socket.create_connection(('127.0.0.1', data['port']), timeout=2) as conn:
                conn.sendall(data['token'].encode('ascii'))
                return conn.recv(2) == b'ok'
        except (OSError, ValueError, KeyError, TypeError):
            return False

    def close(self):
        self.stop.set()
        if self.socket is not None:
            self.socket.close()
        if self.thread is not None:
            self.thread.join(2)
        if self.lock.handle is not None:
            try:
                self.endpoint.unlink(missing_ok=True)
            finally:
                self.lock.__exit__(None, None, None)


def close_action(snapshot, choice=None):
    """Pure decision function; unknown/dismissed choices always cancel."""
    if choice is not None:
        return {'pause': 'pause_exit', 'minimize': 'minimize'}.get(choice, 'cancel')
    if not snapshot['active'] and not snapshot['queued']:
        return 'exit'
    if choice is None:
        return 'confirm'
    return 'confirm'


def apply_close(action, *, pause, exit_app, minimize):
    """Small, independently testable effect dispatcher."""
    if action == 'pause_exit':
        pause()
        exit_app()
    elif action == 'exit':
        exit_app()
    elif action == 'minimize':
        minimize()


CONFIRM_SCRIPT = r"""(() => {
 if (document.getElementById('desktop-close')) return;
 const overlay=document.createElement('div'); overlay.id='desktop-close';
 overlay.style.cssText='position:fixed;inset:0;z-index:9999;background:#0008;display:grid;place-items:center';
 const panel=document.createElement('div');
 panel.style.cssText='background:white;color:#203047;padding:28px;border-radius:12px;max-width:620px;margin:20px';
 panel.setAttribute('role','dialog');panel.setAttribute('aria-modal','true');
 const title=document.createElement('h2');title.textContent='还有文件正在处理或排队';panel.append(title);
 const note=document.createElement('p');note.textContent='暂停后退出会保留队列，下次打开后点“继续”。';panel.append(note);
 for (const [value,label] of [['pause','暂停后退出'],['minimize','继续下载（最小化到任务栏）'],['cancel','取消']]) {
   const b=document.createElement('button');b.textContent=label;b.style.margin='6px';
   b.onclick=()=>{overlay.remove();window.pywebview.api.choose(value)};panel.append(b);
 }
 overlay.append(panel);document.body.append(overlay);panel.lastChild.focus();
})()"""


class Desktop:
    def __init__(self, server):
        self.server = server
        self.window = None
        self.exiting = False
        self.timer = None
        self.mutex = threading.Lock()
        self._checking = False

    def _exit(self):
        if self.exiting:
            return
        self.exiting = True
        # Covers stalled HTTP handlers as well as uninterruptible network reads.
        # OS process teardown releases both file locks. Startup recovery resets
        # interrupted work to queued and leaves the queue paused.
        self.timer = threading.Timer(20, os._exit, args=(0,))
        self.timer.daemon = True
        self.timer.start()
        self.server.coordinator.pause_all()
        self.server.shutdown()
        self.server.coordinator.close(timeout=15)
        self.server.server_close()
        self.window.destroy()

    def closing(self):
        if self.exiting:
            return True
        # WinForms calls this synchronously on the native UI thread. Even
        # evaluate_js waits for that thread, so defer the entire request.
        if not self._checking:
            self._checking = True
            threading.Thread(target=self._request_close, daemon=True,
                             name='bunkrdl-close').start()
        return False

    def _request_close(self):
        try:
            action = close_action(self.server.coordinator.snapshot())
            if action == 'confirm':
                self.window.evaluate_js(CONFIRM_SCRIPT)
            else:
                self._exit()
        finally:
            self._checking = False

    def choose(self, choice):
        with self.mutex:
            if self.exiting:
                return
            # An explicit choice no longer depends on the queue snapshot.
            # In particular, start the watchdog before acquiring scheduler
            # locks: an HTTP mutation may be stuck in slow I/O under that lock.
            action = close_action({}, choice)
            # _exit owns the pause, under the same 20-second deadline.
            apply_close(action, pause=lambda: None,
                        exit_app=self._exit, minimize=self.window.minimize)


class WindowAPI:
    """Only the close choice is exposed to JavaScript, not service internals."""
    def __init__(self, desktop):
        self._desktop = desktop

    def choose(self, choice):
        self._desktop.choose(choice)


def create_window(webview, **kwargs):
    """Thin GUI boundary; tests replace it with a fake."""
    return webview.create_window(TITLE, width=1280, height=820, min_size=(900, 600), **kwargs)


def run(config):
    try:
        webview = load_webview()
    except RuntimeError as error:
        notify(str(error))
        return 1
    # Do not silently fall back to the legacy IE engine: the existing page
    # requires modern JavaScript, and Windows 10 uses WebView2.
    gui_options = {'gui': 'edgechromium'} if os.name == 'nt' else {}
    instance = AppInstance(config.state_db)
    try:
        instance.acquire()
    except JobBusy:
        if not instance.activate():
            notify('Bunkr 下载已经打开，暂时无法唤起窗口。请从任务栏打开已有窗口；若仍在启动，请稍后再试。')
        return 0
    except OSError as error:
        notify(startup_message(error))
        return 1
    server = thread = desktop = None
    try:
        try:
            startup_check(config)
            server = bind_server(config)
        except Exception as error:
            create_window(webview, html='<meta charset="utf-8"><h2>无法启动</h2><p>' + html.escape(startup_message(error)) + '</p>')
            webview.start(**gui_options)
            return 1
        thread = threading.Thread(target=server.serve_forever, daemon=True, name='bunkrdl-web')
        thread.start()
        desktop = Desktop(server)
        window = create_window(webview, url=f'http://127.0.0.1:{server.server_port}/', js_api=WindowAPI(desktop))
        desktop.window = window
        window.events.closing += desktop.closing

        def ready():
            if server.server_port != 8765:
                window.set_title(f'{TITLE} · 端口 {server.server_port}')

            def focus():
                window.restore()
                window.show()
            instance.listen(focus)
        webview.start(ready, **gui_options)
        return 0
    except Exception as error:
        notify('桌面窗口启动失败，请检查 WebView2 Runtime 是否安装。原因：' + str(error))
        return 1
    finally:
        if server is not None:
            if desktop is not None and desktop.timer is None:
                desktop.timer = threading.Timer(20, os._exit, args=(1,))
                desktop.timer.daemon = True
                desktop.timer.start()
            if thread is not None:
                server.shutdown()
                thread.join(2)
            server.server_close()
        instance.close()
        if desktop is not None and desktop.timer is not None:
            desktop.timer.cancel()
