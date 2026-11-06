"""Offline desktop contracts. Never import/start the real GUI."""
import os
import tempfile

os.environ['BUNKRDL_HOME'] = os.path.join(tempfile.gettempdir(), f'bunkrdl-tests-{os.getpid()}')

import errno
import socket
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from bunkrdl import launcher
from bunkrdl.config import Config
from bunkrdl.guard import JobLock
from bunkrdl.models import JobBusy


class Decisions(unittest.TestCase):
    def test_idle_exit(self):
        self.assertEqual(launcher.close_action(dict(active=[], queued=0)), 'exit')

    def test_queued_confirm_even_paused(self):
        self.assertEqual(launcher.close_action(dict(active=[], queued=1, paused=True)), 'confirm')

    def test_active_confirm(self):
        self.assertEqual(launcher.close_action(dict(active=[{}], queued=0)), 'confirm')

    def test_choices(self):
        for choice, expected in [('pause', 'pause_exit'), ('minimize', 'minimize'), ('cancel', 'cancel'), ('oops', 'cancel')]:
            with self.subTest(choice=choice):
                self.assertEqual(launcher.close_action(dict(active=[{}], queued=1), choice), expected)

    def test_cancel_still_cancels_after_last_item_finishes(self):
        self.assertEqual(launcher.close_action(dict(active=[], queued=0), 'cancel'), 'cancel')

    def test_minimize_still_minimizes_after_last_item_finishes(self):
        self.assertEqual(launcher.close_action(dict(active=[], queued=0), 'minimize'), 'minimize')

    def test_pause_before_exit(self):
        trace = []
        launcher.apply_close('pause_exit', pause=lambda: trace.append('pause'),
                             exit_app=lambda: trace.append('exit'), minimize=lambda: trace.append('minimize'))
        self.assertEqual(trace, ['pause', 'exit'])

    def test_minimize_and_cancel(self):
        pause, exit_app, minimize = Mock(), Mock(), Mock()
        launcher.apply_close('minimize', pause=pause, exit_app=exit_app, minimize=minimize)
        launcher.apply_close('cancel', pause=pause, exit_app=exit_app, minimize=minimize)
        minimize.assert_called_once()
        pause.assert_not_called()
        exit_app.assert_not_called()


class Contracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Config(state_db=str(Path(self.temp.name) / 'state.db'))

    def test_missing_webview(self):
        with patch.object(launcher.importlib, 'import_module', side_effect=ModuleNotFoundError('webview')):
            with self.assertRaisesRegex(RuntimeError, 'pip install pywebview'):
                launcher.load_webview()

    def test_missing_dependency_run_is_chinese_and_no_traceback(self):
        with patch.object(launcher, 'load_webview', side_effect=RuntimeError(launcher.INSTALL_MESSAGE)), patch.object(launcher, 'notify') as notify:
            self.assertEqual(launcher.run(self.config), 1)
            notify.assert_called_once_with(launcher.INSTALL_MESSAGE)

    def test_startup_task_lock(self):
        with JobLock(self.config.state_db):
            with self.assertRaises(JobBusy) as caught:
                launcher.startup_check(self.config)
        self.assertIn('命令行', launcher.startup_message(caught.exception))
        self.assertFalse(Path(self.config.state_db).exists())

    def test_startup_database_error(self):
        self.config.state_db = self.temp.name  # a directory cannot be opened as SQLite
        with self.assertRaises(Exception) as caught:
            launcher.startup_check(self.config)
        self.assertIn('状态库路径', launcher.startup_message(caught.exception))

    def test_startup_ok_releases_task_lock(self):
        launcher.startup_check(self.config)
        with JobLock(self.config.state_db):
            pass

    def test_instance_conflict_and_release(self):
        first, second = (launcher.AppInstance(self.config.state_db) for _ in range(2))
        first.acquire()
        try:
            with self.assertRaises(JobBusy):
                second.acquire()
            # Desktop lock never claims the download task lock.
            with JobLock(self.config.state_db):
                pass
        finally:
            first.close()
        second.acquire()
        second.close()

    def test_instance_focus_notification(self):
        first = launcher.AppInstance(self.config.state_db)
        first.acquire()
        self.addCleanup(first.close)
        focused = threading.Event()
        first.listen(focused.set)
        self.assertTrue(launcher.AppInstance(self.config.state_db).activate())
        self.assertTrue(focused.wait(1))

    def test_stale_instance_endpoint(self):
        self.assertFalse(launcher.AppInstance(self.config.state_db).activate())

    def test_occupied_port_selects_next_real_port(self):
        # Keep the original socket bound throughout, so this checks actual binding.
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1', 0))
            occupied.listen()
            start = occupied.getsockname()[1]
            self.config.web_port = start
            selected = []

            def bind(config, host, port):
                sock = socket.socket()
                try:
                    sock.bind((host, port))
                except OSError:
                    sock.close()
                    raise
                selected.append(sock)
                return SimpleNamespace(server_port=port)
            try:
                server = launcher.bind_server(self.config, bind)
                self.assertGreaterEqual(server.server_port, start + 1)
            finally:
                for sock in selected:
                    sock.close()

    def test_unrelated_bind_error_is_not_retried(self):
        factory = Mock(side_effect=PermissionError(errno.EACCES, 'denied'))
        with self.assertRaises(PermissionError):
            launcher.bind_server(self.config, factory)
        factory.assert_called_once()

    def test_port_exhaustion(self):
        self.config.web_port = 65535
        with self.assertRaisesRegex(OSError, '没有可用'):
            launcher.bind_server(self.config, Mock(side_effect=OSError(errno.EADDRINUSE, 'busy')))

    def test_invalid_port(self):
        for port in (0, -1, True, '8765', 65536):
            self.config.web_port = port
            with self.assertRaises(ValueError):
                launcher.bind_server(self.config, Mock())

    def test_error_shown_in_fake_window(self):
        view = Mock()
        with patch.object(launcher, 'load_webview', return_value=view), patch.object(launcher, 'startup_check', side_effect=JobBusy()):
            self.assertEqual(launcher.run(self.config), 1)
        self.assertIn('命令行', view.create_window.call_args.kwargs['html'])
        view.start.assert_called_once_with(**({'gui': 'edgechromium'} if os.name == 'nt' else {}))
        with launcher.AppInstance(self.config.state_db).lock:
            pass

    def test_duplicate_does_not_start_service(self):
        first = launcher.AppInstance(self.config.state_db)
        first.acquire()
        try:
            with patch.object(launcher, 'load_webview', return_value=Mock()), patch.object(launcher, 'bind_server') as bind, patch.object(launcher, 'notify') as notify:
                self.assertEqual(launcher.run(self.config), 0)
            bind.assert_not_called()
            self.assertIn('已经打开', notify.call_args.args[0])
        finally:
            first.close()

    def test_close_overlay_and_minimize_with_fake_window(self):
        server = Mock()
        server.coordinator.snapshot.return_value = dict(active=[{}], queued=1)
        desktop = launcher.Desktop(server)
        desktop.window = Mock()
        desktop._request_close()
        desktop.window.evaluate_js.assert_called_once_with(launcher.CONFIRM_SCRIPT)
        desktop.choose('cancel')
        desktop.choose('minimize')
        desktop.window.minimize.assert_called_once()
        server.coordinator.pause_all.assert_not_called()
        server.coordinator.snapshot.assert_called_once()

    def test_native_close_never_calls_javascript_on_ui_thread(self):
        desktop = launcher.Desktop(Mock())
        desktop.window = Mock()
        with patch.object(launcher.threading, 'Thread') as thread:
            self.assertFalse(desktop.closing())
        desktop.window.evaluate_js.assert_not_called()
        thread.return_value.start.assert_called_once()
        self.assertEqual(thread.call_args.kwargs['target'], desktop._request_close)

    def test_shutdown_order_and_deadline(self):
        trace = []
        server = Mock()
        server.coordinator.pause_all.side_effect = lambda: trace.append('pause')
        server.shutdown.side_effect = lambda: trace.append('shutdown')
        server.coordinator.close.side_effect = lambda **kw: trace.append('coordinator')
        server.server_close.side_effect = lambda: trace.append('server')
        desktop = launcher.Desktop(server)
        desktop.window = Mock()
        desktop.window.destroy.side_effect = lambda: trace.append('destroy')
        with patch.object(launcher.threading, 'Timer') as timer:
            desktop._exit()
        self.assertEqual(trace, ['pause', 'shutdown', 'coordinator', 'server', 'destroy'])
        self.assertEqual(timer.call_args.args[:2], (20, os._exit))
        self.assertTrue(desktop.closing())

    def test_real_service_stops_and_releases_locks_without_gui(self):
        # Process isolation proves no non-daemon Python thread keeps the app alive.
        code = r'''
import sys, threading
from unittest.mock import Mock
from bunkrdl.config import Config
from bunkrdl import launcher
cfg=Config(state_db=sys.argv[1], web_port=0)
from bunkrdl.web import make_server
instance=launcher.AppInstance(cfg.state_db);instance.acquire()
server=make_server(cfg, port=0)
thread=threading.Thread(target=server.serve_forever);thread.start()
desktop=launcher.Desktop(server);desktop.window=Mock()
desktop._exit();thread.join(2);instance.close();desktop.timer.cancel()
assert not thread.is_alive()
'''
        result = subprocess.run([sys.executable, '-c', code, self.config.state_db],
                                env={**os.environ, 'BUNKRDL_HOME': self.temp.name},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        with JobLock(self.config.state_db), launcher.AppInstance(self.config.state_db).lock:
            pass

    def test_run_with_real_service_and_fake_window(self):
        from bunkrdl.web import make_server
        server = make_server(self.config, port=0)
        view = Mock()
        window = MagicMock()
        view.create_window.return_value = window

        def start(ready, **kwargs):
            if os.name == 'nt':
                self.assertEqual(kwargs['gui'], 'edgechromium')
            ready()
            bridge = view.create_window.call_args.kwargs['js_api']
            bridge.choose('pause')
        view.start.side_effect = start
        with patch.object(launcher, 'load_webview', return_value=view), patch.object(launcher, 'bind_server', return_value=server):
            self.assertEqual(launcher.run(self.config), 0)
        kwargs = view.create_window.call_args.kwargs
        self.assertEqual(kwargs['url'], f'http://127.0.0.1:{server.server_port}/')
        self.assertEqual((kwargs['width'], kwargs['height']), (1280, 820))
        window.set_title.assert_called_once_with(f'Bunkr 下载 · 端口 {server.server_port}')
        window.destroy.assert_called_once()
        with JobLock(self.config.state_db), launcher.AppInstance(self.config.state_db).lock:
            pass

    def test_cli_routes_app_before_opening_store(self):
        from bunkrdl import cli
        with patch.object(launcher, 'run', return_value=0) as run:
            self.assertEqual(cli.main(['--db', self.config.state_db, 'app']), 0)
        self.assertEqual(run.call_args.args[0].state_db, self.config.state_db)
        self.assertFalse(Path(self.config.state_db).exists())

    def test_active_work_requeues_and_restarts_paused(self):
        from bunkrdl.models import Album, AlbumFile
        from bunkrdl.store import Store
        from bunkrdl.web import make_server
        from bunkrdl.worktypes import ItemResult, INTERRUPTED
        store = Store(self.config.state_db)
        store.upsert_album(Album('a', '测试', 'https://example.test/a/a',
                                  [AlbumFile('1', 'a.jpg', '1', '/f/1')]))
        store.close()
        server = make_server(self.config, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started = threading.Event()

        def op(ctx, album, file):
            started.set()
            ctx.stop_event.wait(5)
            return ItemResult(INTERRUPTED)
        server.coordinator.ops = SimpleNamespace(download_once=op)
        server.coordinator.client_factory = lambda *args, **kwargs: Mock()
        desktop = launcher.Desktop(server)
        desktop.window = Mock()
        try:
            server.coordinator.enqueue('a', ['1'], 'download')
            self.assertTrue(started.wait(2))
            desktop.choose('pause')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)
            if desktop.timer:
                desktop.timer.cancel()
        restarted = make_server(self.config, port=0)
        try:
            self.assertTrue(restarted.coordinator.snapshot()['paused'])
            self.assertEqual(restarted.coordinator.snapshot()['queued'], 1)
            self.assertEqual(restarted.store.list_work_items()[0].state, 'queued')
        finally:
            restarted.server_close()

    def test_forced_exit_releases_locks_and_recovers_queue(self):
        # Speed up only the watchdog clock in an isolated child, not the app's
        # configured 20-second deadline. No GUI or network I/O is involved.
        code = r'''
import sys, threading
from unittest.mock import Mock
from bunkrdl import launcher
from bunkrdl.guard import JobLock
from bunkrdl.models import Album, AlbumFile
from bunkrdl.store import Store
store=Store(sys.argv[1])
store.upsert_album(Album('a','测试','https://example.test/a/a',[AlbumFile('1','a.jpg','1','/f/1')]))
work=store.add_work_items([('a','1','download','image')])[0]
store.update_work_item(work.work_id,state='running')
store.close()
instance=launcher.AppInstance(sys.argv[1]);instance.acquire()
lock=JobLock(sys.argv[1]);lock.__enter__()
timer=threading.Timer
launcher.threading.Timer=lambda seconds, callback, args: timer(.2,callback,args=args)
server=Mock();server.shutdown.side_effect=lambda: threading.Event().wait()
desktop=launcher.Desktop(server);desktop.window=Mock();desktop._exit()
raise AssertionError('watchdog did not exit')
'''
        result = subprocess.run([sys.executable, '-c', code, self.config.state_db],
                                env={**os.environ, 'BUNKRDL_HOME': self.temp.name},
                                capture_output=True, text=True, timeout=8)
        self.assertEqual(result.returncode, 0, result.stderr)
        with launcher.AppInstance(self.config.state_db).lock:
            pass
        from bunkrdl.web import make_server
        server = make_server(self.config, port=0)
        try:
            self.assertTrue(server.coordinator.snapshot()['paused'])
            self.assertEqual(server.store.list_work_items()[0].state, 'queued')
        finally:
            server.server_close()


if __name__ == '__main__':
    unittest.main()
