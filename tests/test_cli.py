"""Offline CLI contract tests and shared test doubles."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import contextlib
import io
import signal
import sys
import types
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from bunkrdl import cli
from bunkrdl.config import Config
from bunkrdl.guard import JobLock
from bunkrdl.models import (Album, AlbumSummary, ChallengeRequired, ExportResult,
                            FileRecord, QueueResult, ReconcileResult)


class FakeStore:
    dests = None

    def get_album_dest(self, album_id):
        return (self.dests or {}).get(album_id)

    def set_album_dest(self, album_id, path):
        self.dests = {**(self.dests or {}), album_id: path}

    def __init__(self):
        self.album = Album("abc", '测:试/相册', "https://bunkr.cr/a/abc")
        self.records = [
            FileRecord("1", "a.mp4", "a", "https://example/f/a", kind="video",
                       album_id="abc", status="failed", attempts=4, last_error="失败"),
            FileRecord("2", "b.jpg", "b", "https://example/f/b", kind="image", album_id="abc"),
            FileRecord("3", "c.mp4", "c", "https://example/f/c", kind="video",
                       album_id="abc", status="exported"),
            FileRecord("4", "d.bin", "d", "https://example/f/d", kind="other",
                       album_id="abc", status="skipped"),
            FileRecord("5", "e.jpg", "e", "https://example/f/e", kind="image",
                       album_id="abc", status="done"),
        ]
        self.closed = False

    def get_album(self, ident):
        return self.album if self.album and self.album.album_id == ident else None

    def upsert_album(self, album):
        self.album = album

    def list_albums(self, include_hidden=False):
        if not self.album:
            return []
        counts = {s: sum(r.status == s for r in self.records) for s in {r.status for r in self.records}}
        return [AlbumSummary(self.album.album_id, self.album.title, self.album.url, counts)]

    def list_files(self, ident, status=None, kinds=None):
        return [replace(r) for r in self.records if r.album_id == ident and
                (status is None or r.status == status) and (kinds is None or r.kind in kinds)]

    def update_files(self, updates):
        for album_id, file_id, values in updates:
            self.update_file(album_id, file_id, **values)

    def update_file(self, ident, file_id, **fields):
        record = next(r for r in self.records if r.album_id == ident and r.file_id == file_id)
        for key, value in fields.items():
            setattr(record, key, value)

    def recover_interrupted(self):
        for record in self.records:
            if record.status == "downloading":
                record.status = "pending"

    def close(self):
        self.closed = True


def fake_select_ids(store, album_id, preset):
    """The shared selection contract, computed from the fake store in page order."""
    records = store.list_files(album_id)
    if preset == "images":
        chosen = [r for r in records if r.kind in ("image", "other")]
    elif preset == "videos":
        chosen = [r for r in records if r.kind == "video"]
    elif preset == "unfinished":
        chosen = [r for r in records if r.status not in ("done", "skipped")]
    elif preset == "failed":
        chosen = [r for r in records if r.status == "failed"]
    else:
        raise ValueError("未知选择：" + preset)
    return [r.file_id for r in chosen]


def fake_modules(store):
    client = Mock()
    client.get_album.return_value = Album("abc", "测:试/相册", "https://bunkr.cr/a/abc")
    download_url = Mock(side_effect=lambda record, config, names=None:
                        f"https://cdn.test/{record.file_id}?n={record.name}")
    specs = {"resolver": {"BunkrClient": Mock(return_value=client)},
             "store": {"Store": Mock(return_value=store)},
             "downloader": {"run_queue": Mock(return_value=QueueResult(done=1))},
             "export": {"export_links": Mock(return_value=ExportResult(exported=1, out_dir="输出目录")),
                        "download_url": download_url},
             "reconcile": {"reconcile": Mock(return_value=ReconcileResult(completed=["1"]))}}
    modules = {}
    for name, attrs in specs.items():
        module = types.ModuleType("bunkrdl." + name)
        module.__dict__.update(attrs)
        modules[module.__name__] = module
    return modules


class CliTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name) / "迅雷目录"
        self.folder.mkdir()
        self.store = FakeStore()
        self.modules = fake_modules(self.store)
        self.config = Config(state_db=str(Path(self.tmp.name) / "state.db"), download_root="downloads",
                             request_interval=0, file_delay_min=0, file_delay_max=0,
                             batch_pause=0, cooldown=0)
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(sys.modules, self.modules))
        self.stack.enter_context(patch.object(cli, "load_config", return_value=self.config))
        self.output = self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.errors = self.stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
        # flows is imported by cli at module load; patch the shared functions it calls.
        self.real_album_dest = cli.flows.album_dest
        for name in ("select_ids", "download_files", "make_links", "check_folder",
                     "hand_back", "latest_links", "album_dest"):
            self.stack.enter_context(patch.object(cli.flows, name, create=True, new=Mock()))
        cli.flows.select_ids.side_effect = fake_select_ids
        cli.flows.album_dest.side_effect = lambda config, album, store=None: str(
            Path(config.download_root) / "测试相册")
        cli.flows.download_files.return_value = QueueResult(done=1)
        cli.flows.make_links.return_value = ExportResult(exported=2, out_dir="链接目录")
        cli.flows.latest_links.return_value = {"links": ["https://cdn/1", "https://cdn/2"],
                                               "earliest_expiry": 2000000000,
                                               "export_dir": "链接目录"}
        cli.flows.check_folder.return_value = ReconcileResult(completed=["1"])
        cli.flows.hand_back.return_value = 2

    def run_cli(self, *args):
        for stream in (self.output, self.errors):
            stream.seek(0)
            stream.truncate(0)
        code = cli.main(list(args))
        return code, self.output.getvalue(), self.errors.getvalue()


class NewCommandTests(CliTestCase):
    def test_get_defaults_to_all_unfinished(self):
        code, out, _ = self.run_cli("get", "https://bunkr.cr/a/abc")
        self.assertEqual(code, 0)
        cli.flows.select_ids.assert_called_once_with(self.store, "abc", "unfinished")
        call = cli.flows.download_files.call_args
        self.assertEqual(call.args[2], "abc")
        self.assertEqual(call.args[3], str(Path("downloads") / "测试相册"))
        self.assertEqual(call.args[5], ["1", "2", "3"])
        self.assertFalse(call.kwargs["stop_event"].is_set())
        self.assertEqual(out.strip().splitlines()[0], "准备下载 3 个文件")
        self.assertIn("完成 1", out)

    def test_get_images_intersects_with_unfinished(self):
        self.assertEqual(self.run_cli("get", "abc", "--images")[0], 0)
        self.assertEqual([c.args[2] for c in cli.flows.select_ids.call_args_list],
                         ["images", "unfinished"])
        self.assertEqual(cli.flows.download_files.call_args.args[5], ["2"])

    def test_get_videos_intersects_with_unfinished(self):
        self.assertEqual(self.run_cli("get", "abc", "--videos")[0], 0)
        self.assertEqual([c.args[2] for c in cli.flows.select_ids.call_args_list],
                         ["videos", "unfinished"])
        self.assertEqual(cli.flows.download_files.call_args.args[5], ["1", "3"])

    def test_get_reports_nothing_to_do(self):
        for record in self.store.records:
            record.status = "done"
        code, out, _ = self.run_cli("get", "abc")
        self.assertEqual(code, 0)
        self.assertIn("没有需要下载的文件", out)
        cli.flows.download_files.assert_not_called()

    def test_get_adds_missing_album_then_downloads(self):
        self.store.album = None
        self.assertEqual(self.run_cli("get", "https://bunkr.cr/a/abc")[0], 0)
        self.modules["bunkrdl.resolver"].BunkrClient.return_value.get_album.assert_called_once_with(
            "https://bunkr.cr/a/abc")
        self.assertIsNotNone(self.store.album)

    def test_links_defaults_to_videos_not_exported_and_prints_next_steps(self):
        code, out, _ = self.run_cli("links", "https://bunkr.cr/a/abc")
        self.assertEqual(code, 0)
        call = cli.flows.make_links.call_args
        self.assertEqual(call.args[2], "abc")
        self.assertEqual(call.args[5], ["1"])  # video "3" is already exported
        self.assertFalse(call.kwargs["stop_event"].is_set())
        for text in ("链接目录", "链接数：2", "最早过期时间：",
                     "用迅雷、IDM 等下载工具新建任务，粘贴 part-01-links.txt 里的链接",
                     'python -m bunkrdl check "https://bunkr.cr/a/abc" "<外部下载文件夹>"'):
            self.assertIn(text, out)
        self.assertNotIn("download_url", out)

    def test_links_uses_store_backed_album_dest(self):
        self.assertEqual(self.run_cli("links", "abc")[0], 0)
        self.assertEqual(cli.flows.album_dest.call_args.args, (self.config, self.store.album, self.store))
        self.assertEqual(cli.flows.make_links.call_args.args[3], str(Path("downloads") / "测试相册"))

    def test_links_print_prints_download_url_per_file(self):
        code, out, _ = self.run_cli("links", "abc", "--print")
        self.assertEqual(code, 0)
        download_url = self.modules["bunkrdl.export"].download_url
        download_url.assert_called_once()
        record, config, names = download_url.call_args.args
        self.assertEqual(record.file_id, "1")
        self.assertIs(config, self.config)
        self.assertEqual(names["1"], "a.mp4")
        lines = out.splitlines()
        self.assertIn("https://cdn.test/1?n=a.mp4", lines)
        self.assertNotIn("https://cdn/1", lines)
        self.assertIn("链接数：2", out)

    def test_links_without_print_does_not_call_download_url(self):
        self.assertEqual(self.run_cli("links", "abc")[0], 0)
        self.modules["bunkrdl.export"].download_url.assert_not_called()

    def test_links_print_skips_files_without_a_link(self):
        def url(record, config, names=None):
            if record.file_id == "1":
                raise ValueError("还没有生成下载链接")
            return "https://cdn.test/" + record.file_id

        self.modules["bunkrdl.export"].download_url.side_effect = url
        code, out, _ = self.run_cli("links", "abc", "--print")
        self.assertEqual(code, 0)
        self.assertNotIn("还没有生成下载链接", out)

    def test_links_limit_takes_first_n_in_album_order(self):
        for record in self.store.records:
            if record.kind == "video":
                record.status = "pending"
        self.assertEqual(self.run_cli("links", "abc", "--limit", "2")[0], 0)
        self.assertEqual(cli.flows.make_links.call_args.args[5], ["1", "3"])

    def test_links_all_includes_images_but_not_done_or_skipped(self):
        self.assertEqual(self.run_cli("links", "abc", "--all")[0], 0)
        self.assertEqual(cli.flows.make_links.call_args.args[5], ["1", "2"])
        cli.flows.select_ids.assert_called_once_with(self.store, "abc", "unfinished")

    def test_links_nothing_to_do_exit_zero(self):
        for record in self.store.records:
            record.status = "done"
        code, out, _ = self.run_cli("links", "abc")
        self.assertEqual(code, 0)
        self.assertIn("没有需要生成链接的文件", out)
        cli.flows.make_links.assert_not_called()

    def test_links_failure_and_challenge_exit_codes(self):
        cli.flows.make_links.return_value = ExportResult(exported=1, failed=1, out_dir="链接目录")
        self.assertEqual(self.run_cli("links", "abc")[0], 1)
        cli.flows.make_links.return_value = ExportResult(aborted_reason="SECRET english")
        code, out, err = self.run_cli("links", "abc")
        self.assertEqual(code, 3)
        self.assertIn("人机验证", err)
        self.assertNotIn("SECRET", out + err)

    def test_check_without_redownload_prints_counts_and_hint(self):
        cli.flows.check_folder.return_value = ReconcileResult(
            completed=["1"], missing=["2"], mismatched=["3"], unverified=["4"])
        code, out, _ = self.run_cli("check", "abc", str(self.folder))
        self.assertEqual(code, 1)
        cli.flows.check_folder.assert_called_once_with(self.store, "abc", self.folder)
        self.assertIn("已完成 1，没下完 1，不完整 1，需要你确认 1", out)
        for name in ("a.mp4", "b.jpg", "c.mp4", "d.bin"):
            self.assertIn(name, out)
        self.assertIn("--redownload 可由本工具补下", out)
        self.assertIn('python -m bunkrdl check "abc"', out)
        cli.flows.download_files.assert_not_called()

    def test_check_redownload_downloads_missing_and_mismatched(self):
        cli.flows.check_folder.return_value = ReconcileResult(
            completed=["1"], missing=["2"], mismatched=["3"], unverified=["4"])
        cli.flows.download_files.return_value = QueueResult(done=2)
        code, out, _ = self.run_cli("check", "abc", str(self.folder), "--redownload")
        self.assertEqual(code, 0)
        self.assertEqual(cli.flows.download_files.call_args.args[5], ["2", "3"])
        self.assertIn("需要补下 2 个文件", out)
        self.assertIn("补下结束：完成 2", out)
        self.assertNotIn("--redownload 可由本工具补下", out)

    def test_check_redownload_with_nothing_missing_does_not_download(self):
        cli.flows.check_folder.return_value = ReconcileResult(completed=["1"])
        code, out, _ = self.run_cli("check", "abc", str(self.folder), "--redownload")
        self.assertEqual(code, 0)
        self.assertIn("没有需要补下的文件", out)
        cli.flows.download_files.assert_not_called()

    def test_check_all_complete_and_unverified_only(self):
        code, out, _ = self.run_cli("check", "abc", str(self.folder))
        self.assertEqual(code, 0)
        self.assertIn("全部已下好", out)
        cli.flows.check_folder.return_value = ReconcileResult(unverified=["4"])
        code, out, _ = self.run_cli("check", "abc", str(self.folder))
        self.assertEqual(code, 0)
        self.assertNotIn("全部已下好", out)
        self.assertIn("需要你确认", out)

    def test_check_does_not_create_client_without_redownload(self):
        self.run_cli("check", "abc", str(self.folder))
        self.modules["bunkrdl.resolver"].BunkrClient.assert_not_called()

    def test_handback_calls_flow_and_reports_count(self):
        code, out, _ = self.run_cli("handback", "abc", "2", "3")
        self.assertEqual(code, 0)
        cli.flows.hand_back.assert_called_once_with(self.store, "abc", ["2", "3"])
        self.assertIn("已交回本工具下载：2 个文件", out)
        self.modules["bunkrdl.resolver"].BunkrClient.assert_not_called()

    def test_check_and_handback_require_added_album(self):
        self.store.album = None
        self.assertEqual(self.run_cli("check", "abc", str(self.folder))[0], 2)
        self.assertEqual(self.run_cli("handback", "abc", "1")[0], 2)
        cli.flows.check_folder.assert_not_called()
        cli.flows.hand_back.assert_not_called()
        self.modules["bunkrdl.resolver"].BunkrClient.assert_not_called()

    def test_get_thunder_option_is_gone(self):
        code, _, err = self.run_cli("get", "abc", "--thunder")
        self.assertEqual(code, 2)
        self.assertIn("参数错误", err)
        cli.flows.download_files.assert_not_called()


class ExitCodeTests(CliTestCase):
    def test_failed_files_exit_one(self):
        cli.flows.download_files.return_value = QueueResult(failed=2)
        self.assertEqual(self.run_cli("get", "abc")[0], 1)
        cli.flows.check_folder.return_value = ReconcileResult(missing=["2"])
        cli.flows.download_files.return_value = QueueResult(failed=1)
        self.assertEqual(self.run_cli("check", "abc", str(self.folder), "--redownload")[0], 1)

    def test_challenge_exception_and_aborted_queue_exit_three(self):
        self.modules["bunkrdl.resolver"].BunkrClient.return_value.get_album.side_effect = \
            ChallengeRequired("SECRET english")
        code, out, err = self.run_cli("add", "https://bunkr.cr/a/xyz")
        self.assertEqual(code, 3)
        self.assertIn("人机验证", err)
        self.assertNotIn("SECRET", out + err)
        self.modules["bunkrdl.resolver"].BunkrClient.return_value.get_album.side_effect = None
        cli.flows.download_files.return_value = QueueResult(aborted_reason="challenge")
        self.assertEqual(self.run_cli("get", "abc")[0], 3)

    def test_stopped_queue_exit_zero(self):
        cli.flows.download_files.return_value = QueueResult(stopped=True)
        code, out, _ = self.run_cli("get", "abc")
        self.assertEqual(code, 0)
        self.assertIn("已停止，可再次运行继续", out)
        cli.flows.make_links.return_value = ExportResult(stopped=True)
        self.assertEqual(self.run_cli("links", "abc")[0], 0)

    def test_argument_errors(self):
        for args in ([], ["unknown"], ["download", "abc", "--kinds", "bad"], ["add", "../bad"],
                     ["web", "--port", "bad"], ["links", "abc", "--limit", "0"],
                     ["links", "abc", "--limit", "abc"], ["get", "abc", "--images", "--videos"]):
            with self.subTest(args=args):
                self.assertEqual(self.run_cli(*args)[0], 2)
        self.modules["bunkrdl.store"].Store.assert_not_called()

    def test_list_and_web_do_not_take_the_job_lock(self):
        with JobLock(self.config.state_db):
            self.assertEqual(self.run_cli("list")[0], 0)
        self.modules["bunkrdl.store"].Store.assert_called_once_with(self.config.state_db)

    def test_web_dispatch(self):
        with patch("bunkrdl.web.serve") as serve:
            self.assertEqual(self.run_cli("web", "--port", "9876")[0], 0)
            serve.assert_called_once_with(self.config, port=9876)

    def test_web_invalid_port(self):
        self.assertEqual(self.run_cli("web", "--port", "70000")[0], 2)


class LockTests(CliTestCase):
    def test_every_mutating_command_returns_four_when_locked(self):
        with JobLock(self.config.state_db):
            for args in (["get", "abc"], ["get", "abc", "--images"], ["links", "abc"],
                         ["check", "abc", str(self.folder)], ["check", "abc", str(self.folder), "--redownload"],
                         ["handback", "abc", "1"], ["add", "abc"], ["download", "abc"],
                         ["export", "abc"], ["reconcile", "abc", str(self.folder)],
                         ["mark-done", "abc", "1"]):
                with self.subTest(args=args):
                    code, out, err = self.run_cli(*args)
                    self.assertEqual(code, 4)
                    self.assertIn("另一个任务正在运行", err)
        cli.flows.download_files.assert_not_called()
        cli.flows.check_folder.assert_not_called()
        cli.flows.hand_back.assert_not_called()
        self.modules["bunkrdl.resolver"].BunkrClient.assert_not_called()

    def test_lock_is_released_after_a_run(self):
        self.assertEqual(self.run_cli("get", "abc")[0], 0)
        with JobLock(self.config.state_db):
            pass


class LegacyCommandTests(CliTestCase):
    def test_mark_done_checks_existence_and_accepts_explicit_path(self):
        path = Path(self.tmp.name) / 'manual.bin'
        self.assertEqual(self.run_cli("mark-done", "abc", "1", "--path", str(path))[0], 1)
        self.assertEqual(self.store.records[0].status, 'failed')
        path.write_bytes(b'confirmed by user')
        self.assertEqual(self.run_cli("mark-done", "abc", "1", "--path", "1=" + str(path))[0], 0)
        self.assertEqual(self.store.records[0].status, 'done')
        self.assertEqual(self.store.records[0].local_path, str(path))

    def test_add_id_and_config_db(self):
        db = str(Path(self.tmp.name) / 'test.db')
        self.assertEqual(self.run_cli("--config", "cfg.toml", "--db", db, "add", "abc")[0], 0)
        self.modules['bunkrdl.resolver'].BunkrClient.return_value.get_album.assert_called_once_with(
            "https://bunkr.cr/a/abc")
        self.modules['bunkrdl.store'].Store.assert_called_once_with(db)
        cli.load_config.assert_called_once_with("cfg.toml")
        self.assertTrue(self.store.closed)

    def test_list_without_resolver_prints_chinese_status(self):
        self.assertEqual(self.run_cli("list")[0], 0)
        self.assertEqual(self.run_cli("list", "https://bunkr.cr/a/abc")[0], 0)
        out = self.output.getvalue()
        self.assertIn("a.mp4", out)
        self.assertIn("链接已生成", out)
        self.assertNotIn("迅雷", out)
        self.assertIn("已跳过", out)
        self.modules["bunkrdl.resolver"].BunkrClient.assert_not_called()

    def test_download_auto_adds_and_passes_destination(self):
        self.store.album = None
        self.assertEqual(self.run_cli("download", "https://bunkr.cr/a/abc?advanced=1",
                                      "--kinds", "image,video")[0], 0)
        call = self.modules['bunkrdl.downloader'].run_queue.call_args
        self.assertEqual(call.args[2:4], ("abc", Path("downloads/测试相册")))
        self.assertEqual(call.kwargs['kinds'], ("image", "video"))
        self.assertFalse(call.kwargs['stop_event'].is_set())

    def test_download_existing_no_resolve_album_and_explicit_dest(self):
        self.assertEqual(self.run_cli("download", "abc", "--dest", "custom")[0], 0)
        self.modules['bunkrdl.resolver'].BunkrClient.return_value.get_album.assert_not_called()
        self.assertEqual(self.modules['bunkrdl.downloader'].run_queue.call_args.args[3], Path("custom"))

    def test_export_auto_add_default_and_override(self):
        self.store.album = None
        self.assertEqual(self.run_cli("export", "abc")[0], 0)
        call = self.modules['bunkrdl.export'].export_links.call_args
        self.assertEqual(call.args[3], Path("downloads/测试相册/_export"))
        self.assertEqual(call.kwargs['kinds'], ("video",))
        self.assertIn("输出目录", self.output.getvalue())
        self.assertEqual(self.run_cli("export", "abc", "--out", "elsewhere", "--kinds", "image")[0], 0)
        self.assertEqual(self.modules['bunkrdl.export'].export_links.call_args.args[3], Path("elsewhere"))

    def test_export_and_download_failures_exit_one(self):
        self.modules['bunkrdl.downloader'].run_queue.return_value = QueueResult(failed=2)
        self.assertEqual(self.run_cli("download", "abc")[0], 1)
        self.modules['bunkrdl.export'].export_links.return_value = ExportResult(failed=1)
        self.assertEqual(self.run_cli("export", "abc")[0], 1)

    def test_export_challenge_and_stop(self):
        self.modules['bunkrdl.export'].export_links.return_value = ExportResult(aborted_reason="SECRET")
        self.assertEqual(self.run_cli("export", "abc")[0], 3)
        self.modules['bunkrdl.export'].export_links.return_value = ExportResult(stopped=True)
        self.assertEqual(self.run_cli("export", "abc")[0], 0)

    def test_reconcile_no_requeue(self):
        self.assertEqual(self.run_cli("reconcile", "abc", "folder", "--no-requeue")[0], 0)
        self.modules['bunkrdl.reconcile'].reconcile.assert_called_once_with(
            self.store, "abc", Path("folder"), requeue=False)

    def test_reconcile_reports_unverified_names(self):
        self.modules['bunkrdl.reconcile'].reconcile.return_value = ReconcileResult(
            completed=['1'], unverified=['1'])
        self.assertEqual(self.run_cli('reconcile', 'abc', 'folder')[0], 0)
        self.assertIn('需要你确认：a.mp4', self.output.getvalue())

    def test_export_wrapper_forwards_pacer_and_stop_event(self):
        stop, pacer = object(), object()
        cli.call_export(stop_event=stop, pacer=pacer)
        self.modules['bunkrdl.export'].export_links.assert_called_once_with(
            stop_event=stop, pacer=pacer)

    def test_ctrl_c_sets_event_and_restores_handler(self):
        original = signal.getsignal(signal.SIGINT)

        def download(*args, **kwargs):
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            self.assertTrue(kwargs['stop_event'].is_set())
            return QueueResult(stopped=True)

        cli.flows.download_files.side_effect = download
        self.assertEqual(self.run_cli("get", "abc")[0], 0)
        self.assertIn("已停止，可再次运行继续", self.output.getvalue())
        self.assertEqual(signal.getsignal(signal.SIGINT), original)

    def test_export_ctrl_c_at_callback(self):
        def export(*args, **kwargs):
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            kwargs['on_event']({'type': 'exported', 'message': '链接已生成'})
            self.fail('Export must stop at the callback')

        self.modules['bunkrdl.export'].export_links.side_effect = export
        self.assertEqual(self.run_cli("export", "abc")[0], 0)
        self.assertTrue(self.store.closed)

    def test_export_receives_stop_event(self):
        received = []

        def export(*args, stop_event=None, **kwargs):
            received.append(stop_event)
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            self.assertTrue(stop_event.is_set())
            return ExportResult(stopped=True)

        self.modules['bunkrdl.export'].export_links = export
        self.assertEqual(self.run_cli("export", "abc")[0], 0)
        self.assertIsNotNone(received[0])

    def test_directory_cannot_traverse_or_use_reserved_name(self):
        cli.flows.album_dest.side_effect = self.real_album_dest
        for title, expected in (("../../", "abc"), ("CON", "_CON"), ("NUL.txt", "_NUL.txt"), ("a<>:?*", "a")):
            self.store.album.title = title
            self.assertEqual(cli.album_directory(self.config, self.store.album), Path("downloads") / f"{expected} [abc]")
        self.assertEqual(cli.album_directory(self.config, self.store.album, self.store).resolve(),
                         (Path("downloads") / "a [abc]").resolve())

    def test_get_and_download_pass_store_to_album_dest(self):
        self.assertEqual(self.run_cli("get", "abc")[0], 0)
        self.assertEqual(cli.flows.album_dest.call_args.args, (self.config, self.store.album, self.store))
        cli.flows.album_dest.reset_mock()
        self.assertEqual(self.run_cli("download", "abc")[0], 0)
        self.assertIn(self.store, cli.flows.album_dest.call_args.args)
        self.assertEqual(self.modules['bunkrdl.downloader'].run_queue.call_args.args[3],
                         Path("downloads") / "测试相册")

    def test_export_uses_store_backed_album_dest(self):
        self.assertEqual(self.run_cli("export", "abc")[0], 0)
        self.assertEqual(cli.flows.album_dest.call_args.args, (self.config, self.store.album, self.store))
        self.assertEqual(self.modules['bunkrdl.export'].export_links.call_args.args[3],
                         Path("downloads") / "测试相册" / "_export")


class HelpTests(CliTestCase):
    def test_help_lists_new_commands_first_and_marks_advanced(self):
        self.assertEqual(self.run_cli("--help")[0], 0)
        text = self.output.getvalue()
        for name in ("get", "links", "check", "handback", "web", "list", "mark-done",
                     "add", "download", "export", "reconcile"):
            self.assertIn(name, text)
        self.assertLess(text.index("links"), text.index("handback"))
        self.assertLess(text.index("handback"), text.index("mark-done"))
        self.assertLess(text.index("mark-done"), text.index("download"))
        self.assertIn("高级：", text)
        self.modules["bunkrdl.store"].Store.assert_not_called()

    def test_new_command_help_text_is_chinese(self):
        text = ""
        for name in ("get", "links", "check", "handback"):
            with self.subTest(command=name):
                code, out, _ = self.run_cli(name, "--help")
                self.assertEqual(code, 0)
                text += out
        for phrase in ("--images", "--videos", "--limit", "--redownload", "--print"):
            self.assertIn(phrase, text)
        self.assertIn("检查外部下载文件夹", text)

    def test_help_and_hints_avoid_legacy_wording(self):
        text = ""
        for args in (("--help",), ("get", "--help"), ("links", "--help"),
                     ("check", "--help"), ("handback", "--help")):
            code, out, _ = self.run_cli(*args)
            self.assertEqual(code, 0)
            text += out
        self.modules["bunkrdl.resolver"].BunkrClient.return_value.get_album.return_value = \
            Album("abc", "相册", "https://bunkr.cr/a/abc")
        for args in (("links", "abc"), ("links", "abc", "--print"),
                     ("check", "abc", str(self.folder)),
                     ("check", "abc", str(self.folder), "--redownload"),
                     ("get", "abc"), ("download", "abc"), ("export", "abc")):
            code, out, err = self.run_cli(*args)
            text += out + err
        for phrase in ("迅雷链接", "导出", "回补", "模式 A", "模式 B"):
            self.assertNotIn(phrase, text)
        self.assertIn("检查外部下载文件夹", text)
        self.assertIn("链接已生成", text)


class ProgressTests(unittest.TestCase):
    def test_tty_refresh_percent_speed_and_no_raw_exception(self):
        stream = io.StringIO()
        stream.isatty = lambda: True
        printer = cli.EventPrinter(stream, clock=iter([10, 12]).__next__)
        printer({'type': 'start', 'file_id': '1', 'name': '视频.mp4', 'index': 2, 'count': 4})
        for size in (1024, 3072):
            printer({'type': 'progress', 'file_id': '1', 'name': '视频.mp4', 'bytes': size, 'total': 4096})
        printer({'type': 'failed', 'file_id': '1', 'name': '视频.mp4',
                 'error_kind': 'maintenance', 'message': 'SECRET traceback'})
        text = stream.getvalue()
        self.assertIn('\r[2/4] 视频.mp4 75%', text)
        self.assertIn('1.0 KiB/s', text)
        self.assertIn('服务器维护中', text)
        self.assertNotIn('SECRET', text)
        self.assertTrue(text.endswith('\n'))

    def test_failed_line_prefers_chinese_event_message(self):
        stream = io.StringIO()
        cli.EventPrinter(stream)({'type': 'failed', 'file_id': '1', 'name': 'a.mp4',
                                  'error_kind': 'transient',
                                  'message': 'a.mp4：连接超时（可能是代理或网络问题）'})
        text = stream.getvalue()
        self.assertIn('a.mp4：连接超时（可能是代理或网络问题）', text)
        self.assertEqual(text.count('a.mp4'), 1)
        stream = io.StringIO()
        cli.EventPrinter(stream)({'type': 'failed', 'file_id': '1', 'name': 'a.mp4',
                                  'error_kind': 'other',
                                  'message': 'BunkrError: 服务器返回网页，无法保存为文件'})
        self.assertIn('a.mp4：服务器返回网页，无法保存为文件', stream.getvalue())

    def test_retry_line_is_chinese_and_shows_delay(self):
        stream = io.StringIO()
        cli.EventPrinter(stream)({'type': 'retry', 'file_id': '1', 'name': 'a.mp4',
                                  'error_kind': 'rate_limited', 'seconds': 5,
                                  'message': '被网站限速（429）'})
        text = stream.getvalue()
        self.assertIn('被网站限速（429）', text)
        self.assertIn('5 秒后重试', text)

    def test_non_tty_lines_unknown_total_and_speed_reset(self):
        stream = io.StringIO()
        printer = cli.EventPrinter(stream, clock=iter([1, 2, 3]).__next__)
        for ident, size in (('1', 10000), ('1', 20000), ('2', 10)):
            printer({'type': 'progress', 'file_id': ident, 'bytes': size, 'total': None})
        text = stream.getvalue()
        self.assertNotIn('\r', text)
        self.assertEqual(len(text.splitlines()), 3)
        self.assertIn('大小未知', text)
        self.assertTrue(text.splitlines()[-1].endswith('0.0 KiB/s'))

    def test_stopped_event_prints_resume_hint(self):
        stream = io.StringIO()
        cli.EventPrinter(stream)({'type': 'stopped'})
        self.assertIn('已停止，可再次运行继续', stream.getvalue())

    def test_wait_kinds_use_shared_chinese_text_and_continue_time(self):
        stream = io.StringIO()
        printer = cli.EventPrinter(stream)
        for reason in ('interval', 'batch', 'backoff', 'cooldown'):
            printer({'type': 'wait', 'reason': reason, 'until': 2000000000})
        self.assertEqual(stream.getvalue().count('继续'), 4)
        self.assertIn('不用操作', stream.getvalue())

    def test_every_error_kind_has_chinese_explanation(self):
        stream = io.StringIO()
        printer = cli.EventPrinter(stream)
        for kind in cli.messages.ERROR_KINDS:
            printer({'type': 'failed', 'error_kind': kind, 'message': 'English exception'})
            self.assertIn(cli.messages.describe_error(kind), stream.getvalue())
        self.assertNotIn('English exception', stream.getvalue())


if __name__ == "__main__":
    unittest.main()
