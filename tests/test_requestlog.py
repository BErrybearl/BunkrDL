"""Request log and sanitized error context; never writes the real home."""
import io
import os
import re
import tempfile
import time
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests

from bunkrdl import messages, requestlog
from bunkrdl.config import default_home


TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2}$")


class _Home(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        env = patch.dict(os.environ, {"BUNKRDL_HOME": str(self.home)})
        env.start()
        self.addCleanup(env.stop)
        self.addCleanup(requestlog.reset)

    def lines(self):
        text = (self.home / "web.log").read_text(encoding="utf-8")
        return text.strip().splitlines()

    def line(self):
        return self.lines()[-1]


class RequestLogTests(_Home):
    def test_line_records_every_field_without_the_url(self):
        requestlog.log_request(
            "文件体", "GET",
            "https://user:{{PASSWORD_w4kq9c7r}}@mlk-b.cdn.cr:8443/secret?token={{SECRET_a1b2c3d4}}",
            status=429, retry_after="120", bytes=2048, elapsed_ms=87.5)
        line = self.line()
        fields = line.split(" ")
        self.assertTrue(TIME.match(fields[0]))
        self.assertEqual(fields[1:], ["文件体", "GET", "mlk-b.cdn.cr:8443", "status=429",
                                      "retry_after=120", "bytes=2048", "elapsed_ms=87.5"])
        self.assertNotIn("user", line)
        self.assertNotIn("SECRET", line)
        self.assertNotIn("secret", line)
        self.assertNotIn("https", line)

    def test_exception_line_uses_the_type_only_and_elapsed_from_started(self):
        error = requests.Timeout("https://cdn.cr/path?token={{SECRET_a1b2c3d4}}")
        requestlog.log_request("解析", "POST", "https://dl.bunkr.cr/api?token=SENSITIVE",
                               error=error, started=time.perf_counter())
        line = self.line()
        fields = line.split(" ")
        self.assertEqual(fields[1:5], ["解析", "POST", "dl.bunkr.cr", "error=Timeout"])
        self.assertEqual(fields[5], "retry_after=none")
        self.assertEqual(fields[6], "bytes=-")
        self.assertNotIn("elapsed_ms=-", line)
        self.assertNotIn("SENSITIVE", line)
        self.assertNotIn("/api", line)

    def test_unknown_or_missing_values_stay_readable(self):
        requestlog.log_request("未知阶段", "get", "not a url", status=200, retry_after=None,
                               bytes=None, elapsed_ms=1)
        fields = self.line().split(" ")
        self.assertEqual(fields[1:5], ["-", "GET", "-", "status=200"])
        self.assertEqual(fields[5:], ["retry_after=none", "bytes=-", "elapsed_ms=1.0"])

    def test_rotating_handler_configuration_and_path(self):
        config = SimpleNamespace(state_db=str(self.home / "state.db"))
        self.assertEqual(requestlog.log_path(config), self.home / "web.log")
        requestlog.log_request("相册页", "GET", "https://bunkr.cr/a/sample", status=200,
                               config=config)
        logger = requestlog._logger(requestlog.log_path(config))
        handler = next(h for h in logger.handlers if isinstance(h, RotatingFileHandler))
        self.assertEqual(handler.maxBytes, 1024 * 1024)
        self.assertEqual(handler.backupCount, 3)
        self.assertEqual(handler.encoding, "utf-8")
        self.assertEqual(Path(handler.baseFilename), self.home / "web.log")

    def test_path_follows_the_state_database_without_a_home_override(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BUNKRDL_HOME", None)
            config = SimpleNamespace(state_db=str(self.home / "sub" / "state.db"))
            self.assertEqual(requestlog.log_path(config), self.home / "sub" / "web.log")
            self.assertEqual(requestlog.log_path(SimpleNamespace(state_db=":memory:")),
                             default_home() / "web.log")

    def test_retry_after_is_logged_as_normalized_seconds_only(self):
        future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=120), usegmt=True)
        cases = [("120", "120"), (" 240 ", "240"), ("0", "0"),
                 (future, None), ("-5", "invalid"), ("soon", "invalid"), (None, "none")]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                requestlog.log_request("文件体", "GET", "https://cdn.example/data",
                                       status=429, retry_after=raw)
                value = self.line().split("retry_after=", 1)[1].split(" ", 1)[0]
                if expected is None:
                    # An HTTP date is logged as the seconds it resolves to.
                    self.assertRegex(value, r"^\d+$")
                    self.assertIn(int(value), (119, 120, 121))
                else:
                    self.assertEqual(value, expected)

    def test_raw_retry_after_text_never_reaches_the_log_file(self):
        hostile = "not-a-date token={{SECRET_a1b2c3d4}}\r\nX-Evil: 1"
        requestlog.log_request("文件体", "GET", "https://cdn.example/data",
                               status=503, retry_after=hostile)
        line = self.line()
        self.assertIn("retry_after=invalid", line)
        for word in ("SECRET", "X-Evil", "not-a-date"):
            self.assertNotIn(word, line)

    def test_a_handler_write_or_rotation_failure_never_prints_the_record(self):
        requestlog.log_request("文件体", "GET", "https://cdn.example/data", status=200)
        logger = requestlog._logger(requestlog.log_path())
        handler = next(h for h in logger.handlers if isinstance(h, RotatingFileHandler))

        class Broken:
            def write(self, text):
                raise OSError("disk full")

            def flush(self):
                raise OSError("disk full")

        stderr = io.StringIO()
        with patch.object(handler, "stream", Broken()), redirect_stderr(stderr):
            requestlog.log_request("文件体", "GET", "https://cdn.example/secret-file",
                                   status=500, retry_after="{{SECRET_a1b2c3d4}}")
            requestlog.log_request("相册页", "GET", "https://cdn.example/another", status=200)
        # logging's default handleError dumps the traceback and the record to
        # stderr, which would leak exactly what the log must never print.
        self.assertEqual(stderr.getvalue(), "")

    def test_unusable_log_path_is_swallowed_and_the_download_survives(self):
        (self.home / "web.log").mkdir()
        requestlog.log_request("文件体", "GET", "https://cdn.example/data", status=200)
        self.assertTrue((self.home / "web.log").is_dir())
        with patch.object(requestlog, "_logger", side_effect=OSError("disk full")):
            requestlog.log_request("文件体", "GET", "https://cdn.example/data", status=200)

    def test_host_never_carries_credentials_path_or_query(self):
        self.assertEqual(requestlog.host_of("https://user:pass@cdn.example/path?token=x"),
                         "cdn.example")
        self.assertEqual(requestlog.host_of("https://cdn.example:8443/path"), "cdn.example:8443")
        self.assertEqual(requestlog.host_of("https://cdn.example:443/path"), "cdn.example")
        for value in ("not a url", "", None, 42):
            self.assertEqual(requestlog.host_of(value), "")

    def test_body_read_failure_after_the_headers_is_logged_with_the_same_seq(self):
        from bunkrdl.config import Config
        from bunkrdl.downloader import download_file
        from bunkrdl.models import Album, AlbumFile, ResolvedLink, TransientError
        from bunkrdl.store import Store

        store = Store(":memory:")
        self.addCleanup(store.close)
        store.upsert_album(Album("abc", "相册", "url", [
            AlbumFile("1", "a.bin", "1", "url", size=4096)]))

        class Client:
            def resolve(self, record):
                return ResolvedLink("https://mlk-b.cdn.cr/storage/a.bin?token=SECRET",
                                    "https://bunkr.cr/", "a.bin")

        def broken():
            yield b"0123" * 8
            raise requests.exceptions.ReadTimeout("read timed out after the headers")

        response = requests.Response()
        response.status_code = 200
        response.url = "https://mlk-b.cdn.cr/storage/a.bin?token=SECRET"
        response.headers.update({"Content-Length": "4096", "ETag": '"v1"'})
        response._content_consumed = True
        response.iter_content = lambda chunk_size=65536: broken()

        class Transport:
            def get(self, url, **kwargs):
                return response

        with self.assertRaises(TransientError):
            download_file(Client(), store, store.list_files("abc")[0], self.home / "dl",
                          Config(), session=Transport())
        lines = [line.split(" ") for line in self.lines()]
        self.assertEqual(len(lines), 2)
        self.assertEqual([item[1:4] for item in lines],
                         [["文件体", "GET", "mlk-b.cdn.cr"]] * 2)
        self.assertEqual(lines[0][4], "status=200")
        self.assertEqual(lines[1][4], "error=ReadTimeout")
        # The same request number links the header line with its body failure.
        self.assertTrue(lines[0][-1].startswith("seq="))
        self.assertEqual(lines[1][-1], lines[0][-1])
        self.assertTrue(lines[1][-2].startswith("elapsed_ms="))
        self.assertNotIn("SECRET", "\n".join(self.lines()))
        self.assertEqual(store.list_files("abc")[0].status, "failed")


class ErrorContextTests(unittest.TestCase):
    def test_message_suffix_matches_the_documented_format(self):
        text = messages.describe_error("rate_limited", "HTTP 429", stage="文件体",
                                       host="mlk-b.cdn.cr", retry_after=120)
        self.assertEqual(text, "被网站限速（429）· 阶段：文件体 · 主机 mlk-b.cdn.cr · 建议等待 120 秒")

    def test_suffix_drops_unsafe_hosts_unknown_stages_and_bad_retry_after(self):
        text = messages.describe_error("transient", "连接超时", stage="文件体",
                                       host="https://x/y?token=1")
        self.assertEqual(text, "连接超时（可能是代理或网络问题）· 阶段：文件体")
        text = messages.describe_error("other", None, stage="未知", host="cdn.example",
                                       retry_after=float("nan"))
        self.assertEqual(text, "下载失败· 主机 cdn.example")
        for retry_after in (True, "120", -1, float("inf"), None):
            with self.subTest(retry_after=retry_after):
                self.assertEqual(messages.describe_error("transient", None,
                                                          retry_after=retry_after),
                                 "网络不稳定，稍后重试")

    def test_error_context_reads_only_sanitized_attributes(self):
        error = messages.RateLimited("HTTP 429", retry_after=120)
        requestlog.tag_error(error, "签名", "https://glb-apisign.cdn.cr/sign?path=SECRET")
        self.assertEqual(messages.error_context(error),
                         {"stage": "签名", "host": "glb-apisign.cdn.cr", "retry_after": 120})
        self.assertEqual(messages.error_context(None), {})
        self.assertEqual(messages.error_context(RuntimeError("x")), {})
        hostile = RuntimeError("x")
        hostile.stage = "文件体"
        hostile.host = "evil.example/path?token=1"
        hostile.retry_after = float("inf")
        self.assertEqual(messages.error_context(hostile), {"stage": "文件体"})

    def test_first_stage_wins_and_empty_values_are_ignored(self):
        error = RuntimeError("x")
        requestlog.tag_error(error, None, "https://cdn.example/a")
        requestlog.tag_error(error, "文件体", "https://other.example/b?token=1")
        self.assertEqual((error.stage, error.host), ("文件体", "cdn.example"))
        requestlog.tag_error(error, "解析", "https://third.example/c")
        self.assertEqual((error.stage, error.host), ("文件体", "cdn.example"))


class ResolverLogTests(_Home):
    def client(self, *results):
        from bunkrdl.config import Config
        from bunkrdl.resolver import BunkrClient

        class Session:
            def __init__(self):
                self.results = list(results)
                self.calls = []

            def request(self, method, url, **kwargs):
                self.calls.append((method, url))
                result = self.results.pop(0)
                if isinstance(result, Exception):
                    raise result
                result.url = url
                return result

        self.session = Session()
        return BunkrClient(Config(domains=["bunkr.cr"], request_interval=0), self.session)

    @staticmethod
    def response(body=b"", status=200, headers=None):
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else body.encode("utf-8")
        response.encoding = "utf-8"
        response.headers.update(headers or {})
        response._content_consumed = True
        return response

    def test_each_resolver_stage_logs_one_line_without_secrets(self):
        page = ('<meta property="og:title" content="示例">'
                '<script>window.albumFiles=[{id:1,original:"a.jpg",slug:"a",size:4}];</script>')
        client = self.client(
            self.response(page.encode("utf-8")),
            self.response(b'{"url": "https://mlk-b.cdn.cr/storage/a.jpg"}'),
            self.response(b'{"token": "sample", "ex": 1800007200}'))
        album = client.get_album("https://bunkr.cr/a/sample?token={{SECRET_a1b2c3d4}}")
        client.resolve(album.files[0])
        fields = [line.split(" ") for line in self.lines()]
        self.assertEqual([item[1] for item in fields], ["相册页", "解析", "签名"])
        self.assertEqual([item[2] for item in fields], ["GET", "POST", "GET"])
        self.assertEqual([item[3] for item in fields],
                         ["bunkr.cr", "dl.bunkr.cr", "glb-apisign.cdn.cr"])
        self.assertTrue(all(item[4] == "status=200" for item in fields))
        self.assertTrue(all(item[7].startswith("elapsed_ms=") for item in fields))
        text = "\n".join(self.lines())
        self.assertNotIn("SECRET", text)
        self.assertNotIn("token", text)

    def test_body_hops_are_logged_with_host_and_status(self):
        from bunkrdl.config import Config
        from bunkrdl.downloader import download_file
        from bunkrdl.models import Album, AlbumFile, ResolvedLink
        from bunkrdl.store import Store

        store = Store(":memory:")
        self.addCleanup(store.close)
        payload = b"0123456789" * 100
        store.upsert_album(Album("abc", "相册", "url", [
            AlbumFile("1", "a.bin", "1", "url", size=len(payload))]))

        class Client:
            def resolve(self, record):
                return ResolvedLink("https://mlk-b.cdn.cr/storage/a.bin?token=SECRET",
                                    "https://bunkr.cr/", "a.bin")

        response = requests.Response()
        response.status_code = 200
        response.url = "https://mlk-b.cdn.cr/storage/a.bin?token=SECRET"
        response.headers.update({"Content-Length": str(len(payload)), "ETag": '"v1"'})
        response._content = payload
        response._content_consumed = True
        response.iter_content = lambda chunk_size=65536: iter([payload])
        destination = self.home / "dl"
        with patch("bunkrdl.downloader.requests.get", return_value=response):
            download_file(Client(), store, store.list_files("abc")[0], destination, Config())
        fields = [line.split(" ") for line in self.lines()]
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0][1:4], ["文件体", "GET", "mlk-b.cdn.cr"])
        self.assertEqual(fields[0][4], "status=200")
        self.assertNotIn("SECRET", self.lines()[0])


if __name__ == "__main__":
    unittest.main()
