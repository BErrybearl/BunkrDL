"""Request-log summary: parsing, stats, filters and CLI output.

Every temporary home is redirected so the real ``~/.workhub/bunkrdl`` is
never read or written.
"""
import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from bunkrdl import logstat, requestlog

T0 = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)


def at(seconds=0.0, start=T0):
    return start + timedelta(seconds=seconds)


def line(when, *, stage="文件体", method="GET", host="mlk-b.cdn.cr", status=200,
         error=None, retry_after="none", size=None, elapsed=12.3):
    fields = [when.isoformat(timespec="milliseconds"), stage, method, host]
    if error is not None:
        fields.append("error=" + error)
    else:
        fields.append("status=" + ("-" if status is None else str(status)))
    fields.append("retry_after=" + retry_after)
    fields.append("bytes=" + ("-" if size is None else str(size)))
    fields.append("elapsed_ms=" + ("-" if elapsed is None else f"{elapsed:.1f}"))
    return " ".join(fields)


class _Log(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.home = self.dir / "home"
        env = patch.dict(os.environ, {"BUNKRDL_HOME": str(self.home)})
        env.start()
        self.addCleanup(env.stop)
        self.addCleanup(requestlog.reset)

    def write(self, name, lines):
        path = self.dir / name
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return path

    def run_main(self, argv):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = logstat.main(argv)
        return code, output.getvalue()

    def analyze_text(self, lines, path="web.log"):
        loaded = logstat.load(self.write(path, lines))
        return logstat.render(logstat.analyze(loaded.records, files=loaded.files,
                                              ignored=loaded.ignored))


class ParseLineTests(unittest.TestCase):
    def test_full_line(self):
        record = logstat.parse_line(
            "2026-11-03T12:00:00.123+08:00 文件体 GET mlk-b.cdn.cr:8443 "
            "status=429 retry_after=120 bytes=2048 elapsed_ms=87.5")
        self.assertIsNotNone(record)
        self.assertEqual(record.when.utcoffset(), timedelta(hours=8))
        self.assertEqual((record.stage, record.method, record.host),
                         ("文件体", "GET", "mlk-b.cdn.cr:8443"))
        self.assertEqual((record.status, record.error), (429, None))
        self.assertEqual((record.retry_after, record.bytes, record.elapsed_ms),
                         ("120", 2048, 87.5))
        self.assertTrue(record.rate_limited)
        self.assertEqual(record.stage_label, "文件体")

    def test_error_line_and_unknown_stage(self):
        record = logstat.parse_line(
            "2026-11-03T12:00:00+00:00 - POST cdn.example error=Timeout "
            "retry_after=none bytes=- elapsed_ms=-")
        self.assertEqual((record.stage_label, record.status, record.error),
                         ("未标注", None, "Timeout"))
        self.assertIsNone(record.bytes)
        self.assertIsNone(record.elapsed_ms)
        self.assertFalse(record.rate_limited)

    def test_status_dash_means_unknown(self):
        record = logstat.parse_line(
            "2026-11-03T12:00:00+00:00 解析 GET dl.bunkr.cr status=- "
            "retry_after=none bytes=- elapsed_ms=1.0")
        self.assertIsNone(record.status)

    def test_http_date_retry_after_keeps_its_spaces(self):
        record = logstat.parse_line(
            "2026-11-03T12:00:00+00:00 文件体 GET cdn.example status=429 "
            "retry_after=Wed, 21 Oct 2015 07:28:00 GMT bytes=- elapsed_ms=-")
        self.assertEqual(record.retry_after, "Wed, 21 Oct 2015 07:28:00 GMT")

    def test_malformed_lines_are_rejected(self):
        good = line(T0)
        bad = [
            "",
            "随机文字",
            "2026-11-03T12:00:00+00:00 文件体 GET host",
            good.replace("elapsed_ms=12.3", "elapsed=12.3"),
            good.replace("bytes=-", "bytes=x"),
            good.replace("bytes=-", "bytes=-1"),
            good.replace("elapsed_ms=12.3", "elapsed_ms=nan"),
            good.replace("status=200", "status=abc"),
            good.replace("status=200", "status=-1"),
            good.replace("status=200", "error="),
            good.replace("retry_after=none", "retry_after="),
            good.replace("文件体", "未知阶段"),
            good.replace("status=200", "status=200 error=Timeout"),
            good + " extra=1",
            "not-a-time 文件体 GET host status=200 retry_after=none bytes=- elapsed_ms=1.0",
        ]
        for text in bad:
            with self.subTest(text=text):
                self.assertIsNone(logstat.parse_line(text))
        self.assertIsNotNone(logstat.parse_line(good))

    def test_non_string_input(self):
        self.assertIsNone(logstat.parse_line(None))
        self.assertIsNone(logstat.parse_line(42))


class LoadTests(_Log):
    def test_reads_main_file_and_rotated_siblings(self):
        main = self.write("web.log", [line(at(2), host="a.example"),
                                      "坏行"])
        self.write("web.log.1", [line(at(1), host="b.example")])
        self.write("web.log.2", [line(at(0), host="c.example")])
        loaded = logstat.load(main)
        self.assertEqual(loaded.ignored, 1)
        self.assertEqual([record.host for record in loaded.records],
                         ["c.example", "b.example", "a.example"])
        self.assertEqual([path.name for path in loaded.files],
                         ["web.log", "web.log.1", "web.log.2"])
        self.assertFalse(loaded.empty)

    def test_missing_log_reports_no_files(self):
        loaded = logstat.load(self.dir / "nowhere" / "web.log")
        self.assertEqual((loaded.records, loaded.ignored, loaded.files), ([], 0, []))
        self.assertTrue(loaded.empty)

    def test_since_filters_records_but_still_counts_bad_lines(self):
        main = self.write("web.log", [line(at(0)), line(at(3600)), "坏行"])
        loaded = logstat.load(main, since=at(1800))
        self.assertEqual([record.when for record in loaded.records], [at(3600)])
        self.assertEqual(loaded.ignored, 1)

    def test_candidate_files_order(self):
        names = [path.name for path in logstat.candidate_files(self.dir / "web.log")]
        self.assertEqual(names, ["web.log", "web.log.1", "web.log.2", "web.log.3"])

    def test_lines_written_by_requestlog_parse(self):
        requestlog.log_request("文件体", "GET", "https://mlk-b.cdn.cr/a.bin",
                               status=200, bytes=10, elapsed_ms=5, config=None)
        requestlog.reset()
        loaded = logstat.load(self.home / "web.log")
        self.assertEqual(loaded.ignored, 0)
        self.assertEqual(len(loaded.records), 1)
        record = loaded.records[0]
        self.assertEqual((record.stage, record.host, record.status), ("文件体", "mlk-b.cdn.cr", 200))


class AnalyzeTests(unittest.TestCase):
    def records(self, lines):
        return [record for record in (logstat.parse_line(item) for item in lines)
                if record is not None]

    def stats(self, lines):
        return logstat.analyze(self.records(lines))

    def test_distributions(self):
        stats = self.stats([
            line(at(0), status=200, elapsed=100),
            line(at(1), status=429, retry_after="120", elapsed=200),
            line(at(2), stage="解析", host="bunkr.cr", status=200, elapsed=300),
            line(at(3), stage="签名", host="bunkr.cr", error="Timeout", elapsed=None),
            line(at(4), stage="相册页", status=500, elapsed=400),
            line(at(5), stage="-", host="other.example", status=None, elapsed=None),
        ])
        self.assertEqual(stats["requests"], 6)
        self.assertEqual(stats["stages"]["文件体"], 2)
        self.assertEqual(stats["stages"]["解析"], 1)
        self.assertEqual(stats["stages"]["签名"], 1)
        self.assertEqual(stats["stages"]["相册页"], 1)
        self.assertEqual(stats["stages"]["未标注"], 1)
        self.assertEqual(stats["hosts"], {"mlk-b.cdn.cr": 3, "bunkr.cr": 2,
                                          "other.example": 1})
        self.assertEqual(stats["statuses"], {"200": 2, "429": 1, "500": 1, "unknown": 2})
        self.assertEqual(stats["errors"], {"Timeout": 1})
        self.assertEqual(stats["rate_limited"]["count"], 1)
        self.assertAlmostEqual(stats["rate_limited"]["share"], 1 / 6)
        self.assertEqual(stats["rate_limited"]["stages"], {"文件体": 1})
        self.assertEqual(stats["rate_limited"]["retry_after"], {"120": 1})
        self.assertEqual(stats["start"], at(0).isoformat())
        self.assertEqual(stats["end"], at(5).isoformat())
        self.assertEqual(stats["span_seconds"], 5.0)

    def test_rate_per_minute_peak_and_average(self):
        stats = self.stats([line(at(0)), line(at(10)), line(at(60)), line(at(110)),
                            line(at(120))])
        self.assertEqual(stats["rate_per_minute"]["active_minutes"], 3)
        self.assertEqual(stats["rate_per_minute"]["peak"]["count"], 2)
        self.assertEqual(stats["rate_per_minute"]["peak"]["minute"],
                         at(0).replace(second=0, microsecond=0).isoformat())
        self.assertAlmostEqual(stats["rate_per_minute"]["average"], 5 / 2)

    def test_429_pre_window_samples_exclude_the_429_itself(self):
        stats = self.stats([
            line(at(-20)),
            line(at(-10)),
            line(at(-6)),
            line(at(-3)),
            line(at(0), status=429, retry_after="120"),
            line(at(1)),
            line(at(2), status=429, retry_after="none"),
            line(at(30)),
        ])
        window = stats["rate_limited"]["pre_window"]
        self.assertEqual(window["window_seconds"], 10.0)
        self.assertEqual(window["samples"], [3, 4])
        self.assertAlmostEqual(window["average"], 3.5)
        self.assertEqual((window["min"], window["max"]), (3, 4))
        self.assertEqual(stats["rate_limited"]["retry_after"], {"120": 1, "未提供": 1})

    def test_elapsed_average_max_and_timeout_points(self):
        stats = self.stats([
            line(at(0), elapsed=100),
            line(at(1), elapsed=300),
            line(at(2), elapsed=None),
            line(at(3), error="Timeout", elapsed=None),
            line(at(3.4), error="ReadTimeout", elapsed=None),
            line(at(90), error="TimeoutError", elapsed=None),
            line(at(91), error="ConnectionError", elapsed=None),
        ])
        self.assertEqual(stats["elapsed_ms"]["count"], 2)
        self.assertAlmostEqual(stats["elapsed_ms"]["average"], 200.0)
        self.assertEqual(stats["elapsed_ms"]["max"], 300.0)
        self.assertEqual(stats["elapsed_ms"]["max_at"], at(1).isoformat())
        self.assertEqual(stats["timeouts"]["count"], 3)
        points = stats["timeouts"]["points"]
        self.assertEqual([point["count"] for point in points], [2, 1])
        self.assertEqual(datetime.fromisoformat(points[0]["time"]), at(3).replace(microsecond=0))

    def test_empty_records(self):
        stats = logstat.analyze([], files=[], ignored=0, since=None)
        self.assertEqual(stats["requests"], 0)
        self.assertIsNone(stats["start"])
        self.assertIsNone(stats["rate_per_minute"]["peak"])
        self.assertEqual(stats["rate_limited"]["share"], 0.0)
        self.assertIn("没有请求记录", stats["advice"])

    def test_advice_warns_about_concurrency_for_bursts(self):
        lines = [line(at(-offset)) for offset in (9, 8, 7, 6, 5, 4)]
        lines.append(line(at(0), status=429, retry_after="120"))
        stats = self.stats(lines)
        self.assertAlmostEqual(stats["rate_limited"]["pre_window"]["average"], 6.0)
        self.assertIn("并发", stats["advice"])
        self.assertIn("降到 1", stats["advice"])
        self.assertIn("文件体", stats["advice"])

    def test_advice_for_sparse_rate_limiting_mentions_request_rate(self):
        stats = self.stats([line(at(0), status=429, retry_after="120")])
        self.assertIn("请求间隔", stats["advice"])
        self.assertIn("1.5 秒", stats["advice"])

    def test_advice_for_timeouts_and_clean_log(self):
        stats = self.stats([line(at(0), error="Timeout")])
        self.assertIn("超时", stats["advice"])
        self.assertIn("代理", stats["advice"])
        clean = self.stats([line(at(0)), line(at(1))])
        self.assertIn("可以保持", clean["advice"])

    def test_advice_uses_dominant_rate_limited_stage(self):
        stats = self.stats([
            line(at(0), stage="签名", status=429, retry_after="60"),
            line(at(1), stage="文件体", status=429, retry_after="60"),
            line(at(2), stage="文件体", status=429, retry_after="60"),
        ])
        self.assertIn("文件体阶段", stats["advice"])

    def test_advice_for_empty_filtered_range(self):
        stats = logstat.analyze([], since=T0)
        self.assertIn("--since", stats["advice"])


class ParseSinceTests(unittest.TestCase):
    def test_relative_units(self):
        now = at(1000)
        self.assertEqual(logstat.parse_since("2h", now=now), now - timedelta(hours=2))
        self.assertEqual(logstat.parse_since("30m", now=now), now - timedelta(minutes=30))
        self.assertEqual(logstat.parse_since("90s", now=now), now - timedelta(seconds=90))
        self.assertEqual(logstat.parse_since("3d", now=now), now - timedelta(days=3))
        self.assertEqual(logstat.parse_since(" 2H ", now=now), now - timedelta(hours=2))

    def test_iso_time_with_and_without_offset(self):
        self.assertEqual(logstat.parse_since("2026-11-03T12:00:00Z"),
                         datetime(2026, 11, 3, 12, tzinfo=timezone.utc))
        self.assertEqual(logstat.parse_since("2026-11-03T12:00:00+08:00").utcoffset(),
                         timedelta(hours=8))
        naive = logstat.parse_since("2026-11-03 12:00:00")
        self.assertIsNotNone(naive.tzinfo)

    def test_invalid_values_raise(self):
        for text in ("", "昨天", "2x", "0h", "-1h"):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    logstat.parse_since(text)


class MainTests(_Log):
    def test_text_report_covers_every_section(self):
        path = self.write("web.log", [
            line(at(0), stage="解析", host="bunkr.cr"),
            line(at(1), status=429, retry_after="120", elapsed=50),
            line(at(2), error="Timeout", elapsed=None),
        ])
        code, output = self.run_main([str(path)])
        self.assertEqual(code, 0)
        for fragment in ("Bunkr 请求日志分析", "日志文件：", "总请求数：3", "时间范围：",
                         "按阶段：", "按主机：", "状态码：", "429：1 次",
                         "429 前 10 秒请求数：", "Retry-After：", "异常类型：",
                         "请求速率：峰值", "耗时：平均", "超时：1 次", "建议："):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, output)

    def test_json_report_is_machine_readable(self):
        path = self.write("web.log", [line(at(0)), line(at(1), status=429)])
        code, output = self.run_main([str(path), "--json"])
        self.assertEqual(code, 0)
        data = json.loads(output)
        self.assertEqual(data["requests"], 2)
        self.assertEqual(data["rate_limited"]["count"], 1)
        self.assertEqual(data["log_files"], [str(path)])
        self.assertIsInstance(data["advice"], str)
        self.assertEqual(data["stages"]["文件体"], 2)

    def test_since_relative_and_iso(self):
        now = datetime.now().astimezone()
        path = self.write("web.log", [
            line(now - timedelta(hours=3)),
            line(now - timedelta(minutes=5)),
        ])
        code, output = self.run_main([str(path), "--since", "2h", "--json"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["requests"], 1)
        code, output = self.run_main(
            [str(path), "--since", (now - timedelta(hours=4)).isoformat(), "--json"])
        self.assertEqual(json.loads(output)["requests"], 2)

    def test_default_path_uses_bunkrdl_home(self):
        self.home.mkdir(parents=True)
        (self.home / "web.log").write_text(line(at(0)) + "\n", encoding="utf-8")
        code, output = self.run_main([])
        self.assertEqual(code, 0)
        self.assertIn("总请求数：1", output)
        self.assertIn(str(self.home / "web.log"), output)

    def test_explicit_path_overrides_the_default(self):
        self.home.mkdir(parents=True)
        (self.home / "web.log").write_text("\n".join(line(at(i)) for i in range(3)) + "\n",
                                           encoding="utf-8")
        other = self.write("other.log", [line(at(0))])
        code, output = self.run_main([str(other), "--json"])
        self.assertEqual(json.loads(output)["requests"], 1)
        self.assertEqual(json.loads(output)["log_files"], [str(other)])

    def test_missing_log_prints_a_chinese_hint(self):
        missing = self.dir / "missing" / "web.log"
        code, output = self.run_main([str(missing)])
        self.assertEqual(code, 0)
        self.assertIn("没有找到请求日志", output)
        self.assertIn(".1/.2/.3", output)
        code, output = self.run_main([str(missing), "--json"])
        data = json.loads(output)
        self.assertTrue(data["empty"])
        self.assertEqual(data["requests"], 0)
        self.assertIn("没有找到请求日志", data["message"])

    def test_empty_log_reports_zero_requests(self):
        path = self.write("web.log", [])
        code, output = self.run_main([str(path)])
        self.assertEqual(code, 0)
        self.assertIn("总请求数：0", output)
        self.assertIn("日志里还没有请求记录", output)
        code, output = self.run_main([str(path), "--json"])
        self.assertEqual(json.loads(output)["requests"], 0)

    def test_bad_lines_are_counted_and_ignored(self):
        path = self.write("web.log", [line(at(0)), "坏行", "", line(at(1)) + " 多余"])
        code, output = self.run_main([str(path), "--json"])
        data = json.loads(output)
        self.assertEqual(data["requests"], 1)
        self.assertEqual(data["ignored_lines"], 2)

    def test_rotated_files_are_summarized(self):
        main = self.write("web.log", [line(at(2))])
        self.write("web.log.1", [line(at(1))])
        self.write("web.log.3", [line(at(0))])
        code, output = self.run_main([str(main), "--json"])
        data = json.loads(output)
        self.assertEqual(data["requests"], 3)
        self.assertEqual(len(data["log_files"]), 3)
        self.assertIn("另有 2 个轮转文件", self.run_main([str(main)])[1])

    def test_report_avoids_internal_wording(self):
        path = self.write("web.log", [line(at(0), status=429, retry_after="120")])
        output = self.run_main([str(path)])[1]
        for forbidden in ("迅雷链接", "导出", "回补", "模式 A", "模式 B"):
            self.assertNotIn(forbidden, output)


if __name__ == "__main__":
    unittest.main()
