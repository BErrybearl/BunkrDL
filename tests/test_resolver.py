"""Offline resolver checks; fixtures contain synthetic metadata only."""
import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import base64
import json
import threading
import unittest
from unittest.mock import Mock
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests

from bunkrdl.config import Config
from bunkrdl.models import (AlbumFile, BunkrError, ChallengeRequired, Maintenance,
                            NotFound, RateLimited, TransientError)
from bunkrdl.resolver import BunkrClient


FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name):
    return (FIXTURES / ("resolver_" + name + ".html")).read_text(encoding="utf-8")


def response(body="", status=200, headers=None):
    r = requests.Response()
    r.status_code = status
    r._content = (json.dumps(body, ensure_ascii=False) if isinstance(body, (dict, list)) else body).encode("utf-8")
    r.encoding = "utf-8"
    r.headers.update(headers or {})
    # The in-memory body is already consumed, like a non-streaming request.
    r._content_consumed = True
    return r


class Session:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.results:
            raise AssertionError("Unexpected request")
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        result.url = url
        return result


class Clock:
    def __init__(self):
        self.now = 0
        self.waits = []

    def clock(self):
        return self.now

    def sleep(self, delay):
        self.waits.append(delay)
        self.now += delay


class ResolverTests(unittest.TestCase):
    def client(self, *results, **overrides):
        self.session = Session(*results)
        config = Config(domains=["first.example", "second.example"], request_interval=0)
        for key, value in overrides.items():
            setattr(config, key, value)
        return BunkrClient(config, self.session)

    def file(self, file_id="1001"):
        return AlbumFile(file_id, "sample_001.jpg", "sample-slug-001", "https://old.example/f/sample-slug-001")

    def signature(self, token="sample-token"):
        return response({"token": token, "ex": 1800007200})

    def test_album_metadata_and_double_html_title(self):
        album = self.client(response(fixture("album"))).get_album("https://any-mirror.example/a/sample?foo=bar")
        self.assertEqual((album.album_id, album.title), ("sample", 'Sample "album" & demo'))
        first, second = album.files
        self.assertEqual((first.file_id, first.name, first.slug, first.size, first.cdn_name, first.kind, first.uploaded),
                         ("1001", "sample_001.jpg", "sample-slug-001", 182, "storage_001.jpg", "image", "12:00:00 01/10/2026"))
        self.assertEqual(first.page_url, "https://first.example/f/sample-slug-001")
        self.assertEqual(second.kind, "video")
        self.assertIn("advanced=1", self.session.calls[0][1])
        self.assertIn("foo=bar", self.session.calls[0][1])

    def test_unicode_escapes_without_mojibake(self):
        album = self.client(response(fixture("unicode"))).get_album("https://any.example/a/sample")
        first, second = album.files
        self.assertEqual(first.name, "sample_日本語_&'>_😃.jpg")
        self.assertEqual(first.slug, "sample_&'>_003")
        self.assertIn("%26%27%3E", first.page_url)
        self.assertEqual(first.size, 0)
        self.assertEqual(second.name, "sample_'004.zip")
        self.assertEqual(second.kind, "other")
        self.assertIsNone(second.size)

    def test_empty_album(self):
        album = self.client(response("<script>window.albumFiles = [];</script>")).get_album("https://any.example/a/sample")
        self.assertEqual(album.files, [])

    def test_cdn_endpoint_fallback(self):
        body = '<script>window.albumFiles=[{id:1,original:"sample_001.jpg",slug:"one",cdnEndpoint:"/storage_001.jpg"}];</script>'
        album = self.client(response(body)).get_album("https://any.example/a/sample")
        self.assertEqual(album.files[0].cdn_name, "storage_001.jpg")

    def test_strings_containing_js_punctuation(self):
        name = 'sample_ id: value, } ]; \\ " 日本語.jpg'
        data = [{"id": 1, "slug": "sample", "original": name}]
        body = "window.albumFiles = " + json.dumps(data, ensure_ascii=False) + ";"
        album = self.client(response(body)).get_album("https://any.example/a/sample")
        self.assertEqual(album.files[0].name, name)

    def test_plain_url_and_signature(self):
        client = self.client(response({"url": "https://cdn.example/path/storage_001.jpg?n=sample_001.jpg&token=old&ex=1", "encrypted": False}), self.signature())
        result = client.resolve(self.file())
        query = parse_qs(urlsplit(result.url).query)
        self.assertEqual(query, {"n": ["sample_001.jpg"], "token": ["sample-token"], "ex": ["1800007200"]})
        self.assertEqual(result.expires_at, 1800007200)
        self.assertEqual(result.cdn_name, "storage_001.jpg")
        self.assertEqual(result.headers["Referer"], client.config.api_referer)
        self.assertEqual(result.headers["User-Agent"], client.config.user_agent)
        self.assertEqual(self.session.calls[0][0], "POST")
        self.assertEqual(self.session.calls[0][2]["json"], {"id": "1001"})
        self.assertEqual(self.session.calls[0][2]["headers"]["Origin"], "https://dl.bunkr.cr")
        self.assertEqual(self.session.calls[1][2]["params"], {"path": "/path/storage_001.jpg"})

    def test_encrypted_hourly_xor(self):
        stamp = 1800001234
        direct = "https://cdn.example/storage_001.jpg"
        key = ("SECRET_KEY_" + str(stamp // 3600)).encode()
        encrypted = base64.b64encode(bytes(x ^ key[i % len(key)] for i, x in enumerate(direct.encode()))).decode()
        client = self.client(response({"url": encrypted, "encrypted": True, "timestamp": stamp}), self.signature())
        self.assertTrue(client.resolve(self.file()).url.startswith(direct + "?"))

    def test_current_mediafiles_path_api(self):
        client = self.client(response({"mediafiles": "https://cdn.example/base/", "path": "/folder/storage_001.jpg", "original": "sample_001.jpg"}), self.signature())
        result = client.resolve(self.file())
        self.assertTrue(result.url.startswith("https://cdn.example/folder/storage_001.jpg?"))

    def test_signature_uses_decoded_path_like_page_script(self):
        client = self.client(response({"url": "https://cdn.example/sample_%E6%97%A5%E6%9C%AC%E8%AA%9E%26.jpg"}), self.signature())
        result = client.resolve(self.file())
        self.assertEqual(self.session.calls[1][2]["params"], {"path": "/sample_日本語&.jpg"})
        self.assertEqual(result.cdn_name, "sample_日本語&.jpg")

    def test_storage_name_preserves_url_punctuation(self):
        data = [{"id": 1, "slug": "sample", "original": "sample_001.jpg", "name": "sample_#?001.jpg"}]
        client = self.client(response("window.albumFiles=" + json.dumps(data) + ";"))
        self.assertEqual(client.get_album("https://any.example/a/sample").files[0].cdn_name, "sample_#?001.jpg")

    def test_endpoints_and_headers_are_configurable(self):
        client = self.client(response({"url": "https://cdn.example/storage_001.jpg"}), self.signature(),
                             api_url="https://api.example/custom", api_referer="https://referer.example/",
                             sign_url="https://sign.example/custom", user_agent="Sample-UA")
        result = client.resolve(self.file())
        self.assertEqual(self.session.calls[0][1], "https://api.example/custom")
        self.assertEqual(self.session.calls[1][1], "https://sign.example/custom")
        self.assertEqual(result.headers, {"Referer": "https://referer.example/", "User-Agent": "Sample-UA"})

    def test_resolve_signs_again_every_time(self):
        client = self.client(response({"url": "https://cdn.example/storage_001.jpg"}), self.signature("first"),
                             response({"url": "https://cdn.example/storage_001.jpg"}), self.signature("second"))
        self.assertNotEqual(client.resolve(self.file()).url, client.resolve(self.file()).url)
        self.assertEqual(len(self.session.calls), 4)

    def test_all_single_file_routes(self):
        for route in "fvid":
            with self.subTest(route=route):
                item = self.client(response(fixture("file"))).get_file(f"https://any.example/{route}/sample")
                self.assertEqual((item.file_id, item.name, item.slug, item.kind), ("1001", "sample_001.jpg", "sample", "image"))

    def test_resolve_unknown_id_reads_file_page(self):
        client = self.client(response(fixture("file")), response({"url": "https://cdn.example/storage_001.jpg"}), self.signature())
        client.resolve(self.file(""))
        self.assertEqual(len(self.session.calls), 3)

    def test_mirror_fallback_connection_timeout(self):
        for failure in (requests.ConnectionError('offline'), requests.Timeout('timeout')):
            client = self.client(failure, response(fixture('album')))
            client.get_album('https://unlisted.example/a/sample')
            self.assertEqual(len(self.session.calls), 2)

    def test_challenge_tries_next_mirror(self):
        for failure in (response('', 403), response('DDoS-Guard', 503),
                        response('__cf_chl', 429), response('', 200, {'cf-mitigated': 'challenge'})):
            client = self.client(failure, response(fixture('album')))
            client.get_album('https://any.example/a/sample')
            self.assertEqual(len(self.session.calls), 2)

    def test_file_page_challenge_falls_back_but_api_challenge_does_not(self):
        client = self.client(response('DDoS-Guard', 403), response(fixture('file')))
        self.assertEqual(client.get_file('https://any.example/f/sample').file_id, '1001')
        self.assertEqual(len(self.session.calls), 2)
        client = self.client(response('DDoS-Guard', 503), response(fixture('file')))
        with self.assertRaises(ChallengeRequired):
            client.resolve(self.file())
        self.assertEqual(len(self.session.calls), 1)

    def test_all_domains_challenged(self):
        client = self.client(response("cf-chl-test"), response("DDoS-Guard"))
        with self.assertRaises(ChallengeRequired):
            client.get_album("https://any.example/a/sample")

    def test_all_domains_disconnected_is_not_challenge(self):
        client = self.client(requests.ConnectionError(), requests.Timeout())
        with self.assertRaises(BunkrError) as caught:
            client.get_album("https://any.example/a/sample")
        self.assertNotIsInstance(caught.exception, ChallengeRequired)

    def test_1909_stops_without_fallback(self):
        client = self.client(requests.ConnectionError("Windows error 1909"), response(fixture("album")))
        with self.assertRaisesRegex(BunkrError, "1909"):
            client.get_album("https://any.example/a/sample")
        self.assertEqual(len(self.session.calls), 1)

    def test_not_found_page_and_api(self):
        for status in (400, 404):
            with self.subTest(status=status):
                with self.assertRaises(NotFound):
                    self.client(response("", status)).resolve(self.file())
                with self.assertRaises(NotFound):
                    self.client(response("", status)).get_album("https://any.example/a/sample")

    def test_rate_limited_seconds_date_and_invalid(self):
        for status in (429, 503):
            for value, expected in (("12", 12), ("Thu, 01 Jan 1970 00:02:00 GMT", 20), ("bad", None), ("-1", None), ("inf", None), ("nan", None)):
                with self.subTest(status=status, value=value):
                    client = self.client(response("", status, {"Retry-After": value}))
                    client._wall_clock = lambda: 100
                    with self.assertRaises(RateLimited) as caught:
                        client.resolve(self.file())
                    self.assertEqual(caught.exception.retry_after, expected)
                    self.assertEqual(caught.exception.invalid_retry_after, expected is None)

    def test_real_resolve_503_maintenance_classifies_as_rate_limited(self):
        from bunkrdl.messages import error_kind
        client = self.client(response('<html>Server under maintenance</html>', 503,
                                      {'Retry-After': '120'}))
        with self.assertRaises(RateLimited) as caught:
            client.resolve(self.file())
        self.assertEqual(error_kind(caught.exception), 'rate_limited')
        self.assertEqual(caught.exception.retry_after, 120)
        self.assertEqual(self.session.calls[0][0], 'POST')

    def test_503_maintenance_challenge_has_highest_priority(self):
        client = self.client(response('Server under maintenance __cf_chl', 503,
                                      {'Retry-After': '120'}))
        with self.assertRaises(ChallengeRequired):
            client.resolve(self.file())

    def test_maintenance_html_api_url_and_redirect(self):
        cases = (response("<h2>Server under maintenance</h2>", 200),
                 response({"url": "https://cdn.example/maint.mp4"}),
                 response("", 302, {"Location": "https://cdn.example/maintenance-vid.mp4"}))
        for result in cases:
            with self.subTest(result=result.status_code):
                client = self.client(result, response(""))
                with self.assertRaises(Maintenance):
                    client.resolve(self.file())

    def test_signing_errors(self):
        for result in (response("", 404), response("", 429), response("DDoS-Guard")):
            client = self.client(response({"url": "https://cdn.example/storage_001.jpg"}), result)
            with self.assertRaises(BunkrError):
                client.resolve(self.file())

    def test_invalid_api_and_signature(self):
        for body in ("not json", [], {}, {"url": "relative"}, {"url": "!!!", "encrypted": True, "timestamp": 1}):
            with self.subTest(body=body):
                with self.assertRaises(BunkrError):
                    self.client(response(body)).resolve(self.file())
        for signature in ({}, {"token": "x", "ex": "bad"}, {"token": "", "ex": 1}, {"token": "x", "ex": "nan"}):
            with self.subTest(signature=signature):
                client = self.client(response({"url": "https://cdn.example/storage_001.jpg"}), response(signature))
                with self.assertRaises(BunkrError):
                    client.resolve(self.file())

    def test_invalid_album_and_file(self):
        for body in ("", "window.albumFiles = [oops]", "window.albumFiles = {}", "window.albumFiles = [1]", 'window.albumFiles=[{slug:"a",size:"bad"}]'):
            with self.subTest(body=body):
                with self.assertRaises(BunkrError):
                    self.client(response(body)).get_album("https://any.example/a/sample")
        with self.assertRaises(BunkrError):
            self.client(response("<h1>sample_001.jpg</h1>")).get_file("https://any.example/f/sample")

    def test_invalid_input_does_not_request(self):
        client = self.client()
        for url in ("abc", "ftp://any.example/a/sample", "https://any.example/f/sample", "https://any.example/a/sample/extra"):
            with self.assertRaises(BunkrError):
                client.get_album(url)
        self.assertEqual(self.session.calls, [])

    def test_request_throttle_including_api_and_redirect(self):
        client = self.client(response("", 302, {"Location": "/a/sample?advanced=1"}),
                             response(fixture("album")), response({"url": "https://cdn.example/storage_001.jpg"}),
                             self.signature(), request_interval=2)
        clock = Clock()
        client._clock, client._sleep = clock.clock, clock.sleep
        album = client.get_album("https://any.example/a/sample")
        client.resolve(album.files[0])
        self.assertEqual(clock.waits, [2, 2, 2])

    def test_redirect_loses_advanced_parameter(self):
        client = self.client(response("", 302, {"Location": "https://first.example/a/sample"}),
                             response("basic page"), response(fixture("album")))
        self.assertEqual(len(client.get_album("https://any.example/a/sample").files), 2)
        self.assertIn("advanced=1", self.session.calls[-1][1])

    def test_api_transport_error_is_sanitized(self):
        client = self.client(requests.ConnectionError("https://private.example/?token=secret"))
        with self.assertRaises(BunkrError) as caught:
            client.resolve(self.file())
        self.assertNotIn("secret", str(caught.exception))

    def test_network_errors_carry_stage_and_host_for_the_message(self):
        cases = (
            ([requests.Timeout()], '解析', 'dl.bunkr.cr'),
            ([requests.ConnectionError()], '解析', 'dl.bunkr.cr'),
            ([response({'url': 'https://cdn.example/a.jpg'}), requests.Timeout()], '签名',
             'glb-apisign.cdn.cr'),
            ([response({'url': 'https://cdn.example/a.jpg'}),
              response('', 429, {'Retry-After': '120'})], '签名', 'glb-apisign.cdn.cr'),
        )
        for results, stage, host in cases:
            with self.subTest(stage=stage, host=host):
                client = self.client(*results)
                with self.assertRaises(BunkrError) as caught:
                    client.resolve(self.file())
                self.assertEqual(getattr(caught.exception, 'stage', None), stage)
                self.assertEqual(getattr(caught.exception, 'host', None), host)

    def test_page_errors_carry_the_page_stage_and_failing_mirror_host(self):
        client = self.client(requests.Timeout())
        client.fail_fast = True
        with self.assertRaises(TransientError) as caught:
            client.get_album('https://unlisted.example/a/sample')
        self.assertEqual((caught.exception.stage, caught.exception.host),
                         ('相册页', 'first.example'))

    def test_error_messages_never_contain_the_signed_url(self):
        from bunkrdl.messages import describe_error, error_context, error_kind
        client = self.client(requests.Timeout('https://dl.bunkr.cr/api?token={{SECRET_a1b2c3d4}}'))
        with self.assertRaises(BunkrError) as caught:
            client.resolve(self.file())
        error = caught.exception
        message = describe_error(error_kind(error), '连接超时', **error_context(error))
        self.assertIn('阶段：解析', message)
        self.assertIn('主机 dl.bunkr.cr', message)
        self.assertNotIn('SECRET', message)
        self.assertNotIn('https://', message)

    def test_unexpected_http_error(self):
        with self.assertRaisesRegex(BunkrError, "500"):
            self.client(response("", 500)).resolve(self.file())

    def test_redirect_loop_is_bounded(self):
        client = self.client(*(response("", 302, {"Location": "/a/sample"}) for _ in range(9)))
        with self.assertRaisesRegex(BunkrError, "重定向次数"):
            client.get_album("https://any.example/a/sample")
        self.assertEqual(len(self.session.calls), 9)

    def test_throttle_accounts_for_elapsed_request_time(self):
        client = self.client(response(fixture("album")), response(fixture("album")), request_interval=2)
        clock = Clock()
        client._clock, client._sleep = clock.clock, clock.sleep
        client.get_album("https://any.example/a/sample")
        clock.now += 0.75
        client.get_album("https://any.example/a/sample")
        self.assertEqual(clock.waits, [1.25])

    def test_client_exposes_the_shared_throttle_to_the_downloader(self):
        throttle = Mock()
        client = BunkrClient(Config(), Session(), throttle=throttle)
        self.assertIs(client.throttle, throttle)
        self.assertIsNone(BunkrClient(Config(), Session()).throttle)

    def test_shared_throttle_replaces_local_pacing_for_every_request(self):
        throttle = Mock()
        throttle.acquire.return_value = True
        session = Session(response('', 302, {'Location': '/a/sample?advanced=1'}),
                          response(fixture('album')),
                          response({'url': 'https://cdn.example/a.jpg'}), self.signature())
        clock = Clock()
        client = BunkrClient(Config(request_interval=100), session, throttle=throttle,
                             clock=clock.clock, sleep=clock.sleep)
        album = client.get_album('https://any.example/a/sample')
        client.resolve(album.files[0])
        self.assertEqual(throttle.acquire.call_count, 4)
        self.assertEqual(clock.waits, [])

    def test_declined_throttle_does_not_start_request(self):
        throttle = Mock()
        throttle.acquire.return_value = False
        client = BunkrClient(Config(), Session(), throttle=throttle)
        with self.assertRaises(TransientError):
            client.resolve(self.file())
        self.assertEqual(client.session.calls, [])

    def test_fail_fast_page_transport_does_not_try_mirror(self):
        for error, message in [(requests.Timeout(), '连接超时'),
                               (requests.ConnectionError(), '连接失败')]:
            with self.subTest(error=error):
                client = self.client(error, response(fixture('album')))
                client.fail_fast = True
                with self.assertRaisesRegex(TransientError, message):
                    client.get_album('https://any.example/a/sample')
                self.assertEqual(len(self.session.calls), 1)

    def test_fail_fast_challenge_never_tries_mirror(self):
        for result in [response('DDoS-Guard', 403), response('__cf_chl', 503),
                       response('', 200, {'cf-mitigated': 'challenge'})]:
            with self.subTest(status=result.status_code):
                client = self.client(result, response(fixture('file')))
                client.fail_fast = True
                with self.assertRaises(ChallengeRequired):
                    client.get_file('https://any.example/f/sample')
                self.assertEqual(len(self.session.calls), 1)

    def test_fail_fast_plain_403_allows_only_one_mirror_fallback(self):
        for results, success in [([response('', 403), response(fixture('album'))], True),
                                 ([response('', 403), response('', 403),
                                   response(fixture('album'))], False)]:
            with self.subTest(success=success):
                client = self.client(*results, domains=['one.example', 'two.example', 'three.example'])
                client.fail_fast = True
                if success:
                    self.assertEqual(len(client.get_album('https://any.example/a/sample').files), 2)
                else:
                    with self.assertRaises(ChallengeRequired):
                        client.get_album('https://any.example/a/sample')
                self.assertEqual(len(self.session.calls), 2)

    def test_network_request_does_not_hold_instance_lock(self):
        # The barrier only opens if both threads enter the transport concurrently.
        barrier = threading.Barrier(2)
        errors = []
        class ParallelSession:
            def request(self, method, url, **kwargs):
                barrier.wait(timeout=2)
                return response('ok')
        client = BunkrClient(Config(request_interval=0), ParallelSession())
        def request():
            try:
                client._request('GET', 'https://example.test/').close()
            except Exception as error:
                errors.append(error)
        threads = [threading.Thread(target=request) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
