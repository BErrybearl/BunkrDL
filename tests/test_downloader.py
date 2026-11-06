import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import requests
from urllib3 import exceptions as urllib3_errors

from bunkrdl.config import Config
from bunkrdl.downloader import (DownloadStopped, download_file, run_queue,
                                _completed_local, _partial_paths)
from bunkrdl.names import safe_name
from bunkrdl.names import unique_names
from bunkrdl.resolver import BunkrClient
from bunkrdl.models import Album, AlbumFile, BunkrError, ChallengeRequired, Maintenance, NotFound, RateLimited, ResolvedLink, TransientError
from bunkrdl.store import Store


DATA = b'0123456789' * 2000
ERROR_BODIES = {
    '/json-error': b'{"error":"expired"}',
    '/text-error': b'Access denied: expired',
    '/maintenance-text': b'Server under maintenance',
    '/html-error': b'<html>error</html>',
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.seen.append((self.path, dict(self.headers)))
        if self.path.startswith('/hops/'):
            hops = int(self.path.rsplit('/', 1)[1])
            if hops:
                self.send_response(302)
                self.send_header('Location', f'/hops/{hops - 1}')
                self.end_headers()
                return
        if self.path == '/redirect':
            self.send_response(302)
            self.send_header('Location', '/maint.mp4?token=secret')
            self.end_headers()
            return
        if self.path == '/limited' or (self.path == '/once' and self.server.once == 0):
            self.server.once += 1
            self.send_response(429)
            self.send_header('Retry-After', '2')
            self.end_headers()
            return
        if self.path == '/unavailable':
            self.send_response(503)
            self.end_headers()
            return
        if self.path.startswith('/challenge-'):
            self.send_response(int(self.path.rsplit('-', 1)[1]))
            self.send_header('cf-mitigated', 'challenge')
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<html>__cf_chl challenge-platform</html>')
            return
        if self.path in ERROR_BODIES:
            body = ERROR_BODIES[self.path]
        elif self.path in ('/html', '/503-html'):
            body = b'<html>Server under maintenance</html>'
        elif self.path == '/late-html':
            body = b'<html>' + b' ' * 20000 + b'maintenance</html>'
        elif self.path == '/short':
            body = DATA[:50]
        else:
            body = DATA
        status = 200
        if self.path == '/503-html':
            status = 503
        content_range = None
        if self.headers.get('Range') and self.path not in ('/no-range', '/html', '/late-html'):
            start = int(self.headers['Range'].split('=')[1].split('-')[0])
            status = 206
            content_range = f'bytes {start}-{len(body) - 1}/{len(body)}'
            if self.path == '/bad-range':
                content_range = f'bytes 0-{len(body) - 1}/{len(body)}'
            body = body[start:]
        self.send_response(status)
        if self.path == '/503-html':
            self.send_header('Retry-After', '120')
        self.send_header('ETag', '"v1"')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Content-Type', 'text/html' if 'html' in self.path else 'application/octet-stream')
        if content_range:
            self.send_header('Content-Range', content_range)
        self.end_headers()
        self.wfile.write(body)


class FakeClient:
    def __init__(self, base, routes=None, errors=None):
        self.base = base
        self.routes = routes or {}
        self.errors = list(errors or [])
        self.calls = []

    def resolve(self, record):
        self.calls.append(record.file_id)
        if self.errors:
            error = self.errors.pop(0)
            if error:
                raise error
        return ResolvedLink(self.base + self.routes.get(record.file_id, '/data'),
                            'https://example/referer', 'cdn.bin', 123,
                            {'X-Test': 'included'})


class FixedRng:
    def uniform(self, low, high):
        return low


class DownloaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=lambda: cls.server.serve_forever(poll_interval=0.01), daemon=True)
        cls.thread.start()
        cls.base = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dest = Path(self.tmp.name)
        self.store = Store(':memory:')
        self.config = Config(file_delay_min=0, file_delay_max=0, batch_pause=0,
                             backoff_base=1, backoff_max=10, chunk_size=1024, timeout=2)
        self.events = []
        self.waits = []
        self.now = 1000.0
        self.server.seen = []
        self.server.once = 0
        self.client = FakeClient(self.base)
        self.add_files()

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def add_files(self, count=1, names=None, size=len(DATA)):
        self.store.upsert_album(Album('a', '测试', 'url', [
            AlbumFile(str(i), names[i] if names else f'{i}.bin', str(i), 'url', size=size,
                      kind='image' if i % 2 else 'video') for i in range(count)
        ]))

    def record(self, index=0):
        return self.store.list_files('a')[index]

    def single(self, **kwargs):
        return download_file(self.client, self.store, self.record(), self.dest,
                             self.config, on_event=self.events.append, **kwargs)

    def sleeper(self, seconds):
        self.waits.append(seconds)
        self.now += seconds

    def partial(self, data, route='/data', **changes):
        record = self.record()
        part, meta, _ = _partial_paths(self.dest / record.name, record)
        part.write_bytes(data)
        owner = dict(album_id='a', file_id=record.file_id, path=route,
                     size=record.size, etag='"v1"')
        owner.update(changes)
        meta.write_text(json.dumps(owner), encoding='utf-8')
        return part

    def queue(self, **kwargs):
        return run_queue(self.client, self.store, 'a', self.dest, self.config,
                         on_event=self.events.append, sleep=self.sleeper, rng=FixedRng(), clock=lambda: self.now, **kwargs)

    def test_equal_size_error_bodies_are_not_published_and_owned_parts_are_deleted(self):
        for route, body in ERROR_BODIES.items():
            with self.subTest(route=route):
                self.add_files(size=len(body))
                self.client.routes['0'] = route
                # Leave a tool-owned partial that forces a clean response.
                part = self.partial(b'old', route, etag=None)
                foreign = self.dest / 'foreign.part'
                foreign.write_bytes(b'keep')
                with self.assertRaises(BunkrError):
                    self.single()
                self.assertEqual(self.record().status, 'failed')
                self.assertTrue(any(word in self.record().last_error for word in ('失败', '维护')))
                if route in ('/json-error', '/text-error'):
                    # The reason must come from the content check, not from a size
                    # or content-type accident: the length here matches exactly.
                    self.assertIn('内容校验失败', self.record().last_error)
                self.assertFalse((self.dest / '0.bin').exists())
                self.assertFalse(part.exists())
                self.assertFalse(part.with_name(part.name + '.json').exists())
                self.assertEqual(foreign.read_bytes(), b'keep')

    def test_unknown_size_json_error_is_not_marked_done(self):
        self.add_files(names=['0.mp4'], size=None)
        self.client.routes['0'] = '/json-error'
        with self.assertRaisesRegex(BunkrError, '内容校验失败'):
            self.single()
        self.assertEqual(self.record().status, 'failed')
        self.assertFalse((self.dest / '0.mp4').exists())
        self.assertIsNone(next(iter(self.dest.glob('*.bunkrdl.part')), None))

    def test_unknown_size_media_still_completes_after_the_content_check(self):
        # The content is judged at the length actually received, so a record
        # without a known size must not fail every download with a media suffix.
        self.add_files(names=['0.mp4'], size=None)
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(self.record().status, 'done')

    def test_wrong_size_json_error_is_not_kept_as_a_resume_partial(self):
        self.client.routes['0'] = '/json-error'
        part = self.partial(b'old', '/json-error', etag=None)
        with self.assertRaisesRegex(BunkrError, '内容校验失败'):
            self.single()
        self.assertEqual(self.record().status, 'failed')
        self.assertFalse(part.exists())
        self.assertFalse(part.with_name(part.name + '.json').exists())

    def test_crash_recovery_rejects_equal_size_error_bodies(self):
        for body in ERROR_BODIES.values():
            with self.subTest(body=body):
                self.add_files(size=len(body))
                published = (self.dest / '0.bin').resolve()
                published.write_bytes(body)
                part = self.partial(body)
                self.store.update_file('a', '0', status='downloading', local_path=str(published))
                self.assertIsNone(_completed_local(self.record()))
                with self.assertRaises(BunkrError):
                    self.single()
                self.assertEqual(self.record().status, 'failed')
                self.assertIn('内容校验失败', self.record().last_error)
                self.assertEqual(published.read_bytes(), body)
                self.assertFalse(part.exists())
                self.assertFalse(part.with_name(part.name + '.json').exists())
                self.assertEqual(self.client.calls, [])

    def test_recovery_deletion_keeps_another_files_partial(self):
        record = self.record()
        body = ERROR_BODIES['/json-error']
        self.add_files(size=len(body))
        published = (self.dest / '0.bin').resolve()
        published.write_bytes(body)
        # A same-named candidate partial that belongs to another file must stay.
        part, meta, _ = _partial_paths(published, record)
        part.write_bytes(b'other')
        meta.write_text(json.dumps(dict(album_id='other', file_id='x', path='/data',
                                        size=1, etag='"v1"')), encoding='utf-8')
        self.store.update_file('a', '0', status='downloading', local_path=str(published))
        with self.assertRaisesRegex(BunkrError, '内容校验失败'):
            self.single()
        self.assertEqual(self.record().status, 'failed')
        self.assertEqual(part.read_bytes(), b'other')
        self.assertTrue(meta.exists())

    def test_done_record_holding_an_error_page_is_not_reported_done(self):
        body = ERROR_BODIES['/json-error']
        self.add_files(size=len(body))
        published = (self.dest / '0.bin').resolve()
        published.write_bytes(body)
        self.store.update_file('a', '0', status='done', local_path=str(published))
        with self.assertRaisesRegex(BunkrError, '内容校验失败'):
            self.single()
        self.assertEqual(self.record().status, 'failed')
        self.assertEqual(self.client.calls, [])

    def test_file_body_reuses_the_client_session(self):
        urls = []

        class Session:
            def get(self, url, **kwargs):
                urls.append(url)
                return requests.get(url, **kwargs)

        self.client.session = Session()
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(len(urls), 1)
        self.assertIn('/data', urls[0])
        self.assertEqual(self.record().status, 'done')

    def test_explicit_session_overrides_the_client_session(self):
        class Session:
            def __init__(self):
                self.calls = 0

            def get(self, url, **kwargs):
                self.calls += 1
                return requests.get(url, **kwargs)

        client_session, explicit = Session(), Session()
        self.client.session = client_session
        self.assertEqual(self.single(session=explicit).read_bytes(), DATA)
        self.assertEqual((client_session.calls, explicit.calls), (0, 1))

    def test_redirects_gate_every_hop_and_keep_headers(self):
        from bunkrdl.limiter import RequestThrottle
        order, starts, now = [], [], [0.0]
        def sleep(delay):
            now[0] += delay
        class Gate(RequestThrottle):
            def acquire(self, stop_event=None):
                order.append('gate')
                return super().acquire(stop_event)
        class Session:
            def get(self, url, **kwargs):
                order.append('request')
                self_test.assertFalse(kwargs['allow_redirects'])
                starts.append(now[0])
                return requests.get(url, **kwargs)
        self_test = self
        self.client.throttle = Gate(0.2, clock=lambda: now[0], sleep=sleep)
        self.client.routes['0'] = '/hops/5'
        self.assertEqual(self.single(session=Session()).read_bytes(), DATA)
        self.assertEqual(order, ['gate', 'request'] * 6)
        self.assertTrue(all(b - a >= 0.2 - 1e-9 for a, b in zip(starts, starts[1:])))
        self.assertEqual(len(self.server.seen), 6)
        for _, headers in self.server.seen:
            self.assertEqual(headers['Referer'], 'https://example/referer')
            self.assertEqual(headers['X-Test'], 'included')

    def test_redirect_limit_stops_after_five_hops(self):
        self.client.routes['0'] = '/hops/6'
        with self.assertRaisesRegex(BunkrError, '重定向次数过多'):
            self.single()
        self.assertEqual(len(self.server.seen), 6)
        self.assertEqual(self.record().status, 'failed')

    def test_pause_at_redirect_gate_sends_no_next_request(self):
        stop = threading.Event()
        class Gate:
            calls = 0
            def acquire(self, stop_event=None):
                self.calls += 1
                if self.calls == 2:
                    stop_event.set()
                return True
        self.client.throttle = Gate()
        self.client.routes['0'] = '/hops/2'
        with self.assertRaises(DownloadStopped):
            self.single(stop_event=stop)
        self.assertEqual(len(self.server.seen), 1)
        self.assertEqual(self.record().status, 'pending')

    def test_body_errors_after_a_redirect_name_the_hop_host_not_the_link_host(self):
        hop = 'https://cdn-mirror.test'

        def response(status, url, headers=None, body=b'', broken=None):
            result = requests.Response()
            result.status_code = status
            result.url = url
            result.headers.update(headers or {})
            result._content = body
            result._content_consumed = True

            def chunks(chunk_size=65536):
                yield body
                if broken is not None:
                    raise broken

            result.iter_content = chunks
            return result

        class Transport:
            def __init__(self, responses):
                self.responses = list(responses)
                self.urls = []

            def get(self, url, **kwargs):
                self.urls.append(url)
                return self.responses.pop(0)

        redirect = lambda path: response(302, 'https://origin.test/storage/a.bin',
                                         {'Location': hop + path})
        cases = {
            'rate limit': [redirect('/limited'),
                           response(429, hop + '/limited', {'Retry-After': '120'}, b'429')],
            'http error': [redirect('/missing'),
                           response(404, hop + '/missing', {}, b'nope')],
            'body timeout': [redirect('/old'),
                             response(200, hop + '/data', {'Content-Length': str(len(DATA))},
                                      DATA, broken=requests.exceptions.ReadTimeout('timed out'))],
        }
        for name, responses in cases.items():
            with self.subTest(case=name):
                transport = Transport(responses)
                with self.assertRaises((BunkrError, requests.RequestException)):
                    self.single(session=transport)
                self.assertEqual(len(transport.urls), 2)
                message = self.record().last_error
                self.assertIn('阶段：文件体', message)
                # The body came from the redirect target, so that host is named.
                self.assertIn('主机 cdn-mirror.test', message)
                self.assertNotIn('主机 origin.test', message)

    def test_cdn_503_maintenance_keeps_retry_after_and_rate_limit_kind(self):
        from bunkrdl.messages import error_kind
        self.client.routes['0'] = '/503-html'
        with self.assertRaises(RateLimited) as caught:
            self.single()
        self.assertEqual(error_kind(caught.exception), 'rate_limited')
        self.assertEqual(caught.exception.retry_after, 120)
        self.assertEqual(self.events[-1]['error_kind'], 'rate_limited')
        self.assertEqual(self.record().status, 'failed')
        self.assertFalse((self.dest / '0.bin').exists())

    def test_hidden_album_reserved_path_is_not_used_by_real_download(self):
        claimed = (self.dest / '0.bin').resolve()
        self.store.upsert_album(Album('hidden', '隐藏', 'url', [
            AlbumFile('old', '0.bin', 'old', 'url', size=len(DATA))]))
        self.store.update_file('hidden', 'old', local_path=str(claimed))
        self.store.set_album_hidden('hidden', True)
        path = self.single()
        self.assertEqual(path.name, '0 (2).bin')
        self.assertFalse(claimed.exists())
        self.assertEqual(path.read_bytes(), DATA)

    def test_failed_body_clears_link_and_explains_stage_host_retry_after(self):
        self.store.update_file('a', '0', status='exported',
                               link_url='https://cdn.test/old?token={{SECRET_a1b2c3d4}}',
                               link_expires=999)
        self.client.routes['0'] = '/503-html'
        with self.assertRaises(RateLimited):
            self.single()
        record = self.record()
        self.assertEqual(record.status, 'failed')
        self.assertIsNone(record.link_url)
        self.assertIsNone(record.link_expires)
        self.assertIn('阶段：文件体', record.last_error)
        self.assertIn('主机 127.0.0.1', record.last_error)
        self.assertIn('建议等待 120 秒', record.last_error)
        self.assertNotIn('SECRET', record.last_error)
        self.assertNotIn('/503-html', record.last_error)

    def test_successful_download_keeps_the_fresh_link(self):
        self.store.update_file('a', '0', status='exported', link_url='https://cdn.test/old',
                               link_expires=999)
        self.single()
        record = self.record()
        self.assertTrue(record.link_url.startswith(self.base))
        self.assertEqual(record.link_expires, 123)

    def test_request_log_failure_does_not_break_the_download(self):
        with patch('bunkrdl.requestlog.log_request', side_effect=RuntimeError('disk full')):
            path = self.single()
        self.assertEqual(path.read_bytes(), DATA)
        self.assertEqual(self.record().status, 'done')

    def test_normal_headers_state_and_progress_throttle(self):
        path = self.single(clock=lambda: 10)
        self.assertEqual(path.read_bytes(), DATA)
        record = self.record()
        self.assertEqual((record.status, record.attempts, record.cdn_name), ('done', 1, 'cdn.bin'))
        self.assertEqual(record.link_expires, 123)
        headers = self.server.seen[0][1]
        self.assertEqual(headers['Referer'], 'https://example/referer')
        self.assertEqual(headers['X-Test'], 'included')
        self.assertEqual(headers['Accept-Encoding'], 'identity')
        self.assertEqual([e['type'] for e in self.events], ['start', 'progress', 'done'])
        self.assertFalse((self.dest / '0.bin.part').exists())

    def test_finished_file_from_a_crash_is_adopted_not_downloaded_again(self):
        record = self.record()
        published = (self.dest / record.name).resolve()
        published.write_bytes(DATA)
        # A crash between publishing the file and recording status='done'
        # leaves exactly this state behind.
        self.store.update_file('a', record.file_id, status='downloading',
                               local_path=str(published))
        path = self.single()
        self.assertEqual(path, published)
        self.assertEqual(self.record().status, 'done')
        self.assertEqual(self.client.calls, [])
        self.assertEqual([e['type'] for e in self.events], ['start', 'done'])
        self.assertEqual(sorted(p.name for p in self.dest.iterdir()), [record.name])

    def test_completed_local_ignores_error_pages_and_unfinished_records(self):
        self.add_files(names=['0.bin'], size=None)
        published = (self.dest / '0.bin').resolve()
        published.write_bytes(b'<!doctype html><html>error page</html>')
        self.store.update_file('a', '0', status='downloading', local_path=str(published))
        self.assertIsNone(_completed_local(self.record()))

    def test_completed_local_ignores_size_mismatch_and_generated_links(self):
        published = (self.dest / '0.bin').resolve()
        published.write_bytes(DATA + b'extra')
        self.store.update_file('a', '0', status='downloading', local_path=str(published))
        self.assertIsNone(_completed_local(self.record()))
        published.write_bytes(DATA)
        self.store.update_file('a', '0', status='exported', local_path=str(published))
        self.assertIsNone(_completed_local(self.record()))

    def test_completed_local_adopts_verifiable_media_with_unknown_size(self):
        self.add_files(names=['0.png'], size=None)
        published = (self.dest / '0.png').resolve()
        published.write_bytes(b'\x89PNG\r\n\x1a\n' + b'\x00' * 32)
        self.store.update_file('a', '0', status='downloading', local_path=str(published))
        self.assertEqual(_completed_local(self.record()), published)

    def test_file_body_request_waits_for_the_shared_request_throttle(self):
        order = []

        class Throttle:
            def acquire(self, stop_event=None):
                order.append('throttle')
                return True

        class Session:
            def get(self, url, **kwargs):
                order.append('cdn')
                return requests.get(url, **kwargs)

        self.client.throttle = Throttle()
        path = self.single(session=Session())
        self.assertEqual(path.read_bytes(), DATA)
        self.assertEqual(order[:2], ['throttle', 'cdn'])

    def test_pause_while_waiting_for_the_gate_skips_the_file_body_request(self):
        order = []

        class Throttle:
            def acquire(self, stop_event=None):
                order.append('throttle')
                return False

        class Session:
            def get(self, url, **kwargs):
                order.append('cdn')
                raise AssertionError('a paused download must not reach the CDN')

        self.client.throttle = Throttle()
        with self.assertRaises(DownloadStopped):
            self.single(session=Session())
        self.assertEqual(order, ['throttle'])
        self.assertEqual(self.store.list_files('a')[0].status, 'pending')

    def test_progress_identity_bytes_and_resume_offset(self):
        self.partial(DATA[:200])
        self.single(clock=lambda: 10)
        event = next(e for e in self.events if e['type'] == 'progress')
        self.assertEqual((event['file_id'], event['name'], event['total']),
                         ('0', '0.bin', len(DATA)))
        self.assertEqual(event['bytes'], event['downloaded'])
        self.assertGreater(event['bytes'], 200)
        self.assertLessEqual(event['bytes'], len(DATA))

    def test_progress_unknown_total_remains_none(self):
        self.add_files(size=None)
        class Response:
            url = 'https://cdn.example/data'
            status_code = 200
            headers = {}
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def raise_for_status(self):
                pass
            def iter_content(self, **kwargs):
                yield DATA
        with patch('bunkrdl.downloader.requests.get', return_value=Response()):
            self.single()
        event = next(e for e in self.events if e['type'] == 'progress')
        self.assertIsNone(event['total'])
        self.assertEqual(event['bytes'], len(DATA))

    def test_queue_indices_use_eligible_files_and_survive_retry(self):
        self.add_files(5)
        self.store.update_file('a', '1', status='skipped')
        self.client.errors = [RateLimited(retry_after=2)]
        self.queue(kinds=('video',))
        events = [e for e in self.events if e['type'] in ('start', 'done')]
        self.assertEqual([(e['type'], e['index'], e['count']) for e in events],
                         [('start', 1, 3), ('start', 1, 3), ('done', 1, 3),
                          ('start', 2, 3), ('done', 2, 3), ('start', 3, 3), ('done', 3, 3)])
        self.assertTrue(all(e['name'] == e['file_id'] + '.bin' for e in events))
        wait = next(e for e in self.events if e['type'] == 'wait')
        self.assertEqual((wait['reason'], wait['until']), ('backoff', 1002))

    def test_failed_events_classify_exceptions_and_use_chinese_reasons(self):
        cases = [(Maintenance('维护'), 'maintenance', '服务器维护中'),
                 (NotFound('删除'), 'not_found', '文件已被删除'),
                 (ChallengeRequired('验证'), 'challenge', '网站要求人机验证'),
                 (RateLimited('HTTP 429'), 'rate_limited', '被网站限速（429）'),
                 (RateLimited('HTTP 503'), 'rate_limited', '服务器繁忙（503）'),
                 (TransientError('连接超时'), 'transient', '连接超时（可能是代理或网络问题）'),
                 (requests.Timeout('secret'), 'transient', '连接超时（可能是代理或网络问题）'),
                 (requests.ConnectionError('secret'), 'transient', '连接失败（可能是代理或网络问题）'),
                 (BunkrError('文件大小不符'), 'size_mismatch', '文件不完整'),
                 (BunkrError('其他'), 'other', '下载失败')]
        for error, kind, chinese in cases:
            with self.subTest(kind=kind, exception=type(error).__name__):
                self.client.errors = [error]
                with self.assertRaises((BunkrError, requests.RequestException)):
                    self.single()
                event = self.events[-1]
                self.assertEqual((event['type'], event['file_id'], event['name'], event['error_kind']),
                                 ('failed', '0', '0.bin', kind))
                self.assertIn(chinese, event['message'])
                self.assertIn(chinese, self.record().last_error)
                self.assertNotIn('secret', event['message'])
                self.assertNotIn('secret', self.record().last_error)

    def test_range_resume(self):
        self.partial(DATA[:200])
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(self.server.seen[0][1]['Range'], 'bytes=200-')

    def test_unknown_complete_partial_is_preserved(self):
        (self.dest / '0.bin.part').write_bytes(DATA)
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(self.client.calls, ['0'])
        self.assertEqual((self.dest / '0.bin.part').read_bytes(), DATA)
        self.assertEqual(self.record().status, 'done')

    def test_oversized_partial_restarts(self):
        (self.dest / '0.bin.part').write_bytes(DATA + b'extra')
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertNotIn('Range', self.server.seen[0][1])

    def test_range_ignored_restarts(self):
        self.client.routes['0'] = '/no-range'
        self.partial(b'incorrect prefix', '/no-range')
        self.assertEqual(self.single().read_bytes(), DATA)

    def test_invalid_range_keeps_existing_part(self):
        self.client.routes['0'] = '/bad-range'
        partial = self.partial(DATA[:200], '/bad-range')
        with self.assertRaises(BunkrError):
            self.single()
        self.assertEqual(partial.read_bytes(), DATA[:200])

    def test_size_mismatch(self):
        self.client.routes['0'] = '/short'
        with self.assertRaisesRegex(BunkrError, '大小不符'):
            self.single()
        self.assertEqual(self.record().status, 'failed')
        self.assertFalse((self.dest / '0.bin').exists())
        self.client.routes.clear()
        self.assertEqual(self.single().read_bytes(), DATA)

    def test_maintenance_redirect_and_html(self):
        for route in ('/redirect', '/html', '/late-html'):
            with self.subTest(route=route):
                self.client.routes['0'] = route
                with self.assertRaises(Maintenance):
                    self.single()
                self.assertEqual(self.record().status, 'failed')
                self.assertFalse((self.dest / '0.bin').exists())
                self.assertFalse((self.dest / '0.bin.part').exists())

    def test_existing_complete_skips_resolver(self):
        (self.dest / '0.bin').write_bytes(DATA)
        self.store.update_file('a', '0', status='done', local_path=str(self.dest / '0.bin'))
        self.single()
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.record().status, 'done')

    def test_clean_names_and_collisions(self):
        self.assertEqual(safe_name('a<>:"/\\|?*\x00\x1f.txt. '), 'a.txt')
        self.assertEqual(safe_name('... '), '未命名')
        self.assertEqual(safe_name('CON.txt'), '_CON.txt')
        self.add_files(3, ['same?.bin', 'same*.bin', 'same.bin'])
        (self.dest / 'same.bin').write_bytes(b'different')
        result = self.queue()
        self.assertEqual(result.done, 3)
        self.assertEqual((self.dest / 'same.bin').read_bytes(), b'different')
        for index in (2, 3, 4):
            self.assertEqual((self.dest / f'same ({index}).bin').read_bytes(), DATA)

    def test_same_size_other_file_not_reused(self):
        self.add_files(2, ['same.bin', 'same.bin'])
        self.assertEqual(self.queue().done, 2)
        self.assertEqual(len(self.client.calls), 2)
        self.assertNotEqual(self.record(0).local_path, self.record(1).local_path)

    def test_429_retry_after_and_fresh_resolution(self):
        self.client.routes['0'] = '/once'
        result = self.queue()
        self.assertEqual((result.done, result.failed), (1, 0))
        self.assertEqual(self.client.calls, ['0', '0'])
        self.assertEqual(sum(self.waits), 2)
        self.assertEqual(self.record().attempts, 2)
        kinds = [e['type'] for e in self.events]
        self.assertEqual(kinds[:3], ['start', 'failed', 'retry'])
        self.assertIn('wait', kinds)
        self.assertEqual(kinds[-2:], ['done', 'finished'])
        retry = next(e for e in self.events if e['type'] == 'retry')
        self.assertIn('被网站限速（429）', retry['message'])

    def test_quick_retry_budget_is_two_then_marks_failed_and_continues(self):
        self.add_files(2)
        self.client.routes = {'0': '/limited'}
        result = self.queue()
        # File 0: three quick attempts, then file 1 is downloaded before the retry round.
        self.assertEqual((result.done, result.failed), (1, 1))
        self.assertEqual(self.client.calls[:4], ['0', '0', '0', '1'])
        self.assertEqual(self.record(0).attempts, 6)   # 3 quick + 3 in the retry round
        self.assertEqual(self.record(1).status, 'done')
        self.assertIn('被网站限速（429）', self.record(0).last_error)
        self.assertIn('done', [e['type'] for e in self.events if e.get('file_id') == '1'])

    def test_default_quick_backoff_is_five_and_ten_seconds(self):
        self.client.routes['0'] = '/unavailable'
        self.assertEqual(self.queue().failed, 1)
        self.assertEqual([e['seconds'] for e in self.events if e['type'] == 'retry'],
                         [5, 10, 5, 10])
        self.assertEqual(self.record().attempts, 6)
        self.assertIn('服务器繁忙（503）', self.record().last_error)

    def test_retry_after_is_capped_at_quick_backoff_max(self):
        self.client.errors = [RateLimited(retry_after=120)]
        self.assertEqual(self.queue().done, 1)
        self.assertEqual(sum(self.waits), 60)

    def test_failed_files_get_one_extra_pass_at_the_end(self):
        self.add_files(2)
        self.client.routes = {'0': '/limited'}
        result = self.queue()
        self.assertEqual((result.failed, result.done), (1, 1))
        # Round one: file 0 three attempts, then file 1 once.
        # Round two retries only file 0 (three attempts) and is not repeated again.
        self.assertEqual(self.client.calls, ['0', '0', '0', '1', '0', '0', '0'])
        self.assertEqual([e['type'] for e in self.events].count('finished'), 1)
        self.assertEqual(self.record(1).status, 'done')
        self.assertEqual([e['seconds'] for e in self.events if e['type'] == 'retry'],
                         [2, 2, 2, 2])

    def test_round_retry_can_succeed_and_no_third_pass(self):
        self.client.errors = [TransientError('连接超时')] * 3
        result = self.queue()
        # Three failures in round one, then the retry round succeeds at once.
        self.assertEqual((result.done, result.failed), (1, 0))
        self.assertEqual(self.client.calls, ['0', '0', '0', '0'])
        self.assertEqual(self.record().status, 'done')

    def test_not_found_and_challenge_are_not_retried(self):
        self.client.errors = [NotFound('删除')]
        self.assertEqual(self.queue().failed, 1)
        self.assertEqual(self.client.calls, ['0'])
        self.add_files(2)
        self.store.update_file('a', '0', status='pending')
        self.client.errors = [ChallengeRequired('验证')]
        result = self.queue()
        self.assertTrue(result.aborted_reason)
        self.assertEqual(self.client.calls[-1], '0')

    def test_download_timeout_uses_connect_and_read_values(self):
        seen = {}
        class Response:
            url = 'https://cdn.example/data'
            status_code = 200
            headers = {}
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def raise_for_status(self):
                pass
            def iter_content(self, **kwargs):
                yield DATA
        def get(url, **kwargs):
            seen.update(kwargs)
            return Response()
        with patch('bunkrdl.downloader.requests.get', get):
            self.single()
        self.assertEqual(seen['timeout'], (self.config.api_timeout, self.config.read_timeout))
        self.assertEqual((self.config.api_timeout, self.config.read_timeout), (30.0, 30.0))

    def test_stop_during_streaming_keeps_partial_and_reports_stopped(self):
        self.add_files(2)
        stop = threading.Event()
        original = self.events.append
        class Response:
            url = 'https://cdn.example/data'
            status_code = 200
            headers = {}
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def raise_for_status(self):
                pass
            def iter_content(self, **kwargs):
                yield DATA[:1024]
                yield DATA[1024:]
        with patch('bunkrdl.downloader.requests.get', return_value=Response()):
            def event(value):
                original(value)
                if value['type'] == 'progress':
                    stop.set()
            result = run_queue(self.client, self.store, 'a', self.dest, self.config,
                               stop_event=stop, on_event=event, sleep=self.sleeper,
                               rng=FixedRng(), clock=lambda: self.now)
        self.assertTrue(result.stopped)
        self.assertEqual(self.record(0).status, 'pending')
        self.assertEqual(self.record(1).status, 'pending')
        self.assertTrue(any(p.suffix == '.part' for p in self.dest.iterdir()))
        self.assertIn('stopped', [e['type'] for e in self.events])

    def test_only_ids_limits_the_queue(self):
        self.add_files(3)
        result = self.queue(only_ids=['1'])
        self.assertEqual(result.done, 1)
        self.assertEqual(self.client.calls, ['1'])
        self.assertEqual([self.record(i).status for i in range(3)],
                         ['pending', 'done', 'pending'])

    def test_only_ids_and_exclude_ids_combine(self):
        self.add_files(3)
        self.assertEqual(self.queue(only_ids=['0', '2'], exclude_ids=['2']).done, 1)
        self.assertEqual(self.client.calls, ['0'])

    def test_explicit_retry_after_overrides_backoff(self):
        self.client.errors = [RateLimited(retry_after=0.5)]
        self.assertEqual(self.queue().done, 1)
        self.assertEqual(sum(self.waits), 0.5)

    def test_consecutive_limit_cooldown(self):
        self.add_files(4)
        self.config.quick_retries = 0
        self.config.cooldown = 3
        self.client.routes = {str(i): '/limited' for i in range(3)}
        result = self.queue()
        self.assertEqual((result.failed, result.done), (3, 1))
        self.assertEqual(sum(self.waits), 3)
        # One cooldown in each pass over the failing files.
        self.assertEqual(len([e for e in self.events if e['type'] == 'cooldown']), 2)

    def test_nonlimit_final_failure_does_not_cooldown(self):
        self.config.consecutive_limit_failures = 1
        self.client.errors = [Maintenance('维护中')] * 6
        self.assertEqual(self.queue().failed, 1)
        self.assertNotIn('cooldown', [e['type'] for e in self.events])

    def test_timeout_and_connection_retry(self):
        self.client.errors = [requests.Timeout('超时'), requests.ConnectionError('断线')]
        self.assertEqual(self.queue().done, 1)
        self.assertEqual(self.client.calls, ['0'] * 3)
        self.assertEqual(sum(self.waits), 15)

    def test_challenge_aborts(self):
        self.add_files(2)
        self.client.errors = [ChallengeRequired('所有域名需要验证')]
        result = self.queue()
        self.assertEqual(result.aborted_reason, '所有域名需要验证')
        self.assertEqual(self.record(0).status, 'failed')
        self.assertEqual(self.record(1).status, 'pending')
        self.assertIn('aborted', [e['type'] for e in self.events])

    def test_stop_before_queue(self):
        stop = threading.Event()
        stop.set()
        self.assertTrue(self.queue(stop_event=stop).stopped)
        self.assertEqual(self.client.calls, [])

    def test_stop_during_file_aborts_before_resolving(self):
        self.add_files(2)
        stop = threading.Event()
        original = self.events.append

        def event(value):
            original(value)
            if value['type'] == 'start':
                stop.set()

        result = run_queue(self.client, self.store, 'a', self.dest, self.config,
                           stop_event=stop, on_event=event, sleep=self.waits.append)
        self.assertTrue(result.stopped)
        self.assertEqual(result.done, 0)
        self.assertEqual(self.record(0).status, 'pending')
        self.assertEqual(self.record(1).status, 'pending')
        self.assertEqual(self.client.calls, [])

    def test_stop_during_wait(self):
        self.add_files(2)
        stop = threading.Event()
        self.config.file_delay_min = self.config.file_delay_max = 10

        def sleeper(seconds):
            self.waits.append(seconds)
            stop.set()

        result = run_queue(self.client, self.store, 'a', self.dest, self.config,
                           stop_event=stop, sleep=sleeper, rng=FixedRng())
        self.assertTrue(result.stopped)
        self.assertEqual(result.done, 1)
        self.assertLessEqual(sum(self.waits), 0.25)

    def test_kinds_status_filters_and_batch_pause(self):
        self.add_files(6)
        self.config.batch_size = 1
        self.config.batch_pause = 2
        self.store.update_file('a', '0', status='exported')
        self.store.update_file('a', '1', status='failed', attempts=5)
        self.store.update_file('a', '2', status='skipped')
        self.assertEqual(self.queue(kinds=('image',)).done, 3)
        self.assertEqual(self.client.calls, ['1', '3', '5'])
        self.assertEqual(sum(self.waits), 4)

    def test_network_interruption_preserves_partial_and_path(self):
        class BrokenResponse:
            url = 'http://127.0.0.1/data'
            status_code = 200
            headers = {"ETag": '"v1"'}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size):
                yield DATA[:9000]
                raise requests.ConnectionError('断线')

        with patch('bunkrdl.downloader.requests.get', return_value=BrokenResponse()):
            with self.assertRaises(TransientError):
                self.single()
        self.assertEqual(next(self.dest.glob('*.bunkrdl.part')).read_bytes(), DATA[:9000])
        previous = self.record().local_path
        self.single()
        self.assertEqual(self.record().local_path, previous)
        self.assertEqual(self.server.seen[-1][1]['Range'], 'bytes=9000-')

    def test_unknown_same_size_file_is_never_claimed(self):
        original = b'X' * len(DATA)
        (self.dest / '0.bin').write_bytes(original)
        path = self.single()
        self.assertEqual(path.name, '0 (2).bin')
        self.assertEqual((self.dest / '0.bin').read_bytes(), original)
        self.assertEqual(path.read_bytes(), DATA)

    def test_unknown_partial_is_preserved_and_not_resumed(self):
        unknown = self.dest / '0.bin.part'
        unknown.write_bytes(b'BAD')
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(unknown.read_bytes(), b'BAD')
        self.assertNotIn('Range', self.server.seen[0][1])

    def test_unknown_id_bound_partial_is_also_preserved(self):
        part, _, _ = _partial_paths(self.dest / '0.bin', self.record())
        part.write_bytes(b'BAD')
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(part.read_bytes(), b'BAD')

    def test_resource_path_or_size_change_restarts(self):
        for changes in ({'path': '/different'}, {'size': 99}):
            self.store.update_file('a', '0', status='pending', local_path=None)
            target = self.dest / '0.bin'
            if target.exists():
                target.unlink()
            self.partial(b'BAD', **changes)
            self.assertEqual(self.single().read_bytes(), DATA)
            self.assertNotIn('Range', self.server.seen[-1][1])

    def test_if_range_and_changed_etag_restart(self):
        self.partial(b'BAD', etag='"old"')
        self.assertEqual(self.single().read_bytes(), DATA)
        self.assertEqual(self.server.seen[0][1]['If-Range'], '"old"')
        self.assertNotIn('Range', self.server.seen[1][1])

    def test_target_directory_is_rejected(self):
        target = self.dest / '0.bin'
        target.mkdir()
        with self.assertRaises(BunkrError):
            self.single()
        self.assertTrue(target.is_dir())
        self.assertEqual(self.client.calls, [])

    def test_target_symlink_is_rejected(self):
        target = self.dest / '0.bin'
        # Model a symlink without requiring Windows developer-mode privileges.
        original = Path.is_symlink
        with patch.object(Path, 'is_symlink', lambda p: p == target or original(p)):
            with self.assertRaises(BunkrError):
                self.single()
        self.assertEqual(self.client.calls, [])

    def test_cdn_challenges_abort_immediately_for_all_statuses(self):
        self.add_files(2)
        for status in (200, 403, 429, 503):
            self.store.update_file('a', '0', status='pending')
            self.client.calls.clear()
            self.client.routes['0'] = f'/challenge-{status}'
            result = self.queue()
            self.assertTrue(result.aborted_reason)
            self.assertEqual(self.client.calls, ['0'])
            self.assertEqual(self.record(1).status, 'pending')

    def test_partial_206_unknown_size_never_becomes_done(self):
        self.add_files(size=None)
        self.partial(b'ABC')
        class Response:
            url = 'https://cdn.example/data'
            status_code = 206
            headers = {'Content-Range': 'bytes 3-5/100', 'ETag': '"v1"'}
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def raise_for_status(self):
                pass
            def iter_content(self, **kwargs):
                yield b'DEF'
        with patch('bunkrdl.downloader.requests.get', return_value=Response()):
            with self.assertRaises(BunkrError):
                self.single()
        self.assertEqual(self.record().status, 'failed')
        self.assertEqual(next(self.dest.glob('*.bunkrdl.part')).read_bytes(), b'ABCDEF')
        self.assertFalse((self.dest / '0.bin').exists())

    def test_206_checks_end_total_and_actual_received_length(self):
        self.add_files(size=None)
        class Response:
            url = 'https://cdn.example/data'
            status_code = 206
            headers = {}
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def raise_for_status(self):
                pass
            def iter_content(self, **kwargs):
                yield b'DEF'
        for content_range in ('bytes 3-2/6', 'bytes 3-6/6', 'bytes 3-5/*',
                              'bytes 3-4/5', 'bytes 3-7/8'):
            self.partial(b'ABC')
            Response.headers = {'Content-Range': content_range, 'ETag': '"v1"'}
            with patch('bunkrdl.downloader.requests.get', return_value=Response()):
                with self.assertRaises(BunkrError):
                    self.single()
            self.assertFalse((self.dest / '0.bin').exists())

    def test_whole_album_names_remain_stable_for_filtered_queue(self):
        self.add_files(2, ['same.bin', 'same.bin'])
        self.assertEqual(self.queue(kinds=('image',)).done, 1)
        self.assertEqual(Path(self.record(1).local_path).name, 'same (2).bin')
        self.assertEqual(self.queue(kinds=('video',)).done, 1)
        self.assertEqual(Path(self.record(0).local_path).name, 'same.bin')

    def test_long_names_preserve_extension_and_handle_truncated_collisions(self):
        self.add_files(3, ['a' * 300 + '.mp4', 'a' * 299 + 'b.mp4', 'a' * 300 + '.mp4'])
        names = unique_names(self.store.list_files('a'))
        self.assertEqual(len(set(names.values())), 3)
        for name in names.values():
            self.assertLessEqual(len(name), 180)
            self.assertTrue(name.endswith('.mp4'))
        self.assertEqual(self.queue().done, 3)
        self.assertEqual(safe_name('a' * 179 + '. .txt'), 'a' * 136 + '.txt')
        self.assertLessEqual(len(safe_name(' .' + 'x' * 300)), 180)

    def test_chinese_and_emoji_names_fit_encoding_and_owned_suffixes(self):
        self.add_files(4, ['中文' * 150 + '.mp4', '😀' * 200 + '.mp4',
                           '中文' * 150 + '.mp4', '😀' * 200 + '.mp4'])
        rows = self.store.list_files('a')
        names = unique_names(rows)
        self.assertEqual(len(set(names.values())), 4)
        for row in rows:
            target = self.dest / names[row.file_id]
            part, meta, _ = _partial_paths(target, row)
            for path in (target, part, meta):
                self.assertLessEqual(len(path.name.encode('utf-16-le')) // 2, 180)
                self.assertLessEqual(len(path.name.encode('utf-8')), 240)
                path.touch()
            for path in (target, part, meta):
                path.unlink()
        self.assertEqual(self.queue().done, 4)

    def test_real_resolver_timeout_gets_quick_retries_and_one_extra_pass(self):
        class Session:
            def __init__(self):
                self.calls = 0
            def request(self, *args, **kwargs):
                self.calls += 1
                raise requests.Timeout('https://example/api?token=SECRET')
        session = Session()
        self.client = BunkrClient(self.config, session=session, sleep=lambda _: None)
        self.config.request_interval = 0
        self.store.update_file('a', '0', status='failed', attempts=50)
        self.assertEqual(self.queue().failed, 1)
        # Three quick attempts plus three in the end-of-round pass.
        self.assertEqual(session.calls, 6)
        self.assertEqual(self.record().attempts, 56)
        self.assertNotIn('SECRET', self.record().last_error)
        self.assertIn('连接超时', self.record().last_error)
        self.assertEqual(sum(self.waits), 30)
        self.assertNotIn('max_retries', ' '.join(
            e.get('message', '') for e in self.events if e['type'] == 'failed'))

    def broken_body_transport(self, error):
        """A transport that sends headers, part of the body, then breaks off."""
        class Transport:
            def get(self, url, **kwargs):
                response = requests.Response()
                response.status_code = 200
                response.url = url
                response.headers.update({'Content-Length': str(len(DATA))})
                # Nothing to release: the transport owns no socket here.
                response._content_consumed = True

                def chunks(chunk_size=65536):
                    yield DATA[:9000]
                    raise error

                response.iter_content = chunks
                return response
        return Transport()

    def test_body_break_is_a_transient_failure_naming_the_body_stage(self):
        cases = {
            'chunked encoding': requests.exceptions.ChunkedEncodingError(
                'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10)),
            'bare protocol error': urllib3_errors.ProtocolError(
                'Connection aborted.', ConnectionResetError(10054, '重置')),
            'reset': ConnectionResetError(10054, '连接被对方重置'),
        }
        for name, error in cases.items():
            with self.subTest(case=name):
                try:
                    with self.assertRaises(TransientError) as caught:
                        self.single(session=self.broken_body_transport(error))
                    row = self.record()
                    self.assertEqual(row.status, 'failed')
                    self.assertIn('下载中断', row.last_error)
                    self.assertIn('阶段：文件体', row.last_error)
                    self.assertIn('主机 127.0.0.1', row.last_error)
                    self.assertNotIn('文件写入失败', row.last_error)
                    self.assertEqual(self.events[-1]['type'], 'failed')
                    self.assertEqual(self.events[-1]['error_kind'], 'transient')
                    self.assertEqual(str(caught.exception), '下载中断')
                    # The bytes already read stay behind as a resumable partial.
                    part, = self.dest.glob('*.bunkrdl.part')
                    self.assertEqual(part.read_bytes(), DATA[:9000])
                finally:
                    self.store.update_file('a', '0', status='pending', last_error=None)
                    for path in self.dest.iterdir():
                        path.unlink()

    def test_queue_quick_retries_a_body_break_and_finishes_the_file(self):
        class Session:
            def __init__(self):
                self.calls = 0

            def get(self, url, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return self_test.broken_body_transport(
                        requests.exceptions.ChunkedEncodingError(
                            'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10))
                    ).get(url, **kwargs)
                return requests.get(url, **kwargs)
        self_test = self
        self.client.session = Session()
        result = self.queue()
        self.assertEqual((result.done, result.failed), (1, 0))
        self.assertEqual(self.record().status, 'done')
        self.assertEqual(self.record().attempts, 2)
        self.assertEqual(sum(self.waits), 5)
        failed = next(e for e in self.events if e['type'] == 'failed')
        self.assertEqual(failed['error_kind'], 'transient')
        self.assertIn('下载中断', failed['message'])
        self.assertEqual((self.dest / '0.bin').read_bytes(), DATA)


class ReservationTests(unittest.TestCase):
    def test_parallel_same_names_reserve_distinct_paths_before_network(self):
        from bunkrdl import downloader
        with tempfile.TemporaryDirectory() as directory:
            store = Store(':memory:')
            self.addCleanup(store.close)
            for ident in ('a', 'b'):
                store.upsert_album(Album(ident, 'same', 'url', [AlbumFile('1', 'same.bin', 's', 'url', size=4)]))
            reserve_lock = threading.Lock()
            barrier = threading.Barrier(2)
            errors, paths, checks = [], [], []
            original = downloader._target
            update_file = store.update_file
            reservations = []

            def target(*args):
                checks.append(reserve_lock.locked())
                return original(*args)

            def update(*args, **kwargs):
                if kwargs.get('status') == 'downloading':
                    reservations.append(reserve_lock.locked())
                return update_file(*args, **kwargs)

            class Client:
                def resolve(self, record):
                    # Both slots must reach the network before either can finish.
                    barrier.wait(timeout=3)
                    return ResolvedLink('https://fake/data', 'https://fake/', 'data')

            class Response:
                url, status_code, headers = 'https://fake/data', 200, {'Content-Length': '4'}
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    pass
                def raise_for_status(self):
                    pass
                def iter_content(self, **kwargs):
                    yield b'data'

            class Session:
                def get(self, *args, **kwargs):
                    return Response()

            def run(ident):
                try:
                    paths.append(download_file(Client(), store, store.list_files(ident)[0], directory,
                                               Config(), session=Session(), reserve_lock=reserve_lock))
                except Exception as error:
                    errors.append(error)

            with (patch.object(downloader, '_target', side_effect=target),
                  patch.object(store, 'update_file', side_effect=update)):
                threads = [threading.Thread(target=run, args=(ident,)) for ident in ('a', 'b')]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=5)
                    self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(checks, [True, True])
            self.assertEqual(reservations, [True, True])
            self.assertEqual({p.name for p in paths}, {'same.bin', 'same (2).bin'})
            self.assertTrue(all(p.read_bytes() == b'data' for p in paths))
            self.assertTrue(all(store.list_files(ident)[0].status == 'done' for ident in ('a', 'b')))

    def test_hidden_album_path_remains_reserved(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(':memory:')
            self.addCleanup(store.close)
            for ident in ('a', 'b'):
                store.upsert_album(Album(ident, 'same', 'url', [AlbumFile('1', 'same.bin', 's', 'url')]))
            claimed = Path(directory).resolve() / 'same.bin'
            store.update_file('a', '1', local_path=str(claimed))
            store.set_album_hidden('a', True)
            from bunkrdl.downloader import _target
            self.assertEqual(_target(store, store.list_files('b')[0], directory).name, 'same (2).bin')


if __name__ == '__main__':
    unittest.main()
