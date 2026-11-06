"""End-to-end rate limiting with the real Coordinator, Store and BunkrClient.

Every request goes to a loopback ``http.server`` that mimics the three real
endpoints: the album/file page, the resolver API plus signer, and the CDN file
body. Cooldowns, gaps and timeouts are shortened through config, so the whole
file runs in a few seconds. Assertions use events, counters and generous
windows, never exact wall-clock timing.
"""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import json
import re
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from bunkrdl import flows, requestlog
from bunkrdl.config import Config
from bunkrdl.jobqueue import RATE_LIMIT_PAUSE, Coordinator
from bunkrdl.limiter import RateLimitGate
from bunkrdl.resolver import BunkrClient
from bunkrdl.store import Store


def _image(tag):
    # A real JPEG signature keeps content_check happy.
    return b'\xff\xd8\xff' + (tag.encode() * 6)


def _video(tag):
    return b'\x00\x00\x00\x18ftypmp42' + (tag.encode() * 6)


FILES = [
    {'id': '1', 'name': '照片一.jpg', 'slug': 'slug-1', 'data': _image('one')},
    {'id': '2', 'name': '照片二.jpg', 'slug': 'slug-2', 'data': _image('two')},
    {'id': '3', 'name': '照片三.jpg', 'slug': 'slug-3', 'data': _image('three')},
    {'id': '4', 'name': '照片四.jpg', 'slug': 'slug-4', 'data': _image('four')},
    {'id': '5', 'name': '视频五.mp4', 'slug': 'slug-5', 'data': _video('five')},
    {'id': '6', 'name': '照片六.jpg', 'slug': 'slug-6', 'data': _image('six')},
    {'id': '7', 'name': '照片七.jpg', 'slug': 'slug-7', 'data': _image('seven')},
    {'id': '8', 'name': '照片八.jpg', 'slug': 'slug-8', 'data': _image('eight')},
    {'id': '9', 'name': '照片九.jpg', 'slug': 'slug-9', 'data': _image('nine')},
]

# One sanitized web.log line: ISO time, stage, method, host, status|error,
# Retry-After, byte count, elapsed milliseconds. No path, query or URL.
LOG_LINE = re.compile(
    r'^(?P<time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2}) '
    r'(?P<stage>相册页|解析|签名|文件体|-) '
    r'(?P<method>GET|POST) '
    r'(?P<host>[^ ]+) '
    r'(?P<result>status=\d+|error=[A-Za-z]+) '
    r'retry_after=[^ ]+ '
    r'bytes=(-|\d+) '
    r'elapsed_ms=(-|\d+\.\d+)( seq=\d+)?$')


class Simulator:
    """Loopback album page, resolver API, signer and CDN with per-file plans."""

    def __init__(self, files):
        self.files = files
        self.by_id = {item['id']: item for item in files}
        self.by_slug = {item['slug']: item['id'] for item in files}
        self.lock = threading.Lock()
        self.log = []                 # (time.time(), method, path) per request
        self.bodies = {}              # file id -> body request count
        self.inflight = self.peak = 0  # concurrent held body responses
        self.plan = {}                # file id -> list of 'ok' | ('429', retry_after)
        self.api_plan = []            # per-call delays in seconds for POST /api
        self.api_delay = 0.0          # delay for every call without a plan entry
        self.limit_sent_at = None     # when the last 429 body was answered
        self.gate = None              # body handlers wait on this event when set
        self.gates = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, code, body, content_type='application/octet-stream', **headers):
                if isinstance(body, str):
                    body = body.encode('utf-8')
                try:
                    self.send_response(code)
                    self.send_header('Content-Type', content_type)
                    self.send_header('Content-Length', str(len(body)))
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    pass  # the client already gave up (timeout or stop)

            def do_GET(self):
                parts = urlsplit(self.path)
                query = parse_qs(parts.query)
                owner.record('GET', parts.path)
                if parts.path == '/a/demo':
                    if query.get('advanced') != ['1']:
                        return self.reply(400, b'{}', 'application/json')
                    items = [{'id': item['id'], 'original': item['name'],
                              'name': 'cdn-' + item['id'], 'slug': item['slug'],
                              'size': len(item['data'])} for item in owner.files]
                    body = ('<meta property="og:title" content="限速相册">'
                            '<script>window.albumFiles = '
                            + json.dumps(items, ensure_ascii=False) + ';</script>')
                    return self.reply(200, body, 'text/html; charset=utf-8')
                if parts.path.startswith('/f/'):
                    ident = owner.by_slug.get(parts.path[len('/f/'):])
                    if ident is None:
                        return self.reply(404, b'{}')
                    return self.reply(
                        200, f'<h1>{owner.by_id[ident]["name"]}</h1>'
                             f'<div data-file-id="{ident}"></div>',
                        'text/html; charset=utf-8')
                if parts.path == '/sign':
                    if self.headers.get('Referer') != owner.base + '/':
                        return self.reply(403, b'{}')
                    return self.reply(200, json.dumps({'token': 'offline-token',
                                                       'ex': 2000000000}),
                                      'application/json')
                if parts.path.startswith('/cdn/'):
                    return owner.serve_body(self, parts.path[len('/cdn/'):], query)
                return self.reply(404, b'{}')

            def do_POST(self):
                length = int(self.headers.get('Content-Length') or 0)
                raw = self.rfile.read(length)
                owner.record('POST', urlsplit(self.path).path)
                try:
                    payload = json.loads(raw or b'{}')
                except ValueError:
                    payload = {}
                delay = owner.next_api_delay()
                if delay:
                    time.sleep(delay)
                ident = str(payload.get('id')) if isinstance(payload, dict) else ''
                if (urlsplit(self.path).path != '/api'
                        or self.headers.get('Referer') != owner.base + '/'
                        or ident not in owner.by_id):
                    return self.reply(400, b'{}')
                return self.reply(200, json.dumps({'url': owner.base + '/cdn/cdn-' + ident}),
                                  'application/json')

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.base = f'http://127.0.0.1:{self.server.server_port}'
        self.host = f'127.0.0.1:{self.server.server_port}'
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()

    # -- request bookkeeping ------------------------------------------------
    def record(self, method, path):
        with self.lock:
            self.log.append((time.time(), method, path))

    def count(self):
        with self.lock:
            return len(self.log)

    def requests_between(self, start, end=None):
        end = float('inf') if end is None else end
        with self.lock:
            return [(when, method, path) for when, method, path in self.log
                    if start < when < end]

    def api_calls(self):
        with self.lock:
            return sum(1 for _, method, path in self.log if method == 'POST' and path == '/api')

    def body_ids_between(self, start, end=None):
        return [path.rsplit('-', 1)[-1] for _, _, path in self.requests_between(start, end)
                if path.startswith('/cdn/cdn-')]

    # -- canned answers -----------------------------------------------------
    def take(self, ident):
        with self.lock:
            plan = self.plan.get(ident)
            if plan:
                return plan.pop(0)
        return 'ok'

    def next_api_delay(self):
        with self.lock:
            return self.api_plan.pop(0) if self.api_plan else self.api_delay

    def hold(self):
        """New event body handlers must wait on, so a test can watch a phase."""
        gate = threading.Event()
        with self.lock:
            self.gates.append(gate)
            self.gate = gate
        return gate

    def serve_body(self, handler, name, query):
        ident = name[len('cdn-'):]
        if (ident not in self.by_id or query.get('token') != ['offline-token']
                or 'ex' not in query
                or handler.headers.get('Referer') != self.base + '/'):
            return handler.reply(403, b'{}')
        with self.lock:
            gate = self.gate
            self.inflight += 1
            self.peak = max(self.peak, self.inflight)
            self.bodies[ident] = self.bodies.get(ident, 0) + 1
        try:
            if gate is not None:
                gate.wait(2.5)  # shorter than the configured read timeout
            action = self.take(ident)
            if isinstance(action, tuple) and action[0] == '429':
                with self.lock:
                    self.limit_sent_at = time.time()
                return handler.reply(429, b'too many requests', 'text/plain',
                                     **{'Retry-After': action[1]})
            return handler.reply(200, self.by_id[ident]['data'])
        finally:
            with self.lock:
                self.inflight -= 1

    def state(self):
        with self.lock:
            return {'requests': len(self.log), 'inflight': self.inflight,
                    'bodies': dict(self.bodies), 'gate': self.gate is not None}

    def close(self):
        with self.lock:
            gates = list(self.gates)
        for gate in gates:
            gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


class RateLimitE2ETests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        env = patch.dict(os.environ, {'BUNKRDL_HOME': str(self.home)})
        env.start()
        self.addCleanup(env.stop)
        self.addCleanup(requestlog.reset)
        self.sim = Simulator(FILES)
        self.addCleanup(self.sim.close)
        self.config = self.make_config()
        self.store = Store(self.config.state_db)
        self.addCleanup(self.store.close)
        self.addCleanup(self.close_coordinators)
        self.coordinators = []
        # Parse the simulated album with the real client, then persist it.
        album_client = BunkrClient(self.config)
        try:
            self.store.upsert_album(album_client.get_album(self.sim.base + '/a/demo'))
        finally:
            album_client.session.close()

    # -- helpers ------------------------------------------------------------
    def make_config(self, **overrides):
        config = Config(
            domains=[self.sim.base], api_url=self.sim.base + '/api',
            sign_url=self.sim.base + '/sign', api_referer=self.sim.base + '/',
            request_interval=0.0, file_delay_min=0.0, file_delay_max=0.0,
            batch_pause=0.0, backoff_base=0.0, cooldown=0.0, max_retries=1,
            web_request_interval=0.0, image_concurrency=2, video_concurrency=1,
            image_gap=0.0, video_gap_min=0.0, video_gap_max=0.0,
            web_cooldown=0.2, ratelimit_cooldown_max=1.0, ratelimit_pause_after=4,
            api_timeout=3.0, read_timeout=3.0,
            state_db=str(self.root / 'state.db'),
            download_root=str(self.root / 'dl'))
        for key, value in overrides.items():
            setattr(config, key, value)
        return config

    def client_factory(self, config):
        def factory(throttle, fail_fast=True):
            return BunkrClient(config, throttle=throttle, fail_fast=fail_fast)
        return factory

    def make_coordinator(self, config=None, store=None):
        config = config or self.config
        store = store or self.store
        coordinator = Coordinator(config, config.state_db, store,
                                  self.client_factory(config))
        coordinator.start()
        self.coordinators.append(coordinator)
        return coordinator

    def stop(self, coordinator):
        try:
            coordinator.pause_all()
        except Exception:
            pass
        try:
            coordinator.close(3)
        except Exception:
            pass

    def close_coordinators(self):
        for coordinator in reversed(self.coordinators):
            self.stop(coordinator)

    def wait(self, predicate, timeout=6.0, note='条件'):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.01)
        self.fail(f'等待{note}超时：{self.sim.state()}')

    def wait_event(self, coordinator, kind, after=0, timeout=6.0):
        def found():
            return next((event for event in coordinator.events(since=after)['events']
                         if event['type'] == kind), None)
        return self.wait(found, timeout, f'{kind} 事件')

    def wait_uncooled(self, coordinator, timeout=6.0):
        self.wait(lambda: coordinator.snapshot()['lanes']['image']['cooldown_until'] is None,
                  timeout, '冷却结束')

    def wait_idle(self, coordinator, timeout=8.0):
        def idle():
            snapshot = coordinator.snapshot()
            return not snapshot['active'] and snapshot['queued'] == 0
        self.wait(idle, timeout, '调度器空闲')

    def wait_status(self, file_id, wanted, timeout=6.0):
        def current():
            record = next((r for r in self.store.list_files('demo')
                           if r.file_id == file_id), None)
            return record if record is not None and record.status in wanted else None
        return self.wait(current, timeout, f'文件 {file_id} 状态 {wanted}')

    def statuses(self):
        return {record.file_id: record.status for record in self.store.list_files('demo')}

    # 1. A 429 body stops every lane, then exactly one probe, then full concurrency.
    def test_429_cools_every_lane_then_probes_once_and_restores_concurrency(self):
        config = self.make_config(web_cooldown=0.2, ratelimit_cooldown_max=1.0,
                                  image_concurrency=2, video_concurrency=1)
        coordinator = self.make_coordinator(config)
        self.sim.plan['1'] = [('429', '0.5')]
        self.assertEqual(coordinator.enqueue('demo', ['1'], 'download').added, 1)

        event = self.wait_event(coordinator, 'cooldown')
        until = event['until']
        cooldown = until - event['time']
        # Retry-After (0.5) is the floor and beats the 0.2 base step.
        self.assertGreaterEqual(cooldown, 0.3)
        self.assertLess(cooldown, 0.9)
        self.assertEqual(self.store.get_meta(RateLimitGate.STREAK), '1')
        self.assertIsNotNone(self.sim.limit_sent_at)
        limited_at = self.sim.limit_sent_at

        snapshot = coordinator.snapshot()
        self.assertEqual(snapshot['lanes']['image']['setting'], 2)
        # One cooldown covers the exit IP: neither lane may start a file.
        for lane in ('image', 'video'):
            self.assertEqual(snapshot['lanes'][lane]['effective'], 0)
            self.assertAlmostEqual(snapshot['lanes'][lane]['cooldown_until'], until, delta=1e-6)

        # Enqueue more work on both lanes while cooling; that makes no request.
        before = self.sim.count()
        self.assertEqual(coordinator.enqueue('demo', ['2', '3', '4'], 'download').added, 3)
        self.assertEqual(coordinator.enqueue('demo', ['5'], 'download').added, 1)
        self.assertEqual(self.sim.count(), before, '入队本身不发请求')
        self.assertEqual(coordinator.snapshot()['queued'], 4)
        probe_gate = self.sim.hold()

        # Cooldown expires: the whole window carried no request at all.
        self.wait(lambda: time.time() > until, 6, '冷却到期')
        self.assertEqual(self.sim.requests_between(limited_at, until), [],
                         '冷却期内任何通道都不得有新请求')
        # Then exactly one probe file, no second dispatch.
        self.wait(lambda: self.sim.inflight == 1, 6, '试探请求')
        snapshot = coordinator.snapshot()
        self.assertEqual([item['file_id'] for item in snapshot['active']], ['2'])
        self.assertEqual(snapshot['lanes']['image']['effective'], 1)
        self.assertEqual(snapshot['lanes']['video']['effective'], 1)
        self.assertEqual(self.sim.body_ids_between(until), ['2'])
        time.sleep(0.15)
        self.assertEqual(self.sim.inflight, 1, '试探期间只应放一个文件')
        self.assertEqual(len(coordinator.snapshot()['active']), 1)

        # Probe succeeded: streak cleared, configured concurrency is back.
        concurrency_gate = self.sim.hold()
        probe_gate.set()
        self.wait(lambda: self.sim.inflight == 3, 4, '恢复并发的三个请求')
        snapshot = coordinator.snapshot()
        self.assertEqual(snapshot['lanes']['image']['effective'], 2)
        self.assertEqual(snapshot['lanes']['video']['effective'], 1)
        self.assertEqual(sorted(item['file_id'] for item in snapshot['active']),
                         ['3', '4', '5'])
        self.assertEqual(self.store.get_meta(RateLimitGate.STREAK), '0')
        self.sim.gate = None
        concurrency_gate.set()

        self.wait(lambda: coordinator.snapshot()['queued'] == 0
                  and not coordinator.snapshot()['active'], 8, '队列清空')
        self.assertEqual(self.sim.peak, 3, '并发不得超过配置的 2+1')
        statuses = self.statuses()
        self.assertEqual(statuses['1'], 'failed')
        for ident in ('2', '3', '4', '5'):
            self.assertEqual(statuses[ident], 'done')
        failure = next(r for r in self.store.list_files('demo') if r.file_id == '1')
        self.assertIn('限速', failure.last_error)

    # 2. Exponential backoff with a cap, pause after 4, continue probes again.
    def test_backoff_grows_caps_pauses_and_continue_probes_again(self):
        config = self.make_config(web_cooldown=0.15, ratelimit_cooldown_max=0.35,
                                  ratelimit_pause_after=4, image_concurrency=2)
        coordinator = self.make_coordinator(config)
        for ident in ('1', '2', '4', '5', '6', '7'):
            self.sim.plan[ident] = [('429', '0')]
        seen, delays, streaks = 0, [], []

        def limited_round(ident):
            nonlocal seen
            self.assertEqual(coordinator.enqueue('demo', [ident], 'download').added, 1)
            event = self.wait_event(coordinator, 'cooldown', after=seen)
            seen = event['sequence']
            delays.append(event['until'] - event['time'])
            streaks.append(int(self.store.get_meta(RateLimitGate.STREAK)))
            self.wait_uncooled(coordinator)

        limited_round('1')
        limited_round('2')
        # A success in between clears the streak completely.
        self.assertEqual(coordinator.enqueue('demo', ['3'], 'download').added, 1)
        done = self.wait_event(coordinator, 'done', after=seen)
        self.assertEqual(done['file_id'], '3')
        self.assertEqual(self.store.get_meta(RateLimitGate.STREAK), '0')
        for ident in ('4', '5', '6', '7'):
            limited_round(ident)

        self.assertEqual(streaks, [1, 2, 1, 2, 3, 4])
        self.assertEqual(len(delays), 6)
        for value, expected in zip(delays, (0.15, 0.3, 0.15, 0.3, 0.35, 0.35)):
            self.assertLess(abs(value - expected), 0.06, delays)
        self.assertGreater(delays[3], 0.25, 'third step really doubles')
        self.assertLessEqual(max(delays), 0.4, 'cooldown is capped')

        snapshot = coordinator.snapshot()
        self.assertTrue(snapshot['paused'])
        self.assertEqual(snapshot['pause_kind'], 'ratelimit')
        self.assertEqual(snapshot['pause_reason'], RATE_LIMIT_PAUSE)
        paused = self.wait_event(coordinator, 'paused', after=seen)
        self.assertEqual(paused['pause_kind'], 'ratelimit')

        # "继续" drops the cooldown and starts over with a single probe.
        self.assertEqual(coordinator.enqueue('demo', ['8', '9'], 'download').added, 2)
        probe_gate = self.sim.hold()
        coordinator.resume_all()
        snapshot = coordinator.snapshot()
        self.assertFalse(snapshot['paused'])
        self.assertIsNone(snapshot['pause_kind'])
        self.assertEqual(snapshot['lanes']['image']['effective'], 1)
        self.assertEqual(self.store.get_meta(RateLimitGate.STREAK), '0')
        self.wait(lambda: self.sim.inflight == 1, 4, '继续后的试探请求')
        self.assertEqual([item['file_id'] for item in coordinator.snapshot()['active']],
                         ['8'])
        self.assertEqual(coordinator.snapshot()['queued'], 1)

        self.sim.gate = None
        probe_gate.set()
        self.wait_status('8', ('done',))
        self.assertEqual(self.store.get_meta(RateLimitGate.STREAK), '0')
        self.wait(lambda: coordinator.snapshot()['lanes']['image']['effective'] == 2,
                  6, '恢复配置并发')
        self.wait(lambda: coordinator.snapshot()['queued'] == 0
                  and not coordinator.snapshot()['active'], 8, '队列清空')
        statuses = self.statuses()
        self.assertEqual(statuses['8'], 'done')
        self.assertEqual(statuses['9'], 'done')

    # 3. One parse timeout is retried once; two are a failure with stage and host.
    def test_parse_timeout_retries_once_then_reports_stage_and_host(self):
        config = self.make_config(api_timeout=0.4, read_timeout=3.0)
        coordinator = self.make_coordinator(config)
        # The in-place retry really waits TRANSIENT_RETRY_DELAY (5 s); shorten it
        # so the scenario still runs in seconds. No source file is changed.
        with patch.object(flows, 'TRANSIENT_RETRY_DELAY', 0.25):
            self.sim.api_plan = [0.9]
            self.assertEqual(coordinator.enqueue('demo', ['1'], 'download').added, 1)
            # The retried attempt briefly writes status=failed itself, so wait for
            # the whole scheduler to go idle instead of trusting one poll.
            self.wait_idle(coordinator)
            record = next(r for r in self.store.list_files('demo') if r.file_id == '1')
            self.assertEqual(record.status, 'done', record.last_error)
            self.assertIsNone(record.last_error)
            self.assertEqual(self.sim.api_calls(), 2, '一次超时后自动重试一次')
            self.assertTrue((self.root / 'dl').is_dir())

            # Two timeouts in a row are a real failure, with stage and host.
            self.sim.api_delay = 0.9
            self.assertEqual(coordinator.enqueue('demo', ['2'], 'download').added, 1)
            self.wait_idle(coordinator)
            record = next(r for r in self.store.list_files('demo') if r.file_id == '2')
            self.assertEqual(record.status, 'failed')
            self.assertEqual(self.sim.api_calls(), 4, '连续两次超时后不再重试')
            self.assertIn('阶段：解析', record.last_error)
            self.assertIn('主机 ' + self.sim.host, record.last_error)
            self.assertIn('连接超时', record.last_error)
            self.assertNotIn('http', record.last_error)

    # 4. The web.log produced above is well formed and carries no secrets.
    def test_web_log_lines_are_shaped_and_sanitized(self):
        config = self.make_config(web_cooldown=0.2, ratelimit_cooldown_max=1.0)
        coordinator = self.make_coordinator(config)
        self.sim.plan['1'] = [('429', '0.2')]
        self.assertEqual(coordinator.enqueue('demo', ['1'], 'download').added, 1)
        self.wait_event(coordinator, 'cooldown')
        self.wait_status('1', ('failed',))
        self.assertEqual(coordinator.enqueue('demo', ['2'], 'download').added, 1)
        self.wait_status('2', ('done',))

        lines = (self.home / 'web.log').read_text(encoding='utf-8').strip().splitlines()
        self.assertTrue(lines)
        stages = set()
        for line in lines:
            match = LOG_LINE.match(line)
            self.assertIsNotNone(match, line)
            self.assertEqual(match['host'], self.sim.host)
            self.assertNotIn('/', line, line)
            self.assertNotIn('?', line, line)
            self.assertNotIn('token', line, line)
            self.assertNotIn('demo', line, line)
            self.assertNotIn('cdn-', line, line)
            self.assertNotIn('slug', line, line)
            stages.add(match['stage'])
        self.assertEqual(stages, {'相册页', '解析', '签名', '文件体'})
        limited = [line for line in lines
                   if line.split(' ')[1] == '文件体' and 'status=429' in line]
        self.assertEqual(len(limited), 1, limited)
        self.assertRegex(limited[0], r'retry_after=(0|none|invalid)( |$)')  # 日志只记整数秒
        bodies = [line for line in lines if line.split(' ')[1] == '文件体']
        self.assertTrue(bodies)
        self.assertTrue(all('bytes=-' in line for line in bodies), bodies)

    # 5. A restart during the cooldown keeps deadline, streak and probe state.
    def test_restart_keeps_cooldown_and_streak(self):
        config = self.make_config(web_cooldown=1.5, ratelimit_cooldown_max=1.5,
                                  image_concurrency=2)
        coordinator = self.make_coordinator(config)
        self.sim.plan['1'] = [('429', '0')]
        self.assertEqual(coordinator.enqueue('demo', ['1'], 'download').added, 1)
        event = self.wait_event(coordinator, 'cooldown')
        until = event['until']
        limited_at = self.sim.limit_sent_at
        self.assertEqual(self.store.get_meta(RateLimitGate.STREAK), '1')
        # Work queued during the cooldown must survive the restart too.
        self.assertEqual(coordinator.enqueue('demo', ['2', '3'], 'download').added, 2)
        self.stop(coordinator)

        store = Store(config.state_db)
        second = Coordinator(config, config.state_db, store, self.client_factory(config))
        self.addCleanup(store.close)
        self.addCleanup(lambda: self.stop(second))
        second.start()
        # stop() paused explicitly before closing, so the restart restores that pause.
        self.assertEqual(second.snapshot()['pause_kind'], 'user')
        self.assertEqual(second.snapshot()['pause_reason'], '用户已暂停')
        second.resume_all()  # clear the restored pause but keep the gate

        snapshot = second.snapshot()
        self.assertFalse(snapshot['paused'])
        self.assertEqual(snapshot['lanes']['image']['effective'], 0)
        self.assertEqual(snapshot['lanes']['image']['cooldown_until'], until)
        self.assertEqual(snapshot['lanes']['video']['effective'], 0)
        self.assertEqual(store.get_meta(RateLimitGate.STREAK), '1')
        self.assertEqual(snapshot['queued'], 2)

        # The persisted deadline still silences every lane: one whole window
        # without a single request, then only the one probe file.
        probe_gate = self.sim.hold()
        self.wait(lambda: time.time() > until, 6, '冷却到期')
        self.assertEqual(self.sim.requests_between(limited_at, until), [],
                         '重启后冷却期内不得派发')
        self.wait(lambda: self.sim.inflight == 1, 6, '重启后的试探请求')
        self.assertEqual(self.sim.body_ids_between(until), ['2'])
        self.assertEqual(len(second.snapshot()['active']), 1)
        self.assertEqual(second.snapshot()['lanes']['image']['effective'], 1)
        self.assertEqual(second.snapshot()['queued'], 1)
        self.sim.gate = None
        probe_gate.set()
        self.wait(lambda: second.snapshot()['queued'] == 0
                  and not second.snapshot()['active'], 8, '队列清空')
        statuses = {record.file_id: record.status for record in store.list_files('demo')}
        self.assertEqual((statuses['2'], statuses['3']), ('done', 'done'))


if __name__ == '__main__':
    unittest.main()
