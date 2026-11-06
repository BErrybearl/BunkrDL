"""Real module integration against a loopback Bunkr/API/CDN simulation."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import contextlib
import csv
import io
import json
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests

from bunkrdl import cli
from bunkrdl.config import Config
from bunkrdl.downloader import run_queue
from bunkrdl.export import export_links
from bunkrdl.reconcile import reconcile
from bunkrdl.resolver import BunkrClient
from bunkrdl.store import Store
from bunkrdl.web import make_server


class E2ETests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = {'1': b'image-bytes', '2': b'video-one', '3': b'video-two'}
        self.names = {'1': '照片.jpg', '2': '视频 #一.mp4', '3': '视频 #二.mp4'}
        self.calls = []
        self.challenge = False
        self.hold = False
        self.entered, self.release = threading.Event(), threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send(self, code, body, content_type='application/json', **headers):
                if isinstance(body, dict):
                    body = json.dumps(body).encode()
                elif isinstance(body, str):
                    body = body.encode('utf-8')
                self.send_response(code)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(body)))
                for key, value in headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parts = urlsplit(self.path)
                query = parse_qs(parts.query)
                owner.calls.append(('GET', parts.path))
                if parts.path == '/a/demo':
                    if owner.challenge:
                        self.send(403, 'ddos-guard', 'text/html')
                        return
                    if query.get('advanced') != ['1']:
                        self.send(400, {})
                        return
                    files = [dict(id=ident, original=name, name='cdn-' + ident,
                                  slug='slug-' + ident, size=len(owner.data[ident]))
                             for ident, name in owner.names.items()]
                    self.send(200, '<meta property="og:title" content="测试相册">'
                              '<script>window.albumFiles = ' + json.dumps(files) + ';</script>', 'text/html')
                elif parts.path.startswith('/f/slug-'):
                    ident = parts.path.rsplit('-', 1)[-1]
                    self.send(200, f'<h1>{owner.names[ident]}</h1><div data-file-id="{ident}"></div>', 'text/html')
                elif parts.path == '/sign':
                    if self.headers.get('Referer') != owner.base + '/':
                        self.send(403, {})
                    else:
                        self.send(200, {'token': 'offline-token', 'ex': 2000000000})
                elif parts.path.startswith('/cdn/cdn-'):
                    if (query.get('token') != ['offline-token'] or query.get('ex') != ['2000000000']
                            or self.headers.get('Referer') != owner.base + '/'):
                        self.send(403, {})
                        return
                    if owner.hold:
                        owner.entered.set()
                        owner.release.wait(5)
                    ident = parts.path.rsplit('-', 1)[-1]
                    disposition = 'attachment' if 'n' in query else 'inline'
                    self.send(200, owner.data[ident], 'application/octet-stream',
                              **{'Content-Disposition': disposition})
                else:
                    self.send(404, {})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                owner.calls.append(('POST', self.path))
                if owner.challenge:
                    self.send(403, 'ddos-guard', 'text/html')
                elif self.path == '/api' and self.headers.get('Referer') == owner.base + '/':
                    self.send(200, {'url': owner.base + '/cdn/cdn-' + body['id']})
                else:
                    self.send(400, {})

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.base = f'http://127.0.0.1:{self.server.server_port}'
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)
        self.config = Config(domains=[self.base], api_url=self.base + '/api',
                             sign_url=self.base + '/sign', api_referer=self.base + '/',
                             request_interval=0, file_delay_min=0, file_delay_max=0,
                             batch_pause=0, backoff_base=0, cooldown=0, max_retries=1,
                             state_db=str(self.root / 'state.db'), download_root=str(self.root / 'dl'))
        self.client = BunkrClient(self.config)
        self.addCleanup(self.client.session.close)

    def close_server(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def test_download_export_reconcile_and_refill(self):
        store = Store(self.config.state_db)
        self.addCleanup(store.close)
        album = self.client.get_album(self.base + '/a/demo')
        store.upsert_album(album)
        dest = self.root / 'dl'
        events = []
        result = run_queue(self.client, store, 'demo', dest, self.config,
                           kinds=('image',), on_event=events.append)
        self.assertEqual((result.done, result.failed), (1, 0))
        self.assertEqual((dest / self.names['1']).read_bytes(), self.data['1'])
        self.assertTrue(any(e['type'] == 'progress' and e['downloaded'] == len(self.data['1']) for e in events))
        result = export_links(self.client, store, 'demo', self.root / 'export', self.config,
                              on_event=events.append)
        self.assertEqual((result.exported, result.failed, result.stopped), (2, 0, False))
        output = Path(result.out_dir)
        self.assertEqual(output.parent, self.root / 'export')
        self.assertRegex(output.name, r'^\d{8}-\d{6}$')
        self.assertEqual(set(map(Path, result.files)), {
            output / name for name in ('part-01-links.txt', 'part-01-aria2.txt',
                                      'part-01-rename_map.csv', 'index.txt')})
        index = (output / 'index.txt').read_text(encoding='utf-8')
        for path in result.files:
            self.assertTrue(Path(path).is_file())
            if Path(path).name != 'index.txt':
                self.assertIn(Path(path).name, index)
        self.assertIn('最早过期时间', index)
        links = [line for line in (output / 'part-01-links.txt').read_text(encoding='utf-8').splitlines()
                 if not line.startswith('#')]
        self.assertEqual(len(links), 2)
        aria = (output / 'part-01-aria2.txt').read_text(encoding='utf-8')
        for url, ident in zip(links, ('2', '3')):
            self.assertEqual(parse_qs(urlsplit(url).query)['n'], [self.names[ident]])
            self.assertIn(f'{url}\n  out={self.names[ident]}\n  referer={self.base}/\n'
                          f'  header=User-Agent: {self.config.user_agent}\n', aria)
            # Also exercise the simulated CDN using the exported URL.
            response = requests.get(url, headers={'Referer': self.base + '/'}, timeout=2)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['Content-Disposition'], 'attachment')
        mapping = output / 'part-01-rename_map.csv'
        self.assertTrue(mapping.read_bytes().startswith(b'\xef\xbb\xbf'))
        with mapping.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(line for line in stream if not line.startswith('#'))
            self.assertEqual(reader.fieldnames, ['cdn_name', 'name', 'size', 'file_id', 'expires_at'])
            rows = list(reader)
        self.assertEqual([r['name'] for r in rows], [self.names['2'], self.names['3']])
        self.assertEqual([r['cdn_name'] for r in rows], ['cdn-2', 'cdn-3'])
        self.assertEqual([r['file_id'] for r in rows], ['2', '3'])
        self.assertEqual([r['size'] for r in rows], [str(len(self.data[i])) for i in ('2', '3')])
        self.assertTrue(all(r['expires_at'] != '未知' and r['expires_at'] in index for r in rows))
        self.assertNotIn('n', parse_qs(urlsplit(store.list_files('demo')[1].link_url).query))
        (dest / 'cdn-2').write_bytes(self.data['2'])
        result = reconcile(store, 'demo', dest)
        self.assertEqual(result.completed, ['2'])
        self.assertEqual(result.missing, ['3'])
        self.assertEqual(result.requeued, ['3'])
        self.assertEqual(len(result.renamed), 1)
        self.assertEqual((dest / self.names['2']).read_bytes(), self.data['2'])
        result = run_queue(self.client, store, 'demo', dest, self.config)
        self.assertEqual((result.done, result.failed), (1, 0))
        self.assertTrue(all(r.status == 'done' for r in store.list_files('demo')))
        self.assertEqual((dest / self.names['3']).read_bytes(), self.data['3'])
        # Missing/incorrect credentials really are rejected by the local CDN.
        self.assertEqual(requests.get(self.base + '/cdn/cdn-1', timeout=2).status_code, 403)
        self.assertEqual(requests.get(links[0], timeout=2).status_code, 403)

    def write_config(self):
        path = self.root / 'config.toml'
        path.write_text('\n'.join(f'{key} = {json.dumps(value)}' for key, value in vars(self.config).items()), encoding='utf-8')
        return path

    def test_cli_subprocess_album_and_single_file(self):
        cfg = self.write_config()
        prefix = [sys.executable, '-m', 'bunkrdl', '--config', str(cfg)]
        for args in (['add', self.base + '/a/demo'],
                     ['download', 'demo', '--kinds', 'image', '--dest', str(self.root / 'cli-dl')],
                     ['add', self.base + '/f/slug-2'],
                     ['download', self.base + '/f/slug-2', '--dest', str(self.root / 'single')]):
            result = subprocess.run(prefix + args, capture_output=True, text=True, encoding='utf-8',
                                    env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.root / 'cli-dl' / self.names['1']).read_bytes(), self.data['1'])
        self.assertEqual((self.root / 'single' / self.names['2']).read_bytes(), self.data['2'])

    def test_real_challenge_propagates_to_cli_exit_three(self):
        cfg = self.write_config()
        prefix = ['--config', str(cfg)]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(cli.main(prefix + ['add', self.base + '/a/demo']), 0)
            self.challenge = True
            self.assertEqual(cli.main(prefix + ['download', 'demo']), 3)
            self.assertEqual(cli.main(prefix + ['export', 'demo']), 3)
            self.assertEqual(cli.main(prefix + ['add', self.base + '/a/demo']), 3)

    def test_real_web_store_threads_and_single_file(self):
        web = make_server(self.config, port=0)
        thread = threading.Thread(target=web.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        base = f'http://127.0.0.1:{web.server_port}'
        try:
            def file_status():
                return requests.get(base + '/api/albums/file-slug-1', timeout=2).json()['files'][0]['status']

            def wait_status(states, timeout=5):
                deadline = time.time() + timeout
                while time.time() < deadline:
                    status = file_status()
                    if status in states:
                        return status
                    time.sleep(.05)
                self.fail(f'file stuck in {file_status()}')

            response = requests.post(base + '/api/albums', json={'url': self.base + '/f/slug-1'}, timeout=5)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['album_id'], 'file-slug-1')
            file_id = requests.get(base + '/api/albums/file-slug-1', timeout=2).json()['files'][0]['file_id']
            self.hold = True
            response = requests.post(base + '/api/albums/file-slug-1/enqueue',
                                     json={'file_ids': [file_id], 'action': 'download'}, timeout=2)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['added'], 1)
            self.assertTrue(self.entered.wait(5))
            # Polling must not reopen Store and reset the running file to pending.
            self.assertEqual(file_status(), 'downloading')
            state = requests.get(base + '/api/state', timeout=2).json()
            self.assertEqual(len(state['snapshot']['active']), 1)
            # A separate CLI process may query, but must not recover or start a job.
            cfg = self.write_config()
            prefix = [sys.executable, '-m', 'bunkrdl', '--config', str(cfg)]
            queried = subprocess.run(prefix + ['list', 'file-slug-1'], capture_output=True,
                                     env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}, timeout=10)
            self.assertEqual(queried.returncode, 0, queried.stderr)
            blocked = subprocess.run(prefix + ['download', 'file-slug-1'], capture_output=True,
                                     env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}, timeout=10)
            self.assertEqual(blocked.returncode, 4, blocked.stderr)
            self.assertEqual(file_status(), 'downloading')
            self.release.set()
            self.assertEqual(wait_status(('done', 'failed')), 'done')
        finally:
            self.release.set()
            web.shutdown()
            web.server_close()
            thread.join(2)


    def test_real_web_cancel_queued_requeue_and_running_conflict(self):
        self.config.image_concurrency = self.config.video_concurrency = 1
        web = make_server(self.config, port=0)
        thread = threading.Thread(target=web.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        base = f'http://127.0.0.1:{web.server_port}'

        def post(path, body):
            return requests.post(base + path, json=body, timeout=3)

        def detail():
            return {f['file_id']: f for f in requests.get(base + '/api/albums/demo', timeout=3).json()['files']}

        def wait_status(ident, wanted, timeout=5):
            deadline = time.time() + timeout
            while time.time() < deadline:
                status = detail()[ident]['status']
                if status in wanted:
                    return status
                time.sleep(.05)
            self.fail(f'{ident} stuck in {detail()[ident]["status"]}')

        try:
            self.assertEqual(post('/api/albums', {'url': self.base + '/a/demo'}).status_code, 200)
            self.assertEqual(post('/api/queue/pause', {}).status_code, 200)
            response = post('/api/albums/demo/enqueue', {'file_ids': ['1', '2'], 'action': 'download'})
            self.assertEqual(response.json()['added'], 2)
            self.assertTrue(all(detail()[i]['queued'] for i in ('1', '2')))
            response = post('/api/albums/demo/cancel-queue', {'file_ids': ['2']})
            self.assertEqual((response.status_code, response.json()['cancelled']), (200, 1))
            files = detail()
            self.assertFalse(files['2']['queued'])
            self.assertEqual(files['2']['status'], 'pending')
            self.assertTrue(files['1']['queued'])
            self.assertEqual(requests.get(base + '/api/state', timeout=3).json()['snapshot']['queued'], 1)
            response = post('/api/albums/demo/cancel-queue', {'file_ids': ['1']})
            self.assertEqual((response.status_code, response.json()['cancelled']), (200, 1))
            self.assertEqual(requests.get(base + '/api/state', timeout=3).json()['snapshot']['queued'], 0)
            # Cancelled files can be enqueued again and really download.
            response = post('/api/albums/demo/enqueue', {'file_ids': ['1'], 'action': 'download'})
            self.assertEqual(response.json()['added'], 1)
            self.assertEqual(post('/api/queue/resume', {}).status_code, 200)
            self.assertEqual(wait_status('1', ('done', 'failed')), 'done')
            # One file holds a slot while its sibling is still queued: the mixed
            # cancellation is rejected atomically, the queued sibling still can go.
            self.hold = True
            response = post('/api/albums/demo/enqueue', {'file_ids': ['2', '3'], 'action': 'download'})
            self.assertEqual(response.json()['added'], 2)
            self.assertTrue(self.entered.wait(5))
            response = post('/api/albums/demo/cancel-queue', {'file_ids': ['2', '3']})
            self.assertEqual(response.status_code, 409)
            self.assertIn('正在处理', response.json()['error'])
            files = detail()
            self.assertTrue(files['2']['active'])
            self.assertTrue(files['3']['queued'])
            response = post('/api/albums/demo/cancel-queue', {'file_ids': ['3']})
            self.assertEqual((response.status_code, response.json()['cancelled']), (200, 1))
            self.release.set()
            self.assertEqual(wait_status('2', ('done', 'failed')), 'done')
        finally:
            self.release.set()
            web.shutdown()
            web.server_close()
            thread.join(2)


if __name__ == '__main__':
    unittest.main()
