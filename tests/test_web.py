"""Offline web contracts; other v3 modules are replaced with fakes."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import copy
import json
import shutil
import socket
import subprocess
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import bunkrdl
from bunkrdl.config import Config
from bunkrdl.models import (Album, AlbumFile, AlbumSummary, ChallengeRequired, FileRecord,
                            JobBusy, RateLimited, ReconcileResult)
from bunkrdl.store import Store
from bunkrdl.web import PAGE, make_server
from bunkrdl.worktypes import Busy, EnqueueResult, INTERRUPTED, ItemResult, WorkItem


# Shared fake-DOM preamble for the node-based PAGE behaviour checks: enough of
# document/location/history/navigator to let the inline script run without a browser.
PAGE_NODE_HARNESS = r'''
const assert = require('node:assert/strict');
const nodes = new Map();
// In a real browser children is an HTMLCollection: length, index access and item()
// only, no array methods. The fake DOM must be just as unforgiving, so a page script
// that calls children.some(...) fails here instead of only in the browser.
function childList(owner){
 const view={item(i){return i>=0&&i<owner.items.length?owner.items[i]:null},
  [Symbol.iterator](){return owner.items[Symbol.iterator]()}};
 return new Proxy(view,{get(target,prop){
  if(prop==='length')return owner.items.length;
  if(typeof prop==='string'&&/^\d+$/.test(prop))return owner.items[Number(prop)];
  return prop in target?target[prop]:undefined}})}
function node(){const n={items:[],dataset:{},value:'',hidden:false,textContent:'',checked:false,
 indeterminate:false,open:false,disabled:false,scrollTop:0,title:'',
 append(...items){this.items.push(...items)},replaceChildren(...items){this.items=items},
 setAttribute(){},focus(){document.activeElement=this},select(){},scrollIntoView(){}};
 Object.defineProperty(n,'children',{get(){return childList(n)}});
 return n}
global.document={getElementById(id){if(!nodes.has(id))nodes.set(id,node());return nodes.get(id)},
 createElement:node,activeElement:null};
global.location={hash:'',pathname:'/'};
global.history={replaceState(){}};
global.navigator={};
global.window={confirm(){return true}};
function deferred(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve}}
'''


class FakeStore:
    def __init__(self):
        self.albums = {i: Album(i, '相册 '+i, 'https://bunkr.cr/a/'+i) for i in ('abc', 'other')}
        self.records = [FileRecord('1', '同名.jpg', 's1', '/f/s1', size=10, kind='image', album_id='abc'),
                        FileRecord('2', '视频.mp4', 's2', '/f/s2', size=20, kind='video', album_id='abc',
                                   status='failed', last_error='连接超时'),
                        FileRecord('3', '同名.jpg', 's3', '/f/s3', kind='image', album_id='abc',
                                   status='exported', link_url='https://cdn.example/3', link_expires=time.time()+3600),
                        FileRecord('1', '其他.jpg', 'o1', '/f/o1', album_id='other')]
        self.meta, self.hidden, self.work = {}, set(), []
        self.closed = False
        self.trace = []

    def get_album(self, ident):
        return self.albums.get(ident)

    def list_files(self, ident):
        return [r for r in self.records if r.album_id == ident]

    def list_albums(self, include_hidden=False):
        return [AlbumSummary(a.album_id, a.title, a.url,
                             {s: sum(r.status == s for r in self.list_files(a.album_id))
                              for s in ('pending', 'downloading', 'done', 'failed', 'exported', 'skipped')})
                for a in self.albums.values() if include_hidden or a.album_id not in self.hidden]

    def list_work_items(self):
        return list(self.work)

    def upsert_album(self, album):
        self.albums[album.album_id] = album

    def set_album_hidden(self, ident, hidden):
        self.trace.append(('hidden', ident, hidden))
        self.hidden.add(ident) if hidden else self.hidden.discard(ident)

    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        self.meta[key] = value

    def remember_action(self, album_id, file_id, action):
        self.set_meta(f'web:{album_id}:last_action:{file_id}', action)

    def update_files(self, updates):
        for album_id, ident, changes in updates:
            record = next(r for r in self.records if (r.album_id, r.file_id) == (album_id, ident))
            for key, value in changes.items():
                setattr(record, key, value)

    def close(self):
        self.trace.append('store-close')
        self.closed = True


class FakeCoordinator:
    def __init__(self, config, db_path, store, client_factory):
        self.config, self.db_path, self.store, self.client_factory = config, db_path, store, client_factory
        self.started = self.closed = False
        self.calls, self.busy = [], set()
        self.throttle = object()
        self.data = dict(paused=False, pause_kind=None, pause_reason=None, stopping=False,
                         lanes={k: dict(setting=v, effective=v, cooldown_until=None)
                                for k, v in [('image', 4), ('video', 2)]}, active=[], speed=0,
                         queued=0, queued_by_album={}, consecutive_failures=0, recent_failures=[])
        self.event_list = []
        self.cooldown = 0
        self.enqueue = Mock(return_value=EnqueueResult(2, 1, 1, 1, 1, [7, 8]))
        self.retry_failed = Mock(return_value=EnqueueResult(added=1))
        self.cancel_queued = Mock(return_value=2)
        self.purge_album = Mock(return_value=3)
        self.purge_files = Mock(return_value=2)

    def cooldown_remaining(self):
        return self.cooldown

    def register_rate_limit(self, retry_after=None):
        self.calls.append(('rate_limit', retry_after))
        self.cooldown = 120
        return 220

    def start(self):
        self.started = True

    def close(self):
        self.store.trace.append('coordinator-close')
        self.closed = True

    def snapshot(self):
        return copy.deepcopy(self.data)

    def events(self, since=0):
        return {'events': [e for e in self.event_list if e['sequence'] > since],
                'next': max([since] + [e['sequence'] for e in self.event_list])}

    def mutate(self, fn, file_keys=()):
        self.calls.append(('mutate', tuple(file_keys)))
        if set(file_keys) & self.busy:
            raise Busy('忙')
        return fn()

    def ensure_album_idle(self, album_id):
        if any(w.album_id == album_id for w in self.store.work):
            raise Busy('这个相册还有排队或正在处理的文件，请等待结束或取消排队后再添加')

    def ensure_files_idle(self, file_keys):
        if set(file_keys) & self.busy:
            raise Busy('选中的文件正在处理或排队，请等待结束或取消后再操作')

    def pause_for_challenge(self):
        return self.pause_all('challenge')

    def new_client(self):
        return self.client_factory(self.throttle)

    def cancel_album(self, ident):
        self.store.trace.append(('cancel', ident))
        return 2

    def pause_all(self, reason='user'):
        self.data.update(paused=True, pause_kind=reason)
        return self.snapshot()

    def resume_all(self):
        self.data.update(paused=False, pause_kind=None)
        return self.snapshot()

    def set_concurrency(self, lane, value):
        self.calls.append(('concurrency', lane, value))
        self.data['lanes'][lane]['setting'] = value
        return self.snapshot()


class WebTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(state_db=str(Path(self.tmp.name)/'state.db'))
        self.store = FakeStore()
        self.flows = types.SimpleNamespace(album_dest=Mock(return_value='downloads/相册 [abc]'),
            check_folder=Mock(return_value=ReconcileResult(completed=['1'], missing=['2'],
                                                         mismatched=['3'], unverified=['4'])),
            prescan_folder=Mock(return_value={}),
            pick_folder=Mock(return_value=None), open_folder=Mock())
        self.export = types.SimpleNamespace(download_url=Mock(return_value='https://cdn.example/3?n=name'))
        self.client = Mock()
        self.client.get_album.return_value = Album('new', '新相册', 'https://bunkr.cr/a/new')
        self.client.get_file.return_value = AlbumFile('single', '单文件.jpg', 'slug', '/f/slug')
        self.client_class = Mock(return_value=self.client)
        self.modules = {'bunkrdl.flows': self.flows, 'bunkrdl.export': self.export,
                        'bunkrdl.jobqueue': types.SimpleNamespace(Coordinator=FakeCoordinator),
                        'bunkrdl.resolver': types.SimpleNamespace(BunkrClient=self.client_class)}
        self.patcher = patch.dict(sys.modules, self.modules)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.server = make_server(self.config, self.store, port=0)
        self.q = self.server.coordinator
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()

    def request(self, path, body=None, *, raw=None, headers=None):
        data = raw if raw is not None else json.dumps(body).encode() if body is not None else None
        req = Request(f'http://127.0.0.1:{self.server.server_port}'+path, data=data,
                      headers={'Content-Type': 'application/json', **(headers or {})})
        try:
            response = urlopen(req, timeout=3)
        except HTTPError as exc:
            response = exc
        with response:
            raw = response.read().decode('utf-8')
            return response.status, json.loads(raw) if 'application/json' in response.headers['Content-Type'] else raw

    def run_page_checks(self, checks):
        """Run node assertions against the PAGE inline script on the shared fake DOM."""
        script = PAGE.split('<script>', 1)[1].split('</script>', 1)[0].rsplit('tick();', 1)[0]
        completed = subprocess.run(
            [shutil.which('node')],
            input='(async()=>{\n'+PAGE_NODE_HARNESS+script+checks+
                  '\n})().catch(e=>{console.error(e);process.exitCode=1});',
            text=True, encoding='utf-8', capture_output=True, timeout=10)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_state_combines_snapshot_events_sidebar_and_persisted_pause(self):
        self.q.data.update(paused=True, pause_kind='failures', pause_reason='连续 3 个文件失败',
                           queued=2, queued_by_album={'abc': 2})
        self.q.event_list = [{'sequence': 1, 'type': 'failed'}, {'sequence': 3, 'type': 'paused'}]
        status, data = self.request('/api/state?since=1')
        self.assertEqual(status, 200)
        self.assertEqual(set(data), {'snapshot', 'events', 'next', 'albums'})
        self.assertEqual(data['events'], [{'sequence': 3, 'type': 'paused'}])
        self.assertEqual(data['next'], 3)
        self.assertEqual(data['snapshot']['pause_reason'], '连续 3 个文件失败')
        self.assertEqual((data['albums'][0]['failed'], data['albums'][0]['queued'], data['albums'][0]['status']), (1, 2, '已暂停'))
        for query in ('-1', 'x', '', '1&since=2'):
            self.assertEqual(self.request('/api/state?since='+query)[0], 400)
        self.store.records[0].status = 'done'
        self.assertEqual(self.request('/api/state')[1]['albums'][0]['percent'], 33)

    def test_detail_has_valid_links_and_album_scoped_ownership(self):
        self.store.work = [WorkItem(1, 'abc', '1', 'download', 1), WorkItem(2, 'other', '2', 'download', 2)]
        self.q.data['active'] = [{'work_id': 3, 'album_id': 'abc', 'file_id': '2', 'action': 'download',
                                  'bytes': 5, 'total': 20, 'speed': 1}]
        status, data = self.request('/api/albums/abc')
        self.assertEqual(status, 200)
        files = data['files']
        self.assertTrue(files[0]['queued'])
        self.assertTrue(files[1]['active'])
        self.assertEqual(files[1]['progress']['bytes'], 5)
        self.assertIsNone(files[0]['download_url'])
        self.assertIn('?n=', files[2]['download_url'])
        self.assertNotIn('link_url', files[2])
        self.assertEqual(self.export.download_url.call_args.args[2], {'1': '同名.jpg', '2': '视频.mp4', '3': '同名 (2).jpg'})
        self.assertEqual(self.q.calls, [])

    def test_expired_unknown_and_nonexported_links_are_not_exposed(self):
        for expiry in (None, time.time()-1):
            self.store.records[2].link_expires = expiry
            self.assertIsNone(self.request('/api/albums/abc')[1]['files'][2]['download_url'])
        self.store.records[2].link_expires = time.time()+3600
        self.store.records[2].status = 'failed'
        self.assertIsNone(self.request('/api/albums/abc')[1]['files'][2]['download_url'])
        self.export.download_url.assert_not_called()

    def test_add_restore_and_single_file_go_through_mutate(self):
        self.store.hidden.add('new')
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/new'})
        self.assertEqual((status, data['album_id']), (200, 'new'))
        self.assertNotIn('new', self.store.hidden)
        self.assertEqual(self.q.calls, [('mutate', ())])
        self.flows.album_dest.assert_called_with(self.config, self.store.albums['new'], store=self.store)
        self.client_class.assert_called_with(self.config, throttle=self.q.throttle, fail_fast=True)
        self.client.session.close.assert_called_once()
        self.assertEqual(self.request('/api/albums', {'url': 'file-slug'})[1]['album_id'], 'file-slug')
        self.client.get_file.assert_called_with('https://bunkr.cr/f/slug')
        self.assertEqual(self.request('/api/albums', {'url': 'not a link'})[0], 400)

    def test_enqueue_and_retry_forward_actions_and_result(self):
        status, data = self.request('/api/albums/abc/enqueue', {'file_ids': ['1', '1', '2'], 'action': 'link'})
        self.assertEqual(status, 200)
        self.assertEqual(data, dict(added=2, already=1, replaced=1, busy=1, not_applicable=1, work_ids=[7, 8]))
        self.q.enqueue.assert_called_with('abc', ['1', '2'], 'link')
        self.assertEqual(self.request('/api/albums/abc/retry-failed', {})[1]['added'], 1)
        self.q.retry_failed.assert_called_with('abc')
        for body in ({'file_ids': ['1'], 'action': 'links'}, {'file_ids': [], 'action': 'download'},
                     {'file_ids': ['missing'], 'action': 'download'}, {'file_ids': [2], 'action': 'link'}):
            self.assertEqual(self.request('/api/albums/abc/enqueue', body)[0], 400)

    def test_cancel_queue_endpoint_forwards_ids_and_rejects_running(self):
        status, data = self.request('/api/albums/abc/cancel-queue',
                                    {'file_ids': ['1', '1', '2']})
        self.assertEqual((status, data['cancelled']), (200, 2))
        self.q.cancel_queued.assert_called_with('abc', ['1', '2'])
        self.q.cancel_queued.side_effect = Busy('正在处理，请稍后或使用“暂停全部”')
        status, data = self.request('/api/albums/abc/cancel-queue', {'file_ids': ['2']})
        self.assertEqual(status, 409)
        self.assertIn('正在处理', data['error'])
        for body in ({'file_ids': []}, {'file_ids': ['missing']}, {'file_ids': [1]}):
            self.assertEqual(self.request('/api/albums/abc/cancel-queue', body)[0], 400)
        self.assertEqual(self.q.cancel_queued.call_count, 2)

    def test_album_id_with_file_prefix_is_not_a_single_file(self):
        self.assertEqual(self.request('/api/albums', {'url': 'https://bunkr.cr/a/file-example'})[0], 200)
        self.client.get_album.assert_called_once_with('https://bunkr.cr/a/file-example')
        self.client.get_file.assert_not_called()

    def test_add_album_honours_the_shared_cooldown_before_any_request(self):
        self.q.cooldown = 90.4
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/new'})
        self.assertEqual(status, 429)
        self.assertIn('建议等待 91 秒', data['error'])
        self.assertEqual(data['retry_after'], 91)
        self.client.get_album.assert_not_called()
        self.client_class.assert_not_called()
        self.assertEqual(self.q.calls, [])
        # Bad input is still a parameter error, not a cooldown answer.
        self.assertEqual(self.request('/api/albums', {'url': 'not a link'})[0], 400)
        self.assertEqual(self.request('/api/albums', {})[0], 400)

    def test_add_album_rate_limit_is_registered_on_the_gate(self):
        self.client.get_album.side_effect = RateLimited('HTTP 429', retry_after=45)
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/new'})
        self.assertEqual(status, 429)
        self.assertIn('建议等待 120 秒', data['error'])
        self.assertEqual(self.q.calls[-1], ('rate_limit', 45))
        self.client.session.close.assert_called_once()
        for word in ('迅雷链接', '导出', '回补', '模式 A', '模式 B'):
            self.assertNotIn(word, data['error'])

    def test_legacy_errors_are_chinese_and_failed_add_closes_session(self):
        self.store.records[0].last_error = 'Server under maintenance'
        self.assertEqual(self.request('/api/albums/abc')[1]['files'][0]['last_error'],
                         '服务器维护中，稍后点“重试失败的”')
        self.client.get_album.side_effect = ValueError('无效相册')
        self.assertEqual(self.request('/api/albums', {'url': 'https://bunkr.cr/a/new'})[0], 400)
        self.client.session.close.assert_called_once()
        self.assertNotIn('new', self.store.albums)

    def test_remove_cancels_and_hides_in_one_mutate(self):
        self.assertEqual(self.request('/api/albums/abc/remove', {})[1]['cancelled'], 2)
        self.assertEqual(self.store.trace[:2], [('cancel', 'abc'), ('hidden', 'abc', True)])
        self.assertEqual(self.q.calls, [('mutate', ())])
        self.assertIsNotNone(self.store.get_album('abc'))
        self.assertEqual(len(self.store.list_files('abc')), 3)
        self.assertEqual([a['id'] for a in self.request('/api/state')[1]['albums']], ['other'])

    def test_remove_does_nothing_when_the_task_lock_is_busy(self):
        self.q.mutate = Mock(side_effect=JobBusy('已有其他任务运行中'))
        status, data = self.request('/api/albums/abc/remove', {})
        self.assertEqual(status, 409)
        self.assertIn('稍后再操作', data['error'])
        self.assertNotIn(('cancel', 'abc'), self.store.trace)
        self.assertEqual([r.status for r in self.store.list_files('abc')], ['pending', 'failed', 'exported'])

    def test_skip_busy_is_409_and_other_album_untouched(self):
        self.q.busy.add(('abc', '1'))
        self.assertEqual(self.request('/api/albums/abc/skip', {'file_ids': ['1']})[0], 409)
        self.assertEqual(self.store.records[0].status, 'pending')
        self.q.busy.clear()
        self.assertEqual(self.request('/api/albums/abc/skip', {'file_ids': ['1']})[1]['updated'], 1)
        self.assertEqual(self.store.records[0].status, 'skipped')
        self.assertEqual(self.store.records[-1].status, 'pending')
        self.assertEqual(self.q.calls[-1], ('mutate', (('abc', '1'),)))

    def test_mark_done_retains_path_and_ownership_protection(self):
        target = Path(self.tmp.name)/'confirmed.jpg'
        target.write_bytes(b'confirmed')
        body = {'file_ids': ['1'], 'paths': {'1': str(target)}}
        self.store.records[-1].local_path = str(target)
        self.assertEqual(self.request('/api/albums/abc/mark-done', body)[0], 400)
        self.store.records[-1].local_path = None
        self.assertEqual(self.request('/api/albums/abc/mark-done', body)[1]['updated'], 1)
        self.assertEqual(self.store.records[0].status, 'done')
        self.assertEqual(self.request('/api/albums/abc/mark-done', {'file_ids': ['2']})[0], 400)
        self.q.busy.add(('abc', '1'))
        self.assertEqual(self.request('/api/albums/abc/mark-done', body)[0], 409)

    def test_check_guards_whole_album_and_remembers_folder(self):
        self.q.busy.add(('abc', '3'))
        self.assertEqual(self.request('/api/albums/abc/check', {'folder': 'external'})[0], 409)
        self.flows.check_folder.assert_not_called()
        self.assertEqual(self.store.meta, {})
        self.q.busy.clear()
        status, data = self.request('/api/albums/abc/check', {'folder': ' external '})
        self.assertEqual(status, 200)
        self.assertEqual(data['failed_ids'], ['2', '3'])
        self.assertEqual(data['unverified'], ['4'])
        self.assertEqual(self.q.calls[-1], ('mutate', (('abc', '1'), ('abc', '2'), ('abc', '3'))))
        self.assertEqual(self.store.meta, {'web:abc:folder': 'external'})
        self.flows.prescan_folder.assert_called_once_with(self.store, 'abc', 'external')
        self.flows.check_folder.assert_called_with(self.store, 'abc', 'external', checks={})
        self.assertEqual(self.request('/api/albums/abc')[1]['folder'], 'external')
        self.assertEqual(self.request('/api/albums/abc/check', {'folder': ''})[0], 400)

    def test_folder_apis(self):
        self.assertEqual(self.request('/api/albums/abc/open-folder', {})[0], 200)
        self.flows.open_folder.assert_called_with('downloads/相册 [abc]')
        self.assertEqual(self.q.calls[-1], ('mutate', ()))
        self.assertEqual(self.request('/api/pick-folder', {})[1], {'path': None})
        self.flows.pick_folder.return_value = 'external'
        self.assertEqual(self.request('/api/pick-folder', {'initial': 'old'})[1], {'path': 'external'})
        self.flows.pick_folder.assert_called_with('old')
        self.assertEqual(self.request('/api/pick-folder', {'initial': 3})[0], 400)

    def test_pause_resume_and_concurrency_validation(self):
        self.assertTrue(self.request('/api/queue/pause', {})[1]['paused'])
        self.assertFalse(self.request('/api/queue/resume', {})[1]['paused'])
        for lane, value in [('image', 6), ('video', 3)]:
            self.assertEqual(self.request('/api/queue/concurrency', {'lane': lane, 'value': value})[0], 200)
        for lane, value in [('image', 7), ('video', 4), ('image', 0), ('video', True), ('video', '2'), ('x', 1)]:
            self.assertEqual(self.request('/api/queue/concurrency', {'lane': lane, 'value': value})[0], 400)

    def test_purge_endpoints_validate_and_forward(self):
        status, data = self.request('/api/albums/abc/purge', {})
        self.assertEqual((status, data['deleted']), (200, 3))
        self.assertIn('磁盘上已下载的文件未删除', data['message'])
        self.q.purge_album.assert_called_with('abc')
        status, data = self.request('/api/albums/abc/files/delete', {'file_ids': ['1', '1', '2']})
        self.assertEqual((status, data['deleted']), (200, 2))
        self.q.purge_files.assert_called_with('abc', file_ids=['1', '2'])
        self.q.purge_files.reset_mock()
        self.assertEqual(self.request('/api/albums/abc/files/delete',
                                      {'statuses': ['done', 'done', 'skipped']})[0], 200)
        self.q.purge_files.assert_called_with('abc', statuses=['done', 'skipped'])
        self.q.purge_files.reset_mock()
        self.assertEqual(self.request('/api/albums/abc/files/delete',
                                      {'file_ids': ['1', '2'], 'statuses': ['done']})[0], 200)
        self.q.purge_files.assert_called_with('abc', file_ids=['1', '2'], statuses=['done'])
        self.q.purge_files.reset_mock()
        for body in ({}, {'file_ids': []}, {'file_ids': ['missing']}, {'file_ids': [1]},
                     {'statuses': []}, {'statuses': ['nope']}, {'statuses': 'done'}):
            with self.subTest(body=body):
                self.assertEqual(self.request('/api/albums/abc/files/delete', body)[0], 400)
        self.q.purge_files.assert_not_called()
        self.assertEqual(self.request('/api/albums/missing/purge', {})[0], 404)
        self.assertEqual(self.request('/api/albums/missing/files/delete', {'statuses': ['done']})[0], 404)
        self.q.purge_album.side_effect = Busy('还有文件在排队或处理，请先取消排队或暂停后再清除')
        status, data = self.request('/api/albums/abc/purge', {})
        self.assertEqual(status, 409)
        self.assertIn('排队或处理', data['error'])

    def test_transport_guards_and_unknown_routes(self):
        for host in ('evil.example', '127.0.0.1.evil', 'localhost@evil', '[::1]'):
            self.assertEqual(self.request('/', headers={'Host': host})[0], 403)
            self.assertEqual(self.request('/api/queue/pause', {}, headers={'Host': host})[0], 403)
        self.assertEqual(self.request('/', headers={'Host': 'localhost:1234'})[0], 200)
        for raw in (b'{', b'[]', b'null', b'"abc"', b'\xff'):
            self.assertEqual(self.request('/api/albums', raw=raw)[0], 400)
        self.assertEqual(self.request('/api/albums', raw=b'{}', headers={'Content-Type': 'text/plain'})[0], 415)
        self.assertEqual(self.request('/api/albums', raw=b' '*65537)[0], 413)
        for path in ('/unknown', '/api/albums/missing', '/api/albums/missing/skip', '/api/albums/abc/unknown'):
            self.assertEqual(self.request(path, {} if path.endswith(('skip', 'unknown')) else None)[0], 404)

    def test_post_drains_before_rejection(self):
        for headers in (b'Host: evil\r\nContent-Type: application/json',
                        b'Host: localhost\r\nContent-Type: text/plain'):
            with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=2) as conn:
                conn.sendall(b'POST /api/queue/pause HTTP/1.0\r\n'+headers+b'\r\nContent-Length: 2\r\n\r\n{')
                conn.settimeout(.05)
                with self.assertRaises(socket.timeout):
                    conn.recv(1)
                conn.sendall(b'}')
                conn.settimeout(2)
                self.assertRegex(conn.recv(1024), b'HTTP/1.0 (403|415)')

    def test_duplicate_host_and_invalid_lengths(self):
        for headers in (b'Host: localhost\r\nHost: localhost\r\nContent-Length: 2',
                        b'Host: localhost\r\nContent-Length: -1',
                        b'Host: localhost\r\nContent-Length: 2\r\nContent-Length: 2'):
            with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=2) as conn:
                conn.sendall(b'POST /api/queue/pause HTTP/1.0\r\n'+headers+b'\r\n\r\n{}')
                self.assertRegex(conn.recv(1024), b'HTTP/1.0 (400|403)')

    def test_errors_are_json_and_server_survives(self):
        self.q.enqueue.side_effect = RuntimeError('private detail')
        status, data = self.request('/api/albums/abc/enqueue', {'file_ids': ['1'], 'action': 'download'})
        self.assertEqual(status, 500)
        self.assertNotIn('private detail', data['error'])
        self.assertEqual(self.request('/api/state')[0], 200)

    def test_page_controls_and_selection_guards(self):
        status, page = self.request('/')
        self.assertEqual(status, 200)
        for text in ('进行中', '总速度', '图片同时下载数', '视频同时下载数', '暂停全部', '查看失败的',
                     '检查外部下载文件夹', '打开下载文件夹', '重试失败的', '下载链接', '链接已生成',
                     '全选当前筛选', 'navigator.clipboard', 'copy-fallback', 'failed_ids', 'revision!==token',
                     'chosen.clear()', "setFilter('failed')", '当前限为', '冷却至', '正常', '正在暂停…',
                     '取消排队', '正在处理，请稍后或使用“暂停全部”', '排队中：勾选后可取消排队',
                     '清除这个相册的记录', '清除选中的记录', '只清除记录，不会删除已下载的文件'):
            self.assertIn(text, page)
        for text in ('id="resume"', '迅雷链接', '导出', '回补', '模式 A', '模式 B', '任务队列', '下载未完成的',
                     'renderRow'):
            self.assertNotIn(text, page)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_lane_defaults_and_ratelimit_banner_wording(self):
        checks = r'''
assert.equal(Number($('image-concurrency').value),2,'图片默认同时处理数应为 2');
assert.equal(Number($('video-concurrency').value),1,'视频默认同时处理数应为 1');
snapshot={active:[],paused:true,pause_kind:'ratelimit',stopping:false,queued:0,speed:0,
 pause_reason:'',recent_failures:[],
 lanes:{image:{setting:2,effective:0,cooldown_until:0},video:{setting:1,effective:0,cooldown_until:0}}};
albums=[];renderTop();
assert.equal($('banner').hidden,false);
assert.equal($('banner-text').textContent,'已暂停：被网站连续限速，已暂停');
assert.equal($('banner').className,'auto','自动暂停用红色');
assert.equal($('pause').textContent,'继续','顶栏按钮切为继续');
assert.equal($('pause').className,'auto');
// pause_reason 缺失时也不能退回“等待继续”。
assert.ok(!/等待继续/.test($('banner-text').textContent));
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_status_cards_cooldown_and_pause_colors(self):
        checks = r"""
const now=Math.floor(Date.now()/1000);
const mk=(id,name,lane,bytes,total,action)=>({work_id:id,album_id:'a',file_id:id,name,lane,bytes,total,action,speed:512});
snapshot={active:[mk('1','a.jpg','image',512,2048,'download'),mk('2','b.mp4','video',0,4096,'link'),
 mk('3','c.jpg','image',0,0,'download'),mk('4','d.jpg','image',1024,2048,'download')],
 paused:false,pause_kind:'',pause_reason:'',stopping:false,queued:3,speed:2048,recent_failures:[],
 lanes:{image:{setting:2,effective:2,cooldown_until:0},video:{setting:1,effective:1,cooldown_until:0}}};
album=null;albums=[{id:'a',title:'A',percent:0,status:'已暂停',failed:2,queued:0}];
renderTop();
assert.equal($('active-count').textContent,'下载中 4','摘要应显示下载中数量');
assert.equal($('queued').textContent,'等待 3');
assert.equal($('failed-count').textContent,'失败 2','摘要应显示失败数量');
album={album_id:'a',files:[],counts:{failed:1,queued:0}};renderTop();
assert.equal($('failed-count').textContent,'失败 2','失败数按全部相册汇总，不随当前相册跳变');
album=null;
assert.equal($('speed').textContent,'总速度 2.0 KB/秒');
assert.equal($('active-total').textContent,'4 个文件');
assert.equal($('active-list').children.length,4,'三张卡片加一个 +N');
const cards=$('active-list').children;
assert.equal(cards[3].textContent,'+1 个');
assert.equal(cards[0].className,'active-card');
assert.equal(cards[0].children[1].value,25,'进度按字节比');
assert.ok(cards[0].children[2].textContent.includes('%'));
assert.ok(cards[0].children[2].textContent.includes('/秒'));
assert.equal(cards[1].children[1].textContent,'正在获取下载链接 · 512 B/秒','链接任务未收到字节时显示阶段文案');
assert.equal(cards[2].children[1].textContent,'正在连接… · 512 B/秒');
assert.equal($('cooldown-text').textContent,'正常');
assert.equal($('cooldown-bar').hidden,true);
assert.equal($('pause').className,'','未暂停时暂停按钮无状态色');
assert.equal($('banner').hidden,false,'未暂停但有失败时横幅常驻');
assert.equal($('banner').className,'failure');
assert.equal($('banner-text').textContent,'有 2 个文件失败');
assert.equal($('show-failed').hidden,false);
albums=[];renderTop();
assert.equal($('banner').hidden,true,'无暂停无失败时横幅隐藏');
albums=[{id:'a',title:'A',percent:0,status:'已暂停',failed:2,queued:0}];
snapshot.lanes={image:{setting:2,effective:1,cooldown_until:now+240},video:{setting:1,effective:1,cooldown_until:0}};
renderTop();
assert.ok(/^冷却至 \d\d:\d\d（剩 \d+ 秒）$/.test($('cooldown-text').textContent),'冷却文案');
assert.ok($('cooldown-note').textContent.includes('图片限速'));
assert.equal($('cooldown-bar').hidden,false);
const first=$('cooldown-bar').value;
cooldownWatch.image.start-=120;
renderTop();
assert.ok($('cooldown-bar').value>first,'冷却进度条随时间前进');
snapshot.paused=true;snapshot.pause_kind='user';snapshot.active=[];
renderTop();
assert.equal($('pause').textContent,'继续');
assert.equal($('pause').className,'manual','手动暂停用中性色');
assert.equal($('banner').className,'manual');
assert.equal($('banner-text').textContent,'已暂停：手动暂停');
assert.equal($('pause').disabled,false);
assert.equal($('active-list').children[0].textContent,'暂无进行中的文件');
snapshot.pause_kind='failures';snapshot.pause_reason='连续失败';renderTop();
assert.equal($('pause').className,'auto','连续失败暂停用红色');
assert.equal($('banner').className,'auto');
assert.ok($('banner-text').textContent.includes('连续失败'));
snapshot.paused=false;snapshot.pause_kind='';snapshot.pause_reason='';renderTop();
assert.equal($('banner').hidden,false);
assert.equal($('banner').className,'failure');
assert.equal($('banner-text').textContent,'有 2 个文件失败');
assert.equal($('show-failed').hidden,false);
"""
        self.run_page_checks(checks)

    def test_factory_start_close_and_store_ownership(self):
        self.assertTrue(self.q.started)
        self.assertEqual(self.q.db_path, self.config.state_db)
        throttle = object()
        self.q.client_factory(throttle)
        self.client_class.assert_called_with(self.config, throttle=throttle, fail_fast=True)
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()
        self.server.server_close()
        self.assertTrue(self.q.closed)
        self.assertFalse(self.store.closed)
        owned = FakeStore()
        with patch.dict(sys.modules, {'bunkrdl.store': types.SimpleNamespace(Store=Mock(return_value=owned))}):
            server = make_server(self.config, port=0)
        server.server_close()
        self.assertEqual(owned.trace[-2:], ['coordinator-close', 'store-close'])

    def test_bind_port_and_start_failure_cleanup(self):
        for host in ('0.0.0.0', 'example.com', '192.168.0.1'):
            with self.assertRaises(ValueError):
                make_server(self.config, self.store, host=host, port=0)
        for port in (-1, 65536, True, '8765'):
            with self.assertRaises(ValueError):
                make_server(self.config, self.store, port=port)
        owned = FakeStore()
        with patch.dict(sys.modules, {'bunkrdl.store': types.SimpleNamespace(Store=Mock(return_value=owned))}):
            with patch.object(FakeCoordinator, 'start', side_effect=RuntimeError('start failed')):
                with self.assertRaises(RuntimeError):
                    make_server(self.config, port=0)
        self.assertEqual(owned.trace[-2:], ['coordinator-close', 'store-close'])

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_behavior_with_fake_dom_and_out_of_order_responses(self):
        checks = r'''
const data=id=>({album_id:id,title:id,total:2,percent:0,counts:{done:0,failed:1},
 files:[{file_id:'1',name:'one',kind:'image',status:'pending'},
        {file_id:'2',name:'two',kind:'video',status:'failed'}]});
albumId='abc';album=data('abc');chosen.add('1');setFilter('failed');
assert.equal(chosen.size,0);
$('select-visible').onchange({target:{checked:true}});
assert.deepEqual([...chosen],['2']);
await selectAlbum('other').catch(()=>{});
assert.equal(chosen.size,0);
assert.equal(filter,'all');
// A response for a previous album must not replace the current album.
const old=deferred();api=()=>old.promise;
albumId='abc';const pending=loadAlbum();albumId='other';album=data('other');revision++;
old.resolve(data('abc'));await pending;assert.equal(album.album_id,'other');
// A slower response for the same album must not replace newer data either.
const first=deferred(),second=deferred();let calls=0;api=()=>++calls===1?first.promise:second.promise;
const one=loadAlbum(),two=loadAlbum();second.resolve({...data('other'),title:'new'});await two;
first.resolve({...data('other'),title:'old'});await one;assert.equal(album.title,'new');
// Overlapping state requests retain the latest pause snapshot and cursor.
albumId=null;const s1=deferred(),s2=deferred();calls=0;api=()=>++calls===1?s1.promise:s2.promise;
const r1=refresh(),r2=refresh();
const state={active:[],paused:true,pause_kind:'failures',pause_reason:'连续失败',queued:0,
 lanes:{image:{setting:4,effective:4},video:{setting:2,effective:2}},recent_failures:[]};
s2.resolve({snapshot:state,events:[],next:9,albums:[]});await r2;
s1.resolve({snapshot:{...state,paused:false},events:[],next:3,albums:[]});await r1;
assert.equal(snapshot.paused,true);assert.equal(since,9);
// Clipboard denial exposes selectable text; switching albums suppresses late failures.
albumId='abc';const link={download_url:'https://cdn.example/file',link_expires:Date.now()/1000+600};
await copyLinks([link]);assert.equal($('copy-fallback').value,link.download_url);
assert.equal($('copy-fallback').hidden,false);
const clip=deferred();navigator.clipboard={writeText:()=>clip.promise};
const copying=copyLinks([link]);albumId='other';revision++;$('copy-fallback').hidden=true;
clip.resolve();await copying;assert.equal($('copy-fallback').hidden,true);
// The folder check selects only missing and mismatched files in this album.
albumId='abc';album=data('abc');$('folder').value='external';
api=async path=>path.endsWith('/check')?
 {completed:[],missing:['2'],mismatched:[],unverified:['1'],failed_ids:['2']}:data('abc');
await $('check').onclick();assert.equal(filter,'failed');assert.deepEqual([...chosen],['2']);
assert.match($('check-result').textContent,/点“下载”即可补下/);
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_purge_album_and_selected_records(self):
        checks = r'''
const detail={album_id:'abc',title:'abc',total:3,percent:33,folder:'',
 counts:{pending:1,downloading:0,done:1,failed:1,exported:0,skipped:0},
 files:[{file_id:'1',name:'一',kind:'image',status:'done'},
        {file_id:'2',name:'二',kind:'video',status:'failed'},
        {file_id:'3',name:'三',kind:'image',status:'pending'}]};
let sent=[],asked=[],refreshed=0;
window.confirm=text=>{asked.push(text);return true};
api=async(path,body)=>{sent.push([path,body]);
 if(path==='albums/abc')return detail;
 return {deleted:3,message:'已清除 3 条文件记录；磁盘上已下载的文件未删除'}};
refresh=async()=>{refreshed++};
// 左栏：先读相册概况，再二次确认写明不删文件，清除后清空右侧并切回空位。
albumId='abc';album=detail;chosen=new Set(['1']);
await purgeAlbum('abc','abc');
assert.equal(sent[0][0],'albums/abc');
assert.equal(sent[1][0],'albums/abc/purge');
assert.deepEqual(sent[1][1],{});
assert.match(asked[0],/只清除记录，不会删除已下载的文件/);
assert.match(asked[0],/将清除 3 个文件的记录，其中已完成 1 个/);
assert.equal(albumId,null);
assert.equal(chosen.size,0);
assert.equal($('detail').hidden,true);
assert.equal($('empty').hidden,false);
assert.match($('message').textContent,/已清除 3 条文件记录/);
assert.equal(refreshed,1);
// 取消确认时不发清除请求。
sent=[];asked=[];refreshed=0;window.confirm=text=>{asked.push(text);return false};
albumId='abc';album=detail;
await purgeAlbum('abc','abc');
assert.deepEqual(sent.map(s=>s[0]),['albums/abc']);
assert.equal(albumId,'abc');
assert.equal(refreshed,0);
// 右侧：清除选中的记录，确认框写清条数与已完成数。
sent=[];asked=[];refreshed=0;window.confirm=text=>{asked.push(text);return true};
albumId='abc';album=detail;chosen=new Set(['1','3']);
await $('purge-selected').onclick();
assert.equal(sent[0][0],'albums/abc/files/delete');
assert.deepEqual(sent[0][1].file_ids,['1','3']);
assert.match(asked[0],/清除选中的 2 条文件记录/);
assert.match(asked[0],/其中已完成 1 个/);
assert.match(asked[0],/不会删除已下载的文件/);
assert.equal(chosen.size,0);
assert.equal(refreshed,1);
// 取消确认时一条记录也不提交。
sent=[];refreshed=0;window.confirm=text=>false;chosen=new Set(['1']);
await $('purge-selected').onclick();
assert.deepEqual(sent,[]);
assert.equal(chosen.size,1);
// 选中排队中或处理中的文件时“清除选中的记录”禁用。
const withQueued={...detail,files:[{file_id:'q',name:'q',kind:'image',status:'pending',queued:true},
 {file_id:'p',name:'p',kind:'image',status:'pending'}]};
album=withQueued;albumId='abc';snapshot={active:[]};chosen=new Set(['q']);renderFiles();
assert.equal($('purge-selected').disabled,true);
chosen=new Set(['p']);renderFiles();
assert.equal($('purge-selected').disabled,false);
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_queued_and_running_rows_control_actions(self):
        checks = r'''
const files=[
 {file_id:'q',name:'queued',kind:'image',status:'pending',queued:true},
 {file_id:'r',name:'running',kind:'image',status:'downloading',active:true},
 {file_id:'p',name:'plain',kind:'image',status:'pending'},
 {file_id:'d',name:'done',kind:'image',status:'done'}];
album={album_id:'abc',title:'abc',total:4,percent:25,counts:{done:1,failed:0},files};
albumId='abc';snapshot={active:[]};filter='all';chosen.clear();folderTouched=true;renderFiles();
const rows=$('files').children,box=row=>row.children[0].children[0],state=row=>row.children[5],progressCell=row=>row.children[4];
assert.equal(rows[0].children.length,6,'六列结构');
assert.equal(progressCell(rows[0]).children[1].textContent,'—','无进度时进度列为空占位');
assert.equal(rows.length,4);
assert.equal(rows[0].className,'queued');
assert.equal(box(rows[0]).disabled,false);
assert.equal(rows[1].className,'running');
assert.equal(box(rows[1]).disabled,true);
assert.equal(box(rows[1]).title,'正在处理，请稍后或使用“暂停全部”');
assert.equal(state(rows[0]).children[0].children[0].textContent,'排队中');
assert.equal(state(rows[1]).children[0].children[0].textContent,'处理中');
// 全选只选可以操作的文件，正在处理的不能进入选中集合。
chosen.clear();$('select-visible').onchange({target:{checked:true}});
assert.deepEqual([...chosen],['q','p','d']);
assert.equal($('select-visible').checked,true);
$('select-visible').onchange({target:{checked:false}});
assert.equal(chosen.size,0);
chosen.add('q');renderSelection();
assert.equal($('cancel-queue').hidden,false);
assert.equal($('cancel-queue').textContent,'取消排队 (1)');
assert.equal($('download').disabled,true);
assert.equal($('links').disabled,true);
assert.equal($('skip').disabled,true);
assert.match($('selection-note').textContent,/取消排队后可改状态/);
chosen.clear();chosen.add('p');renderSelection();
assert.equal($('cancel-queue').hidden,true);
assert.equal($('download').disabled,false);
assert.equal($('links').disabled,false);
chosen.clear();chosen.add('d');renderSelection();
assert.equal($('download').disabled,true);
assert.match($('selection-note').textContent,/已完成或已跳过/);
// 刷新后正在处理的文件不能留在选中集合里，否则会出现勾选了却不能操作。
chosen.clear();chosen.add('r');chosen.add('q');
api=async()=>album;await loadAlbum();
assert.deepEqual([...chosen],['q']);
// 取消排队只提交排队中的文件，成功后清空选中。
let sent=null;api=async(path,body)=>{sent={path,body};return {cancelled:1,message:'已取消 1 个排队中的文件'}};
refresh=async()=>{};
chosen.clear();chosen.add('q');chosen.add('p');renderSelection();
await $('cancel-queue').onclick();
assert.equal(sent.path,'albums/abc/cancel-queue');
assert.deepEqual(sent.body.file_ids,['q']);
assert.equal($('message').textContent,'已取消 1 个排队中的文件');
assert.equal(chosen.size,0);
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_table_search_sort_and_error_details(self):
        status, page = self.request('/')
        self.assertEqual(status, 200)
        self.assertIn('外部下载文件夹（仅用于检查，不是本相册的保存位置）', page)
        self.assertIn('@media(max-width:900px)', page)
        self.assertNotIn('@media(max-width:760px)', page)
        checks = r"""
const files=[
 {file_id:'a',name:'bravo.jpg',kind:'image',status:'done',size:300},
 {file_id:'b',name:'alpha.jpg',kind:'image',status:'failed',size:100,last_error:'第一行错误\n第二行细节'},
 {file_id:'c',name:'charlie.mp4',kind:'video',status:'pending',size:200}];
album={album_id:'abc',title:'abc',total:3,percent:33,counts:{done:1,failed:1},files};
albumId='abc';snapshot={active:[]};filter='all';search='';sortKey='';sortDir=1;chosen.clear();renderFiles();
const rows=()=>$('files').children;
assert.equal(rows().length,3);
assert.equal(rows()[0].children.length,6,'六列结构');
$('sort-name').onclick();
assert.deepEqual([...rows()].map(r=>r.dataset.fileId),['b','a','c'],'名称升序');
assert.match($('sort-name').textContent,/↑/,'表头显示排序方向');
$('sort-name').onclick();
assert.deepEqual([...rows()].map(r=>r.dataset.fileId),['c','a','b'],'名称降序');
$('sort-size').onclick();
assert.deepEqual([...rows()].map(r=>r.dataset.fileId),['b','c','a'],'大小升序');
$('sort-status').onclick();
assert.equal(rows()[0].dataset.fileId,'c','待下载排在最前');
$('search').value='CHARLIE';$('search').oninput();
assert.equal(rows().length,1,'搜索不区分大小写');
assert.equal(rows()[0].dataset.fileId,'c');
$('search').value='zzz';$('search').oninput();
assert.equal(rows().length,1);
assert.equal(rows()[0].children[0].textContent,'没有符合条件的文件');
assert.equal(rows()[0].children[0].colSpan,6,'空态跨六列');
$('search').value='';$('search').oninput();
chosen.add('b');setFilter('failed');
assert.equal(rows().length,1,'筛选生效');
assert.equal(chosen.size,0,'切换筛选清空选择');
const state=rows()[0].children[5];
assert.equal(state.children[0].children[1].textContent,'失败');
const err=[...state.children].find(n=>n.className==='err');
assert.ok(err,'失败行有错误详情');
assert.ok(!err.open,'错误详情默认收起');
assert.equal(err.children[0].textContent,'查看完整错误');
assert.ok(err.children[1].textContent.includes('第二行细节'),'完整错误可展开查看');
assert.equal(err.children[2].textContent,'复制错误');
let copied=null;navigator.clipboard={writeText:async t=>{copied=t}};
await err.children[2].onclick();
assert.ok(copied&&copied.includes('第二行细节'),'复制完整错误');
const retry=[...state.children].find(n=>n.textContent==='重试本条');
assert.ok(retry,'失败行可重试本条');
let sent=null;api=async(path,body)=>{sent={path,body};return {added:1}};refresh=async()=>{};
await retry.onclick();
assert.equal(sent.path,'albums/abc/enqueue');
assert.deepEqual(sent.body.file_ids,['b']);
assert.equal(sent.body.action,'download','单条重试固定按下载处理');

"""
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_show_failed_button_uses_album_failures_when_paused_after_restart(self):
        checks = r'''
// 重启后 paused、recent_failures 为空但相册里有 failed>0：按钮必须可见，点击跳到该相册并切到失败筛选。
const data=id=>({album_id:id,title:id,total:2,percent:0,counts:{done:0,failed:1},
 files:[{file_id:'1',name:'one',kind:'image',status:'pending'},
        {file_id:'2',name:'two',kind:'video',status:'failed'}]});
const pausedState={active:[],paused:true,pause_kind:'restart',pause_reason:'程序已重启，等待继续',
 queued:0,speed:0,stopping:false,recent_failures:[],
 lanes:{image:{setting:4,effective:4,cooldown_until:0},video:{setting:2,effective:2,cooldown_until:0}}};
const albumsWithFailure=[{id:'abc',title:'abc',percent:0,status:'已暂停',failed:1,queued:0}];
const requested=[];
api=async path=>{requested.push(path);
 if(path.startsWith('state'))return {snapshot:pausedState,events:[],next:1,albums:albumsWithFailure};
 if(path==='albums/abc')return data('abc');
 throw new Error('unexpected path '+path)};
albumId=null;album=null;albums=[];snapshot=null;$('show-failed').hidden=true;
await refresh();
assert.equal($('show-failed').hidden,false,'paused 且相册 failed>0 时按钮应可见');
await $('show-failed').onclick();
assert.ok(requested.includes('albums/abc'),'点击后应对相册 abc 发起详情请求: '+requested.join(','));
assert.equal(albumId,'abc','点击后应跳转到有失败的相册');
assert.equal(filter,'failed','点击后应切到失败筛选');

// 反例：paused 但没有任何失败（recent 空、albums 全 0）时按钮隐藏。
const quietState={...pausedState,recent_failures:[]};
const albumsNoFailure=[{id:'abc',title:'abc',percent:100,status:'已暂停',failed:0,queued:0}];
api=async path=>{if(path.startsWith('state'))return {snapshot:quietState,events:[],next:1,albums:albumsNoFailure};
 throw new Error('unexpected path '+path)};
albumId=null;album=null;albums=[];snapshot=null;$('show-failed').hidden=false;
await refresh();
assert.equal($('show-failed').hidden,true,'paused 但无任何失败时按钮应隐藏');
'''
        self.run_page_checks(checks)


    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_incremental_rendering_reuses_nodes_and_keeps_ui_state(self):
        checks = r'''
// 同 ID 复用行节点：无变化不重建、只更新变化的单元格、换序只重排一次。
const files=[{file_id:'a',name:'alpha.jpg',kind:'image',status:'pending',size:100},
 {file_id:'b',name:'bravo.jpg',kind:'video',status:'pending',size:200},
 {file_id:'c',name:'charlie.jpg',kind:'image',status:'pending',size:300}];
const mk=()=>({album_id:'abc',title:'abc',total:3,percent:0,counts:{done:0,failed:0},files:files.map(f=>({...f}))});
albumId='abc';album=mk();snapshot={active:[],paused:false,pause_kind:null,pause_reason:null,queued:0,stopping:false,speed:0,
 lanes:{image:{setting:2,effective:2,cooldown_until:0},video:{setting:1,effective:1,cooldown_until:0}}};albums=[];
chosen.clear();filter='all';search='';sortKey='';sortDir=1;renderFiles();
const body=$('files'),scroll=$('files-scroll');
let rebuilds=0;const orig=body.replaceChildren.bind(body);body.replaceChildren=(...x)=>{rebuilds++;orig(...x)};
const first=[...body.children];
assert.deepEqual(first.map(r=>r.dataset.fileId),['a','b','c']);
renderFiles();
assert.equal(rebuilds,0,'无变化时不重建 tbody');
assert.ok([...body.children].every((r,i)=>r===first[i]),'无变化时行节点保持');
album.files[0].name='alpha-renamed.jpg';renderFiles();
assert.equal(rebuilds,0,'只改文字也不重建');
assert.equal(body.children[0].children[1].textContent,'alpha-renamed.jpg','名称单元格已更新');
assert.ok(body.children[0]===first[0]&&body.children[1]===first[1],'同 ID 同节点');
chosen.add('a');renderFiles();
assert.equal(body.children[0].children[0].children[0].checked,true,'勾选状态写入复用的行');
scroll.scrollTop=120;document.activeElement=$('search');renderFiles();
assert.equal(scroll.scrollTop,120,'重绘保持滚动位置');
assert.equal(document.activeElement,$('search'),'重绘保持输入焦点');
album.files=[album.files[1],album.files[0],album.files[2]];renderFiles();
assert.equal(rebuilds,1,'顺序变化只重排一次');
assert.deepEqual([...body.children].map(r=>r.dataset.fileId),['b','a','c']);
assert.ok(body.children[0]===first[1]&&body.children[1]===first[0],'换序复用原节点');

// 左栏相册按 id 复用：节点不变、菜单展开状态保留、高亮跟随当前相册。
albums=[{id:'x',title:'X',percent:10,status:'下载中',failed:0,queued:0},
        {id:'y',title:'Y',percent:0,status:'等待',failed:2,queued:1}];
albumId='x';renderAlbums();
const list=$('albums'),rowX=list.children[0],menuX=rowX.children[2],titleX=rowX.children[0];
menuX.open=true;
albums=[{id:'y',title:'Y',percent:40,status:'下载中',failed:1,queued:0},
        {id:'x',title:'X',percent:55,status:'下载中',failed:0,queued:2}];
renderAlbums();
assert.equal(list.children.length,2);
assert.ok([...list.children].includes(rowX),'相册行节点复用');
assert.ok(rowX.children[0]===titleX,'相册标题按钮节点复用');
assert.equal(menuX.open,true,'“更多”菜单展开状态保持');
assert.ok(rowX.children[1].textContent.includes('55%'),'相册摘要更新');
assert.equal(rowX.className,'album current','当前相册高亮保持');

'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_sidebar_reuses_albums_when_count_is_unchanged(self):
        checks = r'''
// 相册数不变时 renderAlbums 只能比较节点；真实浏览器的 children 是 HTMLCollection，
// 没有 some/map 之类方法，这里若调用数组方法会直接抛错。
const list=$('albums');
albums=[{id:'a',title:'A',percent:1,status:'等待',failed:0,queued:0},
        {id:'b',title:'B',percent:2,status:'等待',failed:0,queued:1}];
albumId='a';renderAlbums();
const first=[...list.children];
assert.equal(first.length,2);
let rebuilds=0;const orig=list.replaceChildren.bind(list);
list.replaceChildren=(...x)=>{rebuilds++;orig(...x)};
renderAlbums();
assert.equal(rebuilds,0,'相册数不变时不重建列表');
assert.ok([...list.children].every((n,i)=>n===first[i]),'相册数不变时节点保持');
albums[0]={...albums[0],percent:66,status:'下载中'};
renderAlbums();
assert.equal(rebuilds,0);
assert.ok(list.children[0]===first[0],'内容变化就地更新');
assert.ok(list.children[0].children[1].textContent.includes('66%'));
albums.push({id:'c',title:'C',percent:0,status:'等待',failed:0,queued:0});
renderAlbums();
assert.equal(rebuilds,1,'相册增减时重排一次');
assert.equal(list.children.length,3);
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_reselect_and_active_card_rebuild_file_rows(self):
        checks = r'''
const state={active:[],paused:false,pause_kind:null,pause_reason:null,queued:0,stopping:false,speed:0,
 lanes:{image:{setting:2,effective:2,cooldown_until:0},video:{setting:1,effective:1,cooldown_until:0}}};
const detail={album_id:'abc',title:'abc',total:1,percent:0,counts:{done:0,failed:0},
 files:[{file_id:'a',name:'a.jpg',kind:'image',status:'pending',size:1}]};
let requested=[];
api=async path=>{requested.push(path);
 if(path.startsWith('state'))return {snapshot:state,events:[],next:1,albums:[]};
 if(path==='albums/abc')return detail;
 throw new Error('unexpected '+path)};
// 先让表格有行，记录排序键。
albumId='abc';album=detail;filter='all';search='';sortKey='';chosen.clear();renderFiles();
assert.equal($('files').children.length,1);
// 重选同一相册：手工清空 tbody 必须同时清掉排序键，否则 renderFiles 跳过重建、表格空白。
search='zzz';$('search').value='zzz';sortKey='size';sortDir=-1;
requested=[];await selectAlbum('abc');
assert.equal(requested[0],'albums/abc');
assert.equal($('files').children.length,1,'同 id 重选后表格要有行');
assert.equal($('files').children[0].dataset.fileId,'a');
assert.equal(search,'','换相册清空搜索词');
assert.equal($('search').value,'');
assert.equal(sortKey,'','换相册清空排序');
assert.equal(sortDir,1);
assert.ok(!/↑|↓/.test($('sort-name').textContent+'$'+$('sort-size').textContent+'$'+$('sort-status').textContent),'表头排序标记清空');
// 点顶栏进行中卡片定位当前相册：换相册后同样要有行（不能停在空白表格）。
albumId=null;album=null;chosen.clear();rowOrderKey='';$('files').replaceChildren();rowOrderKey='';
snapshot={...state,active:[{work_id:1,album_id:'abc',file_id:'a',name:'a.jpg',action:'download',lane:'image',bytes:1,total:2,speed:1}]};
renderTop();
const card=$('active-list').children[0];
assert.ok(card,'应有进行中卡片');
await card.onclick();
assert.equal(albumId,'abc');
assert.equal($('files').children.length,1,'卡片定位后文件表应有行');
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_tick_throttles_idle_detail_requests(self):
        checks = r'''
// 走真实 tick()：空闲相册每拍都拉 state，详情明显更少；有活动时每拍都拉详情。
const realNow=Date.now,realSetTimeout=global.setTimeout;
let clock=realNow();Date.now=()=>clock;
const delays=[];global.setTimeout=(fn,ms)=>{delays.push(ms);return 0};
const state={active:[],paused:false,pause_kind:null,pause_reason:null,queued:0,stopping:false,speed:0,
 lanes:{image:{setting:2,effective:2,cooldown_until:0},video:{setting:1,effective:1,cooldown_until:0}}};
const doneAlbum={album_id:'abc',title:'abc',total:1,percent:100,counts:{done:1,failed:0},
 files:[{file_id:'a',name:'a.jpg',kind:'image',status:'done',size:1}]};
let stateCalls=0,detailCalls=0;
api=async path=>{if(path.startsWith('state')){stateCalls++;return {snapshot:state,events:[],next:1,albums:[]}}
 detailCalls++;return doneAlbum};
albumId='abc';album=doneAlbum;detailAt=Date.now();polling=false;
for(let i=0;i<8;i++){await tick();clock+=3000}
assert.equal(stateCalls,8,'每拍都拉 state: '+stateCalls);
assert.ok(detailCalls>=1&&detailCalls<=2,'空闲时详情请求明显更少: '+detailCalls);
assert.equal(delays[delays.length-1],3000,'空闲心跳 3 秒');
// 有活动（hot）时仍每拍拉详情，心跳 1 秒。
stateCalls=0;detailCalls=0;delays.length=0;clock+=60000;
state.active=[{work_id:1,album_id:'abc',file_id:'a',name:'a.jpg',action:'download',lane:'image',bytes:1,total:2,speed:1}];
album={...doneAlbum,files:[{file_id:'a',name:'a.jpg',kind:'image',status:'downloading',size:1}]};
for(let i=0;i<3;i++){await tick();clock+=1000}
assert.equal(stateCalls,3);
assert.equal(detailCalls,3,'有活动时每拍都拉详情');
assert.equal(delays[delays.length-1],1000,'活动时心跳 1 秒');
// 用户操作后 markDirty 仍然立即拉详情。
state.active=[];album=doneAlbum;detailCalls=0;detailAt=Date.now();
markDirty();await refresh();
assert.equal(detailCalls,1,'标记脏后立即拉取详情');
await refresh();
assert.equal(detailCalls,1,'未到期不重复拉取');
Date.now=realNow;global.setTimeout=realSetTimeout;
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_cooldown_bar_value_is_clamped(self):
        checks = r'''
// 首拍或异常比例下冷却进度条不能出现负值，也不能超过 100。
const now=Math.floor(Date.now()/1000),until=now+300;
const mk=until=>({active:[],paused:false,pause_kind:null,pause_reason:null,queued:0,stopping:false,speed:0,
 lanes:{image:{setting:2,effective:0,cooldown_until:until},video:{setting:1,effective:1,cooldown_until:0}}});
const bar=$('cooldown-bar');
snapshot=mk(until);
cooldownWatch={image:{until,start:until},video:{until:0,start:0}};
renderTop();
assert.equal(bar.hidden,false);
assert.equal(bar.value,0,'剩 300 秒、总时长按 1 秒算时比值小于 0，钳制为 0');
snapshot=mk(until);
cooldownWatch={image:{until,start:until-600},video:{until:0,start:0}};
renderTop();
assert.ok(bar.value>=40&&bar.value<=60,'正常比例按剩余时间递减');
snapshot=mk(until);
cooldownWatch={image:{until,start:0},video:{until:0,start:0}};
renderTop();
assert.ok(bar.value>=0&&bar.value<=100,'进度条始终在 0–100');
'''
        self.run_page_checks(checks)

    def test_page_empty_state_markup_and_styles(self):
        status, page = self.request('/')
        self.assertEqual(status, 200)
        for text in ('id="empty-title"', 'id="empty-hint"', 'id="empty-actions"', 'id="empty-paste"',
                     'id="empty-retry"', '还没有选择相册', '检查外部下载文件夹',
                     'td.empty-cell', '#empty{padding:40px 2px', 'id="add-submit"', '正在解析…'):
            self.assertIn(text, page)
        self.assertIn("emptyRow.dataset.empty='1'", page)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_empty_state_and_album_load_error(self):
        checks = r'''
const data=id=>({album_id:id,title:id,total:1,percent:0,counts:{done:0,failed:0},
 files:[{file_id:'1',name:'a.jpg',kind:'image',status:'pending'}]});
function later(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b});return {promise,resolve,reject}}

assert.equal($('empty').hidden,false);
assert.equal($('empty-title').textContent,'还没有选择相册');
assert.equal($('empty-retry').hidden,true);
assert.equal($('empty-paste').hidden,false);
assert.equal($('detail').hidden,true);

setPane('loading');
assert.equal($('empty-title').textContent,'正在读取相册…');
assert.equal($('empty-actions').hidden,true);
setPane('error','相册不存在');
assert.equal($('empty-title').textContent,'相册读取失败');
assert.equal($('empty-hint').textContent,'相册不存在');
assert.equal($('empty-retry').hidden,false);
assert.equal($('empty-paste').hidden,true);

albumId='abc';album=null;setPane('loading');
api=async()=>{throw new Error('相册不存在')};
await loadAlbum();
assert.equal($('empty-title').textContent,'相册读取失败');
assert.equal($('detail').hidden,true);

const stale=later(),fresh=later();let calls=0;
api=()=>++calls===1?stale.promise:fresh.promise;
albumId='abc';album=null;revision=0;
const a=loadAlbum();albumId='other';revision++;const b=loadAlbum();
fresh.resolve(data('other'));await b;
stale.reject(new Error('旧请求失败'));await a;
assert.equal(album.album_id,'other');
assert.equal($('detail').hidden,false);
assert.equal($('empty').hidden,true);

albumId='abc';album=data('abc');setPane('detail');api=async()=>{throw new Error('无法连接')};
await loadAlbum();
assert.equal($('detail').hidden,false,'有数据时不切走详情');
assert.match($('message').textContent,/读取相册详情失败/);

let tries=0;api=async()=>{tries++;if(tries===1)throw new Error('相册不存在');return data('abc')};
albumId='abc';album=null;setPane('loading');await loadAlbum();
assert.equal($('empty-retry').hidden,false);
await $('empty-retry').onclick();
assert.equal(tries,2);
assert.equal($('detail').hidden,false);
assert.equal($('empty-retry').disabled,false);
assert.equal($('empty-retry').textContent,'重试');
'''
        self.run_page_checks(checks)

    @unittest.skipUnless(shutil.which('node'), 'Node unavailable; browser checks run at integration')
    def test_page_add_album_busy_state_and_empty_filter_row(self):
        checks = r'''
const data=id=>({album_id:id,title:id,total:1,percent:0,counts:{done:0,failed:0},
 files:[{file_id:'1',name:'a.jpg',kind:'image',status:'pending'}]});

albumId=null;album=null;setPane('guide');refresh=async()=>{};
let addCalls=[];
api=async(path,body)=>{if(path==='albums'){addCalls.push(body.url);return {album_id:'new'}}return data('new')};
$('url').value='https://bunkr.cr/a/new';
const first=$('add').onsubmit({preventDefault(){}});
assert.equal($('add-submit').disabled,true);
assert.equal($('add-submit').textContent,'正在解析…');
assert.equal($('add').onsubmit({preventDefault(){}}),undefined,'重复提交直接返回');
await first;
assert.equal(addCalls.length,1);
assert.equal($('add-submit').disabled,false);
assert.equal($('add-submit').textContent,'添加');
assert.equal($('url').value,'');

albumId=null;album=null;setPane('guide');$('url').value='https://bunkr.cr/a/bad';addCalls=[];
api=async(path,body)=>{addCalls.push(body.url);throw new Error('网站正在限速，建议等待 120 秒后再添加相册')};
await $('add').onsubmit({preventDefault(){}});
assert.equal(adding,false);
assert.equal($('add-submit').disabled,false);
assert.equal($('add-submit').textContent,'添加');
assert.equal($('url').value,'https://bunkr.cr/a/bad');
assert.match($('message').textContent,/限速/);

notify('');addCalls=[];$('url').value='   ';
await $('add').onsubmit({preventDefault(){}});
assert.equal(addCalls.length,0);
assert.match($('message').textContent,/请先粘贴/);

album=data('abc');albumId='abc';setPane('detail');search='';filter='video';chosen.clear();renderFiles();
assert.equal($('files').children.length,1);
assert.equal($('files').children[0].dataset.empty,'1');
assert.equal($('files').children[0].children[0].textContent,'没有符合条件的文件');
assert.equal($('files').children[0].children[0].colSpan,6);
filter='all';renderFiles();
assert.equal($('files').children[0].dataset.empty,undefined,'有结果时不再显示空态行');

search='不存在的名字';renderFiles();
assert.equal($('files').children[0].dataset.empty,'1');
search='';
'''
        self.run_page_checks(checks)

class RealCoordinatorWebTests(unittest.TestCase):
    """Web mutations against the real scheduler, not the fake coordinator."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(state_db=str(Path(self.tmp.name)/'state.db'),
                             image_concurrency=1, video_concurrency=1,
                             image_gap=0, video_gap_min=0, video_gap_max=0,
                             web_request_interval=0)
        self.store = Store(self.config.state_db)
        self.addCleanup(self.store.close)
        self.started = threading.Event()

        def download_once(ctx, album_id, file_id):
            self.started.set()
            ctx.stop_event.wait(5)
            return ItemResult(INTERRUPTED)

        def generate_link_once(ctx, album_id, file_id):
            return ItemResult(INTERRUPTED)

        self.flows = types.SimpleNamespace(
            album_dest=Mock(return_value='downloads/album'),
            download_once=download_once, generate_link_once=generate_link_once,
            check_folder=Mock(), prescan_folder=Mock(return_value={}),
            pick_folder=Mock(return_value=None), open_folder=Mock())
        self.export = types.SimpleNamespace(generate_link_once=generate_link_once)
        self.client = Mock()
        self.client_class = Mock(return_value=self.client)
        modules = {
            'bunkrdl.flows': self.flows, 'bunkrdl.export': self.export,
            'bunkrdl.resolver': types.SimpleNamespace(BunkrClient=self.client_class)}
        self.patcher = patch.dict(sys.modules, modules)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        # `from . import flows` reads the package attribute, which other test
        # modules may already have bound to the real module.
        for full_name, module in modules.items():
            patcher = patch.object(bunkrdl, full_name.rsplit('.', 1)[-1], module, create=True)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.server = make_server(self.config, self.store, port=0)
        self.q = self.server.coordinator
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()

    def request(self, path, body=None):
        req = Request(f'http://127.0.0.1:{self.server.server_port}'+path,
                      data=json.dumps(body).encode(),
                      headers={'Content-Type': 'application/json'})
        try:
            response = urlopen(req, timeout=3)
        except HTTPError as exc:
            response = exc
        with response:
            return response.status, json.loads(response.read().decode('utf-8'))

    def wait_snapshot(self, predicate):
        deadline = time.time() + 3
        while time.time() < deadline:
            snapshot = self.q.snapshot()
            if predicate(snapshot):
                return snapshot
            time.sleep(.01)
        self.fail(self.q.snapshot())

    def test_add_challenge_pauses_real_scheduler(self):
        self.store.upsert_album(Album('abc', '相册', 'https://bunkr.cr/a/abc',
                                      [AlbumFile('1', '一.jpg', 's1', '/f/s1', size=10, kind='image')]))
        status, _ = self.request('/api/albums/abc/enqueue', {'file_ids': ['1'], 'action': 'download'})
        self.assertEqual(status, 200)
        self.assertTrue(self.started.wait(2))
        self.assertTrue(self.q.snapshot()['active'])
        self.client.get_album.side_effect = ChallengeRequired('challenge required')
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/new'})
        self.assertEqual(status, 502)
        self.assertIn('人机验证', data['error'])
        snapshot = self.wait_snapshot(lambda s: s['paused'])
        self.assertEqual(snapshot['pause_kind'], 'challenge')
        self.assertIn('人机验证', snapshot['pause_reason'])
        self.client.session.close.assert_called_once()

    def test_add_rate_limit_cools_the_shared_gate_and_stops_resending(self):
        self.client.get_album.side_effect = RateLimited('HTTP 429', retry_after=45)
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/new'})
        self.assertEqual(status, 429)
        self.assertRegex(data['error'], r'建议等待 \d+ 秒')
        self.assertGreaterEqual(data['retry_after'], 60)
        # The same IP-wide gate as the file lanes, so both lanes stop dispatching.
        self.assertEqual(self.q.snapshot()['lanes']['image']['effective'], 0)
        self.assertEqual(self.q.snapshot()['lanes']['video']['effective'], 0)
        self.assertEqual(self.store.get_meta('web:ratelimit:streak'), '1')
        self.assertGreater(self.q.cooldown_remaining(), 0)
        self.assertFalse(self.q.snapshot()['paused'])
        self.client.session.close.assert_called_once()
        # A second click inside the cooldown sends no request at all.
        self.client.get_album.reset_mock()
        self.client_class.reset_mock()
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/other'})
        self.assertEqual(status, 429)
        self.assertRegex(data['error'], r'建议等待 \d+ 秒')
        self.client.get_album.assert_not_called()
        self.client_class.assert_not_called()
        for word in ('迅雷链接', '导出', '回补', '模式 A', '模式 B'):
            self.assertNotIn(word, data['error'])

    def test_readding_album_with_queued_work_is_409_and_keeps_records(self):
        self.store.upsert_album(Album('abc', '原相册', 'https://bunkr.cr/a/abc',
                                      [AlbumFile('1', '原名.jpg', 's1', '/f/s1', size=10, kind='image')]))
        self.assertEqual(self.request('/api/queue/pause', {})[0], 200)
        status, _ = self.request('/api/albums/abc/enqueue', {'file_ids': ['1'], 'action': 'download'})
        self.assertEqual(status, 200)
        self.assertEqual(self.q.snapshot()['queued_by_album'], {'abc': 1})
        self.client.get_album.return_value = Album('abc', '改名相册', 'https://bunkr.cr/a/abc',
                                                   [AlbumFile('1', '改名.mp4', 's1', '/f/s1', size=99, kind='video')])
        status, data = self.request('/api/albums', {'url': 'https://bunkr.cr/a/abc'})
        self.assertEqual(status, 409)
        self.assertIn('排队', data['error'])
        album = self.store.get_album('abc')
        self.assertEqual(album.title, '原相册')
        record = self.store.list_files('abc')[0]
        self.assertEqual((record.name, record.size, record.kind), ('原名.jpg', 10, 'image'))
        self.assertEqual(len(self.store.list_work_items()), 1)
        self.client.session.close.assert_called_once()


class PurgeRealStoreTests(unittest.TestCase):
    """清除记录：真实 Store 与调度器，假解析；磁盘上已下载的文件不动。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        from bunkrdl import flows as real_flows
        self.config = Config(state_db=str(self.root / 'state.db'),
                             download_root=str(self.root / 'dl'),
                             image_concurrency=1, video_concurrency=1, image_gap=0,
                             video_gap_min=0, video_gap_max=0, web_request_interval=0)
        self.store = Store(self.config.state_db)
        self.addCleanup(self.store.close)
        self.album = Album('demo', '测试相册', 'https://bunkr.cr/a/demo', [
            AlbumFile('1', '一.jpg', 's1', '/f/s1', size=11, kind='image'),
            AlbumFile('2', '二.mp4', 's2', '/f/s2', size=12, kind='video')])
        self.client = Mock()
        self.client.get_album.return_value = self.album
        self.client_class = Mock(return_value=self.client)

        def interrupted(ctx, album_id, file_id):
            return ItemResult(INTERRUPTED)

        self.modules = {
            'bunkrdl.flows': types.SimpleNamespace(
                album_dest=real_flows.album_dest, download_once=interrupted,
                check_folder=real_flows.check_folder, pick_folder=Mock(return_value=None),
                open_folder=Mock()),
            'bunkrdl.export': types.SimpleNamespace(generate_link_once=interrupted,
                                                    download_url=Mock(return_value=None)),
            'bunkrdl.resolver': types.SimpleNamespace(BunkrClient=self.client_class)}
        patcher = patch.dict(sys.modules, self.modules)
        patcher.start()
        self.addCleanup(patcher.stop)
        # `from . import flows` reads the package attribute, which another test
        # module may already have bound to the real module.
        for full_name, module in self.modules.items():
            attribute = patch.object(bunkrdl, full_name.rsplit('.', 1)[-1], module, create=True)
            attribute.start()
            self.addCleanup(attribute.stop)
        self.server = make_server(self.config, self.store, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()

    def call(self, path, body=None):
        req = Request(f'http://127.0.0.1:{self.server.server_port}' + path, method='GET' if body is None else 'POST',
                      data=None if body is None else json.dumps(body).encode(),
                      headers={'Content-Type': 'application/json'})
        try:
            response = urlopen(req, timeout=3)
        except HTTPError as exc:
            response = exc
        with response:
            return response.status, json.loads(response.read().decode('utf-8'))

    def test_purge_then_readd_same_album_is_a_clean_album_and_keeps_disk_files(self):
        self.assertEqual(self.call('/api/albums', {'url': 'https://bunkr.cr/a/demo'})[0], 200)
        self.assertEqual(Path(self.store.get_album_dest('demo')).name, '测试相册 [demo]')
        # A file the user already has; the purge must never touch it.
        legacy = self.root / 'dl' / '测试相册'
        legacy.mkdir(parents=True)
        downloaded = legacy / '一.jpg'
        downloaded.write_bytes(b'already-downloaded')
        self.store.update_file('demo', '1', status='done', local_path=str(downloaded))
        self.store.set_meta('web:demo:last_action:1', 'download')
        self.store.set_meta('web:demo:folder', 'external')
        self.assertEqual(self.call('/api/albums/demo')[1]['counts']['done'], 1)
        # Queued work blocks the purge: 409 with the Chinese hint, nothing deleted.
        self.assertEqual(self.call('/api/queue/pause', {})[0], 200)
        self.assertEqual(self.call('/api/albums/demo/enqueue',
                                   {'file_ids': ['2'], 'action': 'download'})[0], 200)
        status, data = self.call('/api/albums/demo/purge', {})
        self.assertEqual(status, 409)
        self.assertIn('还有文件在排队或处理，请先取消排队或暂停后再清除', data['error'])
        self.assertIsNotNone(self.store.get_album('demo'))
        self.assertEqual(self.call('/api/albums/demo/cancel-queue', {'file_ids': ['2']})[0], 200)
        status, data = self.call('/api/albums/demo/purge', {})
        self.assertEqual((status, data['deleted']), (200, 2))
        # Records, per-album meta and the sidebar entry are gone; the file stays.
        self.assertIsNone(self.store.get_album('demo'))
        self.assertIsNone(self.store.get_meta('web:demo:folder'))
        self.assertIsNone(self.store.get_meta('web:demo:last_action:1'))
        self.assertEqual(self.store.list_work_items(), [])
        self.assertEqual(downloaded.read_bytes(), b'already-downloaded')
        self.assertEqual(self.call('/api/state')[1]['albums'], [])
        self.assertEqual(self.call('/api/albums/demo')[0], 404)
        # Re-adding the same album behaves like a brand-new album.
        self.assertEqual(self.call('/api/albums', {'url': 'https://bunkr.cr/a/demo'})[0], 200)
        detail = self.call('/api/albums/demo')[1]
        self.assertEqual(detail['total'], 2)
        self.assertEqual(detail['counts']['done'], 0)
        self.assertEqual({f['status'] for f in detail['files']}, {'pending'})
        self.assertEqual(detail['folder'], '')
        # No record history is left, so the destination is decided from scratch:
        # the legacy folder is no longer bound by a done record.
        self.assertEqual(Path(self.store.get_album_dest('demo')).name, '测试相册 [demo]')
        self.assertEqual(downloaded.read_bytes(), b'already-downloaded')


if __name__ == '__main__':
    unittest.main()
