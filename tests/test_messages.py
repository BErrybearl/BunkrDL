"""Shared error wording: what counts as a broken body stream, and how it reads."""

import os
import tempfile

# Keep the request log and the default state db out of the real home.
os.environ["BUNKRDL_HOME"] = os.path.join(tempfile.gettempdir(), f"bunkrdl-tests-{os.getpid()}")

import errno
import http.client
import socket
import unittest

import requests
from urllib3 import exceptions as urllib3_errors

from bunkrdl.messages import (BODY_BREAK_TYPES, body_read_broken, describe_error,
                              error_context, error_kind)
from bunkrdl.models import (BunkrError, ChallengeRequired, Maintenance, NotFound,
                            RateLimited, TransientError)
from bunkrdl.requestlog import tag_error

SECRET_URL = 'https://mlk-b.cdn.cr/path/file.mp4?token={{SECRET_qp8vmn2x}}'


class _StoppedResponse:
    """Stands in for a urllib3 response that stopped mid-chunk."""

    length_remaining = 4

    def tell(self):
        return 0


def wrapped(*errors):
    """Chain errors the way requests does: each one is the inner one's arg."""
    result = errors[-1]
    for error in reversed(errors[:-1]):
        result = error(result)
    return result


class BodyBreakClassificationTests(unittest.TestCase):
    """Every exception a cut-off body can surface as is a transient failure."""

    def check_transient(self, error):
        self.assertTrue(body_read_broken(error), type(error).__name__)
        self.assertEqual(error_kind(error), 'transient', type(error).__name__)

    def test_urllib3_read_failures(self):
        for error in (
            urllib3_errors.ProtocolError('Connection aborted.', ConnectionResetError(10054, '重置')),
            urllib3_errors.ReadTimeoutError(None, SECRET_URL, 'Read timed out.'),
            urllib3_errors.IncompleteRead(7, 1024),
            urllib3_errors.InvalidChunkLength(_StoppedResponse(), b'zz'),
        ):
            with self.subTest(error=type(error).__name__):
                self.check_transient(error)

    def test_http_client_incomplete_read(self):
        error = http.client.IncompleteRead(b'0123', 900)
        # "IncompleteRead" used to be read as a size problem.
        self.assertEqual(error_kind(error), 'transient')

    def test_requests_wrappers(self):
        for error in (
            requests.exceptions.ChunkedEncodingError(
                'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10)),
            requests.exceptions.ChunkedEncodingError(
                urllib3_errors.ProtocolError('Connection aborted.',
                                             ConnectionResetError(10054, '重置'))),
            # requests re-raises a body read timeout as ConnectionError.
            requests.exceptions.ConnectionError(
                urllib3_errors.ReadTimeoutError(None, SECRET_URL, 'Read timed out.')),
        ):
            with self.subTest(reason=str(error)[:40]):
                self.check_transient(error)

    def test_truncated_content_decoding(self):
        error = requests.exceptions.ContentDecodingError(
            urllib3_errors.DecodeError(
                'Error -3 while decompressing data: incomplete or truncated stream'))
        self.check_transient(error)

    def test_socket_level_breaks(self):
        for error in (
            ConnectionResetError(10054, '连接被对方重置'),
            ConnectionAbortedError(10053, '软件导致连接中止'),
            BrokenPipeError(32, '管道断开'),
            socket.timeout('timed out'),
            TimeoutError('timed out'),
        ):
            with self.subTest(error=type(error).__name__):
                self.check_transient(error)

    def test_exception_chain_is_followed(self):
        cause = wrapped(requests.exceptions.ChunkedEncodingError,
                        urllib3_errors.ProtocolError,
                        http.client.IncompleteRead(b'x', 99))
        direct, context = RuntimeError('外层包装'), RuntimeError('外层包装')
        direct.__cause__ = cause
        context.__context__ = cause
        self.check_transient(direct)
        self.check_transient(context)
        # A cycle must not hang the walk.
        loop = RuntimeError('循环')
        loop.__cause__ = loop
        self.assertFalse(body_read_broken(loop))
        self.assertEqual(error_kind(loop), 'other')

    def test_break_types_are_exported_for_except_clauses(self):
        self.assertIn(requests.exceptions.ChunkedEncodingError, BODY_BREAK_TYPES)
        self.assertIn(ConnectionResetError, BODY_BREAK_TYPES)


class NotTransientTests(unittest.TestCase):
    """Errors that must keep their own kind: no retry, no vague network wording."""

    def check_other(self, error):
        self.assertFalse(body_read_broken(error), type(error).__name__)
        self.assertEqual(error_kind(error), 'other', type(error).__name__)

    def test_http_status_errors(self):
        for status in (404, 429, 500, 503):
            response = requests.Response()
            response.status_code = status
            response.url = SECRET_URL
            with self.subTest(status=status):
                self.check_other(requests.exceptions.HTTPError(
                    f'{status} Client Error for url: {SECRET_URL}', response=response))

    def test_http_error_inside_a_wrapper_still_counts_as_a_status_answer(self):
        inner = requests.exceptions.HTTPError('404 Client Error: Not Found')
        error = wrapped(requests.exceptions.ChunkedEncodingError, inner)
        self.check_other(error)

    def test_disk_and_permission_errors(self):
        self.check_other(OSError(errno.ENOSPC, 'No space left on device'))
        self.check_other(PermissionError(errno.EACCES, 'Permission denied'))
        self.check_other(FileNotFoundError(errno.ENOENT, 'No such file or directory'))

    def test_content_and_size_failures_keep_their_kind(self):
        error = BunkrError('文件内容校验失败：服务器返回错误页或内容无效')
        self.check_other(error)
        self.assertEqual(error_kind(BunkrError('文件大小不符：预期 100，实际 50')), 'size_mismatch')
        self.check_other(BunkrError('服务器返回网页，无法保存为文件'))
        self.check_other(BunkrError('服务器重定向次数过多'))

    def test_corrupt_content_is_not_a_broken_stream(self):
        self.check_other(requests.exceptions.ContentDecodingError(
            urllib3_errors.DecodeError(
                'Error -3 while decompressing data: invalid distance too far back')))

    def test_connection_failures_stay_transient_without_claiming_a_broken_body(self):
        # Already transient before this change: never a regression, but the
        # wording must stay "连接失败" rather than "下载中断".
        error = requests.exceptions.ConnectionError(
            'HTTPSConnectionPool: Max retries exceeded (Connection refused)')
        self.assertFalse(body_read_broken(error))
        self.assertEqual(error_kind(error), 'transient')
        self.assertFalse(body_read_broken(urllib3_errors.NewConnectionError(None, 'refused')))

    def test_domain_errors_keep_their_kind(self):
        self.assertEqual(error_kind(ChallengeRequired()), 'challenge')
        self.assertEqual(error_kind(Maintenance()), 'maintenance')
        self.assertEqual(error_kind(NotFound()), 'not_found')
        self.assertEqual(error_kind(RateLimited('HTTP 429', 120)), 'rate_limited')
        self.assertEqual(error_kind(TransientError('连接失败')), 'transient')


class WordingTests(unittest.TestCase):
    def test_broken_body_reads_as_an_interrupted_download(self):
        error = requests.exceptions.ChunkedEncodingError(
            'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10))
        tag_error(error, '文件体', SECRET_URL)
        context = error_context(error)
        self.assertEqual(context, {'stage': '文件体', 'host': 'mlk-b.cdn.cr'})
        kind = error_kind(error)
        message = describe_error(kind, '下载中断', **context)
        self.assertEqual(message, '下载中断（可能是网络问题，可重试）· 阶段：文件体 · 主机 mlk-b.cdn.cr')
        self.assertNotIn('SECRET', message)
        self.assertNotIn('http', message)

    def test_other_transient_wording_is_unchanged(self):
        self.assertEqual(describe_error('transient'), '网络不稳定，稍后重试')
        self.assertEqual(describe_error('transient', '连接超时'),
                         '连接超时（可能是代理或网络问题）')
        self.assertEqual(describe_error('transient', '连接失败'),
                         '连接失败（可能是代理或网络问题）')
        self.assertEqual(describe_error('transient', 'Connection broken: protocol error'),
                         '网络不稳定，稍后重试')

    def test_non_transient_wording_is_unchanged(self):
        self.assertEqual(describe_error('other', '文件写入失败'), '下载失败：文件写入失败')
        self.assertEqual(describe_error('size_mismatch'), '文件不完整，稍后重试')
        self.assertEqual(describe_error('not_found'), '文件已被删除，可跳过')


class DetailWordingTests(unittest.TestCase):
    """downloader._detail is what the user sees next to the kind."""

    def detail(self, error):
        from bunkrdl.downloader import _detail
        return _detail(error)

    def test_broken_bodies_say_download_interrupted_not_write_failed(self):
        for error in (
            requests.exceptions.ChunkedEncodingError(
                'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10)),
            urllib3_errors.ProtocolError('Connection aborted.', ConnectionResetError(10054, '重置')),
            ConnectionResetError(10054, '连接被对方重置'),
            BrokenPipeError(32, '管道断开'),
            socket.timeout('timed out'),
        ):
            with self.subTest(error=type(error).__name__):
                self.assertEqual(self.detail(error), '下载中断')

    def test_unchanged_details(self):
        self.assertEqual(self.detail(requests.ReadTimeout('read timed out')), '连接超时')
        self.assertEqual(self.detail(requests.ConnectionError('Connection refused')), '连接失败')
        self.assertEqual(self.detail(OSError(errno.ENOSPC, 'No space left on device')),
                         '文件写入失败')
        self.assertEqual(self.detail(RateLimited('HTTP 429', 120)), 'HTTP 429')
        self.assertEqual(self.detail(TransientError('连接超时')), '连接超时')
        # A URL inside a wrapped error is never shown.
        self.assertEqual(self.detail(requests.ConnectionError(f'failed for {SECRET_URL}')),
                         '连接失败')

    def test_full_message_from_a_tagged_body_break(self):
        from bunkrdl.downloader import _detail
        error = requests.exceptions.ChunkedEncodingError(
            'Connection broken: %r' % urllib3_errors.IncompleteRead(1, 10))
        tag_error(error, '文件体', SECRET_URL)
        message = describe_error(error_kind(error), _detail(error), **error_context(error))
        self.assertEqual(message, '下载中断（可能是网络问题，可重试）· 阶段：文件体 · 主机 mlk-b.cdn.cr')
        self.assertNotIn('SECRET', message)


if __name__ == '__main__':
    unittest.main()
