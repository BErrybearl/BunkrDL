"""Bunkr metadata, API decoding and fresh CDN signatures."""
from __future__ import annotations

import base64
import html
import json
import math
import re
import threading
import time
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit, urlunsplit

import requests

from .models import (Album, AlbumFile, BunkrError, ChallengeRequired,
                     Maintenance, NotFound, RateLimited, ResolvedLink, TransientError, kind_of)
from .requestlog import (STAGE_PAGE, STAGE_RESOLVE, STAGE_SIGN, log_request,
                         tag_error)


class _Page(HTMLParser):
    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.file_id = ""
        self.heading = []
        self._h1 = False
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta":
            self.meta[attrs.get("property", attrs.get("name", ""))] = attrs.get("content", "")
        if attrs.get("data-file-id"):
            self.file_id = attrs["data-file-id"]
        if tag == "h1":
            self._h1 = True

    def handle_endtag(self, tag):
        if tag == "h1":
            self._h1 = False

    def handle_data(self, data):
        if self._h1:
            self.heading.append(data)


def _album_data(page):
    match = re.search(r"window\.albumFiles\s*=\s*", page)
    if not match:
        raise BunkrError("相册页缺少文件清单")
    source = page[match.end():]
    # Match strings atomically so punctuation inside filenames stays untouched.
    tokens = re.compile(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|([A-Za-z_$][\w$]*)(?=\s*:)|,(?=\s*[}\]])', re.S)

    def convert(m):
        value = m.group()
        if m.group(1):
            return json.dumps(value)
        if value == ",":
            return ""
        if value.startswith('"'):
            return value.replace("\\'", "'")
        value = value[1:-1].replace("\\'", "'").replace('"', '\\"')
        return '"' + value + '"'

    try:
        data, _ = json.JSONDecoder().raw_decode(tokens.sub(convert, source).lstrip())
        if not isinstance(data, list) or any(not isinstance(x, dict) for x in data):
            raise ValueError
        return data
    except (ValueError, TypeError) as exc:
        raise BunkrError("相册文件清单格式无效") from exc


def _maintenance(value):
    return (urlsplit(value).path.lower().endswith(("/maint.mp4", "/maintenance-vid.mp4"))
            or "server under maintenance" in value.lower())


def check_challenge(headers, body):
    """Shared by metadata requests and streamed CDN responses."""
    headers = {k.lower(): str(v).lower() for k, v in headers.items()}
    if isinstance(body, bytes):
        body = body.decode('utf-8', errors='ignore')
    if headers.get('cf-mitigated') == 'challenge' or any(marker in body.lower() for marker in (
            'ddos-guard', '__cf_chl', 'cf-chl-', 'challenge-platform',
            'just a moment...', 'checking your browser', 'attention required! | cloudflare')):
        raise ChallengeRequired('站点要求人机验证，已停止任务')


class _MirrorForbidden(ChallengeRequired):
    """Plain HTTP 403, without challenge evidence; one mirror fallback is allowed."""


class BunkrClient:
    def __init__(self, config, session=None, *, throttle=None, fail_fast=False,
                 clock=None, sleep=None, wall_clock=None):
        self.config = config
        self.session = session if session is not None else requests.Session()
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._wall_clock = wall_clock or time.time
        self._last_request = None
        self._lock = threading.Lock()
        self._throttle = throttle
        self.fail_fast = fail_fast

    @property
    def throttle(self):
        """The shared request gate, also used by the file-body download."""
        return self._throttle

    def _retry_after(self, value):
        if value is None:
            return None
        try:
            number = float(value)
            if not math.isfinite(number) or number < 0:
                return None
            return number
        except (TypeError, ValueError):
            try:
                return max(0.0, parsedate_to_datetime(value).timestamp() - self._wall_clock())
            except (TypeError, ValueError, OverflowError):
                return None

    def _request(self, method, url, stage=None, **kwargs):
        headers = {"User-Agent": self.config.user_agent, **kwargs.pop("headers", {})}
        for _ in range(9):
            if self._throttle is not None:
                if self._throttle.acquire() is False:
                    raise TransientError("请求已停止")
            else:
                with self._lock:
                    if self._last_request is not None:
                        delay = self.config.request_interval - (self._clock() - self._last_request)
                        if delay > 0:
                            self._sleep(delay)
                    self._last_request = self._clock()
            started = time.perf_counter()
            try:
                response = self.session.request(method, url, headers=headers,
                                                timeout=self.config.api_timeout,
                                                allow_redirects=False, **kwargs)
            except requests.RequestException as error:
                tag_error(error, stage, url)
                log_request(stage, method, url, error=error, started=started,
                            config=self.config)
                raise
            response.encoding = "utf-8"
            body = response.text
            status = response.status_code
            log_request(stage, method, url, status=status,
                        retry_after=response.headers.get("Retry-After"),
                        bytes=len(response.content) if response.content is not None else None,
                        started=started, config=self.config)
            try:
                check_challenge(response.headers, body)
            except ChallengeRequired as error:
                response.close()
                tag_error(error, stage, url)
                raise
            if status in (429, 503):
                raw_retry = response.headers.get("Retry-After")
                retry = self._retry_after(raw_retry)
                response.close()
                detail = "：文件服务器正在维护" if _maintenance(url) or "maintenance" in body.lower() else ""
                error = RateLimited(f"HTTP {status}{detail}", retry_after=retry)
                error.invalid_retry_after = raw_retry is not None and retry is None
                tag_error(error, stage, url)
                raise error
            if _maintenance(url) or _maintenance(body):
                response.close()
                error = Maintenance("文件服务器正在维护")
                tag_error(error, stage, url)
                raise error
            if status in (400, 404):
                response.close()
                error = NotFound("相册或文件不存在")
                tag_error(error, stage, url)
                raise error
            if status == 403:
                response.close()
                error = _MirrorForbidden("站点要求人机验证或拒绝访问")
                tag_error(error, stage, url)
                raise error
            if status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                response.close()
                if not location:
                    error = BunkrError("服务器重定向缺少地址")
                    tag_error(error, stage, url)
                    raise error
                url = urljoin(url, location)
                if method != "GET" and status in (301, 302, 303):
                    method = "GET"
                    kwargs.pop("json", None)
                kwargs.pop("params", None)
                continue
            if status >= 400:
                response.close()
                error = BunkrError(f"服务器返回 HTTP {status}")
                tag_error(error, stage, url)
                raise error
            return response
        error = BunkrError("服务器重定向次数过多")
        tag_error(error, stage, url)
        raise error

    def _page(self, url):
        path = urlsplit(url)
        domains = self.config.domains or [path.netloc]
        if self.fail_fast:
            domains = domains[:2]
        challenged = False
        timed_out = False
        target = None
        for domain in domains:
            root = domain.rstrip("/") if "://" in domain else "https://" + domain
            target = root + path.path + ("?" + path.query if path.query else "")
            try:
                response = self._request("GET", target, stage=STAGE_PAGE)
                actual = response.url or target
                # Some mirrors drop advanced=1 during a redirect.
                if dict(parse_qsl(path.query)).get("advanced") == "1" and dict(parse_qsl(urlsplit(actual).query)).get("advanced") != "1":
                    response.close()
                    parts = urlsplit(actual)
                    query = dict(parse_qsl(parts.query), advanced="1")
                    response = self._request("GET", urlunsplit(parts._replace(query=urlencode(query))),
                                             stage=STAGE_PAGE)
                return response
            except _MirrorForbidden:
                challenged = True
                continue
            except ChallengeRequired:
                if self.fail_fast:
                    raise
                challenged = True
                continue
            except requests.Timeout as exc:
                if "1909" in str(exc):
                    raise BunkrError("Windows 账户已锁定（1909），已停止请求") from None
                if self.fail_fast:
                    error = TransientError("连接超时")
                    tag_error(error, STAGE_PAGE, target)
                    raise error from None
                timed_out = True
                continue
            except requests.ConnectionError as exc:
                if "1909" in str(exc):
                    raise BunkrError("Windows 账户已锁定（1909），已停止请求") from None
                if self.fail_fast:
                    error = TransientError("连接失败")
                    tag_error(error, STAGE_PAGE, target)
                    raise error from None
                continue
            except requests.RequestException:
                error = BunkrError("页面请求失败")
                tag_error(error, STAGE_PAGE, target)
                raise error from None
        if challenged:
            raise ChallengeRequired("所有配置镜像均不可用，部分或全部要求人机验证")
        error = TransientError("连接超时" if timed_out else "连接失败")
        tag_error(error, STAGE_PAGE, target)
        raise error

    @staticmethod
    def _input(url, prefix):
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise BunkrError("请输入完整的 Bunkr 链接")
        match = re.fullmatch(r"/(" + prefix + r")/([^/]+)/?", parts.path)
        if not match:
            raise BunkrError("Bunkr 链接路径无效")
        return parts, unquote(match[2])

    def get_album(self, url):
        parts, album_id = self._input(url, "a")
        query = dict(parse_qsl(parts.query), advanced="1")
        response = self._page(urlunsplit(parts._replace(query=urlencode(query), fragment="")))
        try:
            page = _Page(response.text)
            title = html.unescape(page.meta.get("og:title", "")) or album_id
            root = urlunsplit(urlsplit(response.url)._replace(path="", query="", fragment=""))
            files = []
            for item in _album_data(response.text):
                try:
                    slug = str(item["slug"])
                    name = item.get("original") or item.get("name") or slug
                    storage = item.get("name")
                    if not storage and item.get("cdnEndpoint"):
                        storage = unquote(urlsplit(item["cdnEndpoint"]).path.rsplit("/", 1)[-1])
                    if not isinstance(name, str) or not isinstance(item["slug"], str):
                        raise ValueError
                    files.append(AlbumFile(
                        file_id=str(item.get("id") or ""), name=name, slug=slug,
                        page_url=root + "/f/" + quote(slug, safe=""),
                        size=int(item["size"]) if item.get("size") is not None else None,
                        cdn_name=storage or None,
                        kind=kind_of(name), uploaded=item.get("timestamp")))
                except (KeyError, TypeError, ValueError):
                    raise BunkrError("相册文件元数据无效") from None
            return Album(album_id, title, root + parts.path, files)
        finally:
            response.close()

    def get_file(self, url):
        _, slug = self._input(url, "f|v|i|d")
        response = self._page(url)
        try:
            page = _Page(response.text)
            if not page.file_id:
                raise BunkrError("文件页缺少 data-file-id")
            name = unquote(html.unescape("".join(page.heading).strip() or page.meta.get("og:title", ""))) or slug
            return AlbumFile(page.file_id, name, slug, response.url, kind=kind_of(name))
        finally:
            response.close()

    def _json(self, method, url, stage=None, **kwargs):
        try:
            response = self._request(method, url, stage=stage, **kwargs)
        except requests.Timeout:
            error = TransientError("连接超时")
            tag_error(error, stage, url)
            raise error from None
        except requests.ConnectionError as exc:
            if getattr(exc, "winerror", None) == 1909:
                raise
            error = TransientError("连接失败")
            tag_error(error, stage, url)
            raise error from None
        except requests.RequestException:
            error = BunkrError("接口连接失败")
            tag_error(error, stage, url)
            raise error from None
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError
            return data
        except ValueError:
            error = BunkrError("接口返回的 JSON 无效")
            tag_error(error, stage, url)
            raise error from None
        finally:
            response.close()

    def resolve(self, file):
        file_id = file.file_id or self.get_file(file.page_url).file_id
        referer = self.config.api_referer
        origin = urlunsplit(urlsplit(referer)._replace(path="", query="", fragment=""))
        headers = {"Referer": referer, "Origin": origin}
        data = self._json("POST", self.config.api_url, stage=STAGE_RESOLVE,
                          json={"id": str(file_id)}, headers=headers)
        try:
            if data.get("url"):
                direct = data["url"]
                if data.get("encrypted"):
                    key = ("SECRET_KEY_" + str(int(data["timestamp"]) // 3600)).encode()
                    raw = base64.b64decode(direct, validate=True)
                    direct = bytes(b ^ key[i % len(key)] for i, b in enumerate(raw)).decode("utf-8")
            else:
                base = urlsplit(data["mediafiles"])
                direct = urlunsplit(base._replace(path=data["path"], query="", fragment=""))
            parts = urlsplit(direct)
            if parts.scheme not in ("http", "https") or not parts.netloc or not parts.path:
                raise ValueError
        except (KeyError, ValueError, TypeError, UnicodeError):
            error = BunkrError("文件接口的下载地址无效")
            tag_error(error, STAGE_RESOLVE, self.config.api_url)
            raise error from None
        if _maintenance(direct):
            error = Maintenance("文件服务器正在维护")
            tag_error(error, STAGE_RESOLVE, direct)
            raise error
        signature = self._json("GET", self.config.sign_url, stage=STAGE_SIGN,
                               params={"path": unquote(parts.path)}, headers=headers)
        try:
            token = signature["token"]
            expires = float(signature["ex"])
            if not isinstance(token, str) or not token or not 0 < expires < float("inf"):
                raise ValueError
            query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in ("token", "ex")]
            query.extend((("token", token), ("ex", str(signature["ex"]))))
            signed = urlunsplit(parts._replace(query=urlencode(query), fragment=""))
        except (KeyError, ValueError, TypeError):
            error = BunkrError("签名接口的返回值无效")
            tag_error(error, STAGE_SIGN, self.config.sign_url)
            raise error from None
        return ResolvedLink(signed, referer, unquote(parts.path.rsplit("/", 1)[-1]), expires,
                            {"User-Agent": self.config.user_agent, "Referer": referer})
