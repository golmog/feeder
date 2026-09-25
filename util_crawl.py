# -*- coding: utf-8 -*-
import io
import os
import re
import sys
import glob
import time
import math
import yaml
import json
import base64
import requests
import subprocess
import importlib.util
from datetime import datetime, timedelta
from urllib.parse import urlparse, unquote
from html import unescape
from lxml import html

from .setup import *

_CURL_CFFI_AVAILABLE = False
try:
    from curl_cffi import requests as cffi_requests
    _CURL_CFFI_AVAILABLE = True
except ImportError:
    try:
        logger.info("[Crawl] curl_cffi 모듈 설치 시도...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi"])
        from curl_cffi import requests as cffi_requests
        _CURL_CFFI_AVAILABLE = True
        logger.info("[Crawl] curl_cffi 설치 완료")
    except Exception as e:
        logger.error(f"[Crawl] curl_cffi 설치 실패: {e}")
        _CURL_CFFI_AVAILABLE = False

CONFIG_FILEPATH = os.path.join(path_data, 'db', 'feeder_settings.yaml')
CUSTOM_DIR = os.path.join(path_data, 'db', 'feeder_custom')


def get_ddns() -> str:
    try:
        if hasattr(F, 'SystemModelSetting') and F.SystemModelSetting:
            return F.SystemModelSetting.get('ddns') or ''
    except Exception:
        pass
    try:
        if SystemModelSetting:
            return SystemModelSetting.get('ddns') or ''
    except Exception:
        pass
    try:
        return F.config.get('ddns', '')
    except Exception:
        return ''


def get_system_apikey() -> str:
    try:
        if hasattr(F, 'SystemModelSetting') and F.SystemModelSetting:
            return F.SystemModelSetting.get('auth_apikey') or F.SystemModelSetting.get('apikey') or ''
    except Exception:
        pass
    try:
        if SystemModelSetting:
            return SystemModelSetting.get('auth_apikey') or SystemModelSetting.get('apikey') or ''
    except Exception:
        pass
    return ''


def get_tmp_dir(sub_path: str = '') -> str:
    base_dir = os.path.join(path_data, 'tmp', P.package_name)
    target_dir = os.path.join(base_dir, sub_path) if sub_path else base_dir
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


def extract_info_hash(magnet_uri: str) -> str | None:
    if not magnet_uri:
        return None

    if magnet_uri.startswith('magnet:'):
        match = re.search(r'xt=urn:btih:([a-zA-Z0-9]+)', magnet_uri, re.IGNORECASE)
        if match:
            raw_hash = match.group(1).lower()
            if len(raw_hash) == 40:
                return raw_hash
            elif len(raw_hash) == 32:
                try:
                    decoded = base64.b32decode(raw_hash.upper())
                    return decoded.hex().lower()
                except Exception:
                    return raw_hash

    elif magnet_uri.startswith('ed2k://'):
        match = re.search(r'ed2k://\|file\|[^|]+\|[0-9]+\|([a-fA-F0-9]{32})', magnet_uri, re.IGNORECASE)
        if match:
            return match.group(1).lower()

    return None


def extract_info_hash_from_torrent(torrent_bytes: bytes) -> str | None:
    if not torrent_bytes:
        return None
    try:
        info_idx = torrent_bytes.find(b'4:info')
        if info_idx == -1:
            return None

        start = info_idx + 6
        if start >= len(torrent_bytes) or torrent_bytes[start:start+1] != b'd':
            return None

        pos = start
        depth = 0
        while pos < len(torrent_bytes):
            char = torrent_bytes[pos:pos+1]
            if char in (b'd', b'l'):
                depth += 1
                pos += 1
            elif char == b'e':
                depth -= 1
                pos += 1
                if depth == 0:
                    break
            elif char == b'i':
                end_int = torrent_bytes.find(b'e', pos + 1)
                if end_int == -1:
                    break
                pos = end_int + 1
            elif b'0' <= char <= b'9':
                colon = torrent_bytes.find(b':', pos)
                if colon == -1:
                    break
                str_len = int(torrent_bytes[pos:colon])
                pos = colon + 1 + str_len
            else:
                pos += 1

        if depth == 0 and pos > start:
            raw_info = torrent_bytes[start:pos]
            import hashlib
            return hashlib.sha1(raw_info).hexdigest().lower()
    except Exception as ex:
        logger.debug(f"[CrawlUtil] .torrent 마그넷 해시 추출 예외: {ex}")
    return None


def split_magnets(magnet_str: str) -> list[str]:
    if not magnet_str:
        return []
    raw = str(magnet_str).strip()
    if not raw:
        return []

    matches = list(re.finditer(r'(?:magnet:\?|ed2k://)', raw, re.IGNORECASE))
    if not matches:
        sep = '\n' if '\n' in raw else '|'
        return [m.strip() for m in raw.split(sep) if m.strip()]

    result = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
        part = raw[start:end].strip().strip('|').strip()
        if part:
            result.append(part)
    return result


def clean_xml_string(value: str) -> str:
    if not value:
        return ''
    return (
        value.replace('&', '&amp;')
             .replace('<', '&lt;')
             .replace('>', '&gt;')
             .replace('"', '&quot;')
             .replace("'", '&apos;')
    )


def get_paging_info(count, current_page, page_size):
    total_page = math.ceil(count / page_size) if page_size > 0 else 1
    if total_page == 0:
        total_page = 1
    current_page = max(1, min(int(current_page or 1), total_page))

    page_block = 10
    start_page = ((current_page - 1) // page_block) * page_block + 1
    end_page = min(start_page + page_block - 1, total_page)
    page_list = list(range(start_page, end_page + 1))

    prev_p = start_page - 1 if start_page > 1 else 0
    next_p = end_page + 1 if end_page < total_page else 0

    return {
        'page': current_page,
        'current_page': current_page,
        'page_size': page_size,
        'list_step': page_size,
        'total_page': total_page,
        'total_count': count,
        'count': count,
        'start_page': start_page,
        'end_page': end_page,
        'last_page': end_page,
        'page_list': page_list,
        'prev_page': prev_p,
        'next_page': next_p,
    }


class CrawlConfigUtil:

    @classmethod
    def get_filepath(cls) -> str:
        return CONFIG_FILEPATH

    @classmethod
    def load_yaml(cls) -> dict:
        if not os.path.exists(CONFIG_FILEPATH):
            default_data = {
                'GLOBAL': {},
                'CRAWLERS': [],
                'FEEDS': [],
                'DOWNLOADERS': [],
                'DOWNLOAD_PROFILES': [],
                'GDRIVE_ACCOUNTS': []
            }
            cls.save_yaml(default_data)
            return default_data

        try:
            with open(CONFIG_FILEPATH, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}

            if 'CRAWLERS' not in data or not isinstance(data['CRAWLERS'], list):
                data['CRAWLERS'] = []
            if 'FEEDS' not in data or not isinstance(data['FEEDS'], list):
                data['FEEDS'] = []
            if 'GLOBAL' not in data or not isinstance(data['GLOBAL'], dict):
                data['GLOBAL'] = {}
            if 'DOWNLOADERS' not in data or not isinstance(data['DOWNLOADERS'], list):
                data['DOWNLOADERS'] = []
            if 'DOWNLOAD_PROFILES' not in data or not isinstance(data['DOWNLOAD_PROFILES'], list):
                data['DOWNLOAD_PROFILES'] = []
            if 'GDRIVE_ACCOUNTS' not in data or not isinstance(data['GDRIVE_ACCOUNTS'], list):
                data['GDRIVE_ACCOUNTS'] = []

            from .task_crawl import TaskCrawl
            for c in data['CRAWLERS']:
                if 'boards' not in c or not isinstance(c['boards'], list):
                    c['boards'] = []
                for b in c['boards']:
                    b_val = b.get('board', '')
                    s_val = b.get('subcat', '')
                    _, _, f_key = TaskCrawl.parse_board_info(b_val, s_val)
                    b['full_board_key'] = f_key

            return data
        except Exception as e:
            logger.error(f"[CrawlConfig] YAML 로드 실패: {e}")
            return {'GLOBAL': {}, 'CRAWLERS': [], 'FEEDS': [], 'DOWNLOADERS': [], 'DOWNLOAD_PROFILES': [], 'GDRIVE_ACCOUNTS': []}

    @classmethod
    def save_yaml(cls, data: dict) -> bool:
        try:
            os.makedirs(os.path.dirname(CONFIG_FILEPATH), exist_ok=True)
            raw_yaml = yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
            formatted_yaml = re.sub(r'\n([A-Z0-9_]+:)', r'\n\n\1', raw_yaml).lstrip('\n')
            with open(CONFIG_FILEPATH, 'w', encoding='utf-8') as f:
                f.write(formatted_yaml)
            return True
        except Exception as e:
            logger.error(f"[CrawlConfig] YAML 저장 실패: {e}")
            return False

    @classmethod
    def get_crawlers(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('CRAWLERS', [])

    @classmethod
    def get_crawler(cls, crawler_id):
        for c in cls.get_crawlers():
            if str(c.get('id')) == str(crawler_id):
                return c
        return None

    @classmethod
    def get_crawler_by_board(cls, site_name: str, board_id: str, subcat_id: str = None):
        from .task_crawl import TaskCrawl
        _, _, full_key = TaskCrawl.parse_board_info(board_id, subcat_id)
        for c in cls.get_crawlers():
            if c.get('site') == site_name:
                for b in c.get('boards', []):
                    b_key = b.get('full_board_key') or b.get('board')
                    if b_key == full_key:
                        return c
        return None

    @classmethod
    def save_crawler(cls, item: dict) -> str:
        data = cls.load_yaml()
        crawlers = data.get('CRAWLERS', [])
        target_id = item.get('id')

        from .task_crawl import TaskCrawl
        boards = item.get('boards', [])
        normalized_boards = []
        for b in boards:
            b_val = b.get('board', '')
            s_val = b.get('subcat', '')
            _, _, f_key = TaskCrawl.parse_board_info(b_val, s_val)
            normalized_boards.append({
                'board': str(b_val),
                'subcat': str(s_val) if s_val else '',
                'full_board_key': f_key
            })
        item['boards'] = normalized_boards

        if target_id is not None and int(target_id) > 0:
            for idx, c in enumerate(crawlers):
                if str(c.get('id')) == str(target_id):
                    crawlers[idx].update(item)
                    data['CRAWLERS'] = crawlers
                    cls.save_yaml(data)
                    logger.info(f"[CrawlConfig] 수집기 수정 완료: ID={target_id}, Site={item.get('site')}")
                    return 'success_update'
            return 'not_found'

        for c in crawlers:
            if c.get('site') == item.get('site'):
                logger.warning(f"[CrawlConfig] 동일 사이트 수집기 이미 존재: {item.get('site')}")
                return 'already_exist'

        max_id = max([int(c.get('id', 0)) for c in crawlers], default=0)
        item['id'] = max_id + 1
        crawlers.append(item)
        data['CRAWLERS'] = crawlers
        cls.save_yaml(data)
        logger.info(f"[CrawlConfig] 신규 수집기 추가 완료: ID={item['id']}, Site={item.get('site')}")
        return 'success'

    @classmethod
    def delete_crawler(cls, target_id) -> bool:
        data = cls.load_yaml()
        crawlers = data.get('CRAWLERS', [])
        data['CRAWLERS'] = [c for c in crawlers if str(c.get('id')) != str(target_id)]
        cls.save_yaml(data)
        logger.info(f"[CrawlConfig] 수집기 삭제 완료: ID={target_id}")
        return True


class CrawlCustomManager:
    _hooks = {}

    @classmethod
    def get_custom_dir(cls) -> str:
        sites_dir = os.path.join(CUSTOM_DIR, 'sites')
        os.makedirs(sites_dir, exist_ok=True)
        return sites_dir

    @classmethod
    def load_hooks(cls):
        cls._hooks = {}
        custom_dir = cls.get_custom_dir()
        candidate_files = glob.glob(os.path.join(custom_dir, "*.py"))
        candidate_files.extend(glob.glob(os.path.join(CUSTOM_DIR, "site_*.py")))

        for fpath in set(candidate_files):
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue
            module_name = f"feeder_custom_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    obj = getattr(mod, attr_name)
                    if isinstance(obj, type) and hasattr(obj, 'SITE_NAME') and obj.SITE_NAME:
                        cls._hooks[obj.SITE_NAME.lower()] = obj
                        logger.info(f"[CrawlCustom] 커스텀 사이트 훅 등록: '{obj.SITE_NAME}' ({fname})")
            except Exception as e:
                logger.error(f"[CrawlCustom] 커스텀 훅 로드 실패 ({fname}): {e}")

        cls.sync_default_site_info()

    @classmethod
    def sync_default_site_info(cls):
        try:
            with F.app.app_context():
                from .model_crawl import ModelCrawlSite
                for site_key, hook_cls in cls._hooks.items():
                    default_info = getattr(hook_cls, 'DEFAULT_SITE_INFO', None)
                    if default_info and isinstance(default_info, dict):
                        target_name = default_info.get('NAME') or getattr(hook_cls, 'SITE_NAME', '')
                        if not target_name:
                            continue

                        existing = ModelCrawlSite.get(name=target_name)
                        content_str = json.dumps(default_info, ensure_ascii=False, indent=2)
                        if not existing:
                            new_site = ModelCrawlSite('custom', default_info, content_str)
                            db.session.add(new_site)
                            logger.info(f"[CrawlCustom] 사이트 템플릿 신규 등록: '{target_name}'")
                        else:
                            existing.info = default_info
                            existing.content = content_str

                        db.session.commit()
        except Exception as e:
            logger.error(f"[CrawlCustom] sync_default_site_info 에러: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

    @classmethod
    def get_hook(cls, site_name: str):
        if not cls._hooks:
            cls.load_hooks()
        return cls._hooks.get(site_name.lower()) if site_name else None


class CrawlScraper:
    _cf_cookies = {}
    _cf_user_agents = {}
    CF_COOKIE_EXPIRY = 3600
    _selenium_driver = None
    _cffi_sessions = {}
    _requests_sessions = {}
    _proxy_index = 0

    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    @classmethod
    def _get_proxy_list(cls, scheduler_instance=None) -> list[str]:
        if scheduler_instance:
            use_proxy = getattr(scheduler_instance, 'use_proxy', False)
            raw_proxy = getattr(scheduler_instance, 'proxy_url', '')
        else:
            use_proxy = P.ModelSetting.get_bool('crawl_use_proxy')
            raw_proxy = ''

        if not raw_proxy:
            raw_proxy = P.ModelSetting.get('crawl_proxy_url') or ''

        if not use_proxy or not raw_proxy:
            return []

        proxies = []
        for part in raw_proxy.replace('\n', ',').split(','):
            p = part.strip()
            if p and p not in proxies:
                proxies.append(p)
        return proxies

    @classmethod
    def get_current_proxy_url(cls, scheduler_instance=None) -> str:
        p_list = cls._get_proxy_list(scheduler_instance)
        if not p_list:
            return ''
        return p_list[cls._proxy_index % len(p_list)]

    @classmethod
    def rotate_proxy(cls, scheduler_instance=None, reason: str = "차단 감지") -> str:
        p_list = cls._get_proxy_list(scheduler_instance)
        if not p_list:
            return ''
        if len(p_list) > 1:
            cls._proxy_index = (cls._proxy_index + 1) % len(p_list)
            active_proxy = p_list[cls._proxy_index]
            logger.info(f"[CrawlScraper] 프록시 로테이션 ({reason}) -> {active_proxy} ({cls._proxy_index + 1}/{len(p_list)}번)")
            return active_proxy
        return p_list[0]

    @classmethod
    def get_proxies(cls, scheduler_instance=None):
        active_url = cls.get_current_proxy_url(scheduler_instance)
        if active_url:
            return {"http": active_url, "https": active_url}
        return None

    @classmethod
    def _has_valid_clearance(cls, host: str) -> bool:
        cf_data = cls._cf_cookies.get(host, {})
        if not cf_data:
            return False
        timestamp = cf_data.get('_timestamp', 0)
        has_token = bool(cf_data.get('cf_clearance'))
        return has_token and (time.time() - timestamp < cls.CF_COOKIE_EXPIRY)

    @classmethod
    def _sync_clearance_to_sessions(cls, host: str):
        cookies = cls._cf_cookies.get(host, {})
        ua = cls._cf_user_agents.get(host)

        req_sess = cls._requests_sessions.get(host)
        if req_sess:
            if ua:
                req_sess.headers['User-Agent'] = ua
            for k, v in cookies.items():
                if not k.startswith('_'):
                    req_sess.cookies.set(k, v, domain=host)

        cffi_sess = cls._cffi_sessions.get(host)
        if cffi_sess:
            if ua:
                cffi_sess.headers['User-Agent'] = ua
            for k, v in cookies.items():
                if not k.startswith('_'):
                    cffi_sess.cookies.set(k, v, domain=host)

    @classmethod
    def get_html(cls, url: str, site_info: dict = None, scheduler_instance=None, referer: str = None, max_retries: int = None, retry_interval: float = None, wait_tag: str = None) -> str | None:
        extra = site_info.get('EXTRA', []) if site_info else []
        cookie = site_info.get('COOKIE') if site_info else None
        proxies = cls.get_proxies(scheduler_instance)
        host = urlparse(url).hostname or ''

        try:
            if max_retries is None:
                inst_retries = getattr(scheduler_instance, 'max_retries', None) if scheduler_instance else None
                max_retries = int(inst_retries) if inst_retries not in [None, ''] else int(P.ModelSetting.get('crawl_crawler_max_retries') or 3)
            if max_retries < 1:
                max_retries = 1
        except Exception:
            max_retries = 3

        try:
            if retry_interval is None:
                inst_delay = getattr(scheduler_instance, 'delay', None) if scheduler_instance else None
                if inst_delay not in [None, '']:
                    retry_interval = float(inst_delay)
                else:
                    global_delay = P.ModelSetting.get('crawl_crawler_delay')
                    retry_interval = float(global_delay) if global_delay not in [None, ''] else 2.0
            if retry_interval < 0.5:
                retry_interval = 0.5
        except Exception:
            retry_interval = 2.0

        if scheduler_instance:
            use_selenium = getattr(scheduler_instance, 'use_selenium', False)
            use_fs = getattr(scheduler_instance, 'use_flaresolverr', False)
        else:
            use_selenium = P.ModelSetting.get_bool('crawl_use_selenium')
            use_fs = P.ModelSetting.get_bool('crawl_use_flaresolverr')

        if 'USE_SELENIUM' in extra or (site_info and site_info.get('USE_SELENIUM')):
            use_selenium = True
        if 'USE_FLARESOLVERR' in extra or (site_info and site_info.get('USE_FLARESOLVERR')):
            use_fs = True

        if use_selenium:
            target_wait_tag = wait_tag or (site_info.get('SELENIUM_WAIT_TAG') if site_info else None) or 'body'
            for attempt in range(1, max_retries + 1):
                res_source = cls.get_by_remote_selenium(url, wait_tag=target_wait_tag, scheduler_instance=scheduler_instance, site_info=site_info)
                if res_source:
                    return res_source

                if cls._selenium_driver is None:
                    logger.warning(f"[CrawlScraper] 브라우저 세션 초기화 실패로 재시도 중단: {url}")
                    return None

                if attempt < max_retries:
                    logger.debug(f"[CrawlScraper] Selenium 재시도 ({attempt}/{max_retries}): {url}")
                    time.sleep(retry_interval)
            return None

        if use_fs and not cls._has_valid_clearance(host):
            logger.info(f"[CrawlScraper] [{host}] Cloudflare 사이트 감지 -> FlareSolverr 최우선 인가 요청: {url}")
            for fs_try in range(1, max_retries + 1):
                tree, fs_html = cls.get_by_flaresolverr(url, proxies=proxies)
                if fs_html and 'Just a moment...' not in fs_html and 'cf-turnstile' not in fs_html:
                    logger.info(f"[CrawlScraper] [{host}] FlareSolverr 인가 및 1차 페이지 수신 완료")
                    return fs_html
                if fs_try < max_retries:
                    logger.debug(f"[CrawlScraper] [{host}] FlareSolverr 초기 인가 재시도 ({fs_try}/{max_retries})...")
                    time.sleep(retry_interval)

            logger.warning(f"[CrawlScraper] [{host}] FlareSolverr 초기 인가 {max_retries}회 실패, HTTP 세션으로 폴백 시도")

        headers = cls.DEFAULT_HEADERS.copy()
        if host in cls._cf_user_agents:
            headers['User-Agent'] = cls._cf_user_agents[host]
        elif site_info and site_info.get('USER_AGENT'):
            headers['User-Agent'] = site_info['USER_AGENT']

        if referer:
            headers['Referer'] = referer
        if cookie:
            headers['Cookie'] = cookie

        domain_cf = cls._cf_cookies.get(host, {})
        if domain_cf and (time.time() - domain_cf.get('_timestamp', 0) < cls.CF_COOKIE_EXPIRY):
            injected = "; ".join([f"{k}={v}" for k, v in domain_cf.items() if not k.startswith('_')])
            if injected:
                current_cookie_str = headers.get('Cookie', '')
                headers['Cookie'] = f"{current_cookie_str}; {injected}".strip('; ')

        for attempt in range(1, max_retries + 1):
            if _CURL_CFFI_AVAILABLE:
                try:
                    cffi_session = cls._cffi_sessions.get(host)
                    if not cffi_session:
                        cffi_session = cffi_requests.Session()
                        cls._cffi_sessions[host] = cffi_session
                        logger.debug(f"[CrawlScraper] curl_cffi 세션 생성: {host}")

                    cls._sync_clearance_to_sessions(host)
                    res = cffi_session.get(url, headers=headers, proxies=proxies, impersonate="chrome", timeout=25)
                    if res.status_code == 200:
                        if 'Just a moment...' not in res.text and 'cf-turnstile' not in res.text:
                            return res.text
                    logger.debug(f"[CrawlScraper] curl_cffi 응답 코드: {res.status_code} ({url})")
                except Exception as e:
                    logger.debug(f"[CrawlScraper] curl_cffi 요청 실패 ({url}): {e}")

            try:
                req_session = cls._requests_sessions.get(host)
                if not req_session:
                    req_session = requests.Session()
                    cls._requests_sessions[host] = req_session
                    logger.debug(f"[CrawlScraper] requests 세션 생성: {host}")

                cls._sync_clearance_to_sessions(host)
                res = req_session.get(url, headers=headers, proxies=proxies, timeout=25, verify=False)
                if res.status_code == 200:
                    res.encoding = res.apparent_encoding or 'utf-8'
                    if 'Just a moment...' not in res.text and 'cf-turnstile' not in res.text:
                        return res.text
                logger.debug(f"[CrawlScraper] requests 응답 코드: {res.status_code} ({url})")
            except Exception as e:
                logger.debug(f"[CrawlScraper] requests 요청 실패 ({url}): {e}")

            if use_fs:
                logger.warning(f"[CrawlScraper] [{host}] Cloudflare 차단 감지 -> FlareSolverr 재인가: {url}")
                cls._cf_cookies.pop(host, None)

                if host in cls._cffi_sessions:
                    try:
                        cls._cffi_sessions[host].close()
                    except Exception:
                        pass
                    cls._cffi_sessions.pop(host, None)

                if host in cls._requests_sessions:
                    try:
                        cls._requests_sessions[host].close()
                    except Exception:
                        pass
                    cls._requests_sessions.pop(host, None)

                tree, source = cls.get_by_flaresolverr(url, proxies=proxies)
                if source and 'Just a moment...' not in source and 'cf-turnstile' not in source:
                    logger.info(f"[CrawlScraper] [{host}] 세션 복구 및 페이지 수신 성공")
                    return source
                if attempt < max_retries:
                    logger.debug(f"[CrawlScraper] FlareSolverr 재시도 ({attempt}/{max_retries}): {url}")
                    time.sleep(retry_interval)
            else:
                if attempt < max_retries:
                    time.sleep(retry_interval)

        return None

    @classmethod
    def get_by_flaresolverr(cls, url: str, proxies=None):
        fs_url = (P.ModelSetting.get('crawl_flaresolverr_url') or '').rstrip('/')
        if not fs_url:
            return None, None

        parsed = urlparse(url)
        host = parsed.hostname or ''
        session_name = f"feeder_{host.replace('.', '_')}"

        try:
            list_res = requests.post(f"{fs_url}/v1", json={"cmd": "sessions.list"}, timeout=10)
            sessions = list_res.json().get('sessions', []) if list_res.status_code == 200 else []
            if session_name not in sessions:
                create_payload = {"cmd": "sessions.create", "session": session_name}
                if proxies and 'http' in proxies:
                    create_payload["proxy"] = {"url": proxies['http']}
                requests.post(f"{fs_url}/v1", json=create_payload, timeout=20)
        except Exception as sess_err:
            logger.debug(f"[CrawlScraper] FlareSolverr 세션 준비 예외 (무시): {sess_err}")

        payload = {
            "cmd": "request.get",
            "url": url,
            "session": session_name,
            "maxTimeout": 25000,
        }
        if proxies and 'http' in proxies:
            payload["proxy"] = {"url": proxies['http']}

        domain_cf = cls._cf_cookies.get(host, {})
        if domain_cf and (time.time() - domain_cf.get('_timestamp', 0) < cls.CF_COOKIE_EXPIRY):
            payload["cookies"] = [
                {"name": k, "value": v, "domain": host}
                for k, v in domain_cf.items() if not k.startswith('_')
            ]

        try:
            res = requests.post(f"{fs_url}/v1", json=payload, headers={'Content-Type': 'application/json'}, timeout=35)
            if res.status_code == 200:
                data = res.json()
                if data.get('status') == 'ok':
                    solution = data.get('solution', {})
                    html_source = solution.get('response', '')
                    tree = html.fromstring(html_source) if html_source else None

                    ua = solution.get('userAgent')
                    if ua:
                        cls._cf_user_agents[host] = ua

                    sol_cookies = solution.get('cookies') or []
                    if sol_cookies:
                        if host not in cls._cf_cookies:
                            cls._cf_cookies[host] = {}
                        for c in sol_cookies:
                            cls._cf_cookies[host][c['name']] = c['value']
                        cls._cf_cookies[host]['_timestamp'] = time.time()
                        logger.info(f"[CrawlScraper] [{host}] FlareSolverr 쿠키 {len(sol_cookies)}개 동기화 완료")

                    cls._sync_clearance_to_sessions(host)
                    return tree, html_source
                else:
                    logger.warning(f"[CrawlScraper] FlareSolverr 오류 응답: {data.get('message')}")
        except Exception as e:
            logger.warning(f"[CrawlScraper] FlareSolverr 통신 실패 ({url}): {e}")
            try:
                requests.post(f"{fs_url}/v1", json={"cmd": "sessions.destroy", "session": session_name}, timeout=5)
            except Exception:
                pass

        return None, None

    @classmethod
    def get_flaresolverr_clearance(cls, url: str, scheduler_instance=None, max_retries: int = None, retry_interval: float = None) -> tuple[list[dict], str]:
        fs_url = (P.ModelSetting.get('crawl_flaresolverr_url') or '').rstrip('/')
        if not fs_url:
            return [], ""

        parsed = urlparse(url)
        host = parsed.hostname or ''
        fs_endpoint = f"{fs_url}/v1"

        try:
            if max_retries is None:
                inst_retries = getattr(scheduler_instance, 'max_retries', None) if scheduler_instance else None
                max_retries = int(inst_retries) if inst_retries not in [None, ''] else int(P.ModelSetting.get('crawl_crawler_max_retries') or 3)
            if max_retries < 1:
                max_retries = 1
        except Exception:
            max_retries = 3

        try:
            if retry_interval is None:
                inst_delay = getattr(scheduler_instance, 'delay', None) if scheduler_instance else None
                if inst_delay not in [None, '']:
                    retry_interval = float(inst_delay)
                else:
                    global_delay = P.ModelSetting.get('crawl_crawler_delay')
                    retry_interval = float(global_delay) if global_delay not in [None, ''] else 2.0
            if retry_interval < 0.5:
                retry_interval = 0.5
        except Exception:
            retry_interval = 2.0

        p_list = cls._get_proxy_list(scheduler_instance)
        total_tries = max(len(p_list), max_retries) if p_list else max_retries

        for attempt in range(1, total_tries + 1):
            proxies = cls.get_proxies(scheduler_instance)
            proxy_str = proxies.get('http') if (proxies and 'http' in proxies) else '미사용'

            req_payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": 45000,
            }
            if proxies and 'http' in proxies:
                req_payload["proxy"] = {"url": proxies['http']}

            logger.info(f"[CrawlScraper] [{host}] FlareSolverr 인가 요청 ({attempt}/{total_tries}, Proxy: {proxy_str}): {url}")

            try:
                res = requests.post(fs_endpoint, json=req_payload, headers={'Content-Type': 'application/json'}, timeout=55)
                if res.status_code == 200:
                    data = res.json()
                    if data.get('status') == 'ok':
                        solution = data.get('solution', {})
                        sol_cookies = solution.get('cookies') or []
                        user_agent = solution.get('userAgent') or ''
                        html_resp = solution.get('response') or ''
                        has_clearance = any(c.get('name') == 'cf_clearance' and c.get('value') for c in sol_cookies)
                        is_challenge = any(k in html_resp for k in ["Just a moment...", "cf-turnstile", "challenge-platform"])

                        if has_clearance or (not is_challenge and len(sol_cookies) > 0):
                            if host not in cls._cf_cookies:
                                cls._cf_cookies[host] = {}
                            for c in sol_cookies:
                                cls._cf_cookies[host][c['name']] = c['value']
                            cls._cf_cookies[host]['_timestamp'] = time.time()
                            if user_agent:
                                cls._cf_user_agents[host] = user_agent

                            cls._sync_clearance_to_sessions(host)
                            logger.info(f"[CrawlScraper] [{host}] FlareSolverr 인가 성공 (쿠키 {len(sol_cookies)}개)")
                            return sol_cookies, user_agent

                        logger.warning(f"[CrawlScraper] [{host}] 챌린지 미해결 감지 -> 다음 프록시 전환")
                    else:
                        logger.warning(f"[CrawlScraper] [{host}] FlareSolverr 응답 오류: {data.get('message')}")
            except Exception as e:
                logger.warning(f"[CrawlScraper] [{host}] FlareSolverr 통신 실패: {e}")

            if attempt < total_tries:
                cls.rotate_proxy(scheduler_instance=scheduler_instance, reason="FlareSolverr 인가 실패 대응")
                time.sleep(retry_interval)

        return [], ""

    @classmethod
    def solve_turnstile_checkbox(cls, driver, max_wait: int = 6) -> bool:
        from selenium.webdriver.common.by import By
        start_time = time.time()
        while time.time() - start_time < max_wait:
            title = driver.title or ""
            if not any(k in title for k in ["Just a moment", "Cloudflare", "Attention Required"]):
                return True
            try:
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                for frame in iframes:
                    src = frame.get_attribute("src") or ""
                    if any(k in src for k in ["cloudflare", "turnstile", "challenge"]):
                        driver.switch_to.frame(frame)
                        try:
                            targets = driver.find_elements(By.XPATH, "//input[@type='checkbox'] | //span[contains(@class, 'checkbox')] | //div[@id='challenge-stage']//label | //body")
                            if targets:
                                driver.execute_script("arguments[0].click();", targets[0])
                                logger.debug("[CrawlScraper] Turnstile 체크박스 클릭 시도")
                        finally:
                            driver.switch_to.default_content()
                        break
            except Exception:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
            time.sleep(1.2)
        return False

    @classmethod
    def reinit_session(cls, site_info: dict, scheduler_instance=None):
        site_url = (site_info.get('TORRENT_SITE_URL') if site_info else '').rstrip('/')
        parsed_url = urlparse(site_url)
        host = parsed_url.hostname or ''

        cls.rotate_proxy(scheduler_instance=scheduler_instance, reason="지속적 차단 대응")
        logger.warning(f"[CrawlScraper] [{host}] 지속적 차단 감지 -> 브라우저/토큰 캐시 파기 후 재초기화")

        cls.close_sessions()
        if host in cls._cf_cookies:
            cls._cf_cookies.pop(host, None)
        if host in cls._cf_user_agents:
            cls._cf_user_agents.pop(host, None)

        use_fs = getattr(scheduler_instance, 'use_flaresolverr', False) if scheduler_instance else P.ModelSetting.get_bool('crawl_use_flaresolverr')
        if site_info and ('USE_FLARESOLVERR' in site_info.get('EXTRA', []) or site_info.get('USE_FLARESOLVERR')):
            use_fs = True

        if use_fs and site_url:
            cls.get_flaresolverr_clearance(site_url, scheduler_instance=scheduler_instance)

        driver = cls.init_stealth_selenium(site_info or {}, scheduler_instance=scheduler_instance)

        if site_info:
            try:
                hook = CrawlCustomManager.get_hook(site_info.get('NAME'))
                if hook and hasattr(hook, 'on_init_session'):
                    logger.info(f"[{site_info.get('NAME')}] 세션 재초기화 훅(on_init_session) 실행")
                    hook.on_init_session(site_info, scheduler_instance)
                    driver = cls._selenium_driver
            except Exception as hook_err:
                logger.debug(f"[CrawlScraper] 재초기화 훅 예외: {hook_err}")

        return driver

    @classmethod
    def handle_turnstile_challenge(cls, driver, url: str, site_info: dict = None, scheduler_instance=None) -> bool:
        page_title = driver.title or ""
        if not any(k in page_title for k in ["Just a moment", "Cloudflare", "Attention Required"]):
            return True

        logger.info(f"[CrawlScraper] Cloudflare 챌린지 화면 감지 ('{page_title}') -> 해결 시도: {url}")
        if cls.solve_turnstile_checkbox(driver, max_wait=4):
            return True

        new_driver = cls.reinit_session(site_info or {}, scheduler_instance=scheduler_instance)
        if new_driver:
            new_driver.get(url)
            time.sleep(2)
            cls.solve_turnstile_checkbox(new_driver, max_wait=4)

        current_title = (cls._selenium_driver.title if cls._selenium_driver else "") or ""
        return not any(k in current_title for k in ["Just a moment", "Cloudflare", "Attention Required"])

    @classmethod
    def init_stealth_selenium(cls, site_info: dict, scheduler_instance=None):
        site_url = site_info.get('TORRENT_SITE_URL', '').rstrip('/')
        remote_url = site_info.get('SELENIUM_REMOTE_URL') or P.ModelSetting.get('crawl_selenium_remote_url') or 'http://selenium:4444/wd/hub'
        proxies = cls.get_proxies(scheduler_instance)
        parsed_url = urlparse(site_url)
        host = parsed_url.hostname or ''

        if cls._selenium_driver:
            try:
                _ = cls._selenium_driver.current_url
                return cls._selenium_driver
            except Exception:
                cls._selenium_driver = None

        try:
            selenium_timeout = int((site_info.get('SELENIUM_TIMEOUT') if site_info else None) or P.ModelSetting.get('crawl_selenium_timeout', '20'))
            if selenium_timeout <= 0:
                selenium_timeout = 20
        except Exception:
            selenium_timeout = 20

        use_fs = getattr(scheduler_instance, 'use_flaresolverr', False) if scheduler_instance else P.ModelSetting.get_bool('crawl_use_flaresolverr')
        if site_info and ('USE_FLARESOLVERR' in site_info.get('EXTRA', []) or site_info.get('USE_FLARESOLVERR')):
            use_fs = True

        raw_cf_cookies, cf_user_agent = [], ""
        if use_fs:
            raw_cf_cookies, cf_user_agent = cls.get_flaresolverr_clearance(site_url, scheduler_instance=scheduler_instance)
            if not raw_cf_cookies and not cf_user_agent:
                logger.error(f"[CrawlScraper] [{host}] FlareSolverr 인가 실패로 Selenium 기동 중단")
                return None

        from selenium import webdriver
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--start-maximized')

        target_ua = cf_user_agent or site_info.get('USER_AGENT') or cls._cf_user_agents.get(host) or \
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
        options.add_argument(f"user-agent={target_ua}")
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)

        if proxies and 'http' in proxies:
            options.add_argument(f'--proxy-server={proxies["http"]}')
            logger.info(f"[CrawlScraper] Selenium 프록시 적용: {proxies['http']}")

        try:
            driver = webdriver.Remote(command_executor=remote_url, options=options)
            driver.set_page_load_timeout(selenium_timeout)
        except Exception as e:
            logger.error(f"[CrawlScraper] Remote Selenium 연결 실패 ({remote_url}): {e}")
            return None

        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = { runtime: {} };
                    Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'ko-KR', 'ko', 'en-US', 'en']});
                '''
            })
        except Exception:
            pass

        base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        try:
            driver.get(f"{base_domain}/robots.txt")
        except Exception:
            driver.get(site_url)

        injected_count = 0
        if raw_cf_cookies:
            for c in raw_cf_cookies:
                cookie_dict = {'name': c['name'], 'value': c['value'], 'path': c.get('path', '/')}
                dom = c.get('domain', '')
                if dom:
                    cookie_dict['domain'] = dom.lstrip('.')
                try:
                    driver.add_cookie(cookie_dict)
                    injected_count += 1
                except Exception:
                    try:
                        driver.add_cookie({'name': c['name'], 'value': c['value'], 'path': '/'})
                        injected_count += 1
                    except Exception:
                        pass
            logger.info(f"[CrawlScraper] [{host}] Selenium에 clearance 쿠키 {injected_count}개 주입 완료")

        if site_info and site_info.get('COOKIE'):
            for part in site_info['COOKIE'].split(';'):
                if '=' in part:
                    k, v = part.strip().split('=', 1)
                    k_clean = k.strip()
                    if k_clean in ['cf_clearance', '__cf_bm', '_cfuvid']:
                        continue
                    try:
                        driver.add_cookie({'name': k_clean, 'value': v.strip(), 'path': '/'})
                    except Exception:
                        pass

        cls._selenium_driver = driver
        return driver

    @classmethod
    def get_by_remote_selenium(cls, url: str, wait_tag: str = 'body', scheduler_instance=None, site_info: dict = None) -> str | None:
        driver = cls.init_stealth_selenium(site_info or {}, scheduler_instance=scheduler_instance)
        if not driver:
            return None

        try:
            selenium_timeout = int((site_info.get('SELENIUM_TIMEOUT') if site_info else None) or P.ModelSetting.get('crawl_selenium_timeout', '20'))
            if selenium_timeout <= 0:
                selenium_timeout = 20
        except Exception:
            selenium_timeout = 20

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            logger.debug(f"[CrawlScraper] Selenium 페이지 로드: {url}")
            driver.get(url)

            cls.handle_turnstile_challenge(driver, url, site_info=site_info, scheduler_instance=scheduler_instance)
            driver = cls._selenium_driver or driver

            if site_info:
                try:
                    hook = CrawlCustomManager.get_hook(site_info.get('NAME'))
                    if hook and hasattr(hook, 'on_page_loaded'):
                        hook.on_page_loaded(driver, url)
                except Exception:
                    pass

            effective_wait_tag = wait_tag or 'body'
            if effective_wait_tag != 'body':
                try:
                    WebDriverWait(driver, selenium_timeout).until(EC.presence_of_element_located((By.XPATH, effective_wait_tag)))
                except Exception as wait_ex:
                    curr_title = driver.title or ""
                    if any(k in curr_title for k in ["Just a moment", "Cloudflare", "Attention Required"]):
                        logger.warning(f"[CrawlScraper] 태그 대기 중 Cloudflare 재차단 확인 ('{curr_title}') -> 세션 재초기화: {url}")
                        driver = cls.reinit_session(site_info or {}, scheduler_instance=scheduler_instance)
                        if driver:
                            driver.get(url)
                            WebDriverWait(driver, selenium_timeout).until(EC.presence_of_element_located((By.XPATH, effective_wait_tag)))
                    else:
                        logger.warning(f"[CrawlScraper] 태그 대기 타임아웃 ({effective_wait_tag}): {wait_ex}")

            return driver.page_source
        except Exception as e:
            logger.error(f"[CrawlScraper] Remote Selenium 로딩 에러 ({url}): {e}")
            cls.close_selenium_driver()
            return None

    @classmethod
    def close_selenium_driver(cls):
        if cls._selenium_driver:
            try:
                cls._selenium_driver.quit()
                logger.debug("[CrawlScraper] Selenium 드라이버 정상 종료")
            except Exception:
                pass
            cls._selenium_driver = None

    @classmethod
    def close_sessions(cls):
        cls.close_selenium_driver()
        for host, sess in list(cls._cffi_sessions.items()):
            try:
                sess.close()
            except Exception:
                pass
        cls._cffi_sessions.clear()

        for host, sess in list(cls._requests_sessions.items()):
            try:
                sess.close()
            except Exception:
                pass
        cls._requests_sessions.clear()
        logger.debug("[CrawlScraper] HTTP 활성 세션 정리 완료")

    @classmethod
    def download_file_stream(cls, download_url: str, referer: str = None, scheduler_instance=None):
        proxies = cls.get_proxies(scheduler_instance)
        host = urlparse(download_url).hostname or ''
        headers = cls.DEFAULT_HEADERS.copy()

        if host in cls._cf_user_agents:
            headers['User-Agent'] = cls._cf_user_agents[host]
        if referer:
            headers['Referer'] = referer

        domain_cf = cls._cf_cookies.get(host, {})
        if domain_cf and (time.time() - domain_cf.get('_timestamp', 0) < cls.CF_COOKIE_EXPIRY):
            injected = "; ".join([f"{k}={v}" for k, v in domain_cf.items() if not k.startswith('_')])
            if injected:
                headers['Cookie'] = injected

        session = requests.Session()
        session.headers.update(headers)
        if proxies:
            session.proxies.update(proxies)

        logger.debug(f"[CrawlScraper] 파일 스트림 다운로드: {download_url}")
        res = session.get(download_url, stream=True, timeout=60, verify=False)
        res.raise_for_status()

        byte_io = io.BytesIO()
        for chunk in res.iter_content(chunk_size=4096):
            if chunk:
                byte_io.write(chunk)

        byte_io.seek(0)
        return byte_io


class CrawlTorrentInfo:

    @classmethod
    def get_torrent_info(cls, magnet_list: list[str], scheduler_instance=None) -> list[dict] | None:
        if scheduler_instance is not None:
            if not getattr(scheduler_instance, 'use_torrent_info', False):
                return None
        else:
            if not P.ModelSetting.get_bool('crawl_use_torrent_info'):
                return None

        if not magnet_list:
            return None

        method = P.ModelSetting.get('crawl_torrent_info_method') or 'plugin'
        results = []

        for magnet in magnet_list:
            if not str(magnet).startswith('magnet:'):
                logger.debug(f"[CrawlTorrentInfo] 비토렌트 P2P 링크 메타데이터 분석 건너뜀: {magnet[:50]}")
                continue

            try:
                info = None
                if method == 'qbittorrent':
                    info = cls._get_info_via_qbittorrent(magnet)
                else:
                    info = cls._get_info_via_plugin(magnet)

                if info:
                    results.append(info)
            except Exception as e:
                logger.error(f"[CrawlTorrentInfo] 토렌트 정보 취득 실패 ({magnet}): {e}")

        return results if results else None

    @classmethod
    def _get_info_via_plugin(cls, magnet_uri: str) -> dict | None:
        apikey = get_system_apikey()
        local_port = F.config.get('port', 9999)
        candidate_urls = [
            f"http://127.0.0.1:{local_port}/torrent_info/api/m2i",
            f"http://localhost:{local_port}/torrent_info/api/m2i",
        ]
        ddns = get_ddns()
        if ddns:
            candidate_urls.append(f"{ddns.rstrip('/')}/torrent_info/api/m2i")

        payload = {"apikey": apikey, "uri": magnet_uri}

        for target_url in candidate_urls:
            try:
                res = requests.post(target_url, json=payload, headers={'Content-Type': 'application/json'}, timeout=15)
                if res.status_code == 200:
                    data = res.json()
                    if data and isinstance(data, dict) and data.get('info_hash'):
                        return {
                            'name': data.get('name') or data.get('title', ''),
                            'info_hash': data.get('info_hash', '').lower(),
                            'magnet_uri': magnet_uri,
                            'total_size': data.get('total_size', 0),
                            'files': data.get('files', [])
                        }
            except Exception:
                continue

        logger.warning(f"[CrawlTorrentInfo] m2i API 응답 실패: {magnet_uri}")
        return None

    @classmethod
    def _get_info_via_qbittorrent(cls, magnet_uri: str) -> dict | None:
        qb_url = (P.ModelSetting.get('crawl_qb_url') or '').rstrip('/')
        qb_user = P.ModelSetting.get('crawl_qb_username') or ''
        qb_pass = P.ModelSetting.get('crawl_qb_password') or ''
        temp_category = P.ModelSetting.get('crawl_qb_temp_category') or 'feeder_temp'

        if not qb_url:
            logger.error("[CrawlTorrentInfo] qBittorrent URL 미설정")
            return None

        target_hash = extract_info_hash(magnet_uri)
        if not target_hash:
            logger.warning(f"[CrawlTorrentInfo] 유효하지 않은 마그넷 주소: {magnet_uri}")
            return None

        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': f"{qb_url}/",
            'Origin': qb_url
        })

        try:
            login_res = session.post(
                f"{qb_url}/api/v2/auth/login",
                data={'username': qb_user, 'password': qb_pass},
                timeout=10
            )

            is_login_success = (login_res.status_code in [200, 204]) and ('Fails' not in login_res.text)
            if not is_login_success:
                logger.error(f"[CrawlTorrentInfo] qBittorrent 로그인 실패: HTTP {login_res.status_code}")
                return None

            add_data = {
                'urls': magnet_uri,
                'paused': 'true',
                'category': temp_category,
                'tags': 'feeder_meta',
                'autoTMM': 'false',
            }
            session.post(f"{qb_url}/api/v2/torrents/add", data=add_data, timeout=10)

            resolved_info = None
            for _ in range(15):
                time.sleep(1)
                info_res = session.get(f"{qb_url}/api/v2/torrents/info", params={'hashes': target_hash}, timeout=10)
                if info_res.status_code == 200:
                    torrents = info_res.json()
                    if torrents:
                        t_item = torrents[0]
                        if t_item.get('name') and t_item.get('name').lower() != target_hash:
                            files_res = session.get(f"{qb_url}/api/v2/torrents/files", params={'hash': target_hash}, timeout=10)
                            files_data = files_res.json() if files_res.status_code == 200 else []

                            resolved_info = {
                                'name': t_item['name'],
                                'info_hash': target_hash,
                                'magnet_uri': magnet_uri,
                                'total_size': t_item.get('total_size', 0),
                                'files': [{'name': f.get('name'), 'size': f.get('size')} for f in files_data]
                            }
                            break

            session.post(f"{qb_url}/api/v2/torrents/delete", data={'hashes': target_hash, 'deleteFiles': 'true'}, timeout=10)
            return resolved_info

        except Exception as e:
            logger.error(f"[CrawlTorrentInfo] qBittorrent 연동 예외: {e}")
            try:
                session.post(f"{qb_url}/api/v2/torrents/delete", data={'hashes': target_hash, 'deleteFiles': 'true'}, timeout=5)
            except Exception:
                pass
            return None
