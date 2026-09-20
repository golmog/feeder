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
        logger.info("[Feeder] curl_cffi 모듈 설치 시도...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi"])
        from curl_cffi import requests as cffi_requests
        _CURL_CFFI_AVAILABLE = True
        logger.info("[Feeder] curl_cffi 설치 완료")
    except Exception as e:
        logger.error(f"[Feeder] curl_cffi 설치 실패: {e}")
        _CURL_CFFI_AVAILABLE = False

CONFIG_FILEPATH = os.path.join(path_data, 'db', 'feeder_settings.yaml')
CUSTOM_DIR = os.path.join(path_data, 'db', 'feeder_custom')


# --- 기본 순수 헬퍼 함수 ---

def get_ddns() -> str:
    """FF 시스템 DDNS 주소 조회"""
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
    """FF 시스템 API Key 조회"""
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
    """마그넷 URI(BTIH) 또는 ed2k 링크에서 고유 해시 추출"""
    if not magnet_uri:
        return None

    # 마그넷 btih 해시 추출 (40자리 hex 또는 32자리 base32)
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

    # ed2k 파일 해시 추출 (32자리 hex MD4)
    elif magnet_uri.startswith('ed2k://'):
        match = re.search(r'ed2k://\|file\|[^|]+\|[0-9]+\|([a-fA-F0-9]{32})', magnet_uri, re.IGNORECASE)
        if match:
            return match.group(1).lower()

    return None


def clean_xml_string(value: str) -> str:
    """RSS XML 특수문자 이스케이프"""
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
    """웹 UI 페이지네이션 계산"""
    total_page = math.ceil(count / page_size) if page_size > 0 else 1
    if total_page == 0:
        total_page = 1
    current_page = max(1, min(current_page, total_page))

    page_block = 10
    start_page = ((current_page - 1) // page_block) * page_block + 1
    end_page = min(start_page + page_block - 1, total_page)

    return {
        'prev_page': {'is_possible': start_page > 1, 'page': start_page - 1},
        'next_page': {'is_possible': end_page < total_page, 'page': end_page + 1},
        'start_page': start_page,
        'end_page': end_page,
        'current_page': current_page,
        'total_page': total_page,
        'count': count
    }


# --- 설정 YAML 관리 클래스 ---

class FeedConfigUtil:

    @classmethod
    def get_filepath(cls) -> str:
        return CONFIG_FILEPATH

    @classmethod
    def load_yaml(cls) -> dict:
        if not os.path.exists(CONFIG_FILEPATH):
            default_data = {'SCHEDULE': []}
            cls.save_yaml(default_data)
            return default_data
        try:
            with open(CONFIG_FILEPATH, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            if 'SCHEDULE' not in data or not isinstance(data['SCHEDULE'], list):
                data['SCHEDULE'] = []
            return data
        except Exception as e:
            logger.error(f"[Feeder] YAML 로드 실패: {e}")
            return {'SCHEDULE': []}

    @classmethod
    def save_yaml(cls, data: dict) -> bool:
        try:
            os.makedirs(os.path.dirname(CONFIG_FILEPATH), exist_ok=True)
            with open(CONFIG_FILEPATH, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
            return True
        except Exception as e:
            logger.error(f"[Feeder] YAML 저장 실패: {e}")
            return False

    @classmethod
    def get_schedules(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('SCHEDULE', [])

    @classmethod
    def get_schedule(cls, target_id):
        schedules = cls.get_schedules()
        for s in schedules:
            if str(s.get('id')) == str(target_id):
                return s
        return None

    @classmethod
    def save_schedule(cls, item: dict) -> str:
        data = cls.load_yaml()
        schedules = data.get('SCHEDULE', [])
        target_id = item.get('id')

        if target_id is not None and int(target_id) > 0:
            for idx, s in enumerate(schedules):
                if str(s.get('id')) == str(target_id):
                    schedules[idx].update(item)
                    data['SCHEDULE'] = schedules
                    cls.save_yaml(data)
                    logger.info(f"[Feeder] YAML 스케쥴 수정 완료: ID={target_id}")
                    return 'success_update'
            return 'not_found'

        new_site = item.get('site_name', '')
        new_board = str(item.get('board_id', '')).strip()
        new_subcat = str(item.get('subcat_id', '')).strip()

        # 사이트명, 게시판ID, 서브카테고리ID가 모두 동일할 때만 중복으로 판정
        for s in schedules:
            s_site = s.get('site_name', '')
            s_board = str(s.get('board_id', '')).strip()
            s_subcat = str(s.get('subcat_id', '')).strip()

            if s_site == new_site and s_board == new_board and s_subcat == new_subcat:
                subcat_log = f" (서브카테고리: {new_subcat})" if new_subcat else ""
                logger.warning(f"[Feeder] 동일 게시판 스케쥴 중복: {new_site} - {new_board}{subcat_log}")
                return 'already_exist'

        max_id = max([int(s.get('id', 0)) for s in schedules], default=0)
        item['id'] = max_id + 1
        schedules.append(item)
        data['SCHEDULE'] = schedules
        cls.save_yaml(data)

        subcat_info = f" (서브카테고리: {new_subcat})" if new_subcat else ""
        logger.info(f"[Feeder] YAML 신규 스케쥴 추가 완료: ID={item['id']}, Site={new_site}, Board={new_board}{subcat_info}")
        return 'success'

    @classmethod
    def delete_schedule(cls, target_id) -> bool:
        data = cls.load_yaml()
        schedules = data.get('SCHEDULE', [])
        new_schedules = [s for s in schedules if str(s.get('id')) != str(target_id)]
        data['SCHEDULE'] = new_schedules
        cls.save_yaml(data)
        logger.info(f"[Feeder] YAML 스케쥴 삭제 완료: ID={target_id}")
        return True


# --- 커스텀 스크립트 훅 관리 클래스 ---

class FeedCustomManager:
    _hooks = {}

    @classmethod
    def get_custom_dir(cls) -> str:
        os.makedirs(CUSTOM_DIR, exist_ok=True)
        return CUSTOM_DIR

    @classmethod
    def load_hooks(cls):
        cls._hooks = {}
        custom_dir = cls.get_custom_dir()
        py_files = glob.glob(os.path.join(custom_dir, "*.py"))

        for fpath in py_files:
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
                        logger.info(f"[Feeder] 커스텀 사이트 훅 등록: '{obj.SITE_NAME}' ({fname})")
            except Exception as e:
                logger.error(f"[Feeder] 커스텀 훅 로드 실패 ({fname}): {e}")

        cls.sync_default_site_info()

    @classmethod
    def sync_default_site_info(cls):
        try:
            with F.app.app_context():
                from .model_feed import ModelFeedSite
                for site_key, hook_cls in cls._hooks.items():
                    default_info = getattr(hook_cls, 'DEFAULT_SITE_INFO', None)
                    if default_info and isinstance(default_info, dict):
                        target_name = default_info.get('NAME') or getattr(hook_cls, 'SITE_NAME', '')
                        if not target_name:
                            continue

                        existing = ModelFeedSite.get(name=target_name)
                        if not existing:
                            content_str = json.dumps(default_info, ensure_ascii=False, indent=2)
                            new_site = ModelFeedSite('custom', default_info, content_str)
                            db.session.add(new_site)
                            db.session.commit()
                            logger.info(f"[Feeder] 커스텀 훅 기반 사이트 템플릿 자동 등록: '{target_name}'")
        except Exception as e:
            logger.error(f"[Feeder] sync_default_site_info 에러: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

    @classmethod
    def get_hook(cls, site_name: str):
        if not cls._hooks:
            cls.load_hooks()
        return cls._hooks.get(site_name.lower()) if site_name else None


# --- 4단계 스크래핑 엔진 ---

class FeedScraper:
    _cf_cookies = {}      # { 'domain.com': {'cf_clearance': '...', '_timestamp': 12345} }
    _cf_user_agents = {}  # { 'domain.com': 'FlareSolverr User-Agent 문자열' }
    CF_COOKIE_EXPIRY = 3600
    _selenium_driver = None
    _cffi_sessions = {}
    _requests_sessions = {}

    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    @classmethod
    def _sync_cookies_to_sessions(cls, host: str):
        """FlareSolverr에서 획득한 쿠키와 User-Agent를 활성 HTTP 세션에 동기화"""
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
    def get_proxies(cls, scheduler_instance=None):
        if scheduler_instance:
            use_proxy = getattr(scheduler_instance, 'use_proxy', False)
            proxy_url = getattr(scheduler_instance, 'proxy_url', '')
        else:
            use_proxy = P.ModelSetting.get_bool('feed_use_proxy')
            proxy_url = ''

        if not proxy_url:
            proxy_url = P.ModelSetting.get('feed_proxy_url') or ''

        if use_proxy and proxy_url:
            return {"http": proxy_url, "https": proxy_url}
        return None

    @classmethod
    def _has_valid_clearance(cls, host: str) -> bool:
        """도메인에 유효한 Cloudflare clearance 토큰이 존재하는지 검사"""
        cf_data = cls._cf_cookies.get(host, {})
        if not cf_data:
            return False
        timestamp = cf_data.get('_timestamp', 0)
        has_token = bool(cf_data.get('cf_clearance'))
        return has_token and (time.time() - timestamp < cls.CF_COOKIE_EXPIRY)

    @classmethod
    def _sync_clearance_to_sessions(cls, host: str):
        """FlareSolverr에서 인가받은 쿠키와 User-Agent를 하위 HTTP 세션에 하향 동기화"""
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
    def get_html(cls, url: str, site_info: dict = None, scheduler_instance=None, referer: str = None, max_retries: int = 3, retry_interval: float = 1.5, wait_tag: str = None) -> str | None:
        extra = site_info.get('EXTRA', []) if site_info else []
        cookie = site_info.get('COOKIE') if site_info else None
        proxies = cls.get_proxies(scheduler_instance)
        host = urlparse(url).hostname or ''

        # 옵션 해석 (개별 스케줄 우선 -> 사이트 설정 -> 전역 설정)
        if scheduler_instance:
            use_selenium = getattr(scheduler_instance, 'use_selenium', False)
            use_fs = getattr(scheduler_instance, 'use_flaresolverr', False)
        else:
            use_selenium = P.ModelSetting.get_bool('feed_use_selenium')
            use_fs = P.ModelSetting.get_bool('feed_use_flaresolverr')

        if 'USE_SELENIUM' in extra or (site_info and site_info.get('USE_SELENIUM')):
            use_selenium = True
        if 'USE_FLARESOLVERR' in extra or (site_info and site_info.get('USE_FLARESOLVERR')):
            use_fs = True

        # 1. 원격 Selenium 브라우저 모드
        if use_selenium:
            target_wait_tag = wait_tag or (site_info.get('SELENIUM_WAIT_TAG') if site_info else None) or 'body'
            for attempt in range(1, max_retries + 1):
                res_source = cls.get_by_remote_selenium(url, wait_tag=target_wait_tag, scheduler_instance=scheduler_instance, site_info=site_info)
                if res_source:
                    return res_source
                if attempt < max_retries:
                    logger.debug(f"[Scraper] Selenium 재시도 ({attempt}/{max_retries}): {url}")
                    time.sleep(retry_interval)
            return None

        # 2. HTTP 모드: FlareSolverr 우선순위 체계 (FlareSolverr > curl_cffi > requests)
        # FlareSolverr가 켜져 있고 유효한 clearance가 아직 없다면, 403을 유발하지 않고 FlareSolverr를 최우선 호출하여 인가 획득
        if use_fs and not cls._has_valid_clearance(host):
            logger.info(f"[Scraper] [{host}] Cloudflare 사이트 감지 -> FlareSolverr 최우선 인가 요청: {url}")
            tree, fs_html = cls.get_by_flaresolverr(url, proxies=proxies)
            if fs_html and 'Just a moment...' not in fs_html and 'cf-turnstile' not in fs_html:
                logger.info(f"[Scraper] [{host}] FlareSolverr 인가 및 1차 페이지 수신 완료 -> 하위 세션 동기화")
                return fs_html
            logger.warning(f"[Scraper] [{host}] FlareSolverr 1차 인가 실패, HTTP 세션으로 폴백 시도")

        # 인가받은 User-Agent 및 쿠키 헤더 조립
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

        # 3. 고속 세션(curl_cffi -> requests) 순차 실행
        for attempt in range(1, max_retries + 1):
            source_text = None

            # 3-1. curl_cffi 고속 요청 (동기화된 UA/쿠키 탑재)
            if _CURL_CFFI_AVAILABLE:
                try:
                    cffi_session = cls._cffi_sessions.get(host)
                    if not cffi_session:
                        cffi_session = cffi_requests.Session()
                        cls._cffi_sessions[host] = cffi_session
                        logger.debug(f"[Scraper] curl_cffi 세션 생성: {host}")

                    cls._sync_clearance_to_sessions(host)
                    res = cffi_session.get(url, headers=headers, proxies=proxies, impersonate="chrome", timeout=25)
                    if res.status_code == 200:
                        if 'Just a moment...' not in res.text and 'cf-turnstile' not in res.text:
                            return res.text
                    logger.debug(f"[Scraper] curl_cffi 응답 코드: {res.status_code} ({url})")
                except Exception as e:
                    logger.debug(f"[Scraper] curl_cffi 요청 실패 ({url}): {e}")

            # 3-2. requests 폴백 요청
            if not source_text:
                try:
                    req_session = cls._requests_sessions.get(host)
                    if not req_session:
                        req_session = requests.Session()
                        cls._requests_sessions[host] = req_session
                        logger.debug(f"[Scraper] requests 세션 생성: {host}")

                    cls._sync_clearance_to_sessions(host)
                    res = req_session.get(url, headers=headers, proxies=proxies, timeout=25, verify=False)
                    if res.status_code == 200:
                        res.encoding = res.apparent_encoding or 'utf-8'
                        if 'Just a moment...' not in res.text and 'cf-turnstile' not in res.text:
                            return res.text
                    logger.debug(f"[Scraper] requests 응답 코드: {res.status_code} ({url})")
                except Exception as e:
                    logger.debug(f"[Scraper] requests 요청 실패 ({url}): {e}")

            # 3-3. 세션 실행 중 403 차단 또는 토큰 만료 감지 시 FlareSolverr로 토큰 재발급
            if use_fs:
                logger.warning(f"[Scraper] [{host}] HTTP 세션 차단 감지 -> FlareSolverr 챌린지 재우회 요청: {url}")
                cls._cf_cookies.pop(host, None)
                tree, source = cls.get_by_flaresolverr(url, proxies=proxies)
                if source and 'Just a moment...' not in source and 'cf-turnstile' not in source:
                    return source
                if attempt < max_retries:
                    logger.debug(f"[Scraper] FlareSolverr 재시도 ({attempt}/{max_retries}): {url}")
                    time.sleep(retry_interval)
            else:
                if attempt < max_retries:
                    time.sleep(retry_interval)

        return None

    @classmethod
    def get_by_flaresolverr(cls, url: str, proxies=None):
        fs_url = (P.ModelSetting.get('feed_flaresolverr_url') or '').rstrip('/')
        if not fs_url:
            return None, None

        parsed = urlparse(url)
        host = parsed.hostname or ''
        session_name = f"feeder_{host.replace('.', '_')}"

        # 세션 검사 및 자가치유 (필요 시 세션 파기 후 재성성)
        try:
            list_res = requests.post(f"{fs_url}/v1", json={"cmd": "sessions.list"}, timeout=10)
            sessions = list_res.json().get('sessions', []) if list_res.status_code == 200 else []
            if session_name not in sessions:
                create_payload = {"cmd": "sessions.create", "session": session_name}
                if proxies and 'http' in proxies:
                    create_payload["proxy"] = {"url": proxies['http']}
                requests.post(f"{fs_url}/v1", json=create_payload, timeout=20)
        except Exception as sess_err:
            logger.debug(f"[Scraper] FlareSolverr 세션 준비 예외 (무시): {sess_err}")

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
                        logger.debug(f"[Scraper] FlareSolverr User-Agent 인가 동기화 ({host}): {ua}")

                    sol_cookies = solution.get('cookies') or []
                    if sol_cookies:
                        if host not in cls._cf_cookies:
                            cls._cf_cookies[host] = {}
                        for c in sol_cookies:
                            cls._cf_cookies[host][c['name']] = c['value']
                        cls._cf_cookies[host]['_timestamp'] = time.time()
                        logger.info(f"[Scraper] [{host}] FlareSolverr 쿠키 {len(sol_cookies)}개 동기화 완료")

                    cls._sync_clearance_to_sessions(host)
                    return tree, html_source

                else:
                    logger.warning(f"[Scraper] FlareSolverr 오류 응답: {data.get('message')}")
        except Exception as e:
            logger.warning(f"[Scraper] FlareSolverr 통신 실패 ({url}): {e}")
            # 먹통 세션 방지를 위한 세션 파기
            try:
                requests.post(f"{fs_url}/v1", json={"cmd": "sessions.destroy", "session": session_name}, timeout=5)
                logger.debug(f"[Scraper] FlareSolverr 불안정 세션 초기화: {session_name}")
            except Exception:
                pass

        return None, None

    @classmethod
    def get_by_remote_selenium(cls, url: str, wait_tag: str = 'body', scheduler_instance=None, site_info: dict = None) -> str | None:
        remote_url = (P.ModelSetting.get('feed_selenium_remote_url') or '').strip()
        if not remote_url:
            logger.error("[Scraper] Selenium remote URL이 설정되지 않았습니다.")
            return None

        parsed_url = urlparse(url)
        host = parsed_url.hostname or ''
        proxies = cls.get_proxies(scheduler_instance)

        try:
            selenium_timeout = int((site_info.get('SELENIUM_TIMEOUT') if site_info else None) or P.ModelSetting.get('feed_selenium_timeout', '20'))
            if selenium_timeout <= 0:
                selenium_timeout = 20
        except Exception:
            selenium_timeout = 20

        driver = cls._selenium_driver
        if driver:
            try:
                _ = driver.current_url
            except Exception:
                driver = None
                cls._selenium_driver = None

        if not driver:
            try:
                from selenium import webdriver

                use_fs = getattr(scheduler_instance, 'use_flaresolverr', False) if scheduler_instance else P.ModelSetting.get_bool('feed_use_flaresolverr')
                if site_info and ('USE_FLARESOLVERR' in site_info.get('EXTRA', []) or site_info.get('USE_FLARESOLVERR')):
                    use_fs = True

                # Selenium 기동 전 FlareSolverr clearance 토큰 사전 취득
                if use_fs and not cls._has_valid_clearance(host):
                    logger.info(f"[Scraper] Selenium 기동 전 FlareSolverr 토큰 사전 인가: {url}")
                    cls.get_by_flaresolverr(url, proxies=proxies)

                options = webdriver.ChromeOptions()
                options.add_argument('--headless=new')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--start-maximized')

                target_ua = (site_info.get('USER_AGENT') if site_info else None) or \
                            cls._cf_user_agents.get(host) or \
                            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
                options.add_argument(f"user-agent={target_ua}")
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option('excludeSwitches', ['enable-automation'])
                options.add_experimental_option('useAutomationExtension', False)

                if proxies and 'http' in proxies:
                    options.add_argument(f'--proxy-server={proxies["http"]}')
                    logger.debug(f"[Scraper] Selenium 프록시: {proxies['http']}")

                driver = webdriver.Remote(command_executor=remote_url, options=options)
                driver.set_page_load_timeout(selenium_timeout)

                try:
                    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                        'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
                    })
                except Exception:
                    pass

                # site_info 쿠키 및 FlareSolverr cf_cookies 통합 주입
                all_cookies = {}
                if host in cls._cf_cookies:
                    for k, v in cls._cf_cookies[host].items():
                        if not k.startswith('_'):
                            all_cookies[k] = v
                if site_info and site_info.get('COOKIE'):
                    for part in site_info['COOKIE'].split(';'):
                        if '=' in part:
                            k, v = part.strip().split('=', 1)
                            all_cookies[k.strip()] = v.strip()

                if all_cookies:
                    base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    try:
                        driver.get(f"{base_domain}/404notfound")
                        for c_name, c_val in all_cookies.items():
                            driver.add_cookie({'name': c_name, 'value': c_val, 'path': '/'})
                        logger.debug(f"[Scraper] Selenium에 세션 쿠키 {len(all_cookies)}개 주입 완료")
                    except Exception as cookie_err:
                        logger.debug(f"[Scraper] Selenium 쿠키 주입 예외: {cookie_err}")

                cls._selenium_driver = driver
            except Exception as e:
                logger.error(f"[Scraper] Remote Selenium 생성 실패 ({url}): {e}")
                return None

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            logger.debug(f"[Scraper] Selenium 페이지 로드: {url}")
            driver.get(url)

            if site_info:
                try:
                    hook = FeedCustomManager.get_hook(site_info.get('NAME'))
                    if hook and hasattr(hook, 'on_page_loaded'):
                        hook.on_page_loaded(driver, url)
                except Exception:
                    pass

            effective_wait_tag = wait_tag or 'body'
            if effective_wait_tag != 'body':
                try:
                    WebDriverWait(driver, selenium_timeout).until(EC.presence_of_element_located((By.XPATH, effective_wait_tag)))
                except Exception as wait_ex:
                    logger.warning(f"[Scraper] 태그 대기 타임아웃 ({effective_wait_tag}): {wait_ex}")
                    try:
                        debug_path = os.path.join(get_tmp_dir(), 'debug_selenium_failed.png')
                        driver.save_screenshot(debug_path)
                        logger.info(f"[Scraper] 실패 스크린샷 저장: {debug_path}")
                    except Exception:
                        pass

            return driver.page_source
        except Exception as e:
            logger.error(f"[Scraper] Remote Selenium 로딩 에러 ({url}): {e}")
            cls.close_selenium_driver()
            return None

    @classmethod
    def close_selenium_driver(cls):
        if cls._selenium_driver:
            try:
                cls._selenium_driver.quit()
                logger.debug("[Scraper] Selenium 드라이버 정상 종료")
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
        logger.debug("[Scraper] HTTP 활성 세션 정리 완료")

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

        logger.debug(f"[Scraper] 파일 스트림 다운로드: {download_url}")
        res = session.get(download_url, stream=True, timeout=60, verify=False)
        res.raise_for_status()

        byte_io = io.BytesIO()
        for chunk in res.iter_content(chunk_size=4096):
            if chunk:
                byte_io.write(chunk)

        byte_io.seek(0)
        return byte_io


# --- 토렌트 메타데이터 취득 클래스 ---

class FeedTorrentInfo:

    @classmethod
    def get_torrent_info(cls, magnet_list: list[str], scheduler_instance=None) -> list[dict] | None:
        if scheduler_instance is not None:
            if not getattr(scheduler_instance, 'use_torrent_info', False):
                return None
        else:
            if not P.ModelSetting.get_bool('feed_use_torrent_info'):
                return None

        if not magnet_list:
            return None

        method = P.ModelSetting.get('feed_torrent_info_method') or 'plugin'
        results = []

        for magnet in magnet_list:
            # 토렌트/마그넷 프로토콜이 아닌 ed2k 등의 링크는 메타데이터 취득 대상에서 제외
            if not str(magnet).startswith('magnet:'):
                logger.debug(f"[TorrentInfo] 토렌트 메타정보 취득 대상이 아닌 P2P 링크 건너뜀: {magnet[:50]}")
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
                logger.error(f"[TorrentInfo] 토렌트 정보 취득 실패 ({magnet}): {e}")

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

        logger.warning(f"[TorrentInfo] m2i API 응답 실패: {magnet_uri}")
        return None

    @classmethod
    def _get_info_via_qbittorrent(cls, magnet_uri: str) -> dict | None:
        qb_url = (P.ModelSetting.get('feed_qb_url') or '').rstrip('/')
        qb_user = P.ModelSetting.get('feed_qb_username') or ''
        qb_pass = P.ModelSetting.get('feed_qb_password') or ''
        temp_category = P.ModelSetting.get('feed_qb_temp_category') or 'feeder_temp'

        if not qb_url:
            logger.error("[TorrentInfo] qBittorrent URL 미설정")
            return None

        target_hash = extract_info_hash(magnet_uri)
        if not target_hash:
            logger.warning(f"[TorrentInfo] 유효하지 않은 마그넷 주소: {magnet_uri}")
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
                logger.error(f"[TorrentInfo] qBittorrent 로그인 실패: HTTP {login_res.status_code}")
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
            logger.error(f"[TorrentInfo] qBittorrent 연동 예외: {e}")
            try:
                session.post(f"{qb_url}/api/v2/torrents/delete", data={'hashes': target_hash, 'deleteFiles': 'true'}, timeout=5)
            except Exception:
                pass
            return None
