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
            default_data = {
                'GLOBAL': {
                    'regexp': {
                        'reject': [
                            {'\\btrailer\\b': {'from': 'title'}},
                            {'\\bWEBSCR\\b': {'from': 'title'}},
                            {'\\bTS\\b': {'from': 'title'}},
                            {'\\bCam\\b': {'from': 'title'}}
                        ]
                    }
                },
                'CRAWLERS': [],
                'FEEDS': []
            }

            default_data['DOWNLOADERS'] = []
            default_data['DOWNLOAD_PROFILES'] = []
            default_data['GDRIVE_ACCOUNTS'] = []
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

            from .task_feed import Task
            for c in data['CRAWLERS']:
                if 'boards' not in c or not isinstance(c['boards'], list):
                    c['boards'] = []
                for b in c['boards']:
                    b_val = b.get('board', '')
                    s_val = b.get('subcat', '')
                    _, _, f_key = Task.parse_board_info(b_val, s_val)
                    b['full_board_key'] = f_key

            for f in data['FEEDS']:
                if 'sources' not in f or not isinstance(f['sources'], list):
                    f['sources'] = []
                for src in f['sources']:
                    b_val = src.get('board', '')
                    s_val = src.get('subcat', '')
                    _, _, f_key = Task.parse_board_info(b_val, s_val)
                    src['full_board_key'] = f_key

            return data
        except Exception as e:
            logger.error(f"[Feeder] YAML 로드 실패: {e}")
            return {'GLOBAL': {}, 'CRAWLERS': [], 'FEEDS': [], 'DOWNLOADERS': [], 'DOWNLOAD_PROFILES': [], 'GDRIVE_ACCOUNTS': []}

    # 다운로더 인스턴스 (DOWNLOADERS) 관리
    @classmethod
    def get_downloaders(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('DOWNLOADERS', [])

    @classmethod
    def get_downloader_by_name(cls, name: str) -> dict | None:
        target = str(name).strip().lower()
        for d in cls.get_downloaders():
            if str(d.get('name', '')).strip().lower() == target:
                return d
        return None

    @classmethod
    def save_downloader(cls, item: dict) -> str:
        data = cls.load_yaml()
        items = data.get('DOWNLOADERS', [])
        target_name = item.get('name', '').strip()
        if not target_name:
            return 'empty_name'

        for idx, d in enumerate(items):
            if str(d.get('name', '')).strip().lower() == target_name.lower():
                items[idx].update(item)
                data['DOWNLOADERS'] = items
                cls.save_yaml(data)
                logger.info(f"[FeederConfig] 다운로더 설정 갱신: {target_name}")
                return 'success_update'

        items.append(item)
        data['DOWNLOADERS'] = items
        cls.save_yaml(data)
        logger.info(f"[FeederConfig] 신규 다운로더 추가: {target_name}")
        return 'success'

    @classmethod
    def delete_downloader(cls, name: str) -> bool:
        data = cls.load_yaml()
        items = data.get('DOWNLOADERS', [])
        target = str(name).strip().lower()
        data['DOWNLOADERS'] = [d for d in items if str(d.get('name', '')).strip().lower() != target]
        cls.save_yaml(data)
        logger.info(f"[FeederConfig] 다운로더 삭제: {name}")
        return True

    # 다운로드 라우팅 프로필 (DOWNLOAD_PROFILES) 관리
    @classmethod
    def get_download_profiles(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('DOWNLOAD_PROFILES', [])

    @classmethod
    def get_download_profile_by_feed(cls, feed_name: str) -> dict | None:
        target = str(feed_name).strip().lower()
        for p in cls.get_download_profiles():
            feeds = [str(f).strip().lower() for f in p.get('feeds', [])]
            if target in feeds or '*' in feeds:
                return p
        return None

    @classmethod
    def save_download_profile(cls, item: dict) -> str:
        data = cls.load_yaml()
        profiles = data.get('DOWNLOAD_PROFILES', [])
        name = item.get('name', '').strip()
        if not name:
            return 'empty_name'

        for idx, p in enumerate(profiles):
            if str(p.get('name', '')).strip().lower() == name.lower():
                profiles[idx].update(item)
                data['DOWNLOAD_PROFILES'] = profiles
                cls.save_yaml(data)
                logger.info(f"[FeederConfig] 다운로드 프로필 갱신: {name}")
                return 'success_update'

        profiles.append(item)
        data['DOWNLOAD_PROFILES'] = profiles
        cls.save_yaml(data)
        logger.info(f"[FeederConfig] 신규 다운로드 프로필 추가: {name}")
        return 'success'

    # 구글 드라이브 계정 풀 (GDRIVE_ACCOUNTS) 관리
    @classmethod
    def get_gdrive_accounts(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('GDRIVE_ACCOUNTS', [])

    @classmethod
    def save_gdrive_accounts(cls, accounts: list[dict]) -> bool:
        data = cls.load_yaml()
        data['GDRIVE_ACCOUNTS'] = accounts
        return cls.save_yaml(data)

    @classmethod
    def get_global(cls) -> dict:
        data = cls.load_yaml()
        return data.get('GLOBAL', {})

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
            logger.error(f"[Feeder] YAML 저장 실패: {e}")
            return False

    # 수집기 (CRAWLERS) 관리 메서드
    @classmethod
    def get_crawlers(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('CRAWLERS', [])

    @classmethod
    def get_crawler(cls, crawler_id):
        crawlers = cls.get_crawlers()
        for c in crawlers:
            if str(c.get('id')) == str(crawler_id):
                return c
        return None

    @classmethod
    def get_crawler_by_board(cls, site_name: str, board_id: str, subcat_id: str = None):
        """사이트명과 게시판/서브카테고리로 해당 게시판이 속한 수집기 검색"""
        from .task_feed import Task
        _, _, full_key = Task.parse_board_info(board_id, subcat_id)
        crawlers = cls.get_crawlers()
        for c in crawlers:
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

        from .task_feed import Task
        boards = item.get('boards', [])
        normalized_boards = []
        for b in boards:
            b_val = b.get('board', '')
            s_val = b.get('subcat', '')
            _, _, f_key = Task.parse_board_info(b_val, s_val)
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
                    logger.info(f"[Feeder] 수집기 수정 완료: ID={target_id}, Site={item.get('site')}, Boards={len(normalized_boards)}개")
                    return 'success_update'
            return 'not_found'

        # 동일 사이트 수집기 중복 방지 (1 사이트 = 1 크롤러 권장)
        for c in crawlers:
            if c.get('site') == item.get('site'):
                logger.warning(f"[Feeder] 동일 사이트 수집기 이미 존재: {item.get('site')}")
                return 'already_exist'

        max_id = max([int(c.get('id', 0)) for c in crawlers], default=0)
        item['id'] = max_id + 1
        crawlers.append(item)
        data['CRAWLERS'] = crawlers
        cls.save_yaml(data)
        logger.info(f"[Feeder] 신규 수집기 추가 완료: ID={item['id']}, Site={item.get('site')}, Boards={len(normalized_boards)}개")
        return 'success'

    @classmethod
    def delete_crawler(cls, target_id) -> bool:
        data = cls.load_yaml()
        crawlers = data.get('CRAWLERS', [])
        new_crawlers = [c for c in crawlers if str(c.get('id')) != str(target_id)]
        data['CRAWLERS'] = new_crawlers
        cls.save_yaml(data)
        logger.info(f"[Feeder] 수집기 삭제 완료: ID={target_id}")
        return True

    # 피드 발행기 (FEEDS) 관리 메서드
    @classmethod
    def get_feeds(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('FEEDS', [])

    @classmethod
    def get_feed(cls, feed_id):
        feeds = cls.get_feeds()
        for f in feeds:
            if str(f.get('id')) == str(feed_id):
                return f
        return None

    @classmethod
    def get_feed_by_name(cls, feed_name: str):
        feeds = cls.get_feeds()
        target_name = str(feed_name).strip().lower()
        for f in feeds:
            if str(f.get('name', '')).strip().lower() == target_name:
                return f
        return None

    @classmethod
    def save_feed(cls, item: dict) -> str:
        data = cls.load_yaml()
        feeds = data.get('FEEDS', [])
        target_id = item.get('id')

        from .task_feed import Task
        sources = item.get('sources', [])
        normalized_sources = []
        for src in sources:
            b_val = src.get('board', '')
            s_val = src.get('subcat', '')
            _, _, f_key = Task.parse_board_info(b_val, s_val)
            normalized_sources.append({
                'site': src.get('site', ''),
                'board': str(b_val),
                'subcat': str(s_val) if s_val else '',
                'full_board_key': f_key
            })
        item['sources'] = normalized_sources

        if target_id is not None and int(target_id) > 0:
            for idx, f in enumerate(feeds):
                if str(f.get('id')) == str(target_id):
                    feeds[idx].update(item)
                    data['FEEDS'] = feeds
                    cls.save_yaml(data)
                    logger.info(f"[Feeder] 피드 수정 완료: ID={target_id}, Name={item.get('name')}")
                    return 'success_update'
            return 'not_found'

        target_name = str(item.get('name', '')).strip().lower()
        for f in feeds:
            if str(f.get('name', '')).strip().lower() == target_name:
                logger.warning(f"[Feeder] 동일 이름의 피드 중복: {item.get('name')}")
                return 'already_exist'

        max_id = max([int(f.get('id', 0)) for f in feeds], default=0)
        item['id'] = max_id + 1
        feeds.append(item)
        data['FEEDS'] = feeds
        cls.save_yaml(data)
        logger.info(f"[Feeder] 신규 피드 추가 완료: ID={item['id']}, Name={item.get('name')}, Sources={len(normalized_sources)}개")
        return 'success'

    @classmethod
    def delete_feed(cls, target_id) -> bool:
        data = cls.load_yaml()
        feeds = data.get('FEEDS', [])
        new_feeds = [f for f in feeds if str(f.get('id')) != str(target_id)]
        data['FEEDS'] = new_feeds
        cls.save_yaml(data)
        logger.info(f"[Feeder] 피드 삭제 완료: ID={target_id}")
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
        """레거시 및 외부 호출 호환용 별칭 (동일 로직 호출)"""
        cls._sync_clearance_to_sessions(host)

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
                logger.warning(f"[Scraper] [{host}] Cloudflare 차단 감지 -> 기존 소켓 세션 완전 파기 및 FlareSolverr 재인가 시작: {url}")
                cls._cf_cookies.pop(host, None)

                # 차단 플래그가 꽂힌 기존 HTTP 연결 풀 강제 종료 및 파기
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
                    logger.info(f"[Scraper] [{host}] 전역 세션 자가 치유 완료 -> 페이지 수신 성공")
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


# --- Flexget 스타일 정규식 필터링 엔진 ---

class FeedFilter:

    RESOLUTION_MAP = {
        '4320p': 4320, '8k': 4320,
        '2160p': 2160, '4k': 2160, 'uhd': 2160,
        '1080p': 1080, '1080i': 1080, 'fhd': 1080,
        '720p': 720, 'hd': 720,
        '576p': 576, '480p': 480, 'sd': 480,
        '360p': 360,
    }

    @classmethod
    def detect_resolution(cls, item: dict) -> int | None:
        """게시글 제목, 파일명, 마그넷 명칭 등에서 해상도(숫자 높이) 추출"""
        texts = [item.get('title', '')]
        if item.get('download'):
            for d in item['download']:
                if isinstance(d, dict) and d.get('filename'):
                    texts.append(d['filename'])

        combined_text = " ".join([t for t in texts if t])
        if not combined_text:
            return None

        # 표준 태그 검사 (해상도 높은 순서대로 검사하여 정확도 확보)
        if re.search(r'\b(4320p|8k)\b', combined_text, re.IGNORECASE):
            return 4320
        if re.search(r'\b(2160p|4k|uhd)\b', combined_text, re.IGNORECASE):
            return 2160
        if re.search(r'\b(1080p|1080i|fhd)\b', combined_text, re.IGNORECASE):
            return 1080
        if re.search(r'\b(720p)\b', combined_text, re.IGNORECASE):
            return 720
        if re.search(r'\b(576p|480p|sd)\b', combined_text, re.IGNORECASE):
            return 480
        if re.search(r'\b(360p)\b', combined_text, re.IGNORECASE):
            return 360

        # WxH 형식의 해상도 표기 검사 (예: 3840x2160, 1920x1080, 1280x720)
        m = re.search(r'\b\d{3,4}x(?P<res>\d{3,4})\b', combined_text, re.IGNORECASE)
        if m:
            res_val = int(m.group('res'))
            if res_val >= 2100: return 2160
            if res_val >= 1050: return 1080
            if res_val >= 700: return 720
            if res_val >= 450: return 480

        return None

    @classmethod
    def matches_quality(cls, item_res: int | None, requirement: str) -> bool:
        """단일 품질 표현식(2160p+, >=1080p, 720p-1080p 등) 일치 여부 평가"""
        if item_res is None:
            return False

        req = str(requirement).strip().lower()

        # Plus 연산자 처리 (예: 2160p+, 1080p+)
        if req.endswith('+'):
            base_res = cls.RESOLUTION_MAP.get(req[:-1].strip())
            if base_res:
                return item_res >= base_res

        # 부등호 비교 연산자 처리 (예: >=1080p, <=720p, >1080p, <2160p)
        m_op = re.match(r'^(>=|<=|>|<)(.+)$', req)
        if m_op:
            op, target = m_op.group(1), m_op.group(2).strip()
            base_res = cls.RESOLUTION_MAP.get(target)
            if base_res:
                if op == '>=': return item_res >= base_res
                if op == '<=': return item_res <= base_res
                if op == '>': return item_res > base_res
                if op == '<': return item_res < base_res

        # 범위 연산자 처리 (예: 720p-1080p)
        if '-' in req:
            parts = req.split('-', 1)
            r_min = cls.RESOLUTION_MAP.get(parts[0].strip())
            r_max = cls.RESOLUTION_MAP.get(parts[1].strip())
            if r_min and r_max:
                return min(r_min, r_max) <= item_res <= max(r_min, r_max)

        # 단일 고정 해상도 일치 처리 (예: 1080p, 2160p, 4k)
        base_res = cls.RESOLUTION_MAP.get(req)
        if base_res:
            return item_res == base_res

        return False

    @classmethod
    def parse_rule(cls, rule_item) -> tuple[re.Pattern | None, list[str]]:
        """단순 문자열, 단일 키 딕셔너리, {from: [title, link]} 형태 파싱"""
        pattern = None
        raw_from = 'title'

        if isinstance(rule_item, str):
            pattern = rule_item.strip()
        elif isinstance(rule_item, dict):
            if 'pattern' in rule_item or 'regexp' in rule_item:
                pattern = rule_item.get('pattern') or rule_item.get('regexp')
                raw_from = rule_item.get('from', 'title')
            else:
                pattern = list(rule_item.keys())[0]
                val = rule_item[pattern]
                raw_from = val.get('from', 'title') if isinstance(val, dict) else 'title'
        else:
            return None, []

        if not pattern:
            return None, []

        fields = [str(f).strip().lower() for f in raw_from] if isinstance(raw_from, list) else [str(raw_from).strip().lower()]

        try:
            compiled = re.compile(str(pattern), re.IGNORECASE)
            return compiled, fields
        except re.error as e:
            logger.warning(f"[FeedFilter] 올바르지 않은 정규식 패턴 '{pattern}': {e}")
            return None, []

    @classmethod
    def get_field_values(cls, item: dict, field_name: str) -> list[str]:
        values = []
        target = field_name.lower()

        if target == 'title':
            if item.get('title'):
                values.append(str(item['title']))
        elif target in ['link', 'url']:
            if item.get('url'):
                values.append(str(item['url']))
            if item.get('magnet'):
                mags = item['magnet'] if isinstance(item['magnet'], list) else [item['magnet']]
                values.extend([str(m) for m in mags if m])
            if item.get('download'):
                downs = item['download'] if isinstance(item['download'], list) else [item['download']]
                for d in downs:
                    if isinstance(d, dict) and d.get('link'):
                        values.append(str(d['link']))
                    elif isinstance(d, (list, tuple)) and len(d) > 0:
                        values.append(str(d[0]))
        return values

    @classmethod
    def check_match(cls, item: dict, regex: re.Pattern, fields: list[str]) -> bool:
        for f in fields:
            vals = cls.get_field_values(item, f)
            for v in vals:
                if regex.search(v):
                    return True
        return False

    @classmethod
    def evaluate(cls, item: dict, feed_cfg: dict = None, global_cfg: dict = None) -> tuple[bool, str]:
        """
        Flexget 필터 체인 평가 (정규식 필터 및 화질 필터 통합)
        반환: (is_accepted: bool, reason: str)
        """
        feed_dict = feed_cfg if isinstance(feed_cfg, dict) else (vars(feed_cfg) if feed_cfg else {})
        glob_dict = global_cfg if global_cfg is not None else FeedConfigUtil.get_global()

        # GLOBAL reject
        glob_regexp = glob_dict.get('regexp', {}) if isinstance(glob_dict, dict) else {}
        for r in (glob_regexp.get('reject') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return False, f"GLOBAL reject: '{comp.pattern}'"

        # FEED reject
        feed_regexp = feed_dict.get('regexp', {}) if isinstance(feed_dict.get('regexp'), dict) else {}
        for r in (feed_regexp.get('reject') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return False, f"FEED reject: '{comp.pattern}'"

        # 화질(Quality) 필터 평가 (피드 개별 설정 우선 적용 -> 전역 설정 적용)
        target_quality = feed_dict.get('quality')
        if target_quality is None and isinstance(glob_dict, dict):
            target_quality = glob_dict.get('quality')

        if target_quality:
            req_list = target_quality if isinstance(target_quality, list) else [target_quality]
            item_res = cls.detect_resolution(item)
            if item_res is None:
                return False, f"화질(해상도) 식별 불가 (요구조건: {target_quality})"
            matched_quality = any(cls.matches_quality(item_res, req) for req in req_list)
            if not matched_quality:
                return False, f"화질 조건 불일치 ({item_res}p != {target_quality})"

        # GLOBAL reject_excluding
        glob_re_ex = glob_regexp.get('reject_excluding') or []
        if glob_re_ex:
            matched_any = False
            for r in glob_re_ex:
                comp, fields = cls.parse_rule(r)
                if comp and cls.check_match(item, comp, fields):
                    matched_any = True
                    break
            if not matched_any:
                return False, "GLOBAL reject_excluding 조건 불일치"

        # FEED reject_excluding
        feed_re_ex = feed_regexp.get('reject_excluding') or []
        if feed_re_ex:
            matched_any = False
            for r in feed_re_ex:
                comp, fields = cls.parse_rule(r)
                if comp and cls.check_match(item, comp, fields):
                    matched_any = True
                    break
            if not matched_any:
                return False, "FEED reject_excluding 조건 불일치"

        # GLOBAL accept
        for r in (glob_regexp.get('accept') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return True, f"GLOBAL accept: '{comp.pattern}'"

        # FEED accept
        for r in (feed_regexp.get('accept') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return True, f"FEED accept: '{comp.pattern}'"

        # accept_all 검사
        accept_all_flag = feed_dict.get('accept_all')
        if accept_all_flag is None and isinstance(glob_dict, dict):
            accept_all_flag = glob_dict.get('accept_all')
        if str(accept_all_flag).lower() in ['true', 'yes', '1', 'on']:
            return True, "accept_all 허용"

        # 명시적인 accept(화이트리스트) 규칙이 선언되어 있는 경우에만 미일치 항목 탈락
        has_accept_rules = bool(glob_regexp.get('accept') or feed_regexp.get('accept'))
        if has_accept_rules:
            return False, "accept 조건 미충족"

        # reject 및 reject_excluding을 모두 무사히 통과한 항목은 정상 허용
        return True, "기본 허용 (미거부 항목)"


# --- 외부 공유용 RSS XML 파일 관리 클래스 ---

class FeedRssFileWriter:

    @classmethod
    def save_rss_file(cls, feed_cfg: dict) -> bool:
        """피드(FEEDS) 설정을 바탕으로 DB에서 소스 게시판들을 조회/필터링하여 공유용 XML 파일 생성"""
        try:
            feed = feed_cfg if isinstance(feed_cfg, dict) else (vars(feed_cfg) if feed_cfg else {})

            global_make = P.ModelSetting.get_bool('feed_make_rss_file') if P.ModelSetting else False
            feed_use = str(feed.get('use_rss_file', False)).lower() in ['true', 'on', '1']
            if not (global_make and feed_use):
                return False

            save_dir = feed.get('rss_file_path') or (P.ModelSetting.get('feed_rss_file_path') if P.ModelSetting else '')
            save_dir = save_dir.strip() if save_dir else ''
            if not save_dir:
                logger.warning(f"[FeedRssFile] [{feed.get('name')}] RSS 파일 저장 경로가 설정되지 않아 건너뜁니다.")
                return False

            default_filename = f"{feed.get('name', 'feed')}.xml"
            filename = (feed.get('rss_file') or default_filename).strip()
            if not filename.lower().endswith('.xml'):
                filename += '.xml'

            try:
                days_val = int(feed.get('rss_file_days') or P.ModelSetting.get('feed_rss_file_days') or 14)
            except Exception:
                days_val = 14

            try:
                items_val = int(feed.get('rss_file_items') or P.ModelSetting.get('feed_rss_file_items') or P.ModelSetting.get('feed_feed_count') or 100)
            except Exception:
                items_val = 100

            sources = feed.get('sources', [])
            if not sources:
                return False

            from .model_feed import ModelFeedBbs
            from sqlalchemy import and_, or_
            source_conditions = []
            for src in sources:
                s_name = src.get('site')
                b_name = src.get('full_board_key') or src.get('board')
                if s_name and b_name:
                    source_conditions.append(and_(ModelFeedBbs.site == s_name, ModelFeedBbs.board == b_name))

            if not source_conditions:
                return False

            query = db.session.query(ModelFeedBbs).filter(or_(*source_conditions))
            if days_val > 0:
                limit_date = datetime.now() - timedelta(days=days_val)
                query = query.filter(ModelFeedBbs.created_time >= limit_date)

            query_limit = max(items_val * 4, 400)
            candidates = query.order_by(ModelFeedBbs.id.desc()).limit(query_limit).all()

            # 마그넷 중복 제거 및 피드 조건 필터링
            global_cfg = FeedConfigUtil.get_global()
            seen_magnets = set()
            filtered_records = []
            for bbs in candidates:
                bbs_dict = bbs.as_dict()

                # 복수 마그넷 해시 중복 검사 (XML 파일 생성 시 완벽 단일화)
                item_hashes = []
                for m in bbs_dict.get('magnet', []):
                    h = extract_info_hash(m) or m
                    if h:
                        item_hashes.append(h)

                if item_hashes and any(h in seen_magnets for h in item_hashes):
                    continue

                is_pass, _ = FeedFilter.evaluate(bbs_dict, feed, global_cfg)
                if is_pass:
                    for h in item_hashes:
                        seen_magnets.add(h)
                    filtered_records.append(bbs)
                    if len(filtered_records) >= items_val:
                        break

            feed_mod = P.get_module('feed')
            if not feed_mod:
                return False

            feed_title = f"{feed.get('name', 'Feeder')} (Shared Feed)"
            xml_content = feed_mod.generate_rss_feed(feed_title, filtered_records, include_apikey=False)

            os.makedirs(save_dir, exist_ok=True)
            target_filepath = os.path.join(save_dir, filename)
            with open(target_filepath, 'w', encoding='utf-8') as f:
                f.write(xml_content)

            logger.info(f"[FeedRssFile] 피드 공유용 RSS 파일 생성 완료: {target_filepath} (필터 통과: {len(filtered_records)}/{items_val}개)")
            return True
        except Exception as e:
            logger.error(f"[FeedRssFile] 피드 RSS 파일 생성 실패 ({feed_cfg.get('name')}): {e}")
            return False

    @classmethod
    def save_all_rss_files(cls) -> int:
        """등록된 모든 FEEDS 중 파일 생성이 활성화된 피드들의 XML 파일을 일괄 갱신"""
        count = 0
        for f in FeedConfigUtil.get_feeds():
            if str(f.get('use_rss_file', False)).lower() in ['true', 'on', '1']:
                if cls.save_rss_file(f):
                    count += 1
        return count
