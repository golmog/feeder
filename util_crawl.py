# -*- coding: utf-8 -*-
import io
import os
import re
import sys
import glob
import time
import requests
import subprocess
import importlib.util
from urllib.parse import urlparse
from html import unescape
from lxml import html

import json

from .setup import *
from .model_crawl import ModelCrawlSite
from .util_base import FeederUtil

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    _SELENIUM_AVAILABLE = True
except ImportError:
    _SELENIUM_AVAILABLE = False

    webdriver = None
    By = None
    WebDriverWait = None
    EC = None

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


class CrawlUtil:
    """크롤링 스크래핑 엔진, 커스텀 훅 및 토렌트 메타데이터 통합 관리자"""

    _hooks = {}
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

    # --------------------------------------------------------------------------
    # 커스텀 훅 스크립트 관리
    # --------------------------------------------------------------------------
    @classmethod
    def load_hooks(cls):
        """커스텀 사이트 훅 스크립트 동적 로드"""
        cls._hooks = {}
        custom_dir = FeederUtil.SITES_DIR
        candidate_files = glob.glob(os.path.join(custom_dir, "*.py"))
        candidate_files.extend(glob.glob(os.path.join(FeederUtil.CUSTOM_DIR, "site_*.py")))

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
                        logger.info(f"[CrawlUtil] 커스텀 사이트 훅 등록: '{obj.SITE_NAME}' ({fname})")
            except Exception as e:
                logger.error(f"[CrawlUtil] 커스텀 훅 로드 실패 ({fname}): {e}")

        cls.sync_default_site_info()

    @classmethod
    def sync_default_site_info(cls):
        """커스텀 훅 기본 템플릿 DB 동기화"""
        try:
            with F.app.app_context():
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
                            logger.info(f"[CrawlUtil] 사이트 템플릿 신규 등록: '{target_name}'")
                        else:
                            existing.info = default_info
                            existing.content = content_str

                        db.session.commit()
        except Exception as e:
            logger.error(f"[CrawlUtil] sync_default_site_info 에러: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

    @classmethod
    def get_hook(cls, site_name: str):
        if not cls._hooks:
            cls.load_hooks()
        return cls._hooks.get(site_name.lower()) if site_name else None

    # --------------------------------------------------------------------------
    # 프록시 및 네트워크 관리
    # --------------------------------------------------------------------------
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
            logger.info(f"[CrawlUtil] 프록시 로테이션 ({reason}) -> {active_proxy} ({cls._proxy_index + 1}/{len(p_list)}번)")
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

    # --------------------------------------------------------------------------
    # 웹 스크래핑 엔진
    # --------------------------------------------------------------------------
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

        if use_selenium and not _SELENIUM_AVAILABLE:
            logger.warning(f"[CrawlUtil] [{host}] Selenium 사용이 설정되어 있으나 모듈이 없습니다. HTTP 요청으로 자동 폴백합니다.")
            use_selenium = False

        if use_selenium:
            target_wait_tag = wait_tag or (site_info.get('SELENIUM_WAIT_TAG') if site_info else None) or 'body'
            for attempt in range(1, max_retries + 1):
                res_source = cls.get_by_remote_selenium(url, wait_tag=target_wait_tag, scheduler_instance=scheduler_instance, site_info=site_info)
                if res_source:
                    return res_source

                if cls._selenium_driver is None:
                    logger.warning(f"[CrawlUtil] 브라우저 세션 초기화 실패로 재시도 중단: {url}")
                    return None

                if attempt < max_retries:
                    logger.debug(f"[CrawlUtil] Selenium 재시도 ({attempt}/{max_retries}): {url}")
                    time.sleep(retry_interval)
            return None

        if use_fs and not cls._has_valid_clearance(host):
            logger.info(f"[CrawlUtil] [{host}] Cloudflare 사이트 감지 -> FlareSolverr 최우선 인가 요청: {url}")
            for fs_try in range(1, max_retries + 1):
                tree, fs_html = cls.get_by_flaresolverr(url, proxies=proxies)
                if fs_html and 'Just a moment...' not in fs_html and 'cf-turnstile' not in fs_html:
                    logger.info(f"[CrawlUtil] [{host}] FlareSolverr 인가 및 1차 페이지 수신 완료")
                    return fs_html
                if fs_try < max_retries:
                    logger.debug(f"[CrawlUtil] [{host}] FlareSolverr 초기 인가 재시도 ({fs_try}/{max_retries})...")
                    time.sleep(retry_interval)

            logger.warning(f"[CrawlUtil] [{host}] FlareSolverr 초기 인가 {max_retries}회 실패, HTTP 세션으로 폴백 시도")

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
                        logger.debug(f"[CrawlUtil] curl_cffi 세션 생성: {host}")

                    cls._sync_clearance_to_sessions(host)
                    res = cffi_session.get(url, headers=headers, proxies=proxies, impersonate="chrome", timeout=25)
                    if res.status_code == 200:
                        if 'Just a moment...' not in res.text and 'cf-turnstile' not in res.text:
                            return res.text
                    logger.debug(f"[CrawlUtil] curl_cffi 응답 코드: {res.status_code} ({url})")
                except Exception as e:
                    logger.debug(f"[CrawlUtil] curl_cffi 요청 실패 ({url}): {e}")

            try:
                req_session = cls._requests_sessions.get(host)
                if not req_session:
                    req_session = requests.Session()
                    cls._requests_sessions[host] = req_session
                    logger.debug(f"[CrawlUtil] requests 세션 생성: {host}")

                cls._sync_clearance_to_sessions(host)
                res = req_session.get(url, headers=headers, proxies=proxies, timeout=25, verify=False)
                if res.status_code == 200:
                    res.encoding = res.apparent_encoding or 'utf-8'
                    if 'Just a moment...' not in res.text and 'cf-turnstile' not in res.text:
                        return res.text
                logger.debug(f"[CrawlUtil] requests 응답 코드: {res.status_code} ({url})")
            except Exception as e:
                logger.debug(f"[CrawlUtil] requests 요청 실패 ({url}): {e}")

            if use_fs:
                logger.warning(f"[CrawlUtil] [{host}] Cloudflare 차단 감지 -> FlareSolverr 재인가: {url}")
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
                    logger.info(f"[CrawlUtil] [{host}] 세션 복구 및 페이지 수신 성공")
                    return source
                if attempt < max_retries:
                    logger.debug(f"[CrawlUtil] FlareSolverr 재시도 ({attempt}/{max_retries}): {url}")
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
            logger.debug(f"[CrawlUtil] FlareSolverr 세션 준비 예외 (무시): {sess_err}")

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
                        logger.info(f"[CrawlUtil] [{host}] FlareSolverr 쿠키 {len(sol_cookies)}개 동기화 완료")

                    cls._sync_clearance_to_sessions(host)
                    return tree, html_source
                else:
                    logger.warning(f"[CrawlUtil] FlareSolverr 오류 응답: {data.get('message')}")
        except Exception as e:
            logger.warning(f"[CrawlUtil] FlareSolverr 통신 실패 ({url}): {e}")
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

            logger.info(f"[CrawlUtil] [{host}] FlareSolverr 인가 요청 ({attempt}/{total_tries}, Proxy: {proxy_str}): {url}")

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
                            logger.info(f"[CrawlUtil] [{host}] FlareSolverr 인가 성공 (쿠키 {len(sol_cookies)}개)")
                            return sol_cookies, user_agent

                        logger.warning(f"[CrawlUtil] [{host}] 챌린지 미해결 감지 -> 다음 프록시 전환")
                    else:
                        logger.warning(f"[CrawlUtil] [{host}] FlareSolverr 응답 오류: {data.get('message')}")
            except Exception as e:
                logger.warning(f"[CrawlUtil] [{host}] FlareSolverr 통신 실패: {e}")

            if attempt < total_tries:
                cls.rotate_proxy(scheduler_instance=scheduler_instance, reason="FlareSolverr 인가 실패 대응")
                time.sleep(retry_interval)

        return [], ""

    @classmethod
    def solve_turnstile_checkbox(cls, driver, max_wait: int = 6) -> bool:
        if not _SELENIUM_AVAILABLE or not driver:
            return False

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
                                logger.debug("[CrawlUtil] Turnstile 체크박스 클릭 시도")
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
        logger.warning(f"[CrawlUtil] [{host}] 지속적 차단 감지 -> 브라우저/토큰 캐시 파기 후 재초기화")

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
                hook = cls.get_hook(site_info.get('NAME'))
                if hook and hasattr(hook, 'on_init_session'):
                    logger.info(f"[{site_info.get('NAME')}] 세션 재초기화 훅(on_init_session) 실행")
                    hook.on_init_session(site_info, scheduler_instance)
                    driver = cls._selenium_driver
            except Exception as hook_err:
                logger.debug(f"[CrawlUtil] 재초기화 훅 예외: {hook_err}")

        return driver

    @classmethod
    def handle_turnstile_challenge(cls, driver, url: str, site_info: dict = None, scheduler_instance=None) -> bool:
        page_title = driver.title or ""
        if not any(k in page_title for k in ["Just a moment", "Cloudflare", "Attention Required"]):
            return True

        logger.info(f"[CrawlUtil] Cloudflare 챌린지 화면 감지 ('{page_title}') -> 해결 시도: {url}")
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
        if not _SELENIUM_AVAILABLE:
            logger.error("[CrawlUtil] selenium 패키지가 설치되어 있지 않아 원격 브라우저를 구동할 수 없습니다.")
            return None

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
                logger.error(f"[CrawlUtil] [{host}] FlareSolverr 인가 실패로 Selenium 기동 중단")
                return None

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
            logger.info(f"[CrawlUtil] Selenium 프록시 적용: {proxies['http']}")

        try:
            driver = webdriver.Remote(command_executor=remote_url, options=options)
            driver.set_page_load_timeout(selenium_timeout)
        except Exception as e:
            logger.error(f"[CrawlUtil] Remote Selenium 연결 실패 ({remote_url}): {e}")
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
            logger.info(f"[CrawlUtil] [{host}] Selenium에 clearance 쿠키 {injected_count}개 주입 완료")

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
        if not _SELENIUM_AVAILABLE:
            logger.error(f"[CrawlUtil] selenium 모듈 미설치로 원격 렌더링을 수행할 수 없습니다: {url}")
            return None

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
            logger.debug(f"[CrawlUtil] Selenium 페이지 로드: {url}")
            driver.get(url)

            cls.handle_turnstile_challenge(driver, url, site_info=site_info, scheduler_instance=scheduler_instance)
            driver = cls._selenium_driver or driver

            if site_info:
                try:
                    hook = cls.get_hook(site_info.get('NAME'))
                    if hook and hasattr(hook, 'on_page_loaded'):
                        hook.on_page_loaded(driver, url)
                except Exception:
                    pass

            driver = cls._selenium_driver or driver
            if not driver:
                return None

            effective_wait_tag = wait_tag or 'body'
            if effective_wait_tag != 'body' and WebDriverWait and EC and By:
                try:
                    WebDriverWait(driver, selenium_timeout).until(EC.presence_of_element_located((By.XPATH, effective_wait_tag)))
                except Exception as wait_ex:
                    curr_title = (driver.title or "") if driver else ""
                    if any(k in curr_title for k in ["Just a moment", "Cloudflare", "Attention Required"]):
                        logger.warning(f"[CrawlUtil] 태그 대기 중 Cloudflare 재차단 확인 ('{curr_title}') -> 세션 재초기화: {url}")
                        driver = cls.reinit_session(site_info or {}, scheduler_instance=scheduler_instance)
                        driver = cls._selenium_driver or driver
                        if driver:
                            driver.get(url)
                            try:
                                WebDriverWait(driver, selenium_timeout).until(EC.presence_of_element_located((By.XPATH, effective_wait_tag)))
                            except Exception as retry_wait_ex:
                                logger.warning(f"[CrawlUtil] 세션 재초기화 후 태그 대기 타임아웃 ({effective_wait_tag}): {retry_wait_ex}")
                    else:
                        logger.warning(f"[CrawlUtil] 태그 대기 타임아웃 ({effective_wait_tag}): {wait_ex}")

            if not driver:
                return None

            return driver.page_source
        except Exception as e:
            logger.error(f"[CrawlUtil] Remote Selenium 로딩 에러 ({url}): {e}")
            cls.close_selenium_driver()
            return None

    @classmethod
    def close_selenium_driver(cls):
        if cls._selenium_driver:
            try:
                cls._selenium_driver.quit()
                logger.debug("[CrawlUtil] Selenium 드라이버 정상 종료")
            except Exception:
                pass
            cls._selenium_driver = None

    @classmethod
    def close_sessions(cls):
        """활성 브라우저 및 HTTP 세션 정리"""
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
        logger.debug("[CrawlUtil] HTTP 활성 세션 정리 완료")

    @classmethod
    def download_file_stream(cls, download_url: str, referer: str = None, scheduler_instance=None):
        """첨부파일 스트림 다운로드"""
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

        logger.debug(f"[CrawlUtil] 파일 스트림 다운로드: {download_url}")
        res = session.get(download_url, stream=True, timeout=60, verify=False)
        res.raise_for_status()

        byte_io = io.BytesIO()
        for chunk in res.iter_content(chunk_size=4096):
            if chunk:
                byte_io.write(chunk)

        byte_io.seek(0)
        return byte_io

    # --------------------------------------------------------------------------
    # 토렌트 메타데이터 취득 (Torrent Info 플러그인 또는 qBittorrent 연동)
    # --------------------------------------------------------------------------
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
                logger.debug(f"[CrawlUtil] 비토렌트 P2P 링크 메타데이터 분석 건너뜀: {magnet[:50]}")
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
                logger.error(f"[CrawlUtil] 토렌트 정보 취득 실패 ({magnet}): {e}")

        return results if results else None

    @classmethod
    def _get_info_via_plugin(cls, magnet_uri: str) -> dict | None:
        apikey = FeederUtil.get_system_apikey()
        local_port = F.config.get('port', 9999)
        candidate_urls = [
            f"http://127.0.0.1:{local_port}/torrent_info/api/m2i",
            f"http://localhost:{local_port}/torrent_info/api/m2i",
        ]
        ddns = FeederUtil.get_ddns()
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

        logger.warning(f"[CrawlUtil] m2i API 응답 실패: {magnet_uri}")
        return None

    @classmethod
    def _get_info_via_qbittorrent(cls, magnet_uri: str) -> dict | None:
        qb_url = (P.ModelSetting.get('crawl_qb_url') or '').rstrip('/')
        qb_user = P.ModelSetting.get('crawl_qb_username') or ''
        qb_pass = P.ModelSetting.get('crawl_qb_password') or ''
        temp_category = P.ModelSetting.get('crawl_qb_temp_category') or 'feeder_temp'

        if not qb_url:
            logger.error("[CrawlUtil] qBittorrent URL 미설정")
            return None

        target_hash = FeederUtil.extract_info_hash(magnet_uri)
        if not target_hash:
            logger.warning(f"[CrawlUtil] 유효하지 않은 마그넷 주소: {magnet_uri}")
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
                logger.error(f"[CrawlUtil] qBittorrent 로그인 실패: HTTP {login_res.status_code}")
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
            logger.error(f"[CrawlUtil] qBittorrent 연동 예외: {e}")
            try:
                session.post(f"{qb_url}/api/v2/torrents/delete", data={'hashes': target_hash, 'deleteFiles': 'true'}, timeout=5)
            except Exception:
                pass
            return None
