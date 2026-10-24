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
import threading
import uuid
import hashlib
from contextlib import contextmanager

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
    _http_ready = {}
    _fs_locks = {}
    _fs_lock_guard = threading.Lock()
    _fs_sessions = {}
    _fs_owned = set()
    _fs_retired = {}
    FS_OWNED_LIMIT = 64
    _network_context = {}
    _fs_scope = threading.local()
    _execution_lock = threading.RLock()
    _executions = {}
    _cookie_records = {}
    _configured_cookies = {}
    _selenium_context = None
    _cleanup_failures = []

    @classmethod
    @contextmanager
    def execution_scope(cls, owner=None):
        """기존 훅의 클래스 속성을 유지하면서 실행별 자원을 격리한다."""
        active = getattr(cls._fs_scope, 'owner', None)
        pid = os.getpid()
        if active and getattr(cls._fs_scope, 'pid', None) == pid and (owner is None or owner == active):
            yield active
            return
        if owner is None:
            implicit = getattr(cls._fs_scope, 'implicit', None)
            if not implicit or implicit[0] != pid:
                implicit = (pid, uuid.uuid4().hex)
                cls._fs_scope.implicit = implicit
            owner = implicit[1]
        names = ('_cf_cookies', '_cf_user_agents', '_cookie_records', '_configured_cookies', '_requests_sessions',
                 '_cffi_sessions', '_http_ready', '_network_context', '_fs_sessions', '_fs_owned', '_fs_retired',
                 '_selenium_driver', '_selenium_context')
        # 레거시 훅이 클래스 속성의 드라이버를 읽으므로 바인딩하는 동안 직렬화한다.
        with cls._execution_lock:
            saved = {name: getattr(cls, name) for name in names}
            state = cls._executions.setdefault((pid, owner), {name:
                (set() if name == '_fs_owned' else None if name in ('_selenium_driver', '_selenium_context') else {}) for name in names})
            previous_pid = getattr(cls._fs_scope, 'pid', None)
            local_names = ('auth', 'user_cookie', 'clearance_proxy')
            local_saved = {name: getattr(cls._fs_scope, name, None) for name in local_names}
            for name in local_names:
                setattr(cls._fs_scope, name, None)
            for name, value in state.items():
                setattr(cls, name, value)
            cls._fs_scope.owner, cls._fs_scope.pid = owner, pid
            try:
                yield owner
            finally:
                state.update({name: getattr(cls, name) for name in names})
                for name, value in saved.items():
                    setattr(cls, name, value)
                cls._fs_scope.owner, cls._fs_scope.pid = active, previous_pid
                for name, value in local_saved.items():
                    setattr(cls._fs_scope, name, value)

    @classmethod
    def finish_execution(cls):
        with cls.execution_scope():
            cls._finish_execution_current()

    @classmethod
    def _finish_execution_current(cls):
        for cleanup in (cls.close_flaresolverr_sessions, cls.close_sessions):
            try:
                cleanup()
            except Exception as exc:
                logger.warning('[CrawlUtil] 실행 자원 정리 실패: %s', cls._safe_network_log(exc))
        key = (os.getpid(), getattr(cls._fs_scope, 'owner', None))
        if cls._selenium_driver is not None:
            cls._cleanup_failures.append({'owner': key, 'kind': 'selenium',
                'session': getattr(cls._selenium_driver, 'session_id', None)})
        if cls._fs_owned:
            cls._cleanup_failures.extend({'owner': key, 'endpoint': endpoint, 'host': host,
                'session': session_id}
                for endpoint, host, session_id in cls._fs_owned)
            logger.warning('[CrawlUtil] 실행 종료: 원격 세션 정리 미확인 %s개 · 다른 실행에서 인수하지 않습니다.', len(cls._fs_owned))
        cls._cleanup_failures[:] = cls._cleanup_failures[-64:]
        cls._executions.pop(key, None)

    @staticmethod
    def normalize_cookies(cookies, url):
        parsed = urlparse(url)
        host = parsed.hostname or ''
        result = []
        for raw in cookies or []:
            if not isinstance(raw, dict) or not isinstance(raw.get('name'), str) or not isinstance(raw.get('value'), str):
                continue
            domain = str(raw.get('domain') or host).lower()
            bare = domain.lstrip('.')
            if not bare or (bare != host and ('.' not in bare or not host.endswith('.' + bare))):
                continue
            path = raw.get('path') or parsed.path.rpartition('/')[0] or '/'
            if not isinstance(path, str) or not path.startswith('/'):
                continue
            cookie = {key: raw[key] for key in ('name', 'value', 'secure', 'httpOnly') if key in raw}
            cookie.update(domain=domain, path=path)
            expiry = raw.get('expires', raw.get('expiry'))
            if type(expiry) in (int, float) and expiry >= 0:
                cookie['expires'] = float(expiry)
            result.append(cookie)
        return result

    @classmethod
    def _store_solution(cls, host, solution, url):
        now = time.time()
        records = cls._cookie_records.setdefault(host, {})
        cookies = cls.normalize_cookies(solution.get('cookies'), url)
        clearance_seen = False
        for cookie in cookies:
            key = (cookie['name'], cookie['domain'], cookie['path'])
            old = records.get(key)
            deadline = min(now + cls.CF_COOKIE_EXPIRY, cookie.get('expires', now + cls.CF_COOKIE_EXPIRY))
            if old and old['cookie']['value'] == cookie['value']:
                deadline = min(deadline, old['until'])
            records[key] = {'cookie': cookie, 'until': deadline, 'observed': now}
            clearance_seen |= cookie['name'] == 'cf_clearance' and bool(cookie['value'])
        if not clearance_seen:
            for record in records.values():
                if record['cookie']['name'] == 'cf_clearance':
                    record['until'] = min(record['until'], now)
        cls._cf_cookies[host] = {c['name']: c['value'] for c in cls._cached_cookies(host, url)}
        if solution.get('userAgent'):
            cls._cf_user_agents[host] = solution['userAgent']

    @classmethod
    def _cached_cookies(cls, host, url=None):
        parsed = urlparse(url or ('https://' + host + '/'))
        path = parsed.path or '/'
        return [r['cookie'] for r in cls._cookie_records.get(host, {}).values()
                if r['until'] > time.time() and
                ((parsed.hostname or '') == r['cookie']['domain'].lstrip('.') or
                 (r['cookie']['domain'].startswith('.') and (parsed.hostname or '').endswith('.' + r['cookie']['domain'].lstrip('.')))) and
                (path == r['cookie']['path'] or path.startswith(r['cookie']['path'].rstrip('/') + '/')) and
                (not r['cookie'].get('secure') or parsed.scheme == 'https')]

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
            FeederUtil.cooperate()
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue
            module_name = f"feeder_custom_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    FeederUtil.cooperate()
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
                    FeederUtil.cooperate()
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
                            db.session.commit()
                            logger.info(f"[CrawlUtil] 사이트 템플릿 최초 신규 등록: '{target_name}'")
                        else:
                            pass

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
            logger.info(f"[CrawlUtil] 프록시 로테이션 ({reason}) -> {cls._proxy_index + 1}/{len(p_list)}번")
            return active_proxy
        return p_list[0]

    @classmethod
    def get_proxies(cls, scheduler_instance=None):
        active_url = cls.get_current_proxy_url(scheduler_instance)
        if active_url:
            return {"http": active_url, "https": active_url}
        return None

    @classmethod
    def _site_lock(cls, host):
        with cls._fs_lock_guard:
            return cls._fs_locks.setdefault(host, threading.RLock())

    @staticmethod
    def _safe_network_log(value):
        text = str(value)
        markup = re.search(r'(?i)<(?:!doctype\b|/?[a-z][a-z0-9]*\b[^>]*>)', text)
        if markup:
            text = text[:markup.start()] + '[HTML 숨김]'
        text = re.sub(r'(?i)\b(?:https?|socks[45]h?)://[^\s<>\"\']+', '[URL 숨김]', text)
        text = re.sub(r'''(?i)["']?\b(?:set-cookie|cookies?|authorization)["']?\s*[:=]\s*[^\r\n]+''', '[인증 정보 숨김]', text)
        text = re.sub(r'(?i)\b(?:Bearer|Basic)\s+[^\s,;]+', '[인증 정보 숨김]', text)
        return re.sub(r'(?i)\b(password|passwd|token|api[_-]?key|cf_clearance)\s*[:=]\s*[^\s,;]+',
                      r'\1=[인증 정보 숨김]', text).replace('\n', ' ')[:300]

    @classmethod
    def _clear_network_state(cls, host):
        cls._cookie_records.pop(host, None)
        cls._cf_cookies.pop(host, None)
        cls._cf_user_agents.pop(host, None)
        cls._http_ready = {key: stamp for key, stamp in cls._http_ready.items() if key[0] != host}
        for sessions in (cls._requests_sessions, cls._cffi_sessions):
            session = sessions.pop(host, None)
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

    @classmethod
    def _activate_network_context(cls, host, proxies):
        endpoint = (P.ModelSetting.get('crawl_flaresolverr_url') or '').rstrip('/')
        context = (endpoint, (proxies or {}).get('http', ''), getattr(cls._fs_scope, 'auth', ''))
        if host in cls._network_context and cls._network_context[host] != context:
            cls._clear_network_state(host)
            for (server, site), record in cls._fs_sessions.items():
                if site == host:
                    record['state'] = 'unknown'
        cls._network_context[host] = context

    @classmethod
    def _fs_command(cls, endpoint, payload, timeout=10, missing_ok=False):
        try:
            response = requests.post(f'{endpoint}/v1', json=payload, timeout=timeout)
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError('JSON 객체가 아님')
            message = str(data.get('message') or '')
            if payload['cmd'] == 'sessions.create' and 'already exists' in message.lower():
                return data
            absent = 'session' in message.lower() and any(word in message.lower()
                for word in ('not found', 'does not exist', 'doesn\'t exist', 'not exist', 'already removed'))
            if missing_ok and absent and response.status_code in (200, 400, 404, 500):
                return {}
            if response.status_code != 200 or data.get('status') != 'ok':
                logger.warning('[CrawlUtil] FlareSolverr %s 실패 (HTTP %s): %s',
                               payload['cmd'], response.status_code, cls._safe_network_log(message))
                return None
            return data
        except Exception as exc:
            logger.warning('[CrawlUtil] FlareSolverr %s 통신/응답 오류: %s', payload['cmd'], cls._safe_network_log(exc))
            return None

    @classmethod
    def _destroy_fs_session(cls, endpoint, host, session_id=None):
        current = cls._fs_sessions.get((endpoint, host))
        name = session_id or (current or {}).get('id')
        key = (endpoint, host, name)
        if key not in cls._fs_owned:
            return True
        record = current if current and current['id'] == name else cls._fs_retired.get(key)
        if not record:
            return False
        record['state'] = 'unknown'
        if cls._fs_command(endpoint, {'cmd': 'sessions.destroy', 'session': name}, timeout=5, missing_ok=True) is None:
            logger.warning('[CrawlUtil] [%s] FS_SESSION_UNCERTAIN · 소유 세션 삭제 미확인', host)
            return False
        if current and current['id'] == name:
            cls._fs_sessions.pop((endpoint, host), None)
        cls._fs_retired.pop(key, None)
        cls._fs_owned.discard(key)
        return True

    @classmethod
    def _retire_fs_session(cls, endpoint, host):
        record = cls._fs_sessions.pop((endpoint, host), None)
        if record:
            record['state'] = 'unknown'
            cls._fs_retired[(endpoint, host, record['id'])] = record

    @classmethod
    def _create_fs_session(cls, endpoint, host, proxies):
        if len(cls._fs_owned) >= cls.FS_OWNED_LIMIT:
            logger.warning('[CrawlUtil] [%s] FS_SESSION_UNCERTAIN · 실행 내 소유 세션 상한 도달', host)
            return False
        name = 'feeder_' + hashlib.sha256(str(getattr(cls._fs_scope, 'owner', os.getpid())).encode()).hexdigest()[:12] + '_' + uuid.uuid4().hex
        payload = {'cmd': 'sessions.create', 'session': name}
        if (proxies or {}).get('http'):
            payload['proxy'] = {'url': proxies['http']}
        key = (endpoint, host, name)
        cls._fs_sessions[(endpoint, host)] = {'id': name, 'proxy': (proxies or {}).get('http', ''), 'state': 'unknown'}
        cls._fs_owned.add(key)
        data = cls._fs_command(endpoint, payload, timeout=20)
        if data is None:
            return False
        if 'already exists' in str(data.get('message', '')).lower():
            cls._fs_owned.discard(key)
            cls._fs_sessions.pop((endpoint, host), None)
            logger.warning('[CrawlUtil] [%s] 생성 ID 충돌 · 기존 원격 세션을 인수하지 않습니다.', host)
            return False
        if data.get('session') != name:
            logger.warning('[CrawlUtil] [%s] FlareSolverr 세션 생성 응답의 ID가 일치하지 않습니다.', host)
            return False
        cls._fs_sessions[(endpoint, host)]['state'] = 'ready'
        return True

    @classmethod
    def _replace_fs_session(cls, endpoint, host, proxies, budget, recovery=True):
        if recovery and budget['recoveries'] >= 1:
            budget['blocked'] = True
            return False
        if recovery:
            budget['recoveries'] += 1
        cls._clear_network_state(host)
        # 삭제 미확인 ID는 소유 기록을 보존하되 활성 연결에서 격리한다.
        cls._destroy_fs_session(endpoint, host)
        cls._retire_fs_session(endpoint, host)
        if not cls._create_fs_session(endpoint, host, proxies):
            budget.update(blocked=True, reason='FS_SESSION_UNCERTAIN')
            return False
        return True

    @classmethod
    def _retry_fs_proxy(cls, host, scheduler, attempt, total, reason):
        if attempt >= total:
            logger.warning('[CrawlUtil] [%s] FS 재시도 예산 소진 (%s/%s, %s)', host, attempt, total, reason)
            return
        cls.rotate_proxy(scheduler_instance=scheduler, reason=reason)
        count = len(cls._get_proxy_list(scheduler))
        logger.warning('[CrawlUtil] [%s] FS 재시도 (%s/%s, %s) · 다음 프록시 %s/%s',
                       host, attempt, total, reason, cls._proxy_index % count + 1 if count else 0, count)

    @classmethod
    def _has_valid_clearance(cls, host: str, url=None) -> bool:
        if url is not None:
            return any(c['name'] == 'cf_clearance' and c['value'] for c in cls._cached_cookies(host, url))
        return any(record['until'] > time.time() and record['cookie']['name'] == 'cf_clearance'
                   and record['cookie']['value'] for record in cls._cookie_records.get(host, {}).values())

    @staticmethod
    def _is_challenge_html(source: str) -> bool:
        """일반 페이지의 Turnstile 위젯과 브라우저 차단 화면을 구분한다."""
        source = (source or '').lower()
        title_match = re.search(r'<title\b[^>]*>(.*?)</title>', source, re.S)
        title = unescape(re.sub(r'<[^>]+>', '', title_match.group(1))).strip() if title_match else source.strip()
        if title in ('just a moment...', 'just a moment…', 'ddos-guard', 'attention required! | cloudflare', 'access denied'):
            return True
        return bool(re.search(r'''\bid\s*=\s*["'](?:cf-challenge-running|cf-please-wait|challenge-spinner|turnstile-wrapper|cf-error-details)["']''', source) or
                    ('_cf_chl_opt' in source and 'challenge-platform' in source))

    @classmethod
    def _read_flaresolverr_solution(cls, response, host: str, require_html=True):
        """API 응답·해결 메시지·반환 본문을 함께 확인하고 쿠키 유무와 별도로 판정한다."""
        try:
            data = response.json()
        except ValueError:
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 응답 형식 오류 (API HTTP {response.status_code}, JSON 아님)')
            return None
        if not isinstance(data, dict):
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 응답 형식 오류 (API HTTP {response.status_code})')
            return None
        message = cls._safe_network_log(data.get('message') or '')
        if response.status_code != 200 or data.get('status') != 'ok':
            api_status = 'error' if data.get('status') == 'error' else 'invalid'
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 처리 실패 (API HTTP {response.status_code}, status={api_status}): {message or "원인 메시지 없음"}')
            return None
        solution = data.get('solution')
        if not isinstance(solution, dict):
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 응답 형식 오류: solution 없음')
            return None
        source = solution.get('response') or ''
        if not isinstance(source, str):
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 응답 형식 오류: HTML이 문자열이 아님')
            return None
        cookies = solution.get('cookies') or []
        if not isinstance(cookies, list):
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 응답 형식 오류: 쿠키 목록 확인 불가')
            return None
        cookies = [c for c in cookies if isinstance(c, dict) and isinstance(c.get('name'), str) and c['name'] and isinstance(c.get('value'), str)]
        has_clearance = any(c['name'] == 'cf_clearance' and c['value'] for c in cookies)
        if cls._is_challenge_html(source):
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 반환 본문에 챌린지/차단 화면이 남아 있습니다 (응답 메시지: {message or "없음"})')
            return None
        try:
            status = int(solution.get('status') or 200)
        except (TypeError, ValueError):
            status = 0
        if status < 200 or status >= 400:
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 대상 페이지 오류 응답 (solution.status={status})')
            return None
        if not source.strip() and (require_html or not has_clearance):
            logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 정상 페이지 확인 불가: 반환 HTML 없음 (응답 메시지: {message or "없음"})')
            return None
        if source.strip():
            try:
                html.fromstring(source)
            except (ValueError, html.etree.ParserError):
                logger.warning(f'[CrawlUtil] [{host}] FlareSolverr 반환 HTML 해석 실패')
                return None
        outcome = message.lower().strip().rstrip('!.')
        if outcome == 'challenge not detected':
            description = ('챌린지 미감지 · 쿠키 발급 없이 정상 페이지 수신' if not cookies else '챌린지 미감지 · 정상 페이지 수신') if source.strip() else '챌린지 미감지 · 인가 쿠키 수신'
        elif outcome == 'challenge solved':
            description = '챌린지 해결 응답 · 정상 페이지 수신' if source.strip() else '챌린지 해결 응답 · 인가 쿠키 수신'
        else:
            description = '페이지 수신 · 챌린지 감지 여부는 응답에서 확인 불가' if source.strip() else '인가 쿠키 수신 · 챌린지 감지 여부는 응답에서 확인 불가'
        logger.debug(f'[CrawlUtil] [{host}] FlareSolverr {description} (쿠키 {len(cookies)}개, cf_clearance {"있음" if has_clearance else "없음"})')
        return dict(solution, cookies=cookies)

    @classmethod
    def _sync_clearance_to_sessions(cls, host: str):
        # 요청별 헤더에서 jar와 캐시를 합치므로 정상 HTTP 쿠키는 지우지 않는다.
        ua = cls._cf_user_agents.get(host)
        for sessions in (cls._requests_sessions, cls._cffi_sessions):
            session = sessions.get(host)
            if session is not None and ua:
                session.headers['User-Agent'] = ua

    @classmethod
    def _http_cookie_header(cls, host, url, explicit='', session=None):
        merged = {}
        parsed = urlparse(url)
        jar = getattr(session, 'cookies', ())
        jar = getattr(jar, 'jar', jar)
        try:
            for cookie in jar:
                domain = cookie.domain.lstrip('.')
                path = cookie.path or '/'
                matching = ((parsed.hostname == domain or
                            (cookie.domain_initial_dot and (parsed.hostname or '').endswith('.' + domain))) and
                            (parsed.path == path or (parsed.path or '/').startswith(path.rstrip('/') + '/')) and
                            (not cookie.secure or parsed.scheme == 'https'))
                records = [r for r in cls._cookie_records.get(host, {}).values() if r['cookie']['name'] == cookie.name]
                expired_cache = records and all(r['until'] <= time.time() for r in records)
                if matching and not cookie.is_expired() and not expired_cache:
                    merged[cookie.name] = cookie.value
        except (TypeError, AttributeError):
            pass
        merged.update({c['name']: c['value'] for c in cls._cached_cookies(host, url)})
        for part in (explicit or '').split(';'):
            if '=' in part:
                name, value = part.strip().split('=', 1)
                merged[name] = value
        return '; '.join(f'{name}={value}' for name, value in merged.items())

    # --------------------------------------------------------------------------
    # 웹 스크래핑 엔진
    # --------------------------------------------------------------------------
    @classmethod
    def get_html(cls, url: str, site_info: dict = None, scheduler_instance=None, referer: str = None, max_retries: int = None, retry_interval: float = None, wait_tag: str = None) -> str | None:
        previous = getattr(cls._fs_scope, 'request', None)
        cls._fs_scope.request = {'recoveries': 0, 'blocked': False}
        try:
            with cls.execution_scope(), cls._site_lock(urlparse(url).hostname or ''):
                return cls._get_html_in_context(url, site_info, scheduler_instance, referer, max_retries, retry_interval, wait_tag)
        finally:
            cls._fs_scope.request = previous

    @classmethod
    def _get_html_in_context(cls, url: str, site_info: dict = None, scheduler_instance=None, referer: str = None, max_retries: int = None, retry_interval: float = None, wait_tag: str = None) -> str | None:
        extra = site_info.get('EXTRA', []) if site_info else []
        cookie = site_info.get('COOKIE') if site_info else None
        proxies = cls.get_proxies(scheduler_instance)
        host = urlparse(url).hostname or ''
        cls._fs_scope.auth = hashlib.sha256(str(((site_info or {}).get('NAME', ''), cookie, (site_info or {}).get('USER_AGENT', ''))).encode()).hexdigest()
        cls._fs_scope.user_cookie = (host, cookie or '')
        cls._configured_cookies[host] = cookie or ''
        cls._activate_network_context(host, proxies)

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
        effective_retries = max(len(p_list), max_retries) if p_list else max_retries

        if scheduler_instance:
            use_selenium = getattr(scheduler_instance, 'use_selenium', False)
            use_fs = getattr(scheduler_instance, 'use_flaresolverr', False)
        else:
            use_selenium = P.ModelSetting.get_bool('crawl_use_selenium')
            use_fs = P.ModelSetting.get_bool('crawl_use_flaresolverr')

        if scheduler_instance is None:
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
                    logger.warning(f"[CrawlUtil] [{host}] 브라우저 세션 초기화 실패로 재시도 중단")
                    return None

                if attempt < max_retries:
                    logger.debug(f"[CrawlUtil] [{host}] Selenium 재시도 ({attempt}/{max_retries})")
                    time.sleep(retry_interval)
            return None

        headers = cls.DEFAULT_HEADERS.copy()
        if referer:
            headers['Referer'] = referer

        for attempt in range(1, effective_retries + 1):
            proxies = cls.get_proxies(scheduler_instance)
            cls._activate_network_context(host, proxies)
            # 재시도에서 프록시가 바뀌면 이전 Cookie/User-Agent 헤더를 다시 사용하지 않는다.
            headers.pop('Cookie', None)
            headers['User-Agent'] = cls._cf_user_agents.get(host) or (site_info or {}).get('USER_AGENT') or cls.DEFAULT_HEADERS['User-Agent']
            ready_key = (host, (proxies or {}).get('http', ''), cookie or '', (site_info or {}).get('USER_AGENT', ''))
            proxy_display = '프록시 사용' if proxies else 'Direct'

            cls._fs_scope.request.update(blocked=False, inline_recovery=False, reason='FS_CHALLENGE_UNRESOLVED')
            ready_at = cls._http_ready.get(ready_key)
            http_ready = ready_at is not None and 0 <= time.time() - ready_at < cls.CF_COOKIE_EXPIRY
            if use_fs and not cls._has_valid_clearance(host, url) and not http_ready:
                _, source = cls.get_by_flaresolverr(url, proxies=proxies)
                if source and not cls._is_challenge_html(source):
                    cls._http_ready[ready_key] = time.time()
                    return source
                cls._clear_network_state(host)
                cls._retry_fs_proxy(host, scheduler_instance, attempt, effective_retries, cls._fs_scope.request['reason'])
                if attempt < effective_retries:
                    time.sleep(retry_interval)
                continue

            cffi_failed = False
            http_failure = 'HTTP_CONTEXT_UNAVAILABLE'

            if _CURL_CFFI_AVAILABLE:
                cffi_failed = True
                try:
                    cffi_session = cls._cffi_sessions.get(host)
                    if not cffi_session:
                        cffi_session = cffi_requests.Session()
                        cls._cffi_sessions[host] = cffi_session
                        logger.debug(f"[CrawlUtil] curl_cffi 세션 생성 ({proxy_display}): {host}")

                    cls._sync_clearance_to_sessions(host)
                    value = cls._http_cookie_header(host, url, cookie, cffi_session)
                    headers['Cookie'] = value
                    res = cffi_session.get(url, headers=headers, proxies=proxies, impersonate="chrome", timeout=25)
                    if res.status_code == 200:
                        if not cls._is_challenge_html(res.text):
                            logger.debug(f'[CrawlUtil] [{host}] 일반 HTTP 페이지 수신 성공 (curl_cffi)')
                            cls._http_ready[ready_key] = time.time()
                            return res.text
                        logger.warning(f"[CrawlUtil] [{host}] Cloudflare 챌린지 감지 (HTTP 200 Turnstile) -> 인가 갱신 시도")
                        http_failure = 'FS_CHALLENGE_UNRESOLVED'
                    else:
                        http_failure = 'FS_PROXY_OR_TARGET_ERROR' if res.status_code in (403, 429, 503) else 'HTTP_RESPONSE_UNAVAILABLE'
                        logger.debug(f"[CrawlUtil] curl_cffi 응답 코드: {res.status_code} ({host})")

                except Exception as e:
                    err_msg = cls._safe_network_log(e)
                    logger.debug(f"[CrawlUtil] curl_cffi 요청 실패 ({proxy_display} / {host}): {err_msg}")

            if not (use_fs and cffi_failed):
                try:
                    req_session = cls._requests_sessions.get(host)
                    if req_session is None:
                        req_session = requests.Session()
                        cls._requests_sessions[host] = req_session
                    cls._sync_clearance_to_sessions(host)
                    value = cls._http_cookie_header(host, url, cookie, req_session)
                    headers['Cookie'] = value
                    res = req_session.get(url, headers=headers, proxies=proxies, timeout=25)
                    if res.status_code == 200 and not cls._is_challenge_html(res.text):
                        logger.debug(f'[CrawlUtil] [{host}] 일반 HTTP 페이지 수신 성공 (requests)')
                        cls._http_ready[ready_key] = time.time()
                        return res.text
                    http_failure = 'FS_PROXY_OR_TARGET_ERROR' if res.status_code in (403, 429, 503) else 'HTTP_RESPONSE_UNAVAILABLE'
                except Exception as e:
                    logger.debug(f"[CrawlUtil] requests 폴백 실패 ({proxy_display} / {host}): {cls._safe_network_log(e)}")

            if use_fs:
                cls._clear_network_state(host)
                cls._retry_fs_proxy(host, scheduler_instance, attempt, effective_retries, http_failure)
            else:
                cls._http_ready.pop(ready_key, None)
                for sessions in (cls._cffi_sessions, cls._requests_sessions):
                    session = sessions.pop(host, None)
                    if session is not None:
                        try:
                            session.close()
                        except Exception:
                            pass
                if attempt < effective_retries:
                    cls.rotate_proxy(scheduler_instance=scheduler_instance, reason='차단/응답불가 대응')
            if attempt < effective_retries:
                time.sleep(retry_interval)

        return None

    @classmethod
    def get_by_flaresolverr(cls, url: str, proxies=None):
        host = urlparse(url).hostname or ''
        proxies = dict(proxies or {})
        previous = getattr(cls._fs_scope, 'request', None)
        if previous is None:
            cls._fs_scope.request = {'recoveries': 0, 'blocked': False}
        try:
            with cls.execution_scope(), cls._site_lock(host):
                return cls._get_by_flaresolverr_locked(url, proxies)
        finally:
            cls._fs_scope.request = previous

    @classmethod
    def _get_by_flaresolverr_locked(cls, url: str, proxies=None, clearance=False):
        fs_url = (P.ModelSetting.get('crawl_flaresolverr_url') or '').rstrip('/')
        if not fs_url:
            logger.warning('[CrawlUtil] FlareSolverr 주소가 없어 페이지 요청을 실행하지 못했습니다.')
            return None, None

        host = urlparse(url).hostname or ''
        proxies = dict(proxies or {})
        budget = cls._fs_scope.request
        context = (fs_url, host, proxies.get('http', ''), getattr(cls._fs_scope, 'auth', None))
        if budget.get('context') != context:
            budget.update(context=context, blocked=False)
        if budget['blocked']:
            return None, None
        cls._activate_network_context(host, proxies)
        for endpoint, owned_host in list(cls._fs_sessions):
            if owned_host == host and endpoint != fs_url:
                cls._destroy_fs_session(endpoint, host)
                cls._retire_fs_session(endpoint, host)
        data = cls._fs_command(fs_url, {'cmd': 'sessions.list'})
        if data is None or not isinstance(data.get('sessions'), list):
            if data is not None:
                logger.warning('[CrawlUtil] [%s] FlareSolverr 세션 목록 응답 형식 오류', host)
            budget.update(blocked=True, reason='FS_API_UNAVAILABLE')
            return None, None
        key = (fs_url, host)
        record = cls._fs_sessions.get(key)
        if record and (record['state'] != 'ready' or record['proxy'] != proxies.get('http', '')):
            if not cls._replace_fs_session(fs_url, host, proxies, budget, recovery=False):
                return None, None
        elif record and record['id'] not in data['sessions']:
            if not cls._replace_fs_session(fs_url, host, proxies, budget):
                return None, None
        elif not record:
            if not cls._create_fs_session(fs_url, host, proxies):
                budget.update(blocked=True, reason='FS_SESSION_UNCERTAIN')
                return None, None

        for attempt in range(2):
            session_name = cls._fs_sessions[key]['id']
            logger.debug('[CrawlUtil] [%s] FlareSolverr 요청 · owner=%s session=%s 복구=%s',
                         host, getattr(cls._fs_scope, 'owner', ''), session_name, budget['recoveries'])
            payload = {'cmd': 'request.get', 'url': url, 'session': session_name, 'maxTimeout': 45000 if clearance else 25000}
            if proxies.get('http'):
                payload['proxy'] = {'url': proxies['http']}
            # 정상 브라우저의 전체 쿠키를 캐시로 덮어쓰지 않는다.
            record = cls._fs_sessions[key]
            explicit = getattr(cls._fs_scope, 'user_cookie', None)
            if explicit and explicit[0] == host and explicit[1] and not record.get('seeded'):
                seeds = []
                for part in explicit[1].split(';'):
                    if '=' in part:
                        name, value = part.strip().split('=', 1)
                        seeds.append({'name': name, 'value': value, 'path': '/', 'domain': host})
                payload['cookies'] = cls.normalize_cookies(seeds, url)
            try:
                res = requests.post(f'{fs_url}/v1', json=payload, headers={'Content-Type': 'application/json'}, timeout=55 if clearance else 35)
                result = None
                try:
                    result = res.json()
                    message = str(result.get('message') or '').lower()
                    api_failed = res.status_code != 200 or result.get('status') != 'ok'
                except (ValueError, AttributeError):
                    message = ''
                    api_failed = True
                broken = (api_failed and 'session' in message and any(word in message for word in (
                    'bound to a different proxy', 'stuck', 'invalid', 'not found', 'not exist', 'closed')))
                if broken:
                    record['state'] = 'unknown'
                    if not budget.get('inline_recovery', True):
                        budget.update(blocked=True, reason='FS_SESSION_UNCERTAIN')
                        return None, None
                    if attempt == 0:
                        logger.warning('[CrawlUtil] [%s] FlareSolverr 세션 오류 감지: %s', host, cls._safe_network_log(message))
                    if not cls._replace_fs_session(fs_url, host, proxies, budget):
                        return None, None
                    logger.info('[CrawlUtil] [%s] FlareSolverr 세션 복구 후 요청 재시도', host)
                    continue
                solution = cls._read_flaresolverr_solution(res, host, require_html=not clearance)
                if solution is None:
                    budget['reason'] = 'FS_API_UNAVAILABLE' if api_failed else 'FS_CHALLENGE_UNRESOLVED'
                    target = result.get('solution') if isinstance(result, dict) else None
                    if not api_failed and isinstance(target, dict) and str(target.get('status')) in ('403', '429', '503'):
                        budget['reason'] = 'FS_PROXY_OR_TARGET_ERROR'
                    if 'cookies' in payload:
                        # 전송 시도만으로 적용을 확정하지 않고, 같은 세션의 재주입도 막는다.
                        record['state'] = 'unknown'
                        budget['blocked'] = True
                    return None, None
                html_source = solution.get('response') or ''
                tree = solution if clearance else html.fromstring(html_source)
                cls._store_solution(host, solution, url)
                cls._sync_clearance_to_sessions(host)
                if 'cookies' in payload:
                    record['seeded'] = True
                return tree, html_source
            except Exception as exc:
                cls._fs_sessions[key]['state'] = 'unknown'
                budget.update(blocked=True, reason='FS_SESSION_UNCERTAIN')
                logger.warning('[CrawlUtil] [%s] FS_SESSION_UNCERTAIN · FlareSolverr 통신/응답 실패: %s', host, cls._safe_network_log(exc))
                return None, None
        return None, None

    @classmethod
    def get_flaresolverr_clearance(cls, url: str, scheduler_instance=None, max_retries: int = None, retry_interval: float = None) -> tuple[list[dict], str]:
        cookies, user_agent, _ = cls._get_flaresolverr_clearance_result(url, scheduler_instance, max_retries, retry_interval)
        return cookies, user_agent

    @classmethod
    def _get_flaresolverr_clearance_result(cls, url: str, scheduler_instance=None, max_retries: int = None, retry_interval: float = None):
        cls._fs_scope.clearance_proxy = None
        previous = getattr(cls._fs_scope, 'request', None)
        if previous is None:
            cls._fs_scope.request = {'recoveries': 0, 'blocked': False}
        try:
            with cls.execution_scope(), cls._site_lock(urlparse(url).hostname or ''):
                return cls._get_clearance_in_context(url, scheduler_instance, max_retries, retry_interval)
        finally:
            cls._fs_scope.request = previous

    @classmethod
    def _get_clearance_in_context(cls, url: str, scheduler_instance=None, max_retries: int = None, retry_interval: float = None):
        """쿠키가 필요 없었던 정상 응답도 성공으로 구분한다. 기존 공개 반환 형식은 유지한다."""
        fs_url = (P.ModelSetting.get('crawl_flaresolverr_url') or '').rstrip('/')
        if not fs_url:
            logger.warning('[CrawlUtil] FlareSolverr 주소가 없어 인가/페이지 요청을 실행하지 못했습니다.')
            return [], "", False

        parsed = urlparse(url)
        host = parsed.hostname or ''

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
            cls._fs_scope.request.update(blocked=False, inline_recovery=False, reason='FS_CHALLENGE_UNRESOLVED')
            proxies = dict(cls.get_proxies(scheduler_instance) or {})
            cls._activate_network_context(host, proxies)
            proxy_str = '사용' if proxies else '미사용'

            logger.debug(f"[CrawlUtil] [{host}] FlareSolverr 인가 요청 ({attempt}/{total_tries}, Proxy: {proxy_str})")
            try:
                solution, _ = cls._get_by_flaresolverr_locked(url, proxies, clearance=True)
                if solution is not None:
                    sol_cookies = cls.normalize_cookies(solution['cookies'], url)
                    user_agent = solution.get('userAgent') or ''
                    cls._store_solution(host, solution, url)
                    cls._sync_clearance_to_sessions(host)
                    cls._fs_scope.clearance_proxy = (host, dict(proxies or {}))
                    return sol_cookies, user_agent, True
            except Exception as e:
                logger.warning(f"[CrawlUtil] [{host}] FlareSolverr 통신 실패: {cls._safe_network_log(e)}")

            if attempt < total_tries:
                cls._retry_fs_proxy(host, scheduler_instance, attempt, total_tries, cls._fs_scope.request['reason'])
                time.sleep(retry_interval)

        return [], "", False

    @classmethod
    def solve_turnstile_checkbox(cls, driver, max_wait: int = 6) -> bool:
        if not _SELENIUM_AVAILABLE or not driver:
            return False

        start_time = time.time()
        while time.time() - start_time < max_wait:
            if driver.page_source and not cls._is_challenge_html(driver.page_source):
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
        with cls.execution_scope():
            return cls._reinit_in_context(site_info, scheduler_instance)

    @classmethod
    def _reinit_in_context(cls, site_info, scheduler_instance=None):
        site_url = (site_info.get('TORRENT_SITE_URL') if site_info else '').rstrip('/')
        parsed_url = urlparse(site_url)
        host = parsed_url.hostname or ''
        cls._fs_scope.auth = hashlib.sha256(str((site_info.get('NAME', ''), site_info.get('COOKIE'), site_info.get('USER_AGENT', ''))).encode()).hexdigest()
        cls._fs_scope.user_cookie = (host, site_info.get('COOKIE', ''))
        cls._configured_cookies[host] = site_info.get('COOKIE', '')

        cls.rotate_proxy(scheduler_instance=scheduler_instance, reason="지속적 차단 대응")
        logger.warning(f"[CrawlUtil] [{host}] 지속적 차단 감지 -> 브라우저/토큰 캐시 파기 후 재초기화")

        cls.close_sessions()
        cls._http_ready = {key: stamp for key, stamp in cls._http_ready.items() if key[0] != host}
        if host in cls._cf_cookies:
            cls._cf_cookies.pop(host, None)
        if host in cls._cf_user_agents:
            cls._cf_user_agents.pop(host, None)

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
        with cls.execution_scope():
            return cls._handle_challenge_in_context(driver, url, site_info, scheduler_instance)

    @classmethod
    def _handle_challenge_in_context(cls, driver, url, site_info=None, scheduler_instance=None):
        if not driver:
            return False
        page_title = driver.title or ""
        if not driver.page_source:
            return False
        if not cls._is_challenge_html(driver.page_source):
            return True

        logger.info(f"[CrawlUtil] Cloudflare 챌린지 화면 감지 ('{page_title}') -> 해결 시도: {url}")
        if cls.solve_turnstile_checkbox(driver, max_wait=4) and not cls._is_challenge_html(driver.page_source):
            return True

        new_driver = cls.reinit_session(site_info or {}, scheduler_instance=scheduler_instance)
        if new_driver:
            new_driver.get(url)
            time.sleep(2)
            cls.solve_turnstile_checkbox(new_driver, max_wait=4)

        return bool(new_driver and cls._selenium_driver is new_driver
                    and not cls._is_challenge_html(new_driver.page_source))

    @classmethod
    def init_stealth_selenium(cls, site_info: dict, scheduler_instance=None):
        with cls.execution_scope():
            return cls._init_selenium_in_context(site_info, scheduler_instance)

    @classmethod
    def _init_selenium_in_context(cls, site_info: dict, scheduler_instance=None):
        if not _SELENIUM_AVAILABLE:
            logger.error("[CrawlUtil] selenium 패키지가 설치되어 있지 않아 원격 브라우저를 구동할 수 없습니다.")
            return None

        site_url = site_info.get('TORRENT_SITE_URL', '').rstrip('/')
        remote_url = site_info.get('SELENIUM_REMOTE_URL') or P.ModelSetting.get('crawl_selenium_remote_url') or 'http://selenium:4444/wd/hub'
        proxies = cls.get_proxies(scheduler_instance)
        parsed_url = urlparse(site_url)
        host = parsed_url.hostname or ''
        cls._fs_scope.auth = hashlib.sha256(str((site_info.get('NAME', ''), site_info.get('COOKIE'), site_info.get('USER_AGENT', ''))).encode()).hexdigest()
        cls._fs_scope.user_cookie = (host, site_info.get('COOKIE', ''))
        context = (host, site_info.get('NAME', ''), remote_url, (proxies or {}).get('http', ''),
                   site_info.get('COOKIE', ''), site_info.get('USER_AGENT', ''),
                   P.ModelSetting.get('crawl_flaresolverr_url'))
        cls._configured_cookies[host] = site_info.get('COOKIE', '')

        if cls._selenium_driver:
            try:
                if cls._selenium_context != context:
                    raise ValueError('브라우저 요청 컨텍스트 변경')
                _ = cls._selenium_driver.current_url
                return cls._selenium_driver
            except Exception:
                if not cls.close_selenium_driver():
                    return None

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
            raw_cf_cookies, cf_user_agent, fs_succeeded = cls._get_flaresolverr_clearance_result(site_url, scheduler_instance=scheduler_instance)
            if not fs_succeeded:
                logger.error(f"[CrawlUtil] [{host}] FlareSolverr 인가 실패로 Selenium 기동 중단")
                return None
            used_proxy = getattr(cls._fs_scope, 'clearance_proxy', None)
            if used_proxy and used_proxy[0] == host:
                proxies = dict(used_proxy[1])
                context = (host, site_info.get('NAME', ''), remote_url, proxies.get('http', ''),
                           site_info.get('COOKIE', ''), site_info.get('USER_AGENT', ''),
                           P.ModelSetting.get('crawl_flaresolverr_url'))

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
            logger.info('[CrawlUtil] Selenium 프록시 적용')

        driver = None
        try:
            driver = webdriver.Remote(command_executor=remote_url, options=options)
            driver.set_page_load_timeout(selenium_timeout)
            cls._configure_selenium_driver(driver, site_info, parsed_url, raw_cf_cookies)
        except Exception as e:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    logger.warning('[CrawlUtil] 초기화에 실패한 Selenium 브라우저 종료 미확인 · 원격 서비스 상태를 확인해주세요.')
            logger.error(f"[CrawlUtil] Remote Selenium 연결 실패: {cls._safe_network_log(e)}")
            return None

        cls._selenium_driver = driver
        cls._selenium_context = context
        cls._activate_network_context(host, proxies)
        cls._cf_user_agents[host] = target_ua
        return driver

    @classmethod
    def _configure_selenium_driver(cls, driver, site_info, parsed_url, raw_cf_cookies):
        site_url = site_info.get('TORRENT_SITE_URL', '')
        host = parsed_url.hostname or ''
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
                cookie_dict.update({key: c[key] for key in ('secure', 'httpOnly') if key in c})
                expiry = c.get('expires', c.get('expiry'))
                if type(expiry) in (int, float) and expiry > time.time():
                    cookie_dict['expiry'] = int(expiry)
                dom = c.get('domain', '')
                if dom:
                    cookie_dict['domain'] = dom.lstrip('.')
                try:
                    driver.add_cookie(cookie_dict)
                    injected_count += 1
                except Exception:
                    raise RuntimeError('인가 쿠키를 브라우저에 적용하지 못했습니다.') from None
            logger.info(f"[CrawlUtil] [{host}] Selenium에 clearance 쿠키 {injected_count}개 주입 완료")

        if site_info and site_info.get('COOKIE'):
            for part in site_info['COOKIE'].split(';'):
                if '=' in part:
                    k, v = part.strip().split('=', 1)
                    k_clean = k.strip()
                    try:
                        driver.add_cookie({'name': k_clean, 'value': v.strip(), 'path': '/'})
                    except Exception:
                        raise RuntimeError('사용자 쿠키를 브라우저에 적용하지 못했습니다.') from None

    @classmethod
    def get_by_remote_selenium(cls, url: str, wait_tag: str = 'body', scheduler_instance=None, site_info: dict = None) -> str | None:
        with cls.execution_scope():
            return cls._get_selenium_in_context(url, wait_tag, scheduler_instance, site_info)

    @classmethod
    def _get_selenium_in_context(cls, url, wait_tag='body', scheduler_instance=None, site_info=None):
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

            if not cls.handle_turnstile_challenge(driver, url, site_info=site_info, scheduler_instance=scheduler_instance):
                return None
            driver = cls._selenium_driver

            if site_info:
                try:
                    hook = cls.get_hook(site_info.get('NAME'))
                    if hook and hasattr(hook, 'on_page_loaded'):
                        hook.on_page_loaded(driver, url)
                except Exception:
                    pass

            driver = cls._selenium_driver
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
                        driver = cls._selenium_driver
                        if driver:
                            driver.get(url)
                            try:
                                WebDriverWait(driver, selenium_timeout).until(EC.presence_of_element_located((By.XPATH, effective_wait_tag)))
                            except Exception as retry_wait_ex:
                                logger.warning(f"[CrawlUtil] 세션 재초기화 후 태그 대기 타임아웃 ({effective_wait_tag}): {cls._safe_network_log(retry_wait_ex)}")
                                return None
                        else:
                            return None
                    else:
                        logger.warning(f"[CrawlUtil] 태그 대기 타임아웃 ({effective_wait_tag}): {cls._safe_network_log(wait_ex)}")
                        return None

            if not driver:
                return None

            source = driver.page_source
            if not source or cls._is_challenge_html(source):
                return None
            try:
                host = urlparse(url).hostname or ''
                cls._store_solution(host, {'cookies': driver.get_cookies()}, url)
            except Exception as exc:
                logger.debug('[CrawlUtil] 브라우저 쿠키 상태 조회 미확인: %s', cls._safe_network_log(exc))
            return source
        except Exception as e:
            logger.error(f"[CrawlUtil] Remote Selenium 로딩 에러 ({url}): {cls._safe_network_log(e)}")
            cls.close_selenium_driver()
            return None

    @classmethod
    def close_selenium_driver(cls):
        with cls.execution_scope():
            return cls._close_selenium_current()

    @classmethod
    def _close_selenium_current(cls):
        if cls._selenium_driver:
            try:
                cls._selenium_driver.quit()
                logger.debug("[CrawlUtil] Selenium 드라이버 정상 종료")
            except Exception:
                cls._selenium_context = None
                logger.warning('[CrawlUtil] Selenium 종료 미확인 · 같은 실행에서 새 브라우저 생성을 보류합니다.')
                return False
            cls._selenium_driver = None
        cls._selenium_context = None
        return True

    @classmethod
    def close_sessions(cls):
        with cls.execution_scope():
            cls._close_sessions_current()

    @classmethod
    def _close_sessions_current(cls):
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
    def close_flaresolverr_sessions(cls, host=None):
        with cls.execution_scope():
            cls._close_flaresolverr_current(host)

    @classmethod
    def _close_flaresolverr_current(cls, host=None):
        """전체 수집 종료 때 이 프로세스가 사용한 원격 세션만 정리한다."""
        for endpoint, owned_host, session_id in list(cls._fs_owned):
            if host is None or host == owned_host:
                with cls._site_lock(owned_host):
                    if cls._destroy_fs_session(endpoint, owned_host, session_id):
                        cls._clear_network_state(owned_host)

    @classmethod
    def download_file_stream(cls, download_url: str, referer: str = None, scheduler_instance=None):
        with cls.execution_scope():
            return cls._download_stream_in_context(download_url, referer, scheduler_instance)

    @classmethod
    def _download_stream_in_context(cls, download_url, referer=None, scheduler_instance=None):
        """첨부파일 스트림 다운로드"""
        host = urlparse(download_url).hostname or ''
        origin = urlparse(referer or download_url).hostname or ''
        saved_context = cls._network_context.get(origin)
        proxy = saved_context[1] if saved_context else cls.get_current_proxy_url(scheduler_instance)
        proxies = {'http': proxy, 'https': proxy} if proxy else None
        headers = cls.DEFAULT_HEADERS.copy()

        if host in cls._cf_user_agents:
            headers['User-Agent'] = cls._cf_user_agents[host]
        if referer:
            headers['Referer'] = referer

        explicit = cls._configured_cookies.get(origin, '') if host == origin else ''
        page_session = cls._requests_sessions.get(origin) or cls._cffi_sessions.get(origin)
        injected = cls._http_cookie_header(origin, download_url, explicit, page_session)
        if injected:
            headers['Cookie'] = injected

        session = requests.Session()
        res = None
        try:
            session.headers.update(headers)
            if proxies:
                session.proxies.update(proxies)
            logger.debug('[CrawlUtil] [%s] 첨부파일 스트림 다운로드 시작 · 페이지 연결 컨텍스트 사용', host)
            res = session.get(download_url, stream=True, timeout=60, verify=False)
            res.raise_for_status()
            byte_io = io.BytesIO()
            for chunk in res.iter_content(chunk_size=4096):
                FeederUtil.cooperate()
                if chunk:
                    byte_io.write(chunk)
            byte_io.seek(0)
            return byte_io
        finally:
            try:
                if res is not None:
                    res.close()
            finally:
                session.close()

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
            FeederUtil.cooperate()
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
