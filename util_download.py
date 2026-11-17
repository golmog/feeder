# -*- coding: utf-8 -*-
import os
import glob
import shutil
import requests
import subprocess
import importlib.util
import inspect
import base64
import configparser
import json
import re
import hashlib
import threading
import time
import uuid
from urllib.parse import quote, urlsplit

import sys

from .setup import *
from .util_base import FeederUtil
from .util_upload import UploadUtil
from .worker.rclone_logging import redact_log

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
if PLUGIN_DIR not in sys.path:
    sys.path.insert(0, PLUGIN_DIR)

# CD2 gRPC 모듈 안전 로드
CD2_GRPC_AVAILABLE = False
CloudDrive_pb2 = None
CloudDrive_pb2_grpc = None

# Protobuf Gencode/Runtime 버전 불일치 강제 검증(VersionError) 완화 패치
try:
    import google.protobuf.runtime_version as _proto_runtime
    _proto_runtime.ValidateProtobufRuntimeVersion = lambda *args, **kwargs: None
except Exception:
    pass

try:
    import grpc
    import clouddrive_pb2 as CloudDrive_pb2
    import clouddrive_pb2_grpc as CloudDrive_pb2_grpc
    CD2_GRPC_AVAILABLE = True
except Exception:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "grpcio", "protobuf"])
        import grpc
        import clouddrive_pb2 as CloudDrive_pb2
        import clouddrive_pb2_grpc as CloudDrive_pb2_grpc
        CD2_GRPC_AVAILABLE = True
    except Exception as ex:
        logger.debug(f"[DownloadUtil] CD2 gRPC 모듈 대기 (미설치/파일 없음): {ex}")
        CD2_GRPC_AVAILABLE = False


# ==============================================================================
# 커스텀 확장용 베이스 인터페이스 (Base Interfaces)
# ==============================================================================
class BaseDownloadEngine:
    """모든 다운로더 엔진의 표준 인터페이스"""
    ENGINE_ID = "base"
    status_list_complete = True
    REGISTRATION_HASH_LOOKUP = False
    ENGINE_NAME = "Base Engine"
    OUTPUT_TYPE = "local"
    SUPPORTED_PROTOCOLS = ["magnet"]
    CONFIG_SCHEMA = []

    def __init__(self, config: dict):
        self.config = config or {}
        self.name = self.config.get('name', self.ENGINE_ID)
        self.enabled = bool(self.config.get('enabled', True))

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        raise NotImplementedError

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        raise NotImplementedError

    def classify_error(self, context: dict) -> dict | None:
        """Optional API classification. Return category/scope/code/action/origin.

        Only a service API 401 or explicit account lock should opt into disabling.
        context contains operation, error, source_engine_name; tuple APIs stay unchanged.
        Legacy error strings are never classified by the main process.
        """
        return None

    def delete_task(self, task_id: str) -> bool:
        raise NotImplementedError

    def delete_tasks(self, task_ids: list[str]) -> bool:
        """여러 작업을 일괄 삭제 (기본 구현: delete_task 순회 호출)"""
        if not task_ids:
            return True
        success = True
        for tid in task_ids:
            FeederUtil.cooperate()
            if not self.delete_task(tid):
                success = False
        return success

    def restart_task(self, task_id: str) -> bool:
        return False

    def test_connection(self) -> tuple[bool, str]:
        return True, "연결 테스트가 정의되지 않은 엔진입니다."


class BaseTransporter:
    """모든 이송/후처리 핸들러의 표준 인터페이스"""
    TRANSPORTER_ID = "base"
    TRANSPORTER_NAME = "Base Transporter"
    CONFIG_SCHEMA = []

    def __init__(self, config: dict = None):
        self.config = config or {}

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        raise NotImplementedError


# ==============================================================================
# 기본 내장 엔진 및 트랜스포터 (Built-in Implementations)
# ==============================================================================
class QBittorrentEngine(BaseDownloadEngine):
    REGISTRATION_HASH_LOOKUP = True
    ENGINE_ID = "qbittorrent"
    ENGINE_NAME = "qBittorrent (로컬 다운로더)"
    OUTPUT_TYPE = "local"
    SUPPORTED_PROTOCOLS = ["magnet"]

    CONFIG_SCHEMA = [
        {"name": "url", "label": "Web UI 주소", "type": "text", "default": "http://127.0.0.1:8080", "required": True},
        {"name": "username", "label": "사용자명", "type": "text", "default": "admin"},
        {"name": "password", "label": "비밀번호", "type": "password"},
        {"name": "save_path", "label": "다운로드 경로 루트", "type": "text", "default": "/data/downloads", "placeholder": "예: /data/downloads"},
        {"name": "category", "label": "카테고리", "type": "text", "default": "feeder"},
        {"name": "max_active_tasks", "label": "동시 다운로드 큐 제한", "type": "number", "default": 0, "min": 0,
         "desc": "진행·대기 중인 원격 다운로드 작업의 최대 수 (0은 무제한). 완료·오류·업로드 대기 작업은 제외."},
        {"name": "stalled_timeout_hours", "label": "지연 타임아웃 (시간)", "type": "number", "default": 24}
    ]

    def __init__(self, config: dict):
        super(QBittorrentEngine, self).__init__(config)
        self.url = self.config.get('url', 'http://127.0.0.1:8080').rstrip('/')
        self.username = self.config.get('username', 'admin')
        self.password = self.config.get('password', '')
        self.save_path = self.config.get('save_path', '').strip()
        self.category = self.config.get('category', 'feeder').strip()
        self.session = None

    def _api_error(self, response):
        if response.status_code == 401:
            return {'category': 'authentication', 'scope': 'engine', 'code': 'AUTH_UNAUTHORIZED',
                    'action': 'disable_engine', 'origin': 'service_api', 'message': '인증 오류 (401)'}
        return None

    def _get_session(self) -> requests.Session | None:
        if self.session:
            return self.session

        self._login_error = None
        s = requests.Session()
        login_url = f"{self.url}/api/v2/auth/login"
        try:
            res = s.post(login_url, data={'username': self.username, 'password': self.password}, timeout=10)
            self._login_error = self._api_error(res)
            if res.status_code in [200, 204] and 'Fails' not in res.text:
                self.session = s
                return s
            if not self._login_error:
                self._login_error = DownloadUtil.log_engine_error(self, 'POST /api/v2/auth/login',
                    f'HTTP {res.status_code}: {res.text}')
        except Exception as e:
            self._login_error = DownloadUtil.registration_exception(self, e)
        return None

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        """qBittorrent에 마그넷 등록 (동적 하위 저장 경로 지원)"""
        s = self._get_session()
        if not s:
            self._registration_failure = {'kind': 'transient', 'not_submitted': True}
            return False, "", self._login_error or "qBittorrent 로그인 실패"
        if str(link).lower().startswith('ed2k://'):
            return False, "", "ed2k 프로토콜 미지원 (115 등 지원 엔진 필요)"

        add_url = f"{self.url}/api/v2/torrents/add"
        payload = {
            'urls': link,
            'category': self.category,
            'autoTMM': 'false'
        }

        # 기본 저장 경로와 프로필 지정 하위 수신/완료 경로 결합
        effective_save_path = self.save_path
        if upload_path and str(upload_path).strip():
            clean_sub = str(upload_path).strip('/\\')
            if effective_save_path:
                effective_save_path = os.path.join(effective_save_path, clean_sub)
            else:
                effective_save_path = clean_sub

        if effective_save_path:
            payload['savepath'] = effective_save_path

        info_hash = FeederUtil.extract_info_hash(link)

        try:
            res = s.post(add_url, data=payload, timeout=15)
            if res.status_code in [200, 204]:
                task_id = info_hash or link[:40]
                logger.info(f"[QBittorrentEngine] 작업 추가 성공: {task_id} ({title or link[:30]}) -> 저장경로: {effective_save_path or '기본'}")
                return True, task_id, ""
            detail = DownloadUtil.log_engine_error(self, 'POST /api/v2/torrents/add', f'HTTP {res.status_code}: {res.text}')
            self._registration_failure = {'kind': 'transient', 'not_submitted': res.status_code < 500}
            if res.status_code == 415 or 'invalid magnet' in res.text.lower():
                self._registration_failure = {'kind': 'permanent', 'not_submitted': True}
            return False, "", self._api_error(res) or detail
        except Exception as e:
            return False, "", DownloadUtil.registration_exception(self, e, submitted=True)

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        s = self._get_session()
        if not s:
            return [], self._login_error or "qBittorrent 세션 없음"

        info_url = f"{self.url}/api/v2/torrents/info"
        try:
            # 외부 등록 작업도 원격 큐 점유에 포함하므로 전체 상태를 조회한다.
            res = s.get(info_url, timeout=15)
            if res.status_code != 200:
                detail = DownloadUtil.log_engine_error(self, 'GET /api/v2/torrents/info', f'HTTP {res.status_code}: {res.text}')
                return [], self._api_error(res) or detail

            torrents = res.json()
            ret = []
            for t in torrents:
                FeederUtil.cooperate()
                t_hash = t.get('hash', '').lower()
                progress = t.get('progress', 0.0)
                state = t.get('state', '')

                if progress >= 1.0 or state in ['uploading', 'pausedUP', 'queuedUP']:
                    std_status = 'completed'
                elif state in ['error', 'missingFiles']:
                    std_status = 'error'
                else:
                    std_status = 'downloading'

                save_dir = t.get('save_path') or self.save_path
                fname = t.get('name') or ''
                full_local_path = os.path.join(save_dir, fname) if save_dir and fname else save_dir

                progress_val = round(float(t.get('progress', 0.0)) * 100, 1)
                ret.append({
                    'task_id': t_hash,
                    'statusCode': state,
                    'hash': t_hash,
                    'status': std_status,
                    'filename': fname,
                    'file_size': t.get('total_size', 0),
                    'downloaded_bytes': t.get('completed', 0),
                    'download_speed': t.get('dlspeed', 0),
                    'progress': progress_val,
                    'source_path': full_local_path
                })
            return ret, ""
        except Exception as e:
            return [], DownloadUtil.log_engine_error(self, 'GET /api/v2/torrents/info', f'{type(e).__name__}: {e}')

    def delete_task(self, task_id: str, delete_files=False) -> bool:
        s = self._get_session()
        if not s or not task_id:
            return False

        del_url = f"{self.url}/api/v2/torrents/delete"
        try:
            res = s.post(del_url, data={'hashes': task_id, 'deleteFiles': 'true' if delete_files else 'false'}, timeout=10)
            return res.status_code in [200, 204]
        except Exception:
            return False

    def test_connection(self) -> tuple[bool, str]:
        s = self._get_session()
        if not s:
            return False, "qBittorrent 로그인 실패 (URL, 사용자명, 비밀번호 확인 필요)"
        try:
            res = s.get(f"{self.url}/api/v2/app/version", timeout=10)
            if res.status_code == 200:
                return True, f"qBittorrent 연결 성공 (버전: {res.text.strip()})"
            return False, f"qBittorrent 응답 오류: HTTP {res.status_code}"
        except Exception as e:
            return False, f"qBittorrent 통신 예외: {str(e)}"


class PikPakEngine(BaseDownloadEngine):
    """작업 관리는 HTTP API, 완료된 파일 전송은 Rclone 리모트로 처리한다."""
    ENGINE_ID = 'pikpak'
    ENGINE_NAME = 'PikPak'
    OUTPUT_TYPE = 'remote_cloud'
    SUPPORTED_PROTOCOLS = ['magnet']
    CONFIG_SCHEMA = [
        {'name': 'username', 'label': 'PikPak 계정', 'type': 'text', 'desc': 'Rclone 인증 사용 시 선택 사항입니다. 입력하면 리모트 계정과 일치해야 합니다.'},
        {'name': 'password', 'label': '비밀번호', 'type': 'password', 'desc': 'Rclone 리모트가 없는 기존 직접 로그인에서만 필요합니다.'},
        {'name': 'remote_name', 'label': 'Rclone 리모트명', 'type': 'text', 'default': 'pikpak',
         'desc': 'Feeder 관리 계정과 Rclone 리모트의 PikPak 계정이 같아야 합니다.'},
        {'name': 'parent_id', 'label': '다운로드 폴더 ID', 'type': 'text', 'desc': '비워두면 PikPak 기본 다운로드 폴더를 사용합니다.'},
        {'name': 'cached_only', 'label': '캐시된 항목만 다운로드', 'type': 'checkbox', 'default': True,
         'desc': 'PikPak 인덱스에서 모든 파일의 캐시가 확인된 마그넷만 등록합니다. OFF면 기존 VIP 범위에서 일반 등록합니다.'},
        {'name': 'max_active_tasks', 'label': '동시 다운로드 큐 제한', 'type': 'number', 'default': 0, 'min': 0,
         'desc': '진행·대기 중인 원격 다운로드 작업의 최대 수 (0은 무제한). 완료·오류·업로드 대기 작업은 제외.'},
        {'name': 'stalled_timeout_hours', 'label': '지연 타임아웃 (시간)', 'type': 'number', 'default': 24}
    ]
    CLIENT_ID = 'YUMx5nI8ZU8Ap8pm'
    CLIENT_VERSION = '2.0.0'
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0'
    _sessions = {}
    _session_lock = threading.RLock()

    def __init__(self, config):
        super().__init__(config)
        self.username = str(self.config.get('username') or '').strip()
        self.remote_name = str(self.config.get('remote_name') or 'pikpak').strip()
        self.session = {}
        self.rclone_conf = None
        self._auth_error = None
        self._captcha_invalid = False
        self._vip_cache = None
        self._vip_checked_at = 0

    @staticmethod
    def _expiry(value):
        from datetime import datetime, timezone
        import re
        if not value:
            return 0
        # Rclone (Go RFC3339Nano)는 초의 소수부를 9자리까지 기록한다.
        # Python 3.10 datetime은 최대 6자리만 지원하므로 초과 자릿수를 잘라낸다.
        normalized = re.sub(r'(\.\d{6})\d+(?=(?:Z|[+-]\d{2}:\d{2})?$)', r'\1', value)
        parsed = datetime.fromisoformat(normalized.replace('Z', '+00:00'))
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()

    def _read_rclone_auth(self):
        """FF가 지정한 파일만 읽으며 비밀번호와 원본 설정은 반환하지 않는다."""
        if (not self.remote_name or self.remote_name.startswith('-') or
                any(char in self.remote_name for char in ':\\/\r\n')):
            raise RuntimeError('PikPak Rclone 리모트명을 확인해주세요.')
        conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        if not conf:
            return '', None
        conf = os.path.abspath(conf)
        parser = configparser.RawConfigParser(interpolation=None)
        try:
            with open(conf, encoding='utf-8') as stream:
                parser.read_file(stream)
        except FileNotFoundError:
            raise RuntimeError('PikPak Rclone 설정 파일이 없습니다.') from None
        except (OSError, UnicodeError, configparser.Error):
            raise RuntimeError('PikPak Rclone 설정 파일 읽기 실패') from None
        if not parser.has_section(self.remote_name):
            return conf, None
        section = parser[self.remote_name]
        if section.get('type') != 'pikpak':
            raise RuntimeError('선택한 Rclone 리모트가 PikPak 유형이 아닙니다.')
        user = section.get('user', '').strip()
        if self.username and self.username.lower() != user.lower():
            raise RuntimeError('PikPak 계정 불일치: Rclone 리모트 계정을 확인해주세요.')
        try:
            token = json.loads(section.get('token', '{}'))
            captcha = json.loads(section.get('captcha_token') or '{}')
            if not isinstance(token, dict) or not isinstance(captcha, dict):
                raise ValueError()
            access, device = token.get('access_token'), section.get('device_id', '')
            cap = captcha.get('captcha_token') or ''
            if not isinstance(access, str) or not access or len(device) != 32 or not isinstance(cap, str):
                raise ValueError()
            expiry, cap_expiry = self._expiry(token.get('expiry')), self._expiry(captcha.get('expiry'))
            if 'expires_in' in captcha and (type(captcha['expires_in']) not in (int, float) or captcha['expires_in'] < 0):
                raise ValueError()
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise RuntimeError('PikPak Rclone 토큰 정보 읽기 실패') from None
        # Rclone이 교체/폐기할 값도 기존 진단 마스킹 경로에만 보관한다.
        DownloadUtil.engine_error_detail(self, '', sensitive_values={
            'token': token, 'captcha': captcha, 'identity': {
                'device_id': device, 'user': user, 'password': section.get('pass', '')}})
        sub = ''
        try:
            payload = access.split('.')[1]
            identity = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
            sub = str(identity.get('sub') or '')
        except (ValueError, TypeError, IndexError, AttributeError):
            pass
        return conf, dict(access_token=access, expiry=expiry, device_id=device, sub=sub,
            captcha_token=cap, captcha_expiry=cap_expiry - 10, user=user,
            user_agent=section.get('user_agent') or self.DEFAULT_USER_AGENT,
            has_refresh_token=bool(token.get('refresh_token')))

    def _apply_rclone_auth(self, conf, auth):
        self.rclone_conf = conf
        self.session = self._sessions.setdefault(('rclone', conf, self.remote_name), {})
        marker = (auth['device_id'], auth['sub'], auth['captcha_token'], auth['captcha_expiry'])
        # 설정이 그대로라면 유효한 메모리 캡차를 유지하고, 외부 갱신은 우선 반영한다.
        if self.session.get('config_captcha') != marker or self.session.get('captcha_expiry', 0) <= time.time():
            self.session.update(captcha_token=auth['captcha_token'], captcha_expiry=auth['captcha_expiry'])
        self.session.update({key: value for key, value in auth.items() if key not in ('captcha_token', 'captcha_expiry')})
        self.session['config_captcha'] = marker

    def _select_auth(self):
        conf, auth = self._read_rclone_auth()
        if auth is not None:
            self._apply_rclone_auth(conf, auth)
            return
        self.rclone_conf = None
        self.password = str(self.config.get('password') or '')
        key = ('legacy', hashlib.sha256((self.username + '\0' + self.password).encode()).hexdigest())
        if key not in self._sessions:
            self._sessions[key] = {'device_id': uuid.uuid4().hex}
        self.session = self._sessions[key]

    def _check_rclone(self):
        try:
            result = subprocess.run(['rclone', 'lsf', self.remote_name + ':', '--config', self.rclone_conf,
                '--max-depth', '1'], shell=False, timeout=30, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')
        except (subprocess.TimeoutExpired, OSError) as exc:
            detail = DownloadUtil.registration_exception(self, exc, operation='Rclone lsf')
            if getattr(exc, 'stderr', None):
                DownloadUtil.log_engine_error(self, 'Rclone lsf stderr', exc.stderr)
            raise RuntimeError(f'PikPak Rclone lsf {detail}') from None
        if result.returncode:
            self._registration_failure = {'kind': 'transient', 'not_submitted': True}
            detail = DownloadUtil.log_engine_error(self, 'Rclone lsf',
                f'returncode={result.returncode}\n{result.stderr or "stderr 없음"}')
            raise RuntimeError(f'PikPak Rclone lsf {detail}')

    def _refresh_rclone(self, previous, rejected=False):
        auth_error = self._auth_error
        self._auth_error = None
        self._check_rclone()
        conf, auth = self._read_rclone_auth()
        if auth is None:
            raise RuntimeError('PikPak Rclone 리모트 인증 정보를 확인하지 못했습니다.')
        self._apply_rclone_auth(conf, auth)
        if auth['access_token'] == previous or auth['expiry'] <= time.time() + 30:
            if rejected and auth_error:
                self._auth_error = auth_error
                raise RuntimeError(auth_error[0])
            raise RuntimeError('PikPak Rclone 인증 토큰이 갱신되지 않았습니다. 리모트 인증을 확인해주세요.')

    @classmethod
    def rclone_auth_action(cls, remote_name, username='', action='import'):
        """수동 버튼 전용 결과에는 계정명·구성 여부만 포함한다."""
        if action not in ('validate', 'import'):
            return {'ret': 'fail', 'msg': '지원하지 않는 PikPak 인증 작업입니다.'}
        if not str(remote_name or '').strip():
            return {'ret': 'fail', 'msg': 'PikPak Rclone 리모트명을 입력해주세요.'}
        engine = cls({'remote_name': remote_name, 'username': username})
        try:
            with cls._session_lock:
                conf, auth = engine._read_rclone_auth()
                if not conf:
                    raise RuntimeError('PikPak Rclone 설정 파일이 지정되지 않았습니다.')
                if auth is None:
                    raise RuntimeError('PikPak Rclone 리모트가 없습니다.')
                engine._apply_rclone_auth(conf, auth)
                if action == 'validate':
                    engine._check_rclone()
                    conf, auth = engine._read_rclone_auth()
                    if auth is None:
                        raise RuntimeError('PikPak Rclone 리모트가 없습니다.')
                    engine._apply_rclone_auth(conf, auth)
                return {'ret': 'success', 'remote_name': engine.remote_name, 'user': auth['user'],
                    'has_token': bool(auth['access_token']), 'has_captcha': bool(auth['captcha_token']),
                    'msg': ('Rclone 설정·연결 정상 · 계정 일치 확인' if username else 'Rclone 설정·연결 정상 · 리모트 계정 사용')
                           if action == 'validate' else 'Rclone 인증 정보 확인·적용 완료 (토큰은 서버 내부에서만 사용)'}
        except RuntimeError as exc:
            return {'ret': 'fail', 'msg': DownloadUtil.engine_error_detail(engine, exc)}

    def classify_error(self, context):
        if self._auth_error and context.get('error') == self._auth_error[0]:
            return self._auth_error[1]
        return None

    @staticmethod
    def _captcha_diagnostic(method, path, code, stage):
        path = re.sub(r'^(/drive/v1/(?:files|tasks))/.*', r'\1/:id', urlsplit(path).path)
        code = str(code)
        safe_code = code if re.fullmatch(r'[0-9]{1,6}', code) else 'unknown'
        logger.warning('[PikPak] %s %s · error_code=%s · 캡차 %s', method, path, safe_code, stage)

    def _http(self, method, url, **kwargs):
        self._captcha_invalid = False
        DownloadUtil.engine_error_detail(self, '', sensitive_values=kwargs)
        registration = (method == 'POST' and urlsplit(url).path == '/drive/v1/files' and
                        (kwargs.get('json') or {}).get('kind') != 'drive#folder')
        if registration:
            self._registration_sent = True
        headers = {'Referer': 'https://mypikpak.com/', 'x-client-id': self.CLIENT_ID,
                   'x-client-version': self.CLIENT_VERSION, 'x-device-id': self.session['device_id'],
                   'User-Agent': self.session.get('user_agent') or self.DEFAULT_USER_AGENT}
        headers.update(kwargs.pop('headers', {}))
        response = None
        try:
            response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
            try:
                data = response.json() if response.content else {}
            except ValueError:
                if response.status_code != 401:
                    raise
                data = {}
        except (requests.RequestException, ValueError) as exc:
            code = f'HTTP {response.status_code}' if response is not None else '응답 미수신'
            detail = DownloadUtil.registration_exception(self, exc, submitted=registration,
                operation=f'{method} {urlsplit(url).hostname}{urlsplit(url).path} · {code}')
            raise RuntimeError(f'PikPak {method} {urlsplit(url).hostname}{urlsplit(url).path} · {code}: {detail}') from None
        DownloadUtil.engine_error_detail(self, '', sensitive_values=data)
        reason = data.get('error') if isinstance(data, dict) else None
        diagnostic = {key: data[key] for key in ('error', 'error_code', 'code', 'error_description', 'message')
                      if isinstance(data, dict) and key in data}
        detail = ''
        if not 200 <= response.status_code < 300 or reason or not isinstance(data, dict):
            detail = DownloadUtil.log_engine_error(self, f'{method} {urlsplit(url).path}',
                f'HTTP {response.status_code}: {json.dumps(diagnostic, ensure_ascii=False)}')
            self._registration_failure = {'kind': 'transient',
                'not_submitted': not registration or response.status_code < 500}
            if registration and str(reason).lower() in ('invalid_argument', 'invalid_url', 'invalid_magnet', 'unsupported_protocol'):
                self._registration_failure = {'kind': 'permanent', 'not_submitted': True}
            elif registration and str(reason).lower() in ('too_many_tasks', 'task_limit_exceeded', 'queue_full'):
                self._registration_failure = {'kind': 'queue', 'not_submitted': True}
        if reason and 'captcha' in str(reason).lower():
            self._captcha_code = data.get('error_code', data.get('code'))
            self._captcha_invalid = (reason == 'captcha_invalid' and str(self._captcha_code) in ('9', '4002')
                                     and not data.get('url') and not data.get('error_url'))
            self._captcha_diagnostic(method, url, self._captcha_code, '거절')
            raise RuntimeError(f'PikPak 인증/캡차 확인 필요 · {detail}')
        if response.status_code == 401 or reason in ('account_locked', 'account_banned', 'user_locked', 'user_banned'):
            code = 'AUTH_ACCOUNT_LOCKED' if reason in ('account_locked', 'account_banned', 'user_locked', 'user_banned') else 'HTTP_401'
            message = 'PikPak API 인증 오류 (401)' if code == 'HTTP_401' else 'PikPak 계정 잠금/정지'
            self._auth_error = (message, dict(category='authentication', scope='engine', code=code,
                action='disable_engine', origin='service_api'))
            raise RuntimeError(message)
        if not 200 <= response.status_code < 300 or reason or not isinstance(data, dict):
            raise RuntimeError(f'PikPak {method} {urlsplit(url).path} · {detail}')
        return data

    def _captcha(self, method, path):
        if self.session.get('captcha_token') and self.session.get('captcha_expiry', 0) > time.time():
            return self.session['captcha_token']
        action = f'{method}:{path}'
        meta = {'username': self.username}
        if action != 'POST:/v1/auth/signin':
            timestamp = str(int(time.time() * 1000))
            sign = self.CLIENT_ID + self.CLIENT_VERSION + 'mypikpak.com' + self.session['device_id'] + timestamp
            # 공개 Rclone 클라이언트의 웹 클라이언트 서명 규칙을 따른다.
            for salt in ('C9qPpZLN8ucRTaTiUMWYS9cQvWOE', '+r6CQVxjzJV6LCV', 'F', 'pFJRC',
                         '9WXYIDGrwTCz2OiVlgZa90qpECPD6olt', '/750aCr4lm/Sly/c', 'RB+DT/gZCrbV', '',
                         'CyLsf7hdkIRxRm215hl', '7xHvLi2tOYP0Y92b', 'ZGTXXxu8E/MIWaEDB+Sm/', '1UI3',
                         'E7fP5Pfijd+7K+t6Tg/NhuLq0eEUVChpJSkrKxpO', 'ihtqpG6FMt65+Xk+tWUH2', 'NhXXU9rg4XXdzo7u5o'):
                sign = hashlib.md5((sign + salt).encode()).hexdigest()
            meta = {'captcha_sign': '1.' + sign, 'timestamp': timestamp, 'client_version': self.CLIENT_VERSION,
                    'package_name': 'mypikpak.com', 'user_id': self.session.get('sub', '')}
        data = self._http('POST', 'https://user.mypikpak.com/v1/shield/captcha/init', json={
            'action': action, 'client_id': self.CLIENT_ID, 'device_id': self.session['device_id'],
            'captcha_token': self.session.get('captcha_token', ''), 'meta': meta})
        if not data.get('captcha_token') or data.get('url'):
            self._captcha_diagnostic(method, path, None, '발급 실패 또는 수동 인증 필요')
            raise RuntimeError('PikPak 인증/캡차 확인 필요')
        self.session.update(captcha_token=data['captcha_token'], captcha_expiry=time.time() + int(data.get('expires_in') or 300) - 10)
        return data['captcha_token']

    def _token(self, refresh=False):
        if not refresh and self.session.get('access_token') and self.session.get('expiry', 0) > time.time():
            return
        if refresh or self.session.get('refresh_token'):
            if not self.session.get('refresh_token'):
                raise RuntimeError(self._auth_error[0] if self._auth_error else 'PikPak 인증 토큰 갱신 불가: 계정 인증을 확인해주세요.')
            data = self._http('POST', 'https://user.mypikpak.com/v1/auth/token', json={
                'client_id': self.CLIENT_ID, 'grant_type': 'refresh_token', 'refresh_token': self.session['refresh_token']})
        else:
            if not self.username or not self.password:
                raise RuntimeError('PikPak 계정과 비밀번호가 필요합니다.')
            captcha = self._captcha('POST', '/v1/auth/signin')
            data = self._http('POST', 'https://user.mypikpak.com/v1/auth/signin',
                headers={'x-captcha-token': captcha}, json={'username': self.username,
                'password': self.password, 'client_id': self.CLIENT_ID})
        if not data.get('access_token'):
            raise RuntimeError('PikPak 인증 토큰을 확인하지 못했습니다.')
        self.session.update(access_token=data['access_token'], refresh_token=data.get('refresh_token', self.session.get('refresh_token')),
                            sub=data.get('sub', self.session.get('sub', '')), expiry=time.time() + int(data.get('expires_in') or 3600) - 10)

    def _request(self, method, path, **kwargs):
        with self._session_lock:
            self._auth_error = None
            self._select_auth()
            oauth_refreshed = False
            if self.rclone_conf:
                if self.session['expiry'] <= time.time() + 30:
                    self._refresh_rclone(self.session['access_token'])
                    oauth_refreshed = True
            else:
                self._token()
            captcha_refreshed = not (self.session.get('captcha_token') and self.session.get('captcha_expiry', 0) > time.time())
            captcha = self._captcha(method, path)
            while True:
                try:
                    return self._http(method, 'https://api-drive.mypikpak.com' + path,
                        headers={'Authorization': 'Bearer ' + self.session['access_token'], 'x-captcha-token': captcha}, **kwargs)
                except RuntimeError:
                    if self._captcha_invalid and not captcha_refreshed:
                        code = self._captcha_code
                        self._captcha_diagnostic(method, path, code, '갱신 시도')
                        captcha_refreshed = True
                        self.session['captcha_token'] = ''
                        self.session['captcha_expiry'] = 0
                        try:
                            captcha = self._captcha(method, path)
                        except RuntimeError:
                            self._captcha_diagnostic(method, path, code, '갱신 실패')
                            raise
                    elif self._auth_error and self._auth_error[1]['code'] == 'HTTP_401' and not oauth_refreshed:
                        oauth_refreshed = True
                        if self.rclone_conf:
                            self._refresh_rclone(self.session['access_token'], rejected=True)
                            if not (self.session.get('captcha_token') and self.session.get('captcha_expiry', 0) > time.time()):
                                if captcha_refreshed:
                                    raise RuntimeError('PikPak 인증/캡차 확인 필요')
                                captcha_refreshed = True
                            captcha = self._captcha(method, path)
                        else:
                            self._token(refresh=True)
                        self._auth_error = None
                    else:
                        if self._captcha_invalid:
                            self._captcha_diagnostic(method, path, self._captcha_code, '갱신 한도 소진')
                        raise

    @staticmethod
    def _vip_expiry(value):
        """시간대가 명시된 RFC3339만 UTC로 해석한다. 3.10용으로 소수 초를 정규화한다."""
        from datetime import datetime, timezone
        match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})',
                             value if isinstance(value, str) else '')
        if not match:
            raise ValueError('VIP 만료 시각에 유효한 RFC3339 시간대가 필요합니다.')
        fraction = '.' + match[2][:6].ljust(6, '0') if match[2] else ''
        offset = '+00:00' if match[3] == 'Z' else match[3]
        return datetime.fromisoformat(match[1] + fraction + offset).astimezone(timezone.utc)

    def registration_eligibility(self, force=False, refresh=True):
        """계정별 엔진 인스턴스에서 5분간 보관하되 등록 마감은 매번 다시 비교한다."""
        from datetime import datetime, timedelta, timezone
        now = time.time()
        stale = not self._vip_cache or not 0 <= now - self._vip_checked_at < 300
        if force or (refresh and stale):
            state = {'state': 'unknown', 'expires_at_utc': None, 'registration_cutoff_utc': None,
                     'checked_at_utc': None, 'reason': ''}
            try:
                response = self._request('GET', '/drive/v1/privilege/vip')
                data = response.get('data') if isinstance(response, dict) else None
                if (not isinstance(data, dict) or response.get('result') != 'ACCEPTED' or
                        data.get('type') not in ('platinum', 'novip') or data.get('status') not in ('ok', 'invalid')):
                    raise ValueError('VIP 응답 형식 또는 결과를 확인하지 못했습니다.')
                if data['type'] == 'novip' or data['status'] == 'invalid':
                    state.update(state='not_vip', reason='PikPak 일반 계정: VIP 전용 신규 등록 불가')
                else:
                    expires = self._vip_expiry(data.get('expire'))
                    state['expires_at_utc'] = expires.isoformat()
                    state.update(state='eligible', reason='PikPak VIP: 신규 등록 가능',
                                 registration_cutoff_utc=(expires - timedelta(minutes=30)).isoformat())
            except Exception as exc:
                detail = DownloadUtil.engine_error_detail(self, f'{type(exc).__name__}: {exc}', limit=700)
                state['reason'] = f'VIP 조회 실패: 현재 엔진 재확인 대기 · {detail}'
            self._vip_checked_at = time.time()
            state['checked_at_utc'] = datetime.fromtimestamp(self._vip_checked_at, timezone.utc).isoformat()
            self._vip_cache = state
        state = dict(self._vip_cache or {})
        now = time.time()
        if not state or not 0 <= now - self._vip_checked_at < 300:
            state.update(state='unknown', expires_at_utc=None, registration_cutoff_utc=None,
                         reason='VIP 조회 실패: 확인 시각 경과 · 현재 엔진 재확인 대기')
        elif state['state'] == 'eligible' and self._vip_expiry(state['registration_cutoff_utc']).timestamp() <= now:
            expired = self._vip_expiry(state['expires_at_utc']).timestamp() <= now
            state.update(state='cutoff', reason='VIP 만료: 신규 등록 중단' if expired else 'VIP 만료 30분 전: 신규 등록 중단')
        return state

    @staticmethod
    def vip_summary(state):
        """연결 테스트와 항목 안내에는 안전한 판정 결과 및 KST 시각만 표시한다."""
        from datetime import datetime, timedelta, timezone
        kst = timezone(timedelta(hours=9))
        parts = [state['reason']]
        for key, label in (('expires_at_utc', 'VIP 만료(KST)'),
                           ('registration_cutoff_utc', '신규 등록 마감(KST)'), ('checked_at_utc', '마지막 확인 시각(KST)')):
            if state.get(key):
                parts.append(label + ': ' + datetime.fromisoformat(state[key]).astimezone(kst).strftime('%Y-%m-%d %H:%M:%S'))
        return ' · '.join(parts)

    def test_connection(self):
        state = self.registration_eligibility(force=True)
        success = state['state'] != 'unknown'
        return success, ('PikPak API 연결 성공 · ' if success else '') + self.vip_summary(state)

    def cached_magnet(self, link):
        """작업을 만들지 않고 전체 파일의 캐시 유무만 확인한다."""
        if str(self.config.get('cached_only', True)).strip().lower() in ('false', '0', 'off', 'no'):
            return 'cached', ''
        try:
            response = self._request('POST', '/drive/v1/resource/list', json={
                'urls': link, 'page_size': 500, 'thumbnail_type': 'FROM_HASH'})
            listing = response.get('list') if isinstance(response, dict) else None
            if not isinstance(listing, dict) or not isinstance(listing.get('resources'), list):
                raise RuntimeError('PikPak 캐시 응답 형식 확인 불가')
            if response.get('next_page_token'):
                raise RuntimeError('PikPak 캐시 응답 목록이 완전하지 않습니다.')
            pending, files, missing = [listing], 0, False
            while pending:
                FeederUtil.cooperate()
                directory = pending.pop()
                if directory.get('next_page_token'):
                    raise RuntimeError('PikPak 캐시 응답 목록이 완전하지 않습니다.')
                entries = directory.get('resources')
                if not isinstance(entries, list):
                    raise RuntimeError('PikPak 캐시 파일 목록 확인 불가')
                for entry in entries:
                    if not isinstance(entry, dict) or not isinstance(entry.get('name'), str) or not entry['name']:
                        raise RuntimeError('PikPak 캐시 파일 메타데이터 확인 불가')
                    if entry.get('is_dir') is True:
                        child = entry.get('dir')
                        if not isinstance(child, dict):
                            raise RuntimeError('PikPak 캐시 폴더 목록 확인 불가')
                        pending.append(child)
                    else:
                        if entry.get('is_dir') not in (False, None):
                            raise RuntimeError('PikPak 캐시 파일 종류 확인 불가')
                        size = entry.get('file_size')
                        if (isinstance(size, bool) or not re.fullmatch(r'[0-9]+', str(size)) or
                                len(str(size)) > 19 or int(size) > 2**63 - 1):
                            raise RuntimeError('PikPak 캐시 파일 크기 확인 불가')
                        meta = entry.get('meta')
                        if not isinstance(meta, dict):
                            raise RuntimeError('PikPak 캐시 hash 메타데이터 확인 불가')
                        gcid = meta.get('hash')
                        if gcid not in (None, '') and (not isinstance(gcid, str) or not re.fullmatch(r'[0-9a-fA-F]{40}', gcid)):
                            raise RuntimeError('PikPak 캐시 hash 형식 확인 불가')
                        files += 1
                        missing = missing or not gcid
            return ('miss', 'PikPak 캐시 미충족: 전체 파일이 캐시되지 않았습니다.') if missing or not files else ('cached', '')
        except Exception as exc:
            return 'unknown', 'PikPak 캐시 확인 불가: ' + DownloadUtil.engine_error_detail(self, exc)

    def _api_children(self, parent):
        rows, token, seen = [], '', set()
        while True:
            FeederUtil.cooperate()
            data = self._request('GET', '/drive/v1/files', params={'parent_id': parent, 'limit': 500,
                'page_token': token, 'filters': json.dumps({'trashed': {'eq': False}})})
            if not isinstance(data.get('files'), list):
                raise RuntimeError('PikPak 폴더 목록 확인 불가')
            for entry in data['files']:
                if not isinstance(entry, dict) or not entry.get('id') or str(entry.get('parent_id') or '') != str(parent):
                    raise RuntimeError('PikPak 폴더 부모 ID 확인 불가')
                if not entry.get('trashed'):
                    rows.append(entry)
            token = data.get('next_page_token') or ''
            if not token:
                return rows
            if token in seen:
                raise RuntimeError('PikPak 폴더 목록 페이지 확인 실패')
            seen.add(token)

    def _download_root(self):
        parent = str(self.config.get('parent_id') or '').strip()
        if parent:
            node = self._request('GET', '/drive/v1/files/' + quote(parent, safe=''))
            if str(node.get('id') or '') != parent or node.get('kind') != 'drive#folder' or node.get('trashed'):
                raise RuntimeError('PikPak 다운로드 루트 ID 확인 불가')
            return parent
        roots = [node for node in self._api_children('') if node.get('folder_type') == 'DOWNLOAD' and node.get('kind') == 'drive#folder']
        if len(roots) != 1:
            raise RuntimeError('PikPak 기본 다운로드 폴더 ID 확인 불가')
        return str(roots[0]['id'])

    def _profile_parent(self, upload_path, create=True):
        path = str(upload_path or '').strip('/')
        error = FeederUtil.relative_path_error(path) if path else None
        if error:
            raise RuntimeError(error)
        parent = self._download_root()
        protected = {parent}
        for part in path.split('/') if path else []:
            matches = [node for node in self._api_children(parent) if node.get('name') == part]
            if not matches and create:
                data = self._request('POST', '/drive/v1/files', json={'kind': 'drive#folder', 'name': part, 'parent_id': parent})
                node = data.get('file') or {}
                if not node.get('id'):
                    raise RuntimeError('PikPak 프로필 폴더 생성 결과 확인 불가')
                matches = [self._request('GET', '/drive/v1/files/' + quote(str(node['id']), safe=''))]
            if not matches and not create:
                return None, protected
            if (len(matches) != 1 or not matches[0].get('id') or matches[0].get('kind') != 'drive#folder' or
                    str(matches[0].get('parent_id') or '') != parent or matches[0].get('name') != part or matches[0].get('trashed')):
                raise RuntimeError('PikPak 프로필 폴더 ID 확인 불가')
            parent = str(matches[0]['id'])
            protected.add(parent)
        return parent, protected

    def _rclone_entries(self, path):
        conf = self.rclone_conf or P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        cmd = ['rclone', 'lsjson', self.remote_name + ':' + path, '--no-modtime', '--no-mimetype']
        if conf:
            cmd.extend(['--config', conf])
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                    encoding='utf-8', errors='replace', timeout=30, shell=False)
            if result.returncode:
                raise RuntimeError(f'Rclone lsjson returncode={result.returncode}: {result.stderr}')
            rows = json.loads(result.stdout)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError('Rclone 폴더 목록 형식 확인 불가')
            return rows
        except (OSError, subprocess.TimeoutExpired, ValueError, RuntimeError) as exc:
            raise RuntimeError(DownloadUtil.log_engine_error(self, 'Rclone 원본 경로 조회', exc)) from None

    @staticmethod
    def _rclone_child(rows, file_id, kind):
        matches = [row for row in rows if str(row.get('ID') or '') == str(file_id)]
        if len(matches) != 1 or matches[0].get('IsDir') is not (kind == 'drive#folder'):
            raise RuntimeError('PikPak 원본 ID/종류와 Rclone 객체가 일치하지 않습니다.')
        node = matches[0]
        name = node.get('Path') or node.get('Name')
        if (not isinstance(name, str) or not name or name in ('.', '..') or '/' in name or '\x00' in name or
                sum((row.get('Path') or row.get('Name')) == name for row in rows) != 1):
            raise RuntimeError('PikPak Rclone 원본 경로가 모호합니다.')
        return name

    def add_magnet(self, link, title=None, upload_path=None, new_registration=False, vip_snapshot=None):
        self._registration_sent = False
        if not str(link).startswith('magnet:'):
            self._registration_failure = {'kind': 'permanent', 'not_submitted': True}
            return False, '', 'PikPak은 마그넷 링크만 지원합니다.'
        payload = {'kind': 'drive#file', 'upload_type': 'UPLOAD_TYPE_URL', 'url': {'url': link}}
        if title:
            payload['name'] = title
        try:
            parent, _ = self._profile_parent(upload_path)
            payload['parent_id'] = parent
            if new_registration:
                vip = vip_snapshot if vip_snapshot is not None else self.registration_eligibility(refresh=False)
                if vip['state'] != 'eligible':
                    self._registration_failure = {'kind': 'transient', 'not_submitted': True}
                    return False, '', self.vip_summary(vip)
            data = self._request('POST', '/drive/v1/files', json=payload)
            task_id = (data.get('task') or {}).get('id')
            if not task_id:
                self._registration_failure = {'kind': 'unknown', 'not_submitted': False}
                raise RuntimeError('PikPak 등록 응답에 작업 ID가 없습니다.')
            return True, str(task_id), ''
        except RuntimeError as exc:
            if not getattr(self, '_registration_failure', None):
                self._registration_failure = {'kind': 'unknown' if self._registration_sent else 'transient',
                                              'not_submitted': not self._registration_sent}
            return False, '', DownloadUtil.engine_error_detail(self, exc)

    def _file_path(self, file_id, _root=False):
        chain, seen = [], set()
        leaf = None
        conf = self.rclone_conf or P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        parser = configparser.RawConfigParser(interpolation=None)
        if conf:
            parser.read(conf, encoding='utf-8')
        remote_root = parser.get(self.remote_name, 'root_folder_id', fallback='')
        download_root = self._download_root()
        original_id = str(file_id or '')
        if original_id == download_root and not _root:
            raise RuntimeError('PikPak 완료 파일 ID 준비 대기')
        if _root and original_id == remote_root:
            return self._request('GET', '/drive/v1/files/' + quote(original_id, safe='')), self.remote_name + ':'
        try:
            while file_id:
                file_id = str(file_id)
                if remote_root and file_id == remote_root:
                    break
                if file_id in seen:
                    raise RuntimeError('PikPak 파일 부모 경로를 확인하지 못했습니다.')
                seen.add(file_id)
                meta = self._request('GET', '/drive/v1/files/' + quote(str(file_id), safe=''))
                if (str(meta.get('id') or '') != file_id or not meta.get('name') or meta.get('trashed') or
                        meta.get('kind') not in ('drive#file', 'drive#folder')):
                    raise RuntimeError('PikPak 완료 파일 경로 준비 대기')
                leaf = leaf or meta
                chain.append(meta)
                file_id = meta.get('parent_id')
            if (not chain or download_root not in seen | {remote_root} or
                    (remote_root and str(file_id or '') != remote_root)):
                raise RuntimeError('PikPak 완료 파일 ID 준비 대기')
            path = ''
            for meta in reversed(chain):
                name = self._rclone_child(self._rclone_entries(path), meta['id'], meta['kind'])
                path = path + '/' + name if path else name
            return leaf, self.remote_name + ':' + path
        except RuntimeError as original_error:
            if _root or not leaf:
                raise
            # 예상 위치에 없을 때만 같은 다운로드 루트의 직계 하위에서 같은 ID를 찾는다.
            _, root_source = self._file_path(download_root, _root=True)
            root_path = root_source[len(self.remote_name) + 1:]
            try:
                name = self._rclone_child(self._rclone_entries(root_path), original_id, leaf['kind'])
            except RuntimeError:
                raise original_error
            return leaf, root_source + ('/' if root_path else '') + name

    def get_status(self, task_ids=None, resolve_paths=True):
        self.status_list_complete = False
        rows, token, seen = [], '', set()
        try:
            while True:
                data = self._request('GET', '/drive/v1/tasks', params={'type': 'offline', 'limit': 500, 'page_token': token})
                tasks = data.get('tasks')
                if not isinstance(tasks, list):
                    raise RuntimeError('PikPak 작업 목록 응답을 확인하지 못했습니다.')
                for task in tasks:
                    FeederUtil.cooperate()
                    if task_ids is not None and str(task.get('id')) not in {str(t) for t in task_ids}:
                        continue
                    phase = task.get('phase')
                    status = {'PHASE_TYPE_COMPLETE': 'completed', 'PHASE_TYPE_ERROR': 'error',
                              'PHASE_TYPE_PENDING': 'downloading', 'PHASE_TYPE_RUNNING': 'downloading'}.get(phase, 'downloading')
                    size = int(task.get('file_size') or 0)
                    progress = min(100.0, max(0.0, float(task.get('progress') or 0)))
                    name = task.get('file_name') or task.get('name') or ''
                    source, meta, path_error = '', {}, ''
                    if status == 'completed':
                        progress = 100.0
                    if status == 'completed' and resolve_paths:
                        try:
                            meta, source = self._file_path(task.get('file_id'))
                            name = meta['name']
                            size = int(meta.get('size') or size)
                        except Exception as exc:
                            # 다운로드 완료 판정과 업로드용 경로 준비를 분리한다.
                            path_error = redact_log(DownloadUtil.engine_error_detail(self, exc))
                    rows.append(dict(task_id=str(task['id']), hash='', filename=name, file_size=size,
                        downloaded_bytes=int(size * progress / 100), download_speed=0, progress=progress,
                        status=status, source_path=source, source_file_id=meta.get('id') or task.get('file_id'), source_kind=meta.get('kind'),
                        error_message=path_error or ('PikPak 다운로드 작업 오류' if status == 'error' else '')))
                token = data.get('next_page_token') or ''
                if not token:
                    self.status_list_complete = True
                    break
                if token in seen:
                    raise RuntimeError('PikPak 작업 목록 페이지 확인 실패')
                seen.add(token)
            return rows, ''
        except (RuntimeError, ValueError, TypeError, KeyError) as exc:
            return rows, DownloadUtil.engine_error_detail(self, exc) if isinstance(exc, RuntimeError) else 'PikPak 작업 메타데이터 확인 실패'

    def delete_completed_source(self, task_id, upload_path='', expected_source=None):
        """최종 업로드 성공 호출자만 사용하는 단일 원본 영구 삭제. 이력 삭제와 분리한다."""
        try:
            rows, error = self.get_status([str(task_id)])
            matches = [row for row in rows if str(row.get('task_id')) == str(task_id) and row.get('status') == 'completed']
            if error or len(matches) != 1:
                raise RuntimeError(error or 'PikPak 완료 작업 원본 확인 불가')
            row = matches[0]
            file_id, kind, source = row.get('source_file_id'), row.get('source_kind'), row.get('source_path')
            if not file_id or kind not in ('drive#file', 'drive#folder') or not source or (expected_source and source != expected_source):
                raise RuntimeError('PikPak 완료 원본 ID/경로가 일치하지 않습니다.')
            parent, protected = self._profile_parent(upload_path, create=False)
            root = self._download_root()
            leaf, resolved = self._file_path(file_id)
            if (str(file_id) in protected or str(leaf.get('parent_id') or '') not in {root, parent} or resolved != source):
                raise RuntimeError('PikPak 공용 부모 또는 예상 위치 밖 원본은 삭제하지 않습니다.')
            if kind == 'drive#folder':
                folders, visited = [str(file_id)], set()
                while folders:
                    folder = folders.pop()
                    if folder in visited:
                        raise RuntimeError('PikPak 원본 폴더 귀속 확인 불가')
                    visited.add(folder)
                    for child in self._api_children(folder):
                        if child.get('kind') == 'drive#folder':
                            folders.append(str(child['id']))
                        elif child.get('kind') == 'drive#file':
                            detail = self._request('GET', '/drive/v1/files/' + quote(str(child['id']), safe=''))
                            if (str(detail.get('id') or '') != str(child['id']) or str(detail.get('parent_id') or '') != folder or
                                    str((detail.get('params') or {}).get('task_id') or '') != str(task_id)):
                                raise RuntimeError('PikPak 원본 폴더에 작업 귀속을 확인하지 못한 파일이 있습니다.')
                        else:
                            raise RuntimeError('PikPak 원본 파일 종류 확인 불가')
            relative = source[len(self.remote_name) + 1:]
            source_parent, _, name = relative.rpartition('/')
            if self._rclone_child(self._rclone_entries(source_parent), file_id, kind) != name:
                raise RuntimeError('PikPak 삭제 직전 원본 ID 확인 실패')
            conf = self.rclone_conf or P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
            cmd = ['rclone', 'purge' if kind == 'drive#folder' else 'deletefile', source, '--pikpak-use-trash=false']
            if conf:
                cmd.extend(['--config', conf])
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                    encoding='utf-8', errors='replace', timeout=300, shell=False)
            if result.returncode:
                raise RuntimeError(f'PikPak 원본 삭제 returncode={result.returncode}: {result.stderr}')
            after = self._rclone_entries(source_parent)
            if any(str(entry.get('ID') or '') == str(file_id) for entry in after):
                raise RuntimeError('PikPak 원본 제거 여부를 확인하지 못했습니다.')
            return True, ''
        except Exception as exc:
            return False, DownloadUtil.log_engine_error(self, '완료 원본 정리 보류', exc)

    def delete_task(self, task_id, delete_files=False):
        try:
            self._request('DELETE', '/drive/v1/tasks', params={'task_ids': str(task_id), 'delete_files': 'true' if delete_files else 'false'})
            return True
        except RuntimeError as exc:
            FeederUtil.engine_auth_error(self, DownloadUtil.engine_error_detail(self, exc), 'delete_task')
            return False


class CloudDrive2Engine(BaseDownloadEngine):
    REGISTRATION_HASH_LOOKUP = True
    """CloudDrive2 (CD2 gRPC 연동) 공식 내장 다운로더 엔진"""
    ENGINE_ID = "cd2"
    ENGINE_NAME = "CloudDrive2 (CD2)"
    OUTPUT_TYPE = "cloud_storage"
    SUPPORTED_PROTOCOLS = ["magnet", "ed2k"]

    CONFIG_SCHEMA = [
        {"name": "cd2_addr", "label": "CD2 주소", "type": "text", "default": "127.0.0.1", "required": True},
        {"name": "cd2_port", "label": "CD2 포트 (gRPC)", "type": "number", "default": 19798},
        {"name": "cd2_token", "label": "CD2 인증 토큰", "type": "password", "placeholder": "CloudDrive2 JWT 토큰"},
        {"name": "cd2_direct_worker", "label": "원격 워커에 직접 URL 전송", "type": "checkbox", "default": False, "desc": "원격 워커 목적지에서만 CD2 마운트 대신 파일별 직접 다운로드 URL을 사용합니다."},
        {"name": "cd2_mount_path", "label": "CD2 로컬 마운트 경로", "type": "text", "placeholder": "예: /mnt/cd2", "desc": "로컬 시스템에 CD2가 마운트된 FUSE 루트 디렉터리 (예: /mnt/cd2)"},
        {"name": "cd2_virtual_path", "label": "마그넷 저장 가상 경로", "type": "text", "default": "", "placeholder": "예: cloud/offline-downloads", "desc": "CD2 내부에서 오프라인 다운로드가 저장될 클라우드 상대 경로. 로컬 실물 탐색 시 [마운트 경로] + [가상 경로]가 자동 결합됩니다."},
        {"name": "use_local_staging", "label": "로컬 스테이징 기본 사용", "type": "checkbox", "default": False, "desc": "CD2 마운트는 기본적으로 직접 전송합니다. 다운로드 프로필에서 명시하면 프로필 설정이 우선합니다."},
        {"name": "cd2_max_pages", "label": "작업 목록 최대 탐색 페이지 수", "type": "number", "default": 1, "desc": "오프라인 작업 목록 조회 시 최대 탐색할 페이지 수 (기본값: 1페이지)"},
        {"name": "cd2_cleanup_manual_completed", "label": "수동 등록 완료 히스토리 자동 정리", "type": "checkbox", "default": False, "desc": "CD2에서 직접 추가한 작업 중 다운로드가 완료된 기록을 Feeder의 정기 상태 확인 시 정리합니다. 진행 중·실패 작업과 실제 115 파일은 삭제하지 않습니다. CD2 작업 목록 조회 페이지 설정 범위에서만 적용됩니다."},
        {"name": "cd2_timeout", "label": "gRPC 타임아웃 (초)", "type": "number", "default": 30, "desc": "gRPC 요청 제한 시간(초) (기본값: 30초)"},
        {"name": "stalled_timeout_hours", "label": "지연 타임아웃 (시간)", "type": "number", "default": 24, "desc": "해당 시간 동안 미완료 시 다음 엔진으로 자동 폴백 (0 입력 시 무제한 대기)"}
    ]

    def __init__(self, config: dict):
        super(CloudDrive2Engine, self).__init__(config)
        self.addr = self.config.get('cd2_addr', '127.0.0.1')
        self.port = int(self.config.get('cd2_port', 19798))
        self.token = self.config.get('cd2_token', '').strip()
        raw_vpath = str(self.config.get('cd2_virtual_path') or '').strip()
        self.vpath = ("/" + raw_vpath.strip('/')) if raw_vpath else ""
        self.mount_path = str(self.config.get('cd2_mount_path') or '').strip().rstrip('/\\')

        try:
            self.max_pages = int(self.config.get('cd2_max_pages') or 1)
        except Exception:
            self.max_pages = 1
        try:
            self.grpc_timeout = int(self.config.get('cd2_timeout') or 30)
        except Exception:
            self.grpc_timeout = 30

    def get_local_mount_path(self) -> str:
        """로컬 파일시스템에서 마그넷 다운로드 폴더의 실제 절대 경로 반환 (자동 결합)"""
        if not self.mount_path:
            return ''
        if not self.vpath:
            return self.mount_path
        rel_vpath = self.vpath.strip('/')
        if self.mount_path.replace('\\', '/').endswith(rel_vpath):
            return self.mount_path
        return os.path.join(self.mount_path, rel_vpath)

    def direct_worker_enabled(self, destination_type):
        return destination_type == 'remote_worker' and str(self.config.get('cd2_direct_worker', False)).lower() in ('true', '1', 'on')

    def direct_source(self, filename, upload_sub=''):
        """완료 항목의 실제 CD2 경로만 확인하고 URL은 아직 발급하지 않는다."""
        stub, metadata = self._get_grpc_stub()
        if not stub:
            raise RuntimeError('CD2 gRPC 모듈이 준비되지 않았습니다.')
        path = '/'.join(part.strip('/') for part in (self.vpath, upload_sub, filename) if part)
        entry = stub.FindFileByPath(CloudDrive_pb2.FindFileByPathRequest(path='/' + path),
                                   metadata=metadata, timeout=self.grpc_timeout)
        if not entry.fullPathName:
            raise RuntimeError('CD2 완료 항목의 실제 경로를 확인하지 못했습니다.')
        return 'cd2:' + entry.fullPathName

    def direct_files(self, source):
        """새 워커 제출 직전에 파일별 URL·헤더를 발급한다. 반환값은 DB에 저장하지 않는다."""
        stub, metadata = self._get_grpc_stub()
        if not stub or not source.startswith('cd2:/'):
            raise RuntimeError('CD2 직접 전송 소스 경로를 확인해주세요.')
        root = stub.FindFileByPath(CloudDrive_pb2.FindFileByPathRequest(path=source[4:]),
                                  metadata=metadata, timeout=self.grpc_timeout)
        if not root.fullPathName:
            raise RuntimeError('CD2 직접 전송 파일 메타데이터가 없습니다.')
        pending = [(root, '')]
        files = []
        while pending:
            FeederUtil.cooperate()
            entry, relative = pending.pop()
            if entry.isDirectory:
                for page in stub.GetSubFiles(CloudDrive_pb2.ListSubFileRequest(path=entry.fullPathName),
                                            metadata=metadata, timeout=self.grpc_timeout):
                    for child in page.subFiles:
                        pending.append((child, '/'.join(p for p in (relative, child.name) if p)))
                continue
            info = stub.GetDownloadUrlPath(CloudDrive_pb2.GetDownloadUrlPathRequest(
                path=entry.fullPathName, get_direct_url=True), metadata=metadata, timeout=self.grpc_timeout)
            # 프록시 경로의 워커 접근성은 확인할 수 없으므로 directUrl만 사용한다.
            if not info.directUrl or urlsplit(info.directUrl).scheme != 'https':
                raise RuntimeError('CD2 HTTPS 직접 다운로드 URL이 없습니다. 프록시 경로의 워커 접근성을 확인할 수 없습니다.')
            headers = dict(info.additionalHeaders)
            if info.userAgent:
                headers['User-Agent'] = info.userAgent
            files.append({'relative_path': relative or entry.name, 'file_path': entry.fullPathName,
                          'size': entry.size, 'url': info.directUrl, 'headers': headers})
        if not files:
            raise RuntimeError('CD2 직접 전송할 파일이 없습니다.')
        return files

    def _get_grpc_stub(self):
        """gRPC 채널 재사용 및 스텁 생성 (소켓 낭비 방지)"""
        if not CD2_GRPC_AVAILABLE or not CloudDrive_pb2_grpc:
            return None, None
        if not hasattr(self, '_channel') or self._channel is None:
            self._channel = grpc.insecure_channel(f"{self.addr}:{self.port}")
            self._stub = CloudDrive_pb2_grpc.CloudDriveFileSrvStub(self._channel)
        metadata = [('authorization', f"Bearer {self.token}")] if self.token else []
        return self._stub, metadata

    def refresh_cache(self, path: str = None) -> list[str]:
        """CD2 가상경로 디렉터리 캐시 갱신"""
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return []

        target_path = "/" + (path or self.vpath or '').strip().strip('/')
        # 대상 경로가 비어있거나 전체 루트('/')인 경우 CD2 전체 동기화 방지를 위해 스킵
        if not target_path or target_path == "/":
            return []

        timeout_sec = min(20, max(10, self.grpc_timeout))
        found_subfiles = []

        try:
            req = CloudDrive_pb2.ListSubFileRequest(path=target_path, forceRefresh=True)
            response_stream = stub.GetSubFiles(req, metadata=metadata, timeout=timeout_sec)

            for response in response_stream:
                FeederUtil.cooperate()
                if hasattr(response, 'subFiles') and response.subFiles:
                    for item in response.subFiles:
                        FeederUtil.cooperate()
                        fname = getattr(item, 'name', None)
                        if fname:
                            found_subfiles.append(fname)

            if not found_subfiles:
                logger.warning(f"[CloudDrive2Engine] CD2 조회 결과가 비어 있습니다: {target_path}")
            else:
                logger.info(f"[CloudDrive2Engine] CD2 캐시 갱신 완료: {target_path} (총 {len(found_subfiles)}개 요소 발견)")
            return found_subfiles

        except grpc.RpcError as e:
            logger.warning(f"[CloudDrive2Engine] CD2 gRPC 갱신 실패/타임아웃 ({timeout_sec}s): 코드={e.code()} | 상세={e.details()}")
            return []
        except Exception as e:
            logger.error(f"[CloudDrive2Engine] CD2 처리 중 예외: {e}")
            return []

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        """CD2 gRPC를 통한 오프라인 다운로드 추가 (수신 경로 하위 폴더 자동 생성 및 결합)"""
        stub, metadata = self._get_grpc_stub()
        if not stub:
            self._registration_failure = {'kind': 'transient', 'not_submitted': True}
            return False, "", "CD2 gRPC 모듈(clouddrive_pb2)이 준비되지 않았습니다."

        submitted = False
        try:
            target_vpath = self.vpath
            if upload_path and str(upload_path).strip():
                clean_sub = str(upload_path).strip('/')
                target_vpath = f"{self.vpath}/{clean_sub}"

            try:
                parent_dir = os.path.dirname(target_vpath)
                folder_name = os.path.basename(target_vpath)
                req_cf = CloudDrive_pb2.CreateFolderRequest(parentPath=parent_dir, folderName=folder_name)
                stub.CreateFolder(req_cf, metadata=metadata, timeout=5)
            except Exception:
                pass

            req = CloudDrive_pb2.AddOfflineFileRequest(urls=link, toFolder=target_vpath)
            submitted = True
            res = stub.AddOfflineFiles(req, metadata=metadata, timeout=20)

            info_hash = FeederUtil.extract_info_hash(link)
            task_id = info_hash or link[:40]

            if hasattr(res, 'success') and not res.success:
                err_msg = getattr(res, 'errorMessage', 'API Task Add Failed')
                if "10008" in err_msg or "任务已存在" in err_msg:
                    logger.info(f"[CloudDrive2Engine] CD2에 이미 존재하는 작업입니다 (정상 수용): {title or link[:30]}")
                    return True, task_id, ""
                self._registration_failure = {'kind': 'transient', 'not_submitted': True}
                detail = DownloadUtil.log_engine_error(self, 'AddOfflineFiles 응답', err_msg)
                if any(word in str(err_msg).lower() for word in ('invalid magnet', 'invalid url', 'unsupported protocol')):
                    self._registration_failure = {'kind': 'permanent', 'not_submitted': True}
                return False, "", f"CD2 작업 추가 실패: {detail}"

            logger.info(f"[CloudDrive2Engine] CD2 작업 추가 성공: {title or link[:30]}")
            return True, task_id, ""

        except Exception as e:
            err_str = str(e)
            if "10008" in err_str or "任务已存在" in err_str:
                logger.info(f"[CloudDrive2Engine] CD2에 이미 존재하는 작업입니다 (정상 수용): {title or link[:30]}")
                return True, link[:40], ""
            return False, "", DownloadUtil.registration_exception(self, e, submitted=submitted)

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        """CD2 gRPC 오프라인 작업 목록 조회 및 로컬 마운트 경로 매핑"""
        self.status_list_complete = False
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return [], "CD2 gRPC 모듈 미로드"

        try:
            ret = []
            max_pages = max(1, self.max_pages)
            for page_num in range(1, max_pages + 1):
                FeederUtil.cooperate()
                try:
                    req = CloudDrive_pb2.OfflineFileListAllRequest(path=self.vpath, page=page_num)
                    res = stub.ListAllOfflineFiles(req, metadata=metadata, timeout=self.grpc_timeout)
                    if not res.offlineFiles:
                        self.status_list_complete = True
                        break

                    def _get_pb_attr(pb_obj, *attr_names, default=None):
                        for name in attr_names:
                            FeederUtil.cooperate()
                            if hasattr(pb_obj, name):
                                val = getattr(pb_obj, name)
                                if val is not None:
                                    return val
                        return default

                    for t in res.offlineFiles:
                        FeederUtil.cooperate()
                        infohash = _get_pb_attr(t, 'infoHash', 'info_hash', 'infohash', 'hash', default='')
                        infohash = str(infohash).strip().lower()
                        # 외부 이력 정리는 URL에서 추출한 값과 원본 infoHash를 구분한다.
                        cd2_infohash = infohash
                        original_url = getattr(t, 'url', '')
                        if not infohash and original_url:
                            infohash = (FeederUtil.extract_info_hash(original_url) or '').lower()

                        fname = _get_pb_attr(t, 'name', 'fileName', 'file_name', default='')
                        fsize = int(_get_pb_attr(t, 'size', 'totalSize', 'total_size', default=0) or 0)
                        status_code = int(_get_pb_attr(t, 'status', default=0) or 0)

                        # CD2 작업 상태 코드: 2 완료(FINISHED), 3 에러(ERROR), 그 외 다운로드 중
                        if status_code == 2:
                            std_status = 'completed'
                            progress_val = 100.0
                            dl_bytes = fsize
                        elif status_code == 3:
                            std_status = 'error'
                            progress_val = 0.0
                            dl_bytes = 0
                        else:
                            std_status = 'downloading'
                            raw_pct = _get_pb_attr(t, 'percent', 'percentage', 'progress', 'percentDone', default=None)
                            if raw_pct is not None:
                                pct = float(raw_pct)
                                progress_val = round(pct * 100.0, 1) if (0.0 < pct <= 1.0) else round(pct, 1)
                            else:
                                progress_val = 0.0

                            raw_bytes = _get_pb_attr(t, 'downloadedBytes', 'downloaded_bytes', 'downloaded', 'downloadedSize', default=None)
                            if raw_bytes is not None:
                                dl_bytes = int(raw_bytes)
                            elif fsize > 0 and progress_val > 0:
                                dl_bytes = int(fsize * (progress_val / 100.0))
                            else:
                                dl_bytes = 0

                        raw_speed = _get_pb_attr(t, 'speed', 'downloadSpeed', 'download_speed', 'rate', default=0)
                        dl_speed = int(raw_speed) if raw_speed else 0

                        ret.append({
                            'task_id': infohash or original_url[:40],
                            'hash': infohash,
                            'status': std_status,
                            'statusCode': status_code,
                            'cd2_infohash': cd2_infohash,
                            'filename': fname,
                            'file_size': fsize,
                            'downloaded_bytes': dl_bytes,
                            'download_speed': dl_speed,
                            'progress': progress_val,
                            'source_path': ''
                        })

                    page_count = getattr(res, 'pageCount', 0)
                    if page_count > 0 and page_num >= page_count:
                        self.status_list_complete = True
                        break
                except Exception as page_err:
                    detail = DownloadUtil.log_engine_error(self, f'GetOfflineFiles {page_num}p',
                        f'{type(page_err).__name__}: {page_err}')
                    return ret, detail

            return ret, ""

        except Exception as e:
            return [], DownloadUtil.log_engine_error(self, 'GetOfflineFiles', f'{type(e).__name__}: {e}')

    def delete_task(self, task_id: str, delete_files=False) -> bool:
        """CD2 오프라인 다운로드 히스토리에서 해당 작업 삭제"""
        return self.delete_tasks([task_id], delete_files=delete_files)

    def delete_tasks(self, task_ids: list[str], delete_files=False) -> bool:
        """기본은 원본 보존이며, 명시적으로 요청한 최종 완료 잔여만 파일도 삭제한다."""
        if not task_ids:
            return True
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return False

        # stub에서 오프라인 삭제 RPC 메서드 탐색
        method_name = None
        for name in ['DeleteOfflineFiles', 'RemoveOfflineFiles', 'DeleteOfflineFile', 'RemoveOfflineFile', 'CancelOfflineFiles']:
            FeederUtil.cooperate()
            if hasattr(stub, name):
                method_name = name
                break

        if not method_name:
            logger.debug(f"[CloudDrive2Engine] CD2 gRPC 삭제 메서드 부재: {[m for m in dir(stub) if 'offline' in m.lower()]}")
            return False

        # CloudDrive_pb2에서 요청 메시지 클래스 탐색
        req_cls = None
        for name in [f"{method_name}Request", 'DeleteOfflineFilesRequest', 'RemoveOfflineFilesRequest', 'DeleteOfflineFileRequest']:
            FeederUtil.cooperate()
            if hasattr(CloudDrive_pb2, name):
                req_cls = getattr(CloudDrive_pb2, name)
                break

        if not req_cls:
            logger.debug("[CloudDrive2Engine] CD2 gRPC 삭제 요청 클래스 부재")
            return False

        # 다른 호출자의 기본 파일 보존 정책은 유지한다.
        field_names = req_cls.DESCRIPTOR.fields_by_name.keys()
        kwargs = {}
        if 'path' in field_names:
            kwargs['path'] = self.vpath
        if 'deleteFiles' in field_names:
            kwargs['deleteFiles'] = bool(delete_files)
        elif 'delete_files' in field_names:
            kwargs['delete_files'] = bool(delete_files)
        else:
            logger.warning('[CloudDrive2Engine] 원본 파일 보존 옵션 미확인 · 히스토리 삭제 보류')
            return False

        # 32자리와 40자리 infoHash가 섞여도 어느 쪽도 누락하지 않는다.
        hashes = [tid.lower() for tid in task_ids if len(tid) in (32, 40)]
        if not hashes:
            hashes = task_ids

        if 'infoHashes' in field_names:
            kwargs['infoHashes'] = hashes
        elif 'info_hashes' in field_names:
            kwargs['info_hashes'] = hashes
        elif 'infoHash' in field_names and len(hashes) == 1:
            kwargs['infoHash'] = hashes[0]
        elif 'taskIds' in field_names:
            kwargs['taskIds'] = task_ids
        elif 'urls' in field_names:
            kwargs['urls'] = task_ids
        else:
            logger.warning('[CloudDrive2Engine] 개별 작업 식별 필드 미확인 · 히스토리 삭제 보류')
            return False

        try:
            req = req_cls(**kwargs)
            method = getattr(stub, method_name)
            method(req, metadata=metadata, timeout=15)
            logger.info(f"[CloudDrive2Engine] CD2 오프라인 히스토리 레코드 삭제 완료: {len(task_ids)}건")
            return True
        except Exception as ex:
            logger.warning(f"[CloudDrive2Engine] CD2 오프라인 히스토리 삭제 실패 ({len(task_ids)}건): {ex}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        """CloudDrive2 gRPC 통신 및 토큰 유효성 검증"""
        if not CD2_GRPC_AVAILABLE or not CloudDrive_pb2:
            return False, "clouddrive_pb2 모듈 또는 grpc 패키지가 준비되지 않았습니다."

        stub, metadata = self._get_grpc_stub()
        if not stub:
            return False, "gRPC 스텁 생성 실패"

        try:
            req = CloudDrive_pb2.OfflineFileListAllRequest(path=self.vpath, page=1)
            res = stub.ListAllOfflineFiles(req, metadata=metadata, timeout=10)
            task_count = len(res.offlineFiles) if res.offlineFiles else 0
            return True, f"CloudDrive2 (CD2) gRPC 연결 성공! (오프라인 작업 {task_count}개 감지됨)"
        except grpc.RpcError as rpc_err:
            code = rpc_err.code()
            details = rpc_err.details()
            return False, f"CD2 gRPC 인증/연결 실패: [{code.name}] {details}"
        except Exception as e:
            return False, f"CD2 gRPC 통신 예외: {str(e)}"


class LocalTransporter(BaseTransporter):
    TRANSPORTER_ID = "local"
    TRANSPORTER_NAME = "단순 경로 이동"

    CONFIG_SCHEMA = [
        {"name": "complete_path", "label": "최종 완료 이동 경로", "type": "text", "placeholder": "예: /mnt/mergerfs/uploads 또는 uploads/프로필명", "desc": "'/'로 시작하면 시스템 절대경로(mergerfs 마운트 등)로 직접 이동하며, 상대경로로 입력하면 엔진 설정 루트 하위 경로로 자동 결합됩니다."}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        target_folder = (dest_config.get('complete_path') or dest_config.get('target_folder') or '').strip()
        folder_name = getattr(item, 'output_folder_name', None) or item.file_name or os.path.basename(source_path or '')
        if source_path and os.path.isfile(source_path) and getattr(item, 'output_folder_name', None):
            folder_name += os.path.splitext(os.path.basename(source_path))[1]

        if not source_path or not os.path.exists(source_path):
            logger.error(f"[LocalTransporter] 로컬 소스 경로가 존재하지 않음: {source_path}")
            return False, "failed", f"로컬 파일 없음: {source_path}"

        final_path = source_path
        if not target_folder and getattr(item, 'output_folder_name', None):
            target_folder = os.path.dirname(os.path.abspath(source_path))
        if target_folder:
            try:
                os.makedirs(target_folder, exist_ok=True)
                dest_file = os.path.join(target_folder, folder_name)
                if os.path.exists(dest_file) and os.path.abspath(source_path) != os.path.abspath(dest_file):
                    short_hash = (item.infohash[:6] if item.infohash else f"id_{item.id}")
                    if os.path.isdir(source_path):
                        candidate_name = f"{folder_name}_[{short_hash}]"
                    else:
                        n, e = os.path.splitext(folder_name)
                        candidate_name = f"{n}_{short_hash}{e}"
                    dest_file = os.path.join(target_folder, candidate_name)

                    # 동일 hash의 기존 항목까지 있으면 추가 suffix를 붙여 덮어쓰기를 방지한다.
                    suffix = 2
                    while os.path.exists(dest_file):
                        if os.path.isdir(source_path):
                            candidate_name = f"{folder_name}_[{short_hash}]_{suffix}"
                        else:
                            n, e = os.path.splitext(folder_name)
                            candidate_name = f"{n}_{short_hash}_{suffix}{e}"
                        dest_file = os.path.join(target_folder, candidate_name)
                        suffix += 1

                if os.path.abspath(source_path) == os.path.abspath(dest_file):
                    final_path = dest_file
                else:
                    shutil.move(source_path, dest_file)
                    final_path = dest_file

                logger.info(f"[LocalTransporter] 로컬 최종 경로 이동 완료: {final_path}")
            except Exception as e:
                logger.error(f"[LocalTransporter] 로컬 파일 이동 실패: {e}")
                return False, "failed", f"로컬 이동 실패: {str(e)}"

        item.local_path = final_path
        return True, "completed", f"로컬 보존 완료 ({final_path})"


class RcloneSimpleTransporter(BaseTransporter):
    TRANSPORTER_ID = "rclone_simple"
    TRANSPORTER_NAME = "일반 Rclone 리모트 단순 업로드"

    CONFIG_SCHEMA = [
        {"name": "use_local_staging", "label": "로컬 스테이징 사용", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "사용"}, {"value": "false", "label": "사용 안 함"}],
         "desc": "사용: 원격 소스를 로컬 임시 경로에 먼저 수신합니다. 사용 안 함: 로컬 디스크를 거치지 않고 직접 전송합니다."},
        {"name": "upload_path", "label": "임시 업로드 경로", "type": "text", "placeholder": "예: incoming/프로필명", "desc": "Rclone 리모트 안에서 업로드가 먼저 완료되는 임시 경로입니다."},
        {"name": "complete_path", "label": "최종 완료 이동 경로", "type": "text", "placeholder": "예: uploads/프로필명", "desc": "임시 업로드가 성공한 뒤 최종적으로 이동할 Rclone 리모트 경로입니다."},
        {"name": "remote_name", "label": "일반 업로드 리모트명", "type": "text", "placeholder": "미입력 시 기본 설정값 사용", "desc": "비워두면 기본 설정의 일반 업로드 리모트를 사용합니다."}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        from .util_upload import UploadUtil
        if not UploadUtil.resolve_ad_source(item):
            return False, 'downloaded', 'AllDebrid 원본 경로 확인 대기'
        if (FeederUtil.get_downloader_by_name(item.current_engine_name) or {}).get('engine_type') == 'alldebrid':
            source_path = item.local_path
        upload_path = dest_config.get('upload_path')
        complete_path = dest_config.get('complete_path')
        error = FeederUtil.relative_path_error(upload_path) or FeederUtil.relative_path_error(complete_path)
        if error:
            return False, 'failed', error
        if upload_path == complete_path or upload_path.startswith(complete_path + '/') or complete_path.startswith(upload_path + '/'):
            return False, 'failed', '임시 업로드 경로와 최종 이동 경로가 겹치지 않도록 설정해주세요.'

        is_remote_source = bool(source_path and not os.path.exists(source_path) and (':' in source_path or source_path.startswith(('http://', 'https://'))))
        if not source_path or (not is_remote_source and not os.path.exists(source_path)):
            return False, "failed", f"업로드 대상 소스 파일 없음: {source_path}"

        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        rclone_cfg = FeederUtil.load_yaml().get('rclone', {})
        remote_name = (
            dest_config.get('remote_name')
            or P.ModelSetting.get('download_rclone_upload_remote_name')
            or P.ModelSetting.get('download_rclone_remote_name')
            or rclone_cfg.get('upload_remote_name')
            or rclone_cfg.get('remote_name')
            or ''
        ).strip()
        if not remote_name:
            return False, "failed", "일반 업로드 Rclone 리모트명이 설정되지 않았습니다."
        raw_name = item.file_name or os.path.basename(source_path)
        base_name, ext = os.path.splitext(raw_name)
        folder_name = getattr(item, 'output_folder_name', None) or (base_name if DownloadUtil.is_single_file(raw_name, source_path) else raw_name)
        temp_full = f"{remote_name}:{upload_path}/{folder_name}"
        final_full = f"{remote_name}:{complete_path}/{folder_name}"
        cmd = ["rclone", "copy", source_path, temp_full]
        if rclone_conf:
            cmd.extend(["--config", rclone_conf])

        cmd.extend(FeederUtil.get_rclone_transfer_options())

        item_id = item.id
        engine_name, engine_task_id = item.current_engine_name, item.engine_task_id
        logger.info(f"[RcloneTransporter] 임시 업로드 시작 ({'원격 다이렉트' if is_remote_source else '로컬'}): {folder_name} -> {temp_full}")
        try:
            from .util_upload import UploadUtil
            from .util_block import DownloadBlock
            from .model_download import ModelDownload
            cmd.extend(UploadUtil.rclone_stats_options())
            target = db.session.get(ModelDownload, item_id)
            if not target or DownloadBlock.stopped(item_id):
                return False, 'blocked', '차단 또는 취소된 작업입니다.'
            target.upload_state = dict(target.upload_state or {}, incoming=temp_full,
                dest_parent=f'{remote_name}:{complete_path}', folder_name=folder_name,
                rclone_conf=rclone_conf, impersonate_arg=[],
                engine_name=engine_name, engine_task_id=engine_task_id)
            db.session.commit()
            db.session.remove()
            success, output = UploadUtil.run_rclone(cmd, f'업로드 {folder_name}',
                item_id=item_id, execution_timeout=3600)
            if not success and is_remote_source:
                failed = db.session.get(ModelDownload, item_id)
                auth = UploadUtil.source_auth_error(failed, output, source_operation=True) if failed else None
                if auth:
                    FeederUtil.auto_disable_engine(engine_name, auth)
            if success:
                move_cmd = ["rclone", "move", temp_full, final_full, "--delete-empty-src-dirs"]
                if rclone_conf:
                    move_cmd.extend(["--config", rclone_conf])
                move_cmd.extend(FeederUtil.get_rclone_extra_options())
                moved, output = UploadUtil.run_rclone(move_cmd, f'최종 이동 {folder_name}',
                    item_id=item_id, execution_timeout=3600)
                if not moved:
                    err = output.strip() or "최종 완료 경로 이동 실패"
                    logger.error(f"[RcloneTransporter] 최종 이동 오류: {err}")
                    return False, "failed", f"Rclone 최종 이동 실패: {err[:120]}"

                logger.info(f"[RcloneTransporter] 최종 이동 완료: {final_full}")
                if DownloadBlock.stopped(item_id):
                    return False, 'blocked', '차단 또는 취소된 작업입니다.'
                downloader_cfg = FeederUtil.get_downloader_by_name(engine_name)
                if downloader_cfg and engine_task_id:
                    try:
                        engine = DownloadUtil.create_engine(downloader_cfg)
                        if engine and getattr(engine, 'ENGINE_ID', '') == 'pikpak':
                            target = db.session.get(ModelDownload, item_id)
                            cleaned, warning = engine.delete_completed_source(engine_task_id,
                                FeederUtil.get_download_source_subpath(target), source_path if is_remote_source else None)
                            if cleaned:
                                engine.delete_task(engine_task_id)
                            else:
                                logger.warning('[PikPak] 작업 #%s 최종 이동 성공 · 원본 정리 보류: %s', item_id, warning)
                        elif engine and is_remote_source:
                            engine.delete_task(engine_task_id)
                    except Exception as exc:
                        logger.warning('[RcloneTransporter] 작업 #%s 업로드 성공 후 원본 정리 예외: %s',
                            item_id, DownloadUtil.engine_error_detail(None, exc))
                if not is_remote_source:
                    if os.path.isdir(source_path):
                        shutil.rmtree(source_path, ignore_errors=True)
                    elif os.path.isfile(source_path):
                        os.remove(source_path)
                return True, "completed", f"Rclone 업로드 및 최종 이동 완료 ({final_full})"
            else:
                err = output.strip() or "알 수 없는 Rclone 오류"
                logger.error(f"[RcloneTransporter] Rclone 오류: {err}")
                return False, "failed", f"Rclone 실패: {err[:120]}"
        except Exception as ex:
            logger.error(f"[RcloneTransporter] Rclone 실행 예외: {ex}")
            return False, "failed", f"Rclone 실행 예외: {str(ex)}"


class GDrivePoolTransporter(BaseTransporter):
    TRANSPORTER_ID = "gdrive_rotation"
    TRANSPORTER_NAME = "Google Drive 계정 풀 로테이션 (MyDrive 경유용)"

    CONFIG_SCHEMA = [
        {"name": "use_impersonate", "label": "Google Workspace 위임", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "위임 사용"}, {"value": "false", "label": "위임 사용 안 함"}],
         "desc": "계정 풀 사용 시 적용합니다. 위임 사용: SA 리모트에서 계정 이메일로 위임합니다. 사용 안 함: 계정별 독립 리모트를 사용합니다."},
        {"name": "use_local_staging", "label": "로컬 스테이징 사용", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "사용"}, {"value": "false", "label": "사용 안 함"}],
         "desc": "사용: 원격 소스를 로컬 임시 경로에 먼저 수신합니다. 사용 안 함: 로컬 디스크를 거치지 않고 직접 전송합니다."},
        {"name": "upload_path", "label": "임시 업로드 경로", "type": "text", "placeholder": "예: incoming/프로필명", "desc": "Google Drive에서 업로드가 먼저 완료되는 임시 경로입니다."},
        {"name": "mydrive_remote_name", "label": "내 드라이브 SA 리모트명", "type": "text", "placeholder": "미입력 시 기본 설정값 사용"},
        {"name": "shared_remote_name", "label": "목적지 공유 드라이브 리모트명", "type": "text", "placeholder": "미입력 시 기본 설정값 사용"},
        {"name": "shared_drive_id", "label": "공유 드라이브 ID", "type": "text", "placeholder": "미입력 시 기본 설정값 사용", "desc": "최종 라이브러리가 위치할 대상 공유 드라이브 ID"},
        {"name": "complete_path", "label": "최종 완료 이동 경로", "type": "text", "placeholder": "예: uploads/프로필명", "desc": "지정한 공유 드라이브 내 최종 보관 폴더 경로"}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        item.destination_type = "gdrive_rotation"
        error = FeederUtil.validate_destination(dest_config)
        if error:
            return False, 'failed', error
        item.gdrive_upload_path = dest_config['upload_path']
        item.gdrive_complete_path = dest_config['complete_path']
        item.gdrive_remote_id = dest_config.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
        item.local_path = source_path
        item.status = "pending_upload"
        return True, "pending_upload", "구글 드라이브 업로드 대기열 등록 완료"


# ==============================================================================
# 단일 통합 관리자: DownloadUtil
# ==============================================================================
class RemoteWorkerTransporter(BaseTransporter):
    TRANSPORTER_ID = 'remote_worker'
    TRANSPORTER_NAME = '원격 워커'
    FORCE_REMOTE_SOURCE = True
    CONFIG_SCHEMA = [
        {"name": "use_gdrive_pool", "label": "계정 풀 로테이션", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "계정 풀 사용"}, {"value": "false", "label": "기본 리모트 사용"}],
         "desc": "계정 풀을 사용하면 등록된 계정을 순환합니다. 계정 접근 방식은 아래 위임 설정에서 선택합니다."},
        dict(GDrivePoolTransporter.CONFIG_SCHEMA[0], depends_on='use_gdrive_pool'),
        {'name': 'remote_name', 'label': 'Drive 리모트명', 'type': 'text', 'placeholder': '비워두면 기본 업로드 리모트를 사용합니다',
         'desc': '기본 리모트 방식에서 사용하며, 비워두면 전역 기본 업로드 리모트를 사용합니다. Feeder가 작업별로 리모트명을 전달하므로 원격 서버와 FF의 Rclone에 같은 이름으로 설정하세요.'},
        {'name': 'mydrive_remote_name', 'label': '위임용 SA 리모트명', 'type': 'text',
         'placeholder': '비워두면 Drive 리모트명 또는 전역 위임용 리모트를 사용합니다',
         'desc': 'Workspace Impersonate 방식에서 사용합니다. 비우면 위 Drive 리모트명 또는 전역 위임용 리모트를 사용합니다. 워커와 FF 양쪽에 동일하게 설정하세요.'},
        {'name': 'shared_drive_id', 'label': '목적지 드라이브 ID', 'type': 'text',
         'desc': '미입력 시 기본 목적지 공유 드라이브 ID를 사용합니다.'},
        {'name': 'upload_path', 'label': '임시 업로드 경로', 'type': 'text', 'placeholder': '예: incoming/프로필명',
         'desc': '설정한 리모트 내 임시 업로드 경로를 사용합니다. 로컬 스테이징을 사용하지 않습니다.'},
        {'name': 'complete_path', 'label': '최종 완료 경로', 'type': 'text', 'placeholder': 'uploads/my-profile',
         'desc': '원격 서버에서 복사를 완료한 뒤 Feeder가 서버 측 폴더 이동으로 정리합니다.'},
    ]

    def transport(self, item, source_path, dest_config):
        item.destination_type = self.TRANSPORTER_ID
        item.local_path = source_path
        return True, 'pending_upload', '원격 워커 업로드 대기열에 등록했습니다.'


class DownloadUtil:
    @staticmethod
    def is_single_file(name: str, source_path: str = '') -> bool:
        if source_path and os.path.exists(source_path):
            return os.path.isfile(source_path)
        return os.path.splitext(name)[1].lower() in {
            '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.ts', '.m2ts', '.webm',
            '.mp3', '.flac', '.zip', '.rar', '.7z', '.torrent', '.iso', '.srt', '.ass'
        }

    @classmethod
    def reserve_output_folder(cls, item, source_name: str = '') -> str:
        """실물 존재 여부와 무관하게 DB의 이전 이름 이력을 기준으로 출력 폴더명을 확정한다."""
        if item.output_folder_name:
            return item.output_folder_name
        from .model_download import ModelDownload
        raw_name = source_name or item.source_file_name or item.file_name or item.title or f'item_{item.id}'
        base = os.path.splitext(raw_name)[0] if cls.is_single_file(raw_name, item.local_path or '') else raw_name
        rows = db.session.query(ModelDownload.id, ModelDownload.source_file_name,
                                ModelDownload.file_name, ModelDownload.output_folder_name,
                                ModelDownload.title).filter(ModelDownload.id != item.id).all()
        used = set()
        duplicate = False
        for row_id, original, file_name, output, title in rows:
            FeederUtil.cooperate()
            historical = original or file_name or title or ''
            historical_base = os.path.splitext(historical)[0] if cls.is_single_file(historical) else historical
            used.add((output or historical_base).casefold())
            if row_id < item.id and (historical_base.casefold() == base.casefold() or (output or '').casefold() == base.casefold()):
                duplicate = True
        result = base
        if duplicate or base.casefold() in used:
            short_hash = item.infohash[:6] if item.infohash else f'id_{item.id}'
            result = f'{base}_[{short_hash}]'
            if result.casefold() in used:
                result = f'{result}_{item.id}'
        item.source_file_name = raw_name
        item.output_folder_name = result
        db.session.commit()
        return result

    @staticmethod
    def get_queue_limit(config: dict) -> int:
        """원격 진행·대기 큐 제한을 지원하는 엔진의 설정값을 반환한다."""
        if str(config.get('engine_type', '')).lower() not in ('alldebrid', 'qbittorrent', 'pikpak'):
            return 0
        try:
            return max(0, int(config.get('max_active_tasks') or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def get_local_root(config: dict | None) -> str:
        """Return the configured filesystem root for an engine's local output."""
        config = config or {}
        if config.get('engine_type') == 'cd2':
            mount = str(config.get('cd2_mount_path') or '').strip().rstrip('/\\')
            vpath = str(config.get('cd2_virtual_path') or '').strip().strip('/')
            if mount and vpath:
                if mount.replace('\\', '/').endswith(vpath):
                    return mount
                return os.path.join(mount, vpath)
            return mount or vpath

    @staticmethod
    def engine_error_detail(engine, error, limit=16384, sensitive_values=None):
        """진단용 호스트·경로·줄바꿈을 보존하고 인증 값만 가린다."""
        from urllib.parse import urlsplit
        text = error.decode('utf-8', errors='replace') if isinstance(error, bytes) else str(error)
        secrets = set(getattr(engine, '_error_secrets', ())) if getattr(engine, 'ENGINE_ID', '') == 'pikpak' else set()

        def collect(values):
            if not isinstance(values, dict):
                return
            for key, value in values.items():
                if isinstance(value, dict):
                    collect(value)
                elif isinstance(value, str) and value and re.search(
                        r'password|passwd|token|cookie|secret|api.?key|authorization|username|device.?id|user.?id|^sub$|^user$|^url$|error_url', str(key), re.I):
                    secrets.add(value)

        collect(getattr(engine, 'config', {}))
        collect(getattr(engine, 'session', {}))
        collect(sensitive_values)
        if getattr(engine, 'ENGINE_ID', '') == 'pikpak':
            engine._error_secrets = secrets
        # HTTP 라이브러리의 자유 형식 예외 인자는 임의의 폐기된 비밀을 포함할 수 있다.
        # 필드 없는 내용은 제한하고 분류 가능한 전송 원인만 보존한다.
        if (getattr(engine, 'ENGINE_ID', '') == 'pikpak' and isinstance(error, Exception)
                and not isinstance(error, RuntimeError)):
            causes = re.findall(
                r'(?i)\b(?:connection (?:refused|reset|aborted)|(?:read |connect |connection )?timed? out|'
                r'timeout|name or service not known|temporary failure in name resolution|'
                r'nodename nor servname provided|network is unreachable|no route to host|'
                r'certificate verify failed|certificate_verify_failed|SSL|TLS|DNS|TCP|'
                r'errno[ :]*-?\d+)\b', text)
            endpoints = re.findall(r'''(?i)https?://[^\s<>"']+''', text)
            text = ' · '.join(dict.fromkeys(causes + endpoints)) + ' [비구조화 예외 인자 숨김]'
        urls = []

        def safe_url(match):
            try:
                url = urlsplit(match.group())
                host = url.hostname or ''
                if ':' in host:
                    host = '[' + host + ']'
                if url.port:
                    host += ':' + str(url.port)
                value = url.scheme + '://' + host + url.path
                if url.query or url.fragment:
                    value += '?[인증쿼리숨김]'
            except ValueError:
                value = '[인증 URL 숨김]'
            urls.append(value)
            return f'__FEEDER_DIAGNOSTIC_URL_{len(urls)-1}__'

        text = re.sub(r'(?i)https?://[^\s<>"\']+', safe_url, text)
        text = re.sub(r'\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?\b', '[JWT 숨김]', text)
        text = re.sub(r'(?im)\b(?:set-cookie|cookie)\s*:\s*[^\r\n]+', 'Cookie: [인증 정보 숨김]', text)
        text = re.sub(r'''(?ix)(["']?(?:captcha_token|x-captcha-token|cookie|set-cookie|device[_-]?id|user[_-]?id|username|user|sub)["']?\s*[:=]\s*)(?:"[^"\n]*"|'[^'\n]*'|[^\s,;]+)''', r'\1[인증 정보 숨김]', text)
        text = '\n'.join(redact_log(line, limit=len(line) + 1) for line in text.splitlines())
        for index, url in enumerate(urls):
            url = re.sub(r'\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?\b', '[JWT 숨김]', url)
            text = text.replace(f'__FEEDER_DIAGNOSTIC_URL_{index}__', url)
        for secret in sorted(secrets, key=len, reverse=True):
            text = text.replace(secret, '[인증 정보 숨김]')
        return text if len(text) <= limit else text[:limit] + '\n[상세 로그 잘림]'

    @staticmethod
    def log_engine_error(engine, operation, error):
        detail = DownloadUtil.engine_error_detail(engine, error)
        logger.warning('[EngineError][%s] %s\n%s',
            DownloadUtil.engine_error_detail(engine, getattr(engine, 'name', '')),
            DownloadUtil.engine_error_detail(engine, operation), detail)
        return detail

    @staticmethod
    def registration_failure(engine, error):
        """튜플 반환 계약을 유지하면서 등록 실패의 접수 확실성을 구분한다."""
        failure = getattr(engine, '_registration_failure', None)
        if isinstance(failure, dict):
            return dict(failure)
        text = str(error).lower()
        if any(word in text for word in ('maximum allowed', 'too many active', 'too_many_active',
                'active magnets', 'queue full', 'queue_full', 'task limit', 'trial')):
            return {'kind': 'queue', 'not_submitted': True}
        if any(word in text for word in ('invalid magnet', 'invalid torrent', 'invalid_magnet',
                'magnet_invalid', 'unsupported protocol', '프로토콜 미지원', 'no peer')):
            return {'kind': 'permanent', 'not_submitted': True}
        return {'kind': 'unknown', 'not_submitted': False}

    @staticmethod
    def registration_exception(engine, error, submitted=False, operation=None):
        code = error.code() if callable(getattr(error, 'code', None)) else ''
        kind = 'permanent' if str(code).split('.')[-1] in ('INVALID_ARGUMENT', 'UNIMPLEMENTED') else 'unknown' if submitted else 'transient'
        not_submitted = not submitted or isinstance(error, requests.exceptions.ConnectTimeout) or kind == 'permanent'
        engine._registration_failure = {'kind': kind, 'not_submitted': not_submitted}
        detail = DownloadUtil.engine_error_detail(engine, error) if getattr(engine, 'ENGINE_ID', '') == 'pikpak' else str(error)
        return DownloadUtil.log_engine_error(engine, operation or ('등록 요청' if submitted else '연결 확인'),
            f'{type(error).__name__} {code}: {detail}')

    @staticmethod
    def pikpak_config_revision(config):
        return hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()

    @staticmethod
    def pikpak_dashboard_allowed(config):
        if config.get('engine_type') != 'pikpak':
            return True
        try:
            shared = json.loads(P.ModelSetting.get('download_pikpak_vip_snapshot') or '{}')
            vip = shared.get(config['name'], {})
            return (vip.get('config_revision') == DownloadUtil.pikpak_config_revision(config) and
                    vip.get('state') in ('eligible', 'cutoff') and
                    datetime.fromisoformat(vip['expires_at_utc']).timestamp() > time.time())
        except (ValueError, TypeError, KeyError):
            return False

    @staticmethod
    def add_magnet(engine, link: str, title: str = None, upload_path: str = None, new_registration=False, vip_snapshot=None):
        """Call engines with the optional upload_path argument only when supported."""
        signature = inspect.signature(engine.add_magnet)
        accepts_upload_path = (
            'upload_path' in signature.parameters
            or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values())
        )
        engine._registration_failure = None
        try:
            kwargs = {'title': title}
            if accepts_upload_path:
                kwargs['upload_path'] = upload_path
            if new_registration and 'new_registration' in signature.parameters:
                kwargs['new_registration'] = True
            if vip_snapshot is not None and 'vip_snapshot' in signature.parameters:
                kwargs['vip_snapshot'] = vip_snapshot
            result = engine.add_magnet(link, **kwargs)
        except Exception as exc:
            return False, '', DownloadUtil.registration_exception(engine, exc, submitted=True)
        if not result[0]:
            DownloadUtil.log_engine_error(engine, 'add_magnet', result[2])
        return result

    """다운로더 엔진 및 이송 핸들러 통합 관리자"""

    _engine_classes = {}
    _transporter_classes = {}

    # 엔진 관리
    @classmethod
    def load_engines(cls):
        FeederUtil.ensure_custom_dirs()
        cls._engine_classes = {
            QBittorrentEngine.ENGINE_ID: QBittorrentEngine,
            CloudDrive2Engine.ENGINE_ID: CloudDrive2Engine,
            PikPakEngine.ENGINE_ID: PikPakEngine,
        }

        py_files = glob.glob(os.path.join(FeederUtil.ENGINES_DIR, "*.py"))
        for fpath in py_files:
            FeederUtil.cooperate()
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue

            module_name = f"feeder_engine_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    FeederUtil.cooperate()
                    obj = getattr(mod, attr_name)
                    if isinstance(obj, type) and issubclass(obj, BaseDownloadEngine) and obj is not BaseDownloadEngine:
                        e_id = getattr(obj, 'ENGINE_ID', '').lower()
                        if e_id:
                            cls._engine_classes[e_id] = obj
            except Exception as e:
                logger.error(f"[DownloadUtil] 엔진 스크립트 로드 실패 ({fname}): {e}")

    @classmethod
    def get_engine_schemas(cls) -> list[dict]:
        if not cls._engine_classes:
            cls.load_engines()

        schemas = []
        for e_id, e_cls in cls._engine_classes.items():
            config_schema = list(getattr(e_cls, 'CONFIG_SCHEMA', []))
            if e_id == 'alldebrid' and not any(f.get('name') == 'webdav_failure_timeout_hours' for f in config_schema):
                config_schema.append({
                    'name': 'webdav_failure_timeout_hours', 'label': 'WebDAV 실패 유예 (시간)',
                    'type': 'number', 'default': 24, 'min': 0, 'max': 8760, 'step': 1,
                    'desc': '완료된 소스의 최초 읽기 실패부터 경과한 시간. 이후 소스 실패가 다시 확인되면 다음 엔진으로 전환합니다. 0은 계속 대기합니다.'})
            if not any(field.get('name') == 'missing_timeout_hours' for field in config_schema):
                config_schema.append({
                    'name': 'missing_timeout_hours', 'label': '원격 목록 누락 유예 (시간)',
                    'type': 'number', 'default': 24,
                    'desc': '전체 목록 조회 성공 후에도 계속 보이지 않는 작업의 실패 처리 유예 시간. 0은 자동 실패 안 함. CD2 페이지 제한으로 일부 목록만 조회한 경우에는 판정하지 않습니다.'
                })
            schemas.append({
                'engine_id': e_id,
                'engine_name': getattr(e_cls, 'ENGINE_NAME', e_id),
                'output_type': getattr(e_cls, 'OUTPUT_TYPE', 'local'),
                'config_schema': config_schema
            })
        return schemas

    @classmethod
    def create_engine(cls, downloader_cfg: dict) -> BaseDownloadEngine | None:
        if not cls._engine_classes:
            cls.load_engines()

        e_type = downloader_cfg.get('engine_type', '').lower()
        engine_cls = cls._engine_classes.get(e_type)
        if not engine_cls:
            logger.warning(f"[DownloadUtil] 등록되지 않은 다운로더 엔진 타입: '{e_type}'")
            return None

        try:
            return engine_cls(downloader_cfg)
        except Exception as e:
            logger.error(f"[DownloadUtil] 엔진 인스턴스 생성 오류 ({downloader_cfg.get('name')}): {e}")
            return None

    # 트랜스포터 관리
    @classmethod
    def load_transporters(cls):
        FeederUtil.ensure_custom_dirs()
        cls._transporter_classes = {
            LocalTransporter.TRANSPORTER_ID: LocalTransporter,
            RcloneSimpleTransporter.TRANSPORTER_ID: RcloneSimpleTransporter,
            GDrivePoolTransporter.TRANSPORTER_ID: GDrivePoolTransporter,
            RemoteWorkerTransporter.TRANSPORTER_ID: RemoteWorkerTransporter,
        }

        py_files = glob.glob(os.path.join(FeederUtil.TRANSPORTERS_DIR, "*.py"))
        for fpath in py_files:
            FeederUtil.cooperate()
            fname = os.path.basename(fpath)
            if fname.startswith("__") or fname.endswith("_worker.py") or "worker" in fname:
                continue
            if fname in ["trans_local.py", "trans_rclone.py", "trans_gdrive_pool.py"]:
                continue
            module_name = f"feeder_trans_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    FeederUtil.cooperate()
                    obj = getattr(mod, attr_name)
                    if isinstance(obj, type) and issubclass(obj, BaseTransporter) and obj is not BaseTransporter:
                        t_id = getattr(obj, 'TRANSPORTER_ID', '').lower()
                        if t_id:
                            cls._transporter_classes[t_id] = obj
            except (Exception, SystemExit) as e:
                logger.error(f"[DownloadUtil] 이송 스크립트 로드 실패 ({fname}): {e}")

    @classmethod
    def get_transporter_schemas(cls) -> list[dict]:
        if not cls._transporter_classes:
            cls.load_transporters()

        schemas = []
        for t_id, t_cls in cls._transporter_classes.items():
            schemas.append({
                'transporter_id': t_id,
                'transporter_name': getattr(t_cls, 'TRANSPORTER_NAME', t_id),
                'config_schema': getattr(t_cls, 'CONFIG_SCHEMA', [])
            })
        return schemas

    @classmethod
    def get_transporter(cls, transporter_id: str) -> BaseTransporter | None:
        if not cls._transporter_classes:
            cls.load_transporters()

        t_cls = cls._transporter_classes.get(transporter_id.lower()) if transporter_id else None
        if not t_cls:
            logger.warning(f"[DownloadUtil] 등록되지 않은 이송 핸들러 타입: '{transporter_id}'")
            return None
        try:
            return t_cls()
        except Exception as e:
            logger.error(f"[DownloadUtil] 이송 핸들러 생성 오류 ({transporter_id}): {e}")
            return None

    @staticmethod
    def _queue_hash(value, expected=''):
        raw = str(value or '').strip().lower()
        result = FeederUtil.extract_info_hash(raw) or raw.removeprefix('urn:btih:')
        if len(expected) == 40 and len(result) == 32 and not re.fullmatch(r'[a-f0-9]{32}', result):
            result = FeederUtil.extract_info_hash('magnet:?xt=urn:btih:' + result) or result
        return result

    @classmethod
    def _queue_match(cls, row, target_hash, target_tid, same_engine):
        """해시의 명시적 불일치는 ID보다 우선하며 식별 불가는 None으로 남긴다."""
        raw_hash = next((row.get(key) for key in ('hash', 'infohash', 'info_hash', 'infoHash') if row.get(key)), '')
        if not raw_hash:
            raw_hash = FeederUtil.extract_info_hash(row.get('magnet') or row.get('url') or row.get('link') or '') or ''
        row_hash = cls._queue_hash(raw_hash, target_hash)
        row_tid = str(row.get('task_id') or row.get('id') or '')
        if row_hash and target_hash:
            if not re.fullmatch(r'(?:[a-f0-9]{32}|[a-f0-9]{40}|[a-f0-9]{64})', row_hash):
                return None
            if len(row_hash) == 32 and len(target_hash) == 40 and FeederUtil.extract_info_hash('magnet:?xt=urn:btih:' + row_hash) == target_hash:
                # 32자리 16진수와 Base32가 겹치면 서로 다른 콘텐츠를 삭제하지 않는다.
                return None
            return row_hash == target_hash
        if same_engine and target_tid and row_tid == target_tid:
            return True
        return None

    @classmethod
    def _delete_remote_tasks_verified(cls, item, defer_errors=False):
        """프로필 큐의 완전한 조회와 삭제 후 부재 확인을 재다운로드에만 적용한다."""
        target_hash = cls._queue_hash(FeederUtil.extract_info_hash(item.magnet) or item.infohash)
        target_tid = str(item.engine_task_id or '')
        names = list(item.priority_chain or [])
        if not names:
            raise RuntimeError('다운로드 프로필의 엔진 체인이 비어 있습니다.')
        if item.current_engine_name and target_tid:
            names.append(item.current_engine_name)
        seen = set()
        deferred = []
        for engine_name in names:
            FeederUtil.cooperate()
            key = str(engine_name).strip().lower()
            if key in seen:
                continue
            seen.add(key)
            same_engine = key == str(item.current_engine_name or '').strip().lower()
            pending_ids = {target_tid} if same_engine and target_tid else set()
            verified_absent = False
            try:
                cfg = FeederUtil.get_downloader_by_name(engine_name)
                if not cfg:
                    raise RuntimeError(f'{engine_name}: 다운로더 설정을 찾을 수 없습니다.')
                if defer_errors and FeederUtil.disabled_engine_names([cfg]):
                    raise RuntimeError('비활성 엔진 원격 정리 보류')
                try:
                    engine = cls.create_engine(cfg)
                except Exception as exc:
                    raise RuntimeError(f'{engine_name}: 엔진 연결 실패 ({type(exc).__name__})') from None
                if not engine:
                    raise RuntimeError(f'{engine_name}: 다운로더를 로드하지 못했습니다.')

                def read_queue():
                    try:
                        rows, error = engine.get_status()
                    except Exception as exc:
                        raise RuntimeError(f'{engine_name}: 큐 조회 실패 ({type(exc).__name__})') from None
                    if error:
                        raise RuntimeError(f'{engine_name}: 큐 조회 실패: {error}')
                    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                        raise RuntimeError(f'{engine_name}: 큐 응답 형식을 확인하지 못했습니다.')
                    for row in rows:
                        if cls._queue_match(row, target_hash, target_tid, same_engine) is True:
                            tid = str(row.get('task_id') or row.get('id') or '')
                            if tid:
                                pending_ids.add(tid)
                    if getattr(engine, 'status_list_complete', True) is False:
                        raise RuntimeError(f'{engine_name}: 큐 목록이 불완전하여 삭제를 확인할 수 없습니다. 목록 조회 페이지 한도 등 엔진 설정을 확인해주세요.')
                    return rows

                def targets(rows):
                    ids = []
                    for row in rows:
                        match = cls._queue_match(row, target_hash, target_tid, same_engine)
                        if match is None:
                            raise RuntimeError(f'{engine_name}: 콘텐츠 해시 등 식별 정보가 없어 큐 대상 부재를 확인할 수 없습니다.')
                        if match:
                            tid = str(row.get('task_id') or row.get('id') or '')
                            if not tid:
                                raise RuntimeError(f'{engine_name}: 동일 콘텐츠의 작업 ID가 없어 삭제할 수 없습니다.')
                            ids.append(tid)
                    return list(dict.fromkeys(ids))

                ids = targets(read_queue())
                pending_ids = set(ids)
                if ids:
                    failed = False
                    for tid in ids:
                        try:
                            failed = not engine.delete_task(tid) or failed
                        except Exception as exc:
                            raise RuntimeError(f'{engine_name}: 큐 작업 삭제 실패 ({type(exc).__name__})') from None
                    remaining = targets(read_queue())
                    pending_ids = set(remaining)
                    verified_absent = not remaining
                    if failed:
                        raise RuntimeError(f'{engine_name}: 큐 작업 삭제 요청이 실패했습니다.')
                    if remaining:
                        raise RuntimeError(f'{engine_name}: 삭제 후에도 동일 콘텐츠 작업 {len(remaining)}건이 큐에 남아 있습니다. 잠시 후 다시 요청해주세요.')
                logger.info('[DownloadUtil] [%s] 재다운로드 큐 정리 확인 (%s건 삭제)', engine_name, len(ids))
            except Exception:
                if not defer_errors:
                    raise
                if not verified_absent:
                    deferred.extend({'engine_name': str(engine_name).strip(), 'task_id': tid or None,
                                     'infohash': target_hash} for tid in sorted(pending_ids) or [''])
        return deferred if defer_errors else None

    @classmethod
    def delete_remote_tasks(cls, item, strict=False, verify_absent=False, defer_errors=False):
        """일반 정리의 정책은 유지하고 큐 부재 확인은 명시적 요청에만 수행한다."""
        if verify_absent:
            return cls._delete_remote_tasks_verified(item, defer_errors=defer_errors)
        target_hash = cls._queue_hash(FeederUtil.extract_info_hash(item.magnet) or item.infohash)
        target_tid = str(item.engine_task_id or '')
        names = ([item.current_engine_name] if item.current_engine_name else []) + list(item.priority_chain or [])
        if not names:
            names = [cfg.get('name') for cfg in FeederUtil.get_downloaders() if cfg.get('enabled', True)]
        seen = set()
        for engine_name in names:
            key = str(engine_name).strip().lower()
            if key in seen:
                continue
            seen.add(key)
            same_engine = key == str(item.current_engine_name or '').strip().lower()
            cfg = FeederUtil.get_downloader_by_name(engine_name)
            if not cfg:
                if strict and same_engine and target_tid:
                    raise RuntimeError(f'{engine_name}: 다운로더 설정이 없어 원격 작업을 중단하지 못했습니다.')
                continue
            engine = cls.create_engine(cfg)
            if not engine:
                if strict and same_engine and target_tid:
                    raise RuntimeError(f'{engine_name}: 다운로더를 로드하지 못했습니다.')
                continue
            try:
                rows, error = [], ''
                if target_hash or (same_engine and target_tid):
                    try:
                        rows, error = engine.get_status()
                    except Exception as exc:
                        error = str(exc)
                ids = [str(row.get('task_id') or row.get('id') or '') for row in rows
                       if cls._queue_match(row, target_hash, target_tid, same_engine) is True]
                # 직접 저장한 ID도 같은 엔진에서 해시 불일치가 보이면 삭제하지 않는다.
                if same_engine and target_tid and not any(str(row.get('task_id') or row.get('id') or '') == target_tid for row in rows):
                    ids.insert(0, target_tid)
                for tid in dict.fromkeys(tid for tid in ids if tid):
                    deleted = engine.delete_task(tid)
                    if strict and not deleted:
                        statuses, failure = engine.get_status()
                        if failure or any(str(row.get('task_id') or row.get('id') or '') == tid for row in statuses):
                            raise RuntimeError(f'{engine_name}: 원격 작업 삭제 실패 ({tid})')
                    logger.info('[DownloadUtil] [%s] 원격 소스 작업 정리: ID=%s', engine_name, tid)
                if strict and error:
                    raise RuntimeError(f'{engine_name}: 원격 작업 목록 조회 실패: {error}')
            except Exception as exc:
                if strict:
                    raise
                logger.debug('[DownloadUtil] [%s] 원격 소스 삭제 중 예외: %s', engine_name, exc)
