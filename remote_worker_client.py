"""비밀 정보를 노출하지 않는 원격 워커 REST/SSE 클라이언트."""

import ipaddress
import json
import re
import threading
import ast
import base64
import hashlib
from pathlib import Path
import time
import zlib
from urllib.parse import urlsplit

import requests
from urllib3.exceptions import HTTPError as TransportError


def bundled_worker_version():
    path = Path(__file__).parent / 'worker/app/__init__.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == 'VERSION' for target in node.targets))


def worker_update_package():
    root = Path(__file__).parent / 'worker'
    paths = sorted((root / 'app').glob('*.py')) + [root / 'rclone_logging.py', root / 'requirements.txt']
    files = {path.relative_to(root).as_posix(): path.read_text(encoding='utf-8') for path in paths}
    raw = json.dumps(files, sort_keys=True, ensure_ascii=True, separators=(',', ':')).encode()
    return {'version': bundled_worker_version(), 'sha256': hashlib.sha256(raw).hexdigest(),
            'archive': base64.b64encode(zlib.compress(raw, 9)).decode()}


class WorkerError(RuntimeError):
    def __init__(self, message, status=None, code=None):
        super().__init__(message)
        self.status = status
        self.code = code or ('updating' if status == 202 else 'unavailable')


def validate_endpoint(value):
    try:
        url = urlsplit(str(value).rstrip('/'))
        address = ipaddress.ip_address(url.hostname or '')
        network = ipaddress.ip_network('100.64.0.0/10' if address.version == 4 else 'fd7a:115c:a1e0::/48')
        if (url.scheme not in ('http', 'https') or url.username or url.password or url.query or url.fragment
                or url.path not in ('', '/api/v1') or not (address.is_loopback or address in network)
                or not 1 <= (url.port or (443 if url.scheme == 'https' else 80)) <= 65535):
            raise ValueError
    except (ValueError, TypeError):
        raise WorkerError('워커 주소에는 인증 정보 없는 Tailscale 또는 루프백 주소를 지정해주세요.') from None
    return f'{url.scheme}://{url.netloc}/api/v1'


class WorkerClient:
    _updates_lock = threading.Lock()
    _update_attempts = {}
    def __init__(self, endpoint, token):
        self.endpoint = validate_endpoint(endpoint)
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_.~-]{32,512}', token):
            raise WorkerError('워커 설치 시 안내된 인증 토큰을 입력해주세요 (32~512자).')
        self._headers = {'Authorization': 'Bearer ' + token}

    def _request(self, method, path, payload=None, stream=False, headers=None, params=None):
        session = requests.Session()
        session.trust_env = False
        try:
            response = session.request(method, self.endpoint + path, json=payload,
                headers=dict(self._headers, **(headers or {})), params=params,
                timeout=(3, 20 if stream else 10), stream=True, allow_redirects=False)
            if stream and response.status_code == 200:
                return session, response
            body = response.raw.read(65537, decode_content=True)
            if len(body) > 65536:
                raise WorkerError('워커 응답이 허용 크기를 초과했습니다.')
            status = response.status_code
            if status == 404 and method == 'GET' and path.startswith('/jobs/'):
                return None
            if status == 404 and method == 'POST' and path.endswith('/cancel'):
                if response.headers.get('X-Worker-Cancel-Tombstone') == '1':
                    return {'status': 'cancelled', 'cancel_requested': True}
                raise WorkerError('등록 전 취소 보호를 지원하는 워커 버전이 필요합니다.', status)
            if status not in (200, 202):
                try:
                    error = json.loads(body).get('error') or {}
                    raw_code = error.get('code')
                except (ValueError, TypeError, AttributeError):
                    raw_code = None
                code = ('auth' if status in (401, 403) else 'configuration' if status in (400, 409)
                        else 'updating' if status == 503 and raw_code == 'updating' else 'unavailable')
                message = {'updating': '워커 업데이트 중입니다. 기존 작업 ID를 유지하고 재확인합니다.',
                           'auth': '워커 인증이 거부됐습니다. 연결 설정을 확인해주세요.',
                           'configuration': '워커 요청 설정이 거부됐습니다. 연결 및 작업 설정을 확인해주세요.'}.get(code,
                           f'워커 요청을 확인하지 못했습니다 (HTTP {status}).')
                raise WorkerError(message, status, code)
            try:
                result = json.loads(body)
                if not isinstance(result, dict): raise ValueError
            except (ValueError, UnicodeError):
                raise WorkerError('워커 응답 형식을 확인하지 못했습니다.') from None
            if status == 202 and (result.get('status') == 'updating' or
                    isinstance(result.get('error'), dict) and result['error'].get('code') == 'updating'):
                raise WorkerError('워커 업데이트 중입니다. 기존 작업 ID를 유지하고 재확인합니다.', 202, 'updating')
            return result
        except (requests.RequestException, TransportError, OSError):
            raise WorkerError('워커와 통신하지 못했습니다. 기존 작업 ID를 유지하고 재확인합니다.') from None
        finally:
            if not stream or 'response' not in locals() or response.status_code != 200:
                if 'response' in locals(): response.close()
                session.close()

    def health(self):
        health = self._request('GET', '/health')
        if (not isinstance(health.get('version'), str) or any(type(health.get(key)) is not int or health[key] < 0
                for key in ('active_jobs', 'queued_jobs'))):
            raise WorkerError('워커 상태 응답을 확인하지 못했습니다.', code='invalid_response')
        return health

    def configuration(self):
        configuration = self._request('GET', '/config')
        if type(configuration.get('config_version')) is not int or configuration['config_version'] < 1:
            raise WorkerError('워커 설정 버전을 확인하지 못했습니다.', code='invalid_response')
        return configuration

    def ensure_version(self, force_update=False):
        health = self.health()
        expected = bundled_worker_version()
        if health.get('version') == expected:
            if (health.get('update') or {}).get('status') in ('validating', 'draining', 'restarting'):
                raise WorkerError('워커 업데이트 중입니다. 기존 전송 종료와 재로딩을 기다립니다.', 202)
            return health
        try:
            if tuple(map(int, health['version'].split('.'))) > tuple(map(int, expected.split('.'))):
                raise WorkerError('워커가 Feeder보다 최신 버전입니다. Feeder를 업데이트해주세요.')
        except (ValueError, KeyError, AttributeError):
            raise WorkerError('워커 버전을 확인하지 못했습니다.') from None
        if not health.get('self_update'):
            raise WorkerError('이 워커는 자동 업데이트를 지원하지 않습니다. 설치 스크립트로 한 번 업데이트하면 이후부터 자동 갱신됩니다.')
        update = health.get('update') or {}
        if update.get('status') in ('validating', 'draining', 'restarting') and update.get('target_version') == expected:
            raise WorkerError(f'워커 {expected} 업데이트 진행 중입니다. 기존 전송 종료와 재로딩을 기다립니다.', 202)
        if update.get('status') == 'failed' and update.get('target_version') == expected and not force_update:
            raise WorkerError('워커 업데이트에 실패했습니다. 워커 서비스 로그를 확인하고 연결 테스트로 다시 시도해주세요.', code='update_failed')
        with self._updates_lock:
            key = (self.endpoint, expected)
            now = time.monotonic()
            if force_update or now - self._update_attempts.get(key, -1000) >= 60:
                self._request('POST', '/update', worker_update_package())
                self._update_attempts[key] = now
        raise WorkerError(f'워커 {expected} 자동 업데이트를 요청했습니다. 기존 전송 종료 후 재로딩을 기다립니다.', 202)

    @staticmethod
    def _job_id(value):
        if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value):
            raise WorkerError('저장된 원격 작업 ID가 유효하지 않습니다.')
        return value

    def job(self, job_id): return self._request('GET', '/jobs/' + self._job_id(job_id))
    def submit(self, payload): return self._request('POST', '/jobs', payload)
    def cancel(self, job_id): return self._request('POST', '/jobs/' + self._job_id(job_id) + '/cancel')

    def events(self, cursor=None, stream_id=None, stop=None):
        headers = {'Last-Event-ID': str(cursor)} if cursor is not None else {}
        params = {'stream_id': stream_id} if stream_id else None
        session, response = self._request('GET', '/events', stream=True, headers=headers, params=params)
        size, event = 0, {}
        try:
            while stop is None or not stop.is_set():
                line = response.raw.readline(65537)
                if not line: return
                size += len(line)
                if len(line) > 65536 or size > 131072:
                    raise WorkerError('워커 이벤트가 허용 크기를 초과했습니다.')
                if line in (b'\n', b'\r\n'):
                    if 'data' in event:
                        try:
                            event['data'] = json.loads(event['data'])
                            yield event
                        except (ValueError, TypeError):
                            raise WorkerError('워커 이벤트 형식이 올바르지 않습니다.') from None
                    event, size = {}, 0
                elif not line.startswith(b':'):
                    key, sep, value = line.decode('utf-8').strip().partition(':')
                    if sep and key in ('id', 'event', 'data'): event[key] = value.lstrip()
        except (requests.RequestException, TransportError, OSError, UnicodeError):
            raise WorkerError('워커 이벤트 연결이 종료됐습니다.') from None
        finally:
            response.close(); session.close()


class WorkerEvents:
    """프로세스당 한 연결을 공유하고 REST 최신 상태 조회를 깨운다."""
    _lock = threading.Lock()
    _instances = {}

    def __init__(self, client):
        self.client = client
        self.condition = threading.Condition()
        self.stop = threading.Event()
        self.users = 0
        self.generation = 0
        self.thread = threading.Thread(target=self._run, name='feeder-worker-events', daemon=True)

    @classmethod
    def acquire(cls, client):
        with cls._lock:
            hub = cls._instances.get(client.endpoint)
            if hub is None or hub.stop.is_set():
                hub = cls(client); cls._instances[client.endpoint] = hub
                hub.thread.start()
            hub.users += 1
            return hub

    def release(self):
        with self._lock:
            self.users -= 1
            if self.users == 0:
                self.stop.set()
                if self._instances.get(self.client.endpoint) is self:
                    self._instances.pop(self.client.endpoint, None)

    def _run(self):
        cursor, identity = None, None
        while not self.stop.is_set():
            try:
                for event in self.client.events(cursor, identity, self.stop):
                    data = event.get('data') or {}
                    if isinstance(data, dict) and data.get('stream_id'): identity = data['stream_id']
                    raw_id = event.get('id', '')
                    if re.fullmatch(r'[0-9]{1,19}', raw_id): cursor = int(raw_id)
                    with self.condition:
                        self.generation += 1
                        self.condition.notify_all()
            except Exception:
                # REST 재조회가 최종 상태를 보장하며 인증 헤더/예외 원문은 기록하지 않는다.
                pass
            self.stop.wait(3)

    def wait(self, generation, timeout=3):
        with self.condition:
            if self.generation == generation: self.condition.wait(timeout)
            return self.generation
