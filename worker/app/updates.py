"""인증된 Feeder의 실행 코드를 검증하고 전송 종료 후 새 릴리스로 전환한다."""

import ast
import base64
from contextlib import closing
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import zlib

logger = logging.getLogger(__name__)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(json.dumps(value).encode())
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_state(root):
    path = root / '.updates/state.json'
    return json.loads(path.read_text()) if path.exists() else {}


def release_path(root, identity):
    if not isinstance(identity, str) or not re.fullmatch(r'[a-f0-9]{64}', identity):
        raise ValueError('릴리스 ID가 유효하지 않습니다.')
    path = root / '.updates/releases' / identity
    if path.resolve() != root.resolve() / '.updates/releases' / identity or path.is_symlink():
        raise ValueError('릴리스 경로가 유효하지 않습니다.')
    return path


def restore_database(root, database):
    backup = root / '.updates/rollback.sqlite'
    if backup.exists():
        with closing(sqlite3.connect(backup)) as source, closing(sqlite3.connect(database)) as target:
            source.backup(target)


def bootstrap(root, settings):
    """고정 설치 코드에서 현재 릴리스로 이동하고 미확인 기동 실패를 복구한다."""
    state = read_state(root)
    identity = state.get('active')
    if not identity:
        return
    target = release_path(root, identity)
    current = Path(__file__).resolve().parents[1]
    marker = os.environ.get('FEEDER_WORKER_BOOT')
    if state.get('pending') and state.get('attempted') and marker != identity:
        restore_database(root, settings.database)
        identity = state.get('previous')
        state.update(active=identity, pending=False, status='failed', error='새 워커 기동 실패로 이전 코드와 DB를 복구했습니다.')
        write_json(root / '.updates/state.json', state)
        target = release_path(root, identity) if identity else root
    elif state.get('pending') and not state.get('attempted'):
        state['attempted'] = True
        write_json(root / '.updates/state.json', state)
    if target.resolve() != current:
        os.environ['FEEDER_WORKER_ROOT'] = str(root)
        os.environ['FEEDER_WORKER_BOOT'] = identity or ''
        os.environ['PYTHONPATH'] = str(target)
        os.chdir(target)
        os.execv(sys.executable, [sys.executable, '-B', '-m', 'app.main'])


class Updates:
    def __init__(self, root, settings, database, version):
        self.root = Path(root).resolve()
        self.settings, self.database, self.version = settings, database, version
        self.lock = threading.RLock()
        self.callback = None
        self.busy = False
        self.ready = False
        self.error = None
        self.target_version = None
        self.identity = None
        self.phase = 'idle'
        state = read_state(self.root)
        if state.get('status') == 'failed':
            self.phase, self.error = 'failed', state.get('error')
            self.target_version = state.get('target_version')

    def status(self):
        with self.lock:
            return {'status': self.phase, 'target_version': self.target_version, 'error': self.error}

    def receive(self, payload):
        if not isinstance(payload, dict) or set(payload) != {'version', 'sha256', 'archive'}:
            raise ValueError('업데이트 패키지 형식이 유효하지 않습니다.')
        version = payload['version']
        if not isinstance(version, str) or not re.fullmatch(r'\d+\.\d+\.\d+', version):
            raise ValueError('업데이트 버전이 유효하지 않습니다.')
        if tuple(map(int, version.split('.'))) <= tuple(map(int, self.version.split('.'))):
            raise ValueError('현재 워커보다 새로운 버전만 설치할 수 있습니다.')
        try:
            packed = base64.b64decode(payload['archive'], validate=True)
            decoder = zlib.decompressobj()
            raw = decoder.decompress(packed, 2 * 1024 * 1024 + 1)
            if len(raw) > 2 * 1024 * 1024 or not decoder.eof or decoder.unused_data:
                raise ValueError
            identity = hashlib.sha256(raw).hexdigest()
            if identity != payload['sha256']:
                raise ValueError
            files = json.loads(raw)
            required = {'app/__init__.py', 'app/main.py', 'app/updates.py', 'app/api.py',
                        'app/jobs.py', 'app/runner.py', 'app/settings.py', 'app/database.py',
                        'app/auth.py', 'app/events.py', 'app/configuration.py', 'app/child.py',
                        'rclone_logging.py', 'requirements.txt'}
            if not isinstance(files, dict) or not required <= files.keys() or len(files) > 64:
                raise ValueError
            for name, content in files.items():
                if not isinstance(content, str) or not (re.fullmatch(r'app/[a-z_]+\.py', name) or name in ('rclone_logging.py', 'requirements.txt')):
                    raise ValueError
                if name.endswith('.py'):
                    ast.parse(content)
            tree = ast.parse(files['app/__init__.py'])
            declared = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                            and any(isinstance(target, ast.Name) and target.id == 'VERSION' for target in node.targets))
            if declared != version:
                raise ValueError
        except (ValueError, TypeError, SyntaxError, StopIteration, UnicodeError, zlib.error):
            raise ValueError('업데이트 패키지의 경로·내용·해시·버전을 확인해주세요.') from None
        with self.lock:
            if self.callback is None:
                raise ValueError('자동 업데이트는 app.main으로 실행한 워커에서 지원합니다.')
            if self.busy:
                if self.identity != identity:
                    raise ValueError('다른 업데이트가 진행 중입니다.')
                return self.status()
            requirements = self.root / 'requirements.txt'
            if not requirements.exists() or files['requirements.txt'] != requirements.read_text():
                raise ValueError('필수 패키지가 변경된 버전입니다. 설치 스크립트로 업데이트해주세요.')
            destination = release_path(self.root, identity)
            area = self.root / '.updates'
            area.mkdir(exist_ok=True, mode=0o700)
            area.chmod(0o700)
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if not destination.exists():
                staging = Path(tempfile.mkdtemp(dir=destination.parent))
                try:
                    for name, content in files.items():
                        path = staging / name
                        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                        path.write_text(content, encoding='utf-8')
                        path.chmod(0o600)
                    staging.replace(destination)
                finally:
                    if staging.exists():
                        shutil.rmtree(staging)
            self.busy, self.ready, self.error = True, False, None
            self.identity, self.target_version, self.phase = identity, version, 'draining'
            logger.info('워커 %s 업데이트 수신 · 기존 작업 종료 후 새 코드 검증', version)
            try:
                threading.Thread(target=self._prepare, name='worker-update', daemon=True).start()
            except Exception:
                self.busy = False
                raise
            return self.status()

    def _prepare(self):
        try:
            # 메모리가 작은 서버에서 검증 프로세스와 실제 전송이 겹치지 않게 한다.
            while any(self.database.counts().values()):
                time.sleep(.5)
            with self.lock:
                self.phase = 'validating'
            destination = release_path(self.root, self.identity)
            # 운영 DB와 인증 설정을 변경하지 않고 복사본에서 새 코드의 기동을 검사한다.
            with tempfile.TemporaryDirectory(dir=self.root / '.updates') as folder:
                validation_db = Path(folder) / 'validation.sqlite'
                with closing(self.database.connect()) as source, closing(sqlite3.connect(validation_db)) as target:
                    source.backup(target)
                program = ('from dataclasses import replace; from pathlib import Path; import sys; '
                    'from app import create_app,VERSION; from app.main import main; from app.settings import Settings; '
                    'assert VERSION==sys.argv[3]; '
                    'create_app(replace(Settings.load(Path(sys.argv[1])),database=Path(sys.argv[2]))); '
                    'Path(sys.argv[2]+".ok").write_text(VERSION)')
                environment = dict(os.environ, PYTHONPATH=str(destination), PYTHONDONTWRITEBYTECODE='1')
                result = subprocess.run([sys.executable, '-B', '-c', program, str(self.root), str(validation_db), self.target_version],
                    cwd=destination, env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
                marker = Path(str(validation_db) + '.ok')
                if result.returncode or not marker.exists() or marker.read_text() != self.target_version:
                    raise RuntimeError('새 워커 코드의 기동 검증에 실패했습니다.')
            logger.info('워커 %s 기동 검증 완료 · 재로딩 시작', self.target_version)
            with self.lock:
                self.phase, self.ready = 'restarting', True
            self.callback()
        except Exception as exc:
            with self.lock:
                self.phase, self.busy, self.ready = 'failed', False, False
                self.error = '업데이트 준비에 실패했습니다. 기존 워커와 작업은 유지됩니다.'
            logger.error('워커 업데이트 준비 실패: %s', type(exc).__name__)

    def activate(self):
        """모든 전송·API 종료 후 복구용 DB와 새 릴리스 포인터를 저장한다."""
        if not self.ready or any(self.database.counts().values()):
            raise RuntimeError('기존 작업 종료 전에 워커를 교체할 수 없습니다.')
        backup = self.root / '.updates/rollback.sqlite'
        with closing(self.database.connect()) as source, closing(sqlite3.connect(backup)) as target:
            source.backup(target)
        backup.chmod(0o600)
        previous = read_state(self.root).get('active')
        write_json(self.root / '.updates/state.json', {'active': self.identity, 'previous': previous,
            'pending': True, 'attempted': False, 'status': 'restarting', 'target_version': self.target_version})
        os.environ['FEEDER_WORKER_ROOT'] = str(self.root)
        os.environ.pop('FEEDER_WORKER_BOOT', None)
        os.environ['PYTHONPATH'] = str(self.root)
        os.chdir(self.root)
        os.execv(sys.executable, [sys.executable, '-B', '-m', 'app.main'])

    def confirm_started(self):
        state = read_state(self.root)
        if state.get('pending'):
            state.update(pending=False, status='completed', error=None)
            write_json(self.root / '.updates/state.json', state)
        # 현재·직전 릴리스만 보관하고 나머지는 관리 영역 안에서 정리한다.
        releases = self.root / '.updates/releases'
        keep = {state.get('active'), state.get('previous')}
        if releases.exists():
            for path in releases.iterdir():
                if re.fullmatch(r'[a-f0-9]{64}', path.name) and path.name not in keep and path.is_dir() and not path.is_symlink():
                    try:
                        shutil.rmtree(path)
                    except OSError:
                        logger.warning('이전 워커 릴리스 정리를 다음 기동으로 미룹니다.')
