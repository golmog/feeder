"""환경변수와 로컬 환경 파일을 읽고 시작 조건을 검증한다."""

from dataclasses import dataclass, field
import ipaddress
import configparser
import os
from pathlib import Path
import re
import shlex
from typing import Mapping


ROOT = Path(__file__).resolve().parents[1]
KEYS = frozenset({
    'WORKER_BIND', 'WORKER_PORT', 'WORKER_API_TOKEN', 'WORKER_DATABASE',
    'WORKER_RCLONE_BINARY', 'WORKER_RCLONE_CONFIG', 'WORKER_THREADS',
    'WORKER_MAX_REQUEST_BYTES', 'WORKER_LOG_LEVEL',
    'WORKER_SOURCE_REMOTES', 'WORKER_DESTINATION_REMOTES', 'WORKER_MAX_CONCURRENT_JOBS',
    'WORKER_EVENT_RETENTION', 'WORKER_SSE_MAX_CLIENTS', 'WORKER_SSE_HEARTBEAT_SECONDS',
})


def read_environment_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if path.stat().st_size > 65536:
        raise ValueError('환경 파일은 64KiB 이하여야 합니다.')
    values = {}
    for number, line in enumerate(path.read_text(encoding='utf-8').splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:].lstrip()
        key, separator, raw = line.partition('=')
        key = key.strip()
        if not separator or key not in KEYS:
            raise ValueError(f'환경 파일 {number}행의 설정 이름이 유효하지 않습니다.')
        try:
            parts = shlex.split(raw, comments=True, posix=True)
        except ValueError:
            raise ValueError(f'환경 파일 {number}행의 따옴표를 확인해주세요.') from None
        if len(parts) > 1:
            raise ValueError(f'환경 파일 {number}행의 공백이 포함된 값은 따옴표로 감싸주세요.')
        values[key] = parts[0] if parts else ''
    return values


def _integer(values: Mapping[str, str], key: str, default: int,
             minimum: int, maximum: int) -> int:
    try:
        value = int(values.get(key, str(default)))
    except (ValueError, TypeError):
        raise ValueError(f'{key}에는 정수를 지정해주세요.') from None
    if not minimum <= value <= maximum:
        raise ValueError(f'{key}의 허용 범위는 {minimum}~{maximum}입니다.')
    return value


def _path(values: Mapping[str, str], key: str, default: Path) -> Path:
    path = Path(values.get(key, str(default))).expanduser()
    if not path.is_absolute():
        raise ValueError(f'{key}에는 절대 경로를 지정해주세요.')
    return path


@dataclass(frozen=True)
class Settings:
    bind: str
    port: int
    api_token: str = field(repr=False)
    database: Path
    rclone_binary: Path
    rclone_config: Path
    threads: int
    max_request_bytes: int
    log_level: str
    source_remotes: tuple[str, ...] = ('ad',)
    destination_remotes: tuple[str, ...] = ('in',)
    max_concurrent_jobs: int = 1
    event_retention: int = 2000
    sse_max_clients: int = 1
    sse_heartbeat_seconds: int = 15

    @classmethod
    def load(cls, root: Path = ROOT,
             environ: Mapping[str, str] | None = None) -> 'Settings':
        values = read_environment_file(root / '.env')
        values.update(os.environ if environ is None else environ)
        token = values.get('WORKER_API_TOKEN', '')
        if not re.fullmatch(r'[A-Za-z0-9_.~-]{32,512}', token):
            raise ValueError('WORKER_API_TOKEN에 32~512자의 충분히 무작위인 토큰을 지정해주세요.')
        bind = values.get('WORKER_BIND', '127.0.0.1')
        try:
            address = ipaddress.ip_address(bind)
        except ValueError:
            raise ValueError('WORKER_BIND에는 Tailscale IP 또는 루프백 IP를 지정해주세요.') from None
        tailscale = ipaddress.ip_network('100.64.0.0/10') if address.version == 4 else ipaddress.ip_network('fd7a:115c:a1e0::/48')
        if not (address.is_loopback or address in tailscale):
            raise ValueError('공개/전체 인터페이스 바인딩은 허용하지 않습니다. Tailscale IP를 사용해주세요.')
        log_level = values.get('WORKER_LOG_LEVEL', 'INFO').upper()
        if log_level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            raise ValueError('WORKER_LOG_LEVEL이 유효하지 않습니다.')
        result = cls(
            bind=str(address),
            port=_integer(values, 'WORKER_PORT', 8000, 1, 65535),
            api_token=token,
            database=_path(values, 'WORKER_DATABASE', root / 'data' / 'worker.db'),
            rclone_binary=_path(values, 'WORKER_RCLONE_BINARY', Path('/usr/bin/rclone')),
            rclone_config=_path(values, 'WORKER_RCLONE_CONFIG', Path.home() / '.config' / 'rclone' / 'rclone.conf'),
            threads=_integer(values, 'WORKER_THREADS', 8, 2, 32),
            max_request_bytes=_integer(values, 'WORKER_MAX_REQUEST_BYTES', 65536, 1024, 1048576),
            log_level=log_level,
            source_remotes=tuple(values.get('WORKER_SOURCE_REMOTES', 'ad').split(',')),
            destination_remotes=tuple(values.get('WORKER_DESTINATION_REMOTES', 'in').split(',')),
            max_concurrent_jobs=_integer(values, 'WORKER_MAX_CONCURRENT_JOBS', 1, 1, 2),
            event_retention=_integer(values, 'WORKER_EVENT_RETENTION', 2000, 100, 100000),
            sse_max_clients=_integer(values, 'WORKER_SSE_MAX_CLIENTS', 1, 1, 4),
            sse_heartbeat_seconds=_integer(values, 'WORKER_SSE_HEARTBEAT_SECONDS', 15, 5, 60),
        )
        if result.sse_max_clients >= result.threads:
            raise ValueError('SSE 연결 수보다 많은 Waitress 스레드를 지정해 REST 요청 처리를 확보해주세요.')
        result.validate_paths()
        result.validate_remotes()
        return result

    def validate_remotes(self) -> None:
        config = configparser.RawConfigParser(interpolation=None)
        try:
            with self.rclone_config.open(encoding='utf-8') as stream:
                config.read_file(stream)
        except (OSError, configparser.Error, UnicodeError):
            raise ValueError('허용 리모트를 확인할 수 없습니다. 읽을 수 있는 Rclone INI 설정이 필요합니다.') from None
        for names, kind in ((self.source_remotes, 'webdav'), (self.destination_remotes, 'drive')):
            if not names or any(not re.fullmatch(r'[A-Za-z0-9_-]+', name) for name in names):
                raise ValueError('허용 리모트는 실제 이름을 공백 없이 쉼표로 구분해주세요.')
            if any(config.get(name, 'type', fallback='') != kind for name in names):
                raise ValueError('허용 리모트가 없거나 종류가 맞지 않습니다. 소스는 WebDAV, 목적지는 Drive를 지정해주세요.')

    def validate_paths(self) -> None:
        if not self.rclone_binary.is_file() or not os.access(self.rclone_binary, os.X_OK):
            raise ValueError('Rclone 실행 파일이 없거나 실행 권한이 없습니다.')
        if not self.rclone_config.is_file() or not os.access(self.rclone_config, os.R_OK):
            raise ValueError('Rclone 설정 파일이 없거나 읽을 수 없습니다.')
        if self.database.resolve() in (self.rclone_binary.resolve(), self.rclone_config.resolve()):
            raise ValueError('SQLite 경로를 Rclone 실행 파일/설정과 동일하게 지정할 수 없습니다.')
