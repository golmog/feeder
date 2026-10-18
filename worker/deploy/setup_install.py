"""설치 입력 검증, 기존 파일 보존, 최초 인증 토큰 생성과 연결 안내."""

import argparse
import configparser
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request


def settings_module(source):
    spec = importlib.util.spec_from_file_location('installer_settings', source / 'app/settings.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def choose(value, candidates, label):
    if value:
        return value
    if len(candidates) == 1:
        return candidates[0]
    try:
        with open('/dev/tty', 'r+') as terminal:
            terminal.write(f'{label} ({", ".join(candidates) or "자동 확인 불가"}): ')
            terminal.flush()
            answer = terminal.readline().strip()
            if answer:
                return answer
    except OSError:
        pass
    raise ValueError(f'{label}을 옵션으로 지정해주세요. --help에서 사용법을 확인할 수 있습니다.')


def configure(args):
    source = args.source.expanduser().resolve()
    root = args.root.expanduser()
    if not root.is_absolute() or root.is_symlink() or any(c in str(root) for c in ('%', '\\', '"', '\n', '\r')):
        raise ValueError('설치 경로는 일반 절대 경로여야 하며 %, 따옴표, 역슬래시, 줄바꿈을 포함할 수 없습니다.')
    root = root.resolve()
    if root in (Path('/'), Path.home().resolve()) or source == root:
        raise ValueError('전용 설치 폴더를 지정해주세요. 소스 폴더와 설치 폴더는 달라야 합니다.')
    if root.exists() and any(p.is_symlink() for p in root.rglob('*')):
        # 가상환경 내부의 기본 파이썬 링크는 설치 후 정상적으로 생성된다.
        if any(p.is_symlink() and 'venv' not in p.relative_to(root).parts for p in root.rglob('*')):
            raise ValueError('설치 폴더에 심볼릭 링크가 있습니다. 전용 일반 폴더를 사용해주세요.')
    module = settings_module(source)
    environment = root / '.env'
    if environment.exists():
        module.Settings.load(root, environ={})
        if any(getattr(args, key) is not None for key in ('bind', 'port', 'rclone_config', 'rclone_binary', 'source_remotes', 'destination_remotes')):
            raise ValueError('기존 .env를 보존합니다. 설정을 변경하려면 서비스를 중지하고 .env를 직접 수정해주세요.')
    else:
        binary = Path(args.rclone_binary or shutil.which('rclone') or '/usr/bin/rclone').expanduser()
        config = Path(args.rclone_config or Path.home() / '.config/rclone/rclone.conf').expanduser()
        parser = configparser.RawConfigParser(interpolation=None)
        with config.open(encoding='utf-8') as stream:
            parser.read_file(stream)
        sources = [name for name in parser.sections() if parser.get(name, 'type', fallback='') == 'webdav']
        destinations = [name for name in parser.sections() if parser.get(name, 'type', fallback='') == 'drive']
        bind = args.bind
        if not bind:
            try:
                result = subprocess.run(['tailscale', 'ip', '-4'], capture_output=True, text=True, timeout=10, check=True)
                bind = result.stdout.strip().splitlines()[0]
            except (OSError, subprocess.SubprocessError, IndexError):
                bind = choose(None, [], 'Tailscale IP (--bind)')
        values = {
            'WORKER_API_TOKEN': secrets.token_urlsafe(48),
            'WORKER_BIND': bind, 'WORKER_PORT': str(args.port or 8000),
            'WORKER_RCLONE_BINARY': str(binary), 'WORKER_RCLONE_CONFIG': str(config),
            'WORKER_SOURCE_REMOTES': choose(args.source_remotes, sources, 'WebDAV 리모트 (--source-remotes)'),
            'WORKER_DESTINATION_REMOTES': choose(args.destination_remotes, destinations, 'Drive 리모트 (--destination-remotes)'),
        }
        # 저장 전에 경로·IP·리모트 종류를 검증하고 비밀 값을 오류에 포함하지 않는다.
        module.Settings.load(root, environ=values)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(environment, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write('\n'.join(f'{key}={shlex.quote(value)}' for key, value in values.items()) + '\n')
    environment.chmod(0o600)
    files = [p for folder in ('app', 'deploy') for p in (source / folder).rglob('*')
             if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc']
    files += [source / 'requirements.txt', source / '.env.example', source / 'install.sh', source / 'README.md']
    for path in files:
        if path.is_symlink():
            raise ValueError('배포 소스의 심볼릭 링크는 허용하지 않습니다.')
    backup = root / 'backups' / ('source-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + secrets.token_hex(4))
    for path in files:
        destination = root / path.relative_to(source)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if destination.exists():
            previous = backup / path.relative_to(source)
            previous.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(destination, previous)
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(path.read_bytes())
        temporary.replace(destination)
    print('설치 파일 준비 완료. 기존 Rclone, 리모트 인증, 가상환경, 작업 DB와 토큰은 보존합니다.')


def show_connection(root):
    module = settings_module(root)
    settings = module.Settings.load(root, environ={})
    address = f'[{settings.bind}]' if ':' in settings.bind else settings.bind
    url = f'http://{address}:{settings.port}'
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(10):
        try:
            request = urllib.request.Request(url + '/api/v1/health', headers={'Authorization': 'Bearer ' + settings.api_token})
            with opener.open(request, timeout=3) as response:
                health = json.loads(response.read(65536))
            if health.get('status') == 'ok':
                break
        except (OSError, ValueError):
            pass
        time.sleep(1)
    else:
        raise ValueError('서비스 연결 확인 실패. journalctl --user -u feeder-worker -n 50 명령으로 확인해주세요.')
    print('\n설치와 인증 확인을 마쳤습니다. Feeder DOWNLOAD → 기본 설정 → 원격 워커에 입력하세요.')
    print('워커 주소: ' + url)
    print('인증 토큰: ' + settings.api_token)
    print('토큰은 이 서버의 .env에 권한 600으로 보관합니다. 외부에 공유하거나 로그에 붙여넣지 마세요.')
    print('Feeder에서 연결 테스트 후 연결 설정 저장을 누르고 프로필의 이송 핸들러를 지정하세요.')


def main():
    parser = argparse.ArgumentParser(description='워커 설치 준비와 연결 안내')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--source', type=Path)
    parser.add_argument('--show-connection', action='store_true')
    for name in ('bind', 'port', 'rclone-config', 'rclone-binary', 'source-remotes', 'destination-remotes'):
        parser.add_argument('--' + name)
    args = parser.parse_args()
    try:
        if args.show_connection:
            show_connection(args.root.expanduser().resolve())
        elif args.source:
            configure(args)
        else:
            parser.error('--source를 지정해주세요.')
    except UnicodeError:
        print('설정 파일을 UTF-8로 읽지 못했습니다. 파일 인코딩을 확인해주세요.', file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OSError, configparser.Error):
        # 설정 내용과 인증 정보가 포함된 원시 예외는 출력하지 않는다.
        print('설치 설정을 확인하지 못했습니다. IP·절대 경로·리모트 종류·기존 .env·파일 권한을 확인해주세요. 비대화형 설치라면 --bind와 리모트 옵션을 지정하세요.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
