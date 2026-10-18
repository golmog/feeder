#!/bin/bash
# 기존 인증 파일을 유지하며 부팅 시 실행되는 사용자 서비스를 설치한다.
set -euo pipefail
umask 077

WORKER_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
WORKER_USER=$(id -un)
cd "$WORKER_ROOT"
if systemctl is-active --quiet feeder-worker.service; then
    echo '시스템 서비스가 실행 중입니다. 중복 등록을 중단합니다.' >&2
    exit 1
fi
if systemctl --user is-active --quiet feeder-worker.service; then
    echo '사용자 서비스가 이미 실행 중입니다. 점검 후 별도로 재시작해주세요.' >&2
    exit 1
fi
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -m app.manage check
PYTHONDONTWRITEBYTECODE=1 venv/bin/python - <<'PY'
from app import create_app
app = create_app()
counts = app.extensions['worker_database'].counts()
if counts['active_jobs'] or counts['queued_jobs']:
    raise SystemExit('기존 대기/실행 작업이 있습니다. 작업 상태를 먼저 정리해주세요.')
PY
loginctl --no-ask-password enable-linger "$WORKER_USER" || true
if [[ $(loginctl show-user "$WORKER_USER" -p Linger --value) != yes ]]; then
    echo "부팅 자동 실행을 위해 관리자가 sudo loginctl enable-linger $WORKER_USER 명령을 실행한 뒤 다시 설치해주세요." >&2
    exit 1
fi
WORKER_UNIT_DIR=${XDG_CONFIG_HOME:-"$HOME/.config"}/systemd/user
mkdir -p "$WORKER_UNIT_DIR"
WORKER_UNIT="$WORKER_UNIT_DIR/feeder-worker.service"
if [[ -f "$WORKER_UNIT" ]]; then
    cp -p "$WORKER_UNIT" "$WORKER_UNIT.backup-$(date -u +%Y%m%dT%H%M%S)"
fi
venv/bin/python - "$WORKER_ROOT" "$WORKER_UNIT" <<'PY'
from pathlib import Path
import subprocess
import sys
import tempfile
root, destination = map(Path, sys.argv[1:])
if any(c in str(root) for c in ('%', '\\', '"', '\n', '\r')):
    raise SystemExit('설치 경로에 %, 따옴표, 역슬래시, 줄바꿈은 사용할 수 없습니다.')
text = (root / 'deploy/feeder-worker-user.service').read_text()
text = text.replace('WorkingDirectory=%h/feeder-worker', f'WorkingDirectory={root}')
text = text.replace('EnvironmentFile=%h/feeder-worker/.env', f'EnvironmentFile="{root}/.env"')
text = text.replace('ExecStart=%h/feeder-worker/venv/bin/python', f'ExecStart="{root}/venv/bin/python"')
with tempfile.TemporaryDirectory(prefix='feeder-unit-', dir=destination.parent) as temporary:
    candidate = Path(temporary) / 'feeder-worker.service'
    candidate.write_text(text)
    candidate.chmod(0o600)
    # 실제 systemd 파서 검증에 통과한 파일만 원자적으로 반영한다.
    subprocess.run(['systemd-analyze', '--user', 'verify', str(candidate)], check=True)
    candidate.replace(destination)
PY
systemctl --user daemon-reload
systemctl --user enable --now feeder-worker.service
systemctl --user is-active feeder-worker.service
