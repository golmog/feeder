#!/usr/bin/env bash
# 기존 Rclone과 인증 파일을 보존하고 현재 사용자용 워커를 설치한다.
set -euo pipefail
umask 077

WORKER_INSTALL_ROOT=${WORKER_INSTALL_DIR:-"$HOME/feeder-worker"}
WORKER_OPTIONS=()
while (($#)); do
    case "$1" in
        --install-dir) [[ $# -ge 2 ]] || exit 2; WORKER_INSTALL_ROOT=$2; shift 2 ;;
        --bind|--port|--rclone-config|--rclone-binary|--source-remotes|--destination-remotes)
            [[ $# -ge 2 ]] || exit 2; WORKER_OPTIONS+=("$1" "$2"); shift 2 ;;
        --help)
            echo '사용법: bash install.sh [--install-dir 경로] [--bind Tailscale_IP] [--port 8000]'
            echo '  [--rclone-config 절대경로] [--rclone-binary 절대경로]'
            echo '  [--source-remotes WebDAV리모트] [--destination-remotes Drive리모트]'
            echo '기존 Rclone과 리모트 인증은 먼저 준비해주세요. 토큰은 최초 설치 시 자동 생성합니다.'
            exit 0 ;;
        *) echo "알 수 없는 옵션: $1" >&2; exit 2 ;;
    esac
done
command -v python3 >/dev/null || { echo 'Python 3.10 이상과 python3-venv를 설치해주세요.' >&2; exit 1; }
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' || { echo 'Python 3.10 이상이 필요합니다.' >&2; exit 1; }
if systemctl --user is-active --quiet feeder-worker.service 2>/dev/null || systemctl is-active --quiet feeder-worker.service 2>/dev/null; then
    echo '워커가 실행 중입니다. Feeder에서 작업 중단 후 현재 업로드 완료를 확인하고 워커 서비스를 중지한 뒤 다시 설치해주세요.' >&2
    exit 1
fi
WORKER_TEMP=$(mktemp -d)
trap 'rm -rf -- "$WORKER_TEMP"' EXIT
if [[ -n ${WORKER_LOCAL_SOURCE:-} ]]; then
    WORKER_SOURCE=$WORKER_LOCAL_SOURCE
else
    command -v curl >/dev/null || { echo 'curl을 설치해주세요.' >&2; exit 1; }
    curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 --max-time 120 --max-filesize 20971520 \
        https://codeload.github.com/golmog/feeder/tar.gz/refs/heads/main -o "$WORKER_TEMP/source.tar.gz"
    python3 - "$WORKER_TEMP" <<'PY'
from pathlib import Path, PurePosixPath
import sys
import tarfile
root = Path(sys.argv[1])
with tarfile.open(root / 'source.tar.gz') as archive:
    members = []
    total = 0
    for member in archive.getmembers():
        parts = PurePosixPath(member.name).parts
        if len(parts) < 3 or parts[:2] != ('feeder-main', 'worker'):
            continue
        relative = PurePosixPath(*parts[2:])
        if '..' in relative.parts or relative.is_absolute() or not (member.isdir() or member.isfile()):
            raise SystemExit('설치 파일의 경로 또는 형식이 유효하지 않습니다.')
        total += member.size
        if total > 20 * 1024 * 1024:
            raise SystemExit('설치 파일 크기가 허용 범위를 초과했습니다.')
        members.append((member, relative))
    for member, relative in members:
        target = root / 'source' / relative
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.extractfile(member).read())
PY
    WORKER_SOURCE=$WORKER_TEMP/source
fi
[[ -f "$WORKER_SOURCE/deploy/setup_install.py" ]] || { echo '배포 파일에 워커가 없습니다. 배포 버전을 확인해주세요.' >&2; exit 1; }
python3 "$WORKER_SOURCE/deploy/setup_install.py" --root "$WORKER_INSTALL_ROOT" --source "$WORKER_SOURCE" "${WORKER_OPTIONS[@]}"
cd -- "$WORKER_INSTALL_ROOT"
if [[ ! -x venv/bin/python ]]; then
    python3 -m venv venv || { echo '가상환경 생성 실패: python3-venv 패키지를 설치한 뒤 다시 실행해주세요.' >&2; exit 1; }
fi
venv/bin/python -m pip install --disable-pip-version-check -r requirements.txt
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -m app.manage check
bash deploy/install-user-service.sh
venv/bin/python deploy/setup_install.py --root "$WORKER_INSTALL_ROOT" --show-connection
