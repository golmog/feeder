#!/usr/bin/env bash
# 기존 Rclone과 인증 파일을 보존하고 현재 사용자용 워커를 설치한다.
set -euo pipefail
umask 077

WORKER_INSTALL_ROOT=${WORKER_INSTALL_DIR:-"$HOME/feeder-worker"}
WORKER_OPTIONS=()
while (($#)); do
    case "$1" in
        --install-dir) [[ $# -ge 2 ]] || exit 2; WORKER_INSTALL_ROOT=$2; shift 2 ;;
        --bind|--port|--rclone-config|--rclone-binary)
            [[ $# -ge 2 ]] || exit 2; WORKER_OPTIONS+=("$1" "$2"); shift 2 ;;
        --help)
            echo '사용법: bash install.sh [--install-dir 경로] [--bind Tailscale_IP] [--port 8000]'
            echo '  [--rclone-config 절대경로] [--rclone-binary 절대경로]'
            echo '소스·목적지 리모트는 Feeder가 작업별로 지정합니다.'
            echo 'Debian/Ubuntu에서는 누락된 Python·venv·curl·인증서 패키지를 apt로 준비합니다(sudo 권한 필요).'
            echo '기존 Rclone과 리모트 인증은 먼저 준비해주세요. 토큰은 최초 설치 시 자동 생성합니다.'
            exit 0 ;;
        *) echo "알 수 없는 옵션: $1" >&2; exit 2 ;;
    esac
done
if systemctl --user is-active --quiet feeder-worker.service 2>/dev/null || systemctl is-active --quiet feeder-worker.service 2>/dev/null; then
    echo '워커가 실행 중입니다. Feeder에서 작업 중단 후 현재 업로드 완료를 확인하고 워커 서비스를 중지한 뒤 다시 설치해주세요.' >&2
    exit 1
fi
WORKER_TEMP=$(mktemp -d)
trap 'rm -rf -- "$WORKER_TEMP"' EXIT

WORKER_APT_UPDATED=0
worker_install_packages() {
    local privilege=()
    if ! command -v apt-get >/dev/null; then
        echo "자동 패키지 설치는 Debian/Ubuntu의 apt-get 환경을 지원합니다. 필요한 패키지를 준비한 뒤 다시 실행해주세요: $*" >&2
        exit 1
    fi
    if [[ $EUID -ne 0 ]]; then
        if ! command -v sudo >/dev/null || ! sudo -v; then
            echo "패키지 설치 권한이 없습니다. 관리자가 다음 명령으로 준비한 뒤 일반 사용자 계정에서 다시 실행해주세요: sudo apt-get update && sudo apt-get install $*" >&2
            exit 1
        fi
        privilege=(sudo)
    fi
    echo "누락된 필수 패키지 준비: $*"
    if [[ $WORKER_APT_UPDATED -eq 0 ]]; then
        "${privilege[@]}" apt-get update
        WORKER_APT_UPDATED=1
    fi
    if ! "${privilege[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@"; then
        echo "필수 패키지 설치 실패: $*. 별도로 설치한 Python을 사용한다면 해당 버전에 맞는 venv·ensurepip 지원을 준비해주세요." >&2
        exit 1
    fi
}

WORKER_PACKAGES=()
if ! command -v python3 >/dev/null; then
    WORKER_PACKAGES+=(python3 python3-venv)
else
    python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' || {
        echo '현재 python3가 Python 3.10 미만입니다. Python 3.10 이상을 준비한 뒤 다시 실행해주세요.' >&2
        exit 1
    }
    if ! python3 -c 'import venv, ensurepip' >/dev/null 2>&1; then
        WORKER_PACKAGES+=("$(python3 -c 'import sys; print("python%d.%d-venv" % sys.version_info[:2])')")
    fi
fi
if ! command -v curl >/dev/null; then
    WORKER_PACKAGES+=(curl)
fi
if command -v apt-get >/dev/null && command -v dpkg-query >/dev/null; then
    if [[ $(dpkg-query -W -f='${Status}' ca-certificates 2>/dev/null || true) != 'install ok installed' ]]; then
        WORKER_PACKAGES+=(ca-certificates)
    fi
fi
if ((${#WORKER_PACKAGES[@]})); then
    worker_install_packages "${WORKER_PACKAGES[@]}"
fi
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' || {
    echo '현재 python3가 Python 3.10 미만입니다. Python 3.10 이상을 준비한 뒤 다시 실행해주세요.' >&2
    exit 1
}

# 모듈이 있어도 ensurepip용 패키지가 빠진 환경이 있어 실제 생성으로 확인한다.
if ! python3 -m venv "$WORKER_TEMP/venv-check" >"$WORKER_TEMP/venv-check.log" 2>&1; then
    worker_install_packages "$(python3 -c 'import sys; print("python%d.%d-venv" % sys.version_info[:2])')"
    if ! python3 -m venv "$WORKER_TEMP/venv-check" >"$WORKER_TEMP/venv-check.log" 2>&1; then
        echo '가상환경 생성 실패: 현재 python3와 python3-venv의 버전이 맞는지 확인해주세요. 기존 워커 가상환경은 변경하지 않았습니다.' >&2
        cat "$WORKER_TEMP/venv-check.log" >&2
        exit 1
    fi
fi
if ! "$WORKER_TEMP/venv-check/bin/python" -m pip --version >/dev/null 2>&1; then
    echo '가상환경의 pip를 준비하지 못했습니다. 현재 Python의 venv·ensurepip 설치를 확인해주세요.' >&2
    exit 1
fi
rm -rf -- "$WORKER_TEMP/venv-check"

if [[ -n ${WORKER_LOCAL_SOURCE:-} ]]; then
    WORKER_SOURCE=$WORKER_LOCAL_SOURCE
else
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
    echo '워커 전용 가상환경을 생성합니다.'
    python3 -m venv venv
fi
if ! venv/bin/python -m pip --version >/dev/null 2>&1; then
    echo '기존 가상환경에 pip를 준비합니다.'
    venv/bin/python -m ensurepip --upgrade
fi
echo '워커 필수 Python 패키지를 가상환경에 설치합니다.'
venv/bin/python -m pip install --disable-pip-version-check -r requirements.txt
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -m app.manage check
bash deploy/install-user-service.sh
venv/bin/python deploy/setup_install.py --root "$WORKER_INSTALL_ROOT" --show-connection
