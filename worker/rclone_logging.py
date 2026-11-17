"""전송 오류에서 인증 값과 URL을 가리고 표시 길이를 제한한다."""

import re
import json
import math
import subprocess
import tempfile
import time
from html.parser import HTMLParser


def validate_extra_args(arguments):
    """성능 플래그 허용 목록 없이 argv 구조와 Feeder 소유 설정만 보호한다."""
    if type(arguments) is not list or len(arguments) > 128:
        raise ValueError('Rclone 추가 옵션은 최대 128개 인자의 문자열 목록이어야 합니다.')
    result, may_have_value, total = [], False, 0
    owned = {'--config', '--drive-impersonate', '--transfers',
             '--rc', '--rc-addr', '--rc-user', '--rc-pass', '--rc-htpasswd'}
    for token in arguments:
        if (type(token) is not str or not token or len(token) > 4096 or
                any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF
                    for char in token)):
            raise ValueError('Rclone 추가 옵션에 유효하지 않은 문자열이 있습니다.')
        total += len(token)
        if total > 16384 or token == '--':
            raise ValueError('Rclone 추가 옵션의 길이 또는 경계가 유효하지 않습니다.')
        flag = re.fullmatch(r'-{1,2}[A-Za-z][A-Za-z0-9_-]*(?:=.*)?', token)
        if flag:
            if token.split('=', 1)[0].lower() in owned:
                raise ValueError('Rclone 실행 경로·인증·전용 전송 수·관리 서버 옵션은 Feeder 설정에서 지정합니다.')
            may_have_value = '=' not in token
        elif not may_have_value:
            raise ValueError('Rclone 추가 옵션에는 플래그와 해당 값만 지정할 수 있습니다.')
        else:
            may_have_value = False
        result.append(token)
    return result


class _ErrorPageText(HTMLParser):
    """HTML 머리말과 실행 코드를 제외하고 화면의 오류 문구만 모은다."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ignored = []
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.ignored.clear()
        if tag in ('head', 'script', 'style', 'noscript', 'template'):
            self.ignored.append(tag)

    def handle_endtag(self, tag):
        if tag in self.ignored:
            self.ignored = self.ignored[:self.ignored.index(tag)]

    def handle_data(self, data):
        if not self.ignored and data.strip():
            self.parts.append(data.strip())


def _html_error_text(text):
    marker = re.search(r'(?i)<(?:!doctype\s+html\b|html\b|head\b|body\b)', text)
    if marker is None:
        return text
    parser = _ErrorPageText()
    parser.feed(text[marker.start():])
    parser.close()
    body = ' '.join(parser.parts) or 'HTML 오류 응답 (본문 없음)'
    # 파일명·실행 문맥이 길더라도 실제 오류 본문을 표시할 공간을 남긴다.
    prefix = text[:marker.start()].strip()
    if len(prefix) > 160:
        prefix = prefix[:157] + '...'
    return (prefix + ' ' + body).strip()


def redact_log(value, secrets=(), limit=400):
    text = _html_error_text(str(value))
    for secret in sorted((str(s) for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, '[인증 정보 숨김]')
    text = re.sub(r'(?i)https?://[^\s<>"\']+', '[URL 숨김]', text)
    text = re.sub(r'(?i)\b(?:Bearer|Basic)\s+[^\s,;"\']+', '[인증 정보 숨김]', text)
    text = re.sub(
        r'''(?ix)(["']?(?:access_token|refresh_token|api[_-]?key|token|password|passwd|authorization|client_secret)["']?\s*[:=]\s*)(?:"[^"\n]*"|'[^'\n]*'|[^\s,;]+)''',
        r'\1[인증 정보 숨김]', text)
    text = ''.join(c if ord(c) >= 32 and ord(c) != 127 else ' ' for c in text)
    return text[:limit]


def classify_auth_error(message, source=None, destination=None):
    """Use explicit operations or filesystem paths verified against both endpoints."""
    def matches_endpoint(filesystem, endpoint):
        if not isinstance(endpoint, str):
            return False
        # Rclone may initialize an ancestor of the requested path.
        root = filesystem.rstrip('/')
        return endpoint.rstrip('/') == root or endpoint.startswith(
            root if root.endswith(':') else root + '/')

    matches = []
    for line in str(message or '').splitlines():
        try:
            entry = json.loads(line)
            if isinstance(entry, dict) and isinstance(entry.get('msg'), str):
                line = entry['msg']
        except (ValueError, TypeError):
            pass
        text = line.lower()
        if re.search(r'\b(?:403|404|429|500|502|503|504)\b|timeout|timed out|connection reset|connection refused|network is unreachable', text):
            continue
        source_open = bool(re.search(r'failed to open source object|error opening source', text))
        destination_open = bool(re.search(r'googleapi:|google drive|destination', text))
        locked = bool(re.search(r'account (?:is |has been )?(?:locked|suspended|banned)', text))
        unauthorized = bool(re.search(r'\b401\b', text))
        if not (locked or unauthorized):
            continue
        origin = 'source' if source_open and not destination_open else 'destination' if destination_open and not source_open else 'unknown'
        filesystems = re.findall(r'failed to create file system for "([^"\n]+)"', line, re.IGNORECASE)
        if filesystems:
            filesystem_origin = 'unknown'
            if len(filesystems) == 1 and source and destination:
                source_match = matches_endpoint(filesystems[0], source)
                destination_match = matches_endpoint(filesystems[0], destination)
                if source_match != destination_match:
                    filesystem_origin = 'source' if source_match else 'destination'
            if (source_open or destination_open) and origin != filesystem_origin:
                origin = 'unknown'
            else:
                origin = filesystem_origin
        matches.append((origin, 'AUTH_ACCOUNT_LOCKED' if locked else 'AUTH_UNAUTHORIZED'))
    if not matches:
        return None
    origins = {origin for origin, _ in matches}
    origin = next(iter(origins)) if len(origins) == 1 else 'unknown'
    code = 'AUTH_ACCOUNT_LOCKED' if all(code == 'AUTH_ACCOUNT_LOCKED' for _, code in matches) else 'AUTH_UNAUTHORIZED'
    return {'category': 'authentication', 'scope': 'engine', 'code': code,
            'action': 'disable_engine', 'origin': origin,
            'message': '계정 잠금' if code == 'AUTH_ACCOUNT_LOCKED' else '인증 오류 (401)'}


def classify_error(message):
    """상태 코드만으로 원본 오류를 Drive 오류로 오인하지 않는다."""
    text = str(message or '').lower()
    if 'storagequotaexceeded' in text:
        return 'DRIVE_STORAGE_QUOTA'
    if ('failed to open source object' in text or 'error opening source' in text or
            ('alldebrid' in text and ('not found' in text or 'server not allowed' in text))):
        return 'SOURCE_UNAVAILABLE'
    drive = 'googleapi:' in text or 'google drive' in text
    if 'ratelimitexceeded' in text or (drive and ('rate limit' in text or re.search(r'\b429\b', text))):
        return 'DRIVE_RATE_LIMIT'
    if drive and re.search(r'\b(?:500|502|503)\b', text):
        return 'DRIVE_SERVER_ERROR'
    if any(word in text for word in ('timeout', 'timed out', 'connection reset', 'unexpected eof',
                                      'connection refused', 'network is unreachable')) or re.search(r'\beof\b', text):
        return 'NETWORK_ERROR'
    return 'UNKNOWN_ERROR'


class FileQuota:
    """일부라도 전송한 파일의 전체 크기를 집계하며 불명확하면 예약량을 보존한다."""

    def __init__(self, sizes=None):
        self.sizes = sizes or {}
        self.started = {}
        self.covered = {}
        self.bytes = 0
        self.stats_seen = False
        self.unknown = False
        self.failed = set()
        self.source_failure = False

    @classmethod
    def inspect_source(cls, command, stopped=None):
        # 메타데이터만 조회하며 대량 목록은 메모리에 적재하지 않는다.
        try:
            with tempfile.TemporaryFile() as output:
                process = subprocess.Popen(command, stdout=output, stderr=subprocess.DEVNULL)
                deadline = time.monotonic() + 30
                try:
                    while process.poll() is None:
                        if time.monotonic() >= deadline or (stopped and stopped()):
                            return cls()
                        time.sleep(.2)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait()
                if process.returncode or output.tell() > 4 * 1024 * 1024:
                    return cls()
                output.seek(0)
                rows = json.load(output)
            if not isinstance(rows, list):
                return cls()
            sizes = {row['Path']: int(row['Size']) for row in rows
                     if not row.get('IsDir') and int(row.get('Size', -1)) >= 0}
            return cls(sizes)
        except (OSError, ValueError, TypeError, KeyError, subprocess.TimeoutExpired):
            return cls()

    def observe(self, log):
        error_type = classify_error(' '.join(str(log.get(key) or '') for key in ('msg', 'error')))
        if error_type == 'SOURCE_UNAVAILABLE':
            self.source_failure = True
            self.failed.discard(log.get('object'))
        stats = log.get('stats')
        if isinstance(stats, dict):
            value = stats.get('bytes')
            if type(value) in (int, float) and math.isfinite(value) and value >= 0:
                self.stats_seen = True
                self.bytes = max(self.bytes, int(value))
            for entry in stats.get('transferring') or []:
                if isinstance(entry, dict) and (entry.get('bytes') or 0) > 0:
                    self._start(entry.get('name'), entry.get('size'), entry['bytes'])
        if 'copied (' in str(log.get('msg', '')).lower():
            name = log.get('object')
            self._start(name, log.get('size'))
            if name in self.started:
                self.covered[name] = self.started[name]
        if (log.get('level') in ('error', 'critical') and log.get('object') and
                error_type != 'SOURCE_UNAVAILABLE'):
            self.failed.add(log['object'])

    def _start(self, name, size=None, transferred=0):
        if not isinstance(name, str):
            self.unknown = True
            return
        size = self.sizes.get(name, size)
        if type(size) not in (int, float) or not math.isfinite(size) or size < 0:
            self.unknown = True
            return
        self.started[name] = max(self.started.get(name, 0), int(size))
        self.covered[name] = max(self.covered.get(name, 0), int(transferred))

    def charged_bytes(self):
        if self.bytes > 0:
            for name in self.failed:
                self._start(name)
        if self.unknown or (self.bytes > 0 and not self.started):
            return None
        if not self.stats_seen and not self.started and not self.source_failure:
            return None
        if self.bytes > sum(self.covered.values()):
            return None
        return sum(self.started.values())
