"""경로 검증과 트랜잭션 기반의 영속 작업 큐."""

from contextlib import closing
from datetime import datetime, timezone
import math
import re
import json
from urllib.parse import unquote


STATUSES = ('queued', 'running', 'completed', 'failed', 'cancelled')


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


class ConflictError(ValueError):
    pass


def validate_payload(payload, settings) -> dict:
    if not isinstance(payload, dict) or set(payload) - {'job_id', 'source', 'destination', 'config_version', 'exclude_patterns', 'drive_impersonate'}:
        raise ValueError('job_id, source, destination, config_version, exclude_patterns, drive_impersonate만 지정할 수 있습니다.')
    job_id = payload.get('job_id')
    if not isinstance(job_id, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', job_id):
        raise ValueError('job_id는 영문·숫자로 시작하는 1~128자의 영문·숫자·점·밑줄·하이픈이어야 합니다.')
    remotes = settings.validate_remotes()
    for field, allowed in remotes.items():
        value = payload.get(field)
        if not isinstance(value, str) or not 1 <= len(value) <= 4096 or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError(f'{field}의 경로가 유효하지 않습니다.')
        match = re.fullmatch(r'([A-Za-z0-9_-]+):(.+)', value)
        if not match or match[1] not in allowed:
            raise ValueError(f'{field}에는 워커 Rclone 설정에 정의된 지원 타입의 리모트 경로를 지정해주세요.')
        decoded = match[2]
        for _ in range(3):
            decoded = unquote(decoded)
        parts = decoded.replace('\\', '/').split('/')
        if (decoded.startswith(('/', '\\')) or any(p in ('.', '..') for p in parts)
                or not decoded.strip('/') or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in decoded)):
            raise ValueError(f'{field}에 절대/상위 탐색 경로를 사용할 수 없습니다.')
        if field == 'destination' and re.fullmatch(r'\{[^}]+\}/?', decoded):
            raise ValueError('목적지에는 드라이브 루트 아래의 작업 폴더를 지정해주세요.')
    version = payload.get('config_version', 1)
    if type(version) is not int or version < 1:
        raise ValueError('config_version은 양의 정수여야 합니다.')
    result = {**{key: payload[key] for key in ('job_id', 'source', 'destination')}, 'config_version': version}
    if 'drive_impersonate' in payload:
        email = payload['drive_impersonate']
        if (not isinstance(email, str) or len(email) > 254 or
                (email and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._%+-]{0,63}@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?', email))):
            raise ValueError('drive_impersonate에는 위임할 계정 이메일을 지정해주세요.')
        result['drive_impersonate'] = email
    if 'exclude_patterns' in payload:
        patterns = payload['exclude_patterns']
        if (not isinstance(patterns, list) or len(patterns) > 128 or
                any(not isinstance(value, str) or not 1 <= len(value) <= 512 or
                    any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in value)
                    for value in patterns) or sum(len(value) for value in patterns) > 16384):
            raise ValueError('전송 제외 패턴은 128개·개별 512자·총 16384자 이내의 문자열 목록이어야 합니다.')
        result['exclude_patterns'] = list(patterns)
    return result


def as_dict(row) -> dict:
    job = dict(row)
    job['cancel_requested'] = bool(job['cancel_requested'])
    job['exclude_patterns'] = json.loads(job['exclude_patterns'])
    total = job['total_bytes']
    job['progress'] = {
        'bytes_transferred': job['bytes_transferred'], 'total_bytes': total,
        'percent': min(100.0, 100 * job['bytes_transferred'] / total) if total else (100.0 if job['status'] == 'completed' else 0.0),
        'speed_bytes_per_sec': job['speed'], 'eta_seconds': job['eta'],
    }
    return job


class JobStore:
    def __init__(self, database, events):
        self.database = database
        self.events = events

    def commit(self, connection):
        connection.commit()
        self.events.notify()

    def event(self, connection, job_id, event_type):
        row = connection.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
        if row is not None:
            job = as_dict(row)
            payload = {key: job[key] for key in ('job_id', 'status', 'progress', 'cancel_requested',
                'created_at', 'started_at', 'finished_at', 'error_message', 'rclone_exit_code', 'config_version')}
            self.events.append(connection, event_type, payload)

    def worker_status(self, running, failure=False):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            counts = {r['status']: r['count'] for r in connection.execute(
                "SELECT status,count(*) AS count FROM jobs WHERE status IN ('running','queued') GROUP BY status")}
            self.events.append(connection, 'worker.status', {'running': running, 'failure': failure,
                'active_jobs': counts.get('running', 0), 'queued_jobs': counts.get('queued', 0)})
            self.commit(connection)

    @staticmethod
    def log(connection, job_id, message):
        connection.execute('INSERT INTO job_logs(job_id,timestamp,message) VALUES (?,?,?)', (job_id, now(), message))
        connection.execute('DELETE FROM job_logs WHERE job_id=? AND id NOT IN (SELECT id FROM job_logs WHERE job_id=? ORDER BY id DESC LIMIT 50)', (job_id, job_id))

    def submit(self, data):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            try:
                row = connection.execute('SELECT * FROM jobs WHERE job_id=?', (data['job_id'],)).fetchone()
                if row:
                    if any(row[key] != data[key] for key in ('source', 'destination', 'config_version')):
                        raise ConflictError('동일 job_id에 다른 작업 내용이 등록되어 있습니다.')
                    if json.loads(row['exclude_patterns']) != data.get('exclude_patterns', []):
                        raise ConflictError('동일 job_id의 전송 제외 패턴이 다릅니다.')
                    if row['drive_impersonate'] != data.get('drive_impersonate', ''):
                        raise ConflictError('동일 job_id의 위임 계정이 다릅니다.')
                    return as_dict(row)
                if connection.execute('SELECT 1 FROM cancelled_job_ids WHERE job_id=?', (data['job_id'],)).fetchone():
                    raise ConflictError('등록 전에 취소된 작업 ID입니다.')
                from .configuration import Configurations
                if data['config_version'] != Configurations.read(connection)['config_version']:
                    raise ValueError('작업은 현재 활성 설정 버전으로 등록해야 합니다.')
                connection.execute('INSERT INTO jobs(job_id,status,source,destination,created_at,config_version) VALUES (?,\'queued\',?,?,?,?)',
                                   (data['job_id'], data['source'], data['destination'], now(), data['config_version']))
                connection.execute('UPDATE jobs SET exclude_patterns=? WHERE job_id=?',
                    (json.dumps(data.get('exclude_patterns', [])), data['job_id']))
                connection.execute('UPDATE jobs SET drive_impersonate=? WHERE job_id=?',
                    (data.get('drive_impersonate', ''), data['job_id']))
                self.log(connection, data['job_id'], '작업 등록')
                self.event(connection, data['job_id'], 'job.queued')
                self.commit(connection)
            except Exception:
                connection.rollback()
                raise
        return self.get(data['job_id'])

    def get(self, job_id):
        with closing(self.database.connect()) as connection:
            row = connection.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
        return as_dict(row) if row else None

    def list(self, status, limit, offset):
        with closing(self.database.connect()) as connection:
            where = ' WHERE status=?' if status else ''
            params = [status] if status else []
            rows = connection.execute('SELECT * FROM jobs' + where + ' ORDER BY created_at DESC,rowid DESC LIMIT ? OFFSET ?', params + [limit, offset]).fetchall()
        return [as_dict(row) for row in rows]

    def cancel(self, job_id):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', job_id):
            raise ValueError('작업 ID가 유효하지 않습니다.')
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute('SELECT status,cancel_requested FROM jobs WHERE job_id=?', (job_id,)).fetchone()
            if row is None:
                connection.execute('INSERT OR IGNORE INTO cancelled_job_ids(job_id,created_at) VALUES (?,?)', (job_id, now()))
                self.commit(connection)
                return None
            if row['status'] == 'queued':
                connection.execute("UPDATE jobs SET status='cancelled',cancel_requested=1,finished_at=?,speed=0,eta=NULL WHERE job_id=?", (now(), job_id))
                self.log(connection, job_id, '대기 작업 취소')
                self.event(connection, job_id, 'job.cancelled')
            elif row['status'] == 'running' and not row['cancel_requested']:
                connection.execute('UPDATE jobs SET cancel_requested=1 WHERE job_id=?', (job_id,))
                self.log(connection, job_id, '실행 작업 취소 요청')
                self.event(connection, job_id, 'job.progress')
            self.commit(connection)
        return self.get(job_id)

    def recover(self):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            rows = connection.execute("SELECT job_id,cancel_requested FROM jobs WHERE status='running'").fetchall()
            for row in rows:
                connection.execute('UPDATE jobs SET status=?,finished_at=?,speed=0,eta=NULL,error_message=? WHERE job_id=?',
                    ('cancelled' if row['cancel_requested'] else 'failed', now(), '워커가 재시작되어 이전 전송이 중단됐습니다.', row['job_id']))
                self.log(connection, row['job_id'], '재시작 중단 복구')
                self.event(connection, row['job_id'], 'job.cancelled' if row['cancel_requested'] else 'job.failed')
            self.commit(connection)

    def claim(self, respect_limit=True):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY created_at,rowid LIMIT 1").fetchone()
            if row is None:
                return None
            connection.execute("UPDATE jobs SET status='running',started_at=? WHERE job_id=? AND status='queued'", (now(), row['job_id']))
            self.log(connection, row['job_id'], '전송 시작')
            self.event(connection, row['job_id'], 'job.started')
            self.commit(connection)
        return self.get(row['job_id'])

    def progress(self, job_id, stats):
        def number(key, integer=False):
            value = stats.get(key)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                return None
            if value > (2**63-1):
                return None
            return int(value) if integer else float(value)
        transferred, total, speed, eta = number('bytes', True), number('totalBytes', True), number('speed'), number('eta')
        with closing(self.database.connect()) as connection:
            changed = connection.execute("UPDATE jobs SET bytes_transferred=MAX(bytes_transferred,COALESCE(?,bytes_transferred)),total_bytes=COALESCE(?,total_bytes),speed=COALESCE(?,speed),eta=? WHERE job_id=? AND status='running'",
                               (transferred, total, speed, eta, job_id))
            if changed.rowcount:
                self.event(connection, job_id, 'job.progress')
            self.commit(connection)

    def finish(self, job_id, exit_code, error=None):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute('SELECT status,cancel_requested FROM jobs WHERE job_id=?', (job_id,)).fetchone()
            if row is None or row['status'] != 'running':
                return
            status = 'cancelled' if row['cancel_requested'] else ('completed' if exit_code == 0 and error is None else 'failed')
            message = None if status in ('completed', 'cancelled') else (error or f'Rclone 종료 코드 {exit_code}')
            connection.execute('UPDATE jobs SET status=?,finished_at=?,speed=0,eta=NULL,rclone_exit_code=?,error_message=? WHERE job_id=?',
                               (status, now(), exit_code, message, job_id))
            self.log(connection, job_id, f'전송 종료: {status}')
            if message:
                self.log(connection, job_id, message)
            self.event(connection, job_id, 'job.'+status)
            self.commit(connection)
