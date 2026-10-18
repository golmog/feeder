"""검증한 실행 설정과 이전 버전을 SQLite에 원자적으로 보관한다."""

from contextlib import closing
from datetime import datetime, timezone
import json

from .jobs import ConflictError


def validate_configuration(payload):
    if not isinstance(payload, dict) or set(payload) != {'config_version', 'worker', 'rclone'}:
        raise ValueError('config_version, worker, rclone의 전체 설정을 지정해주세요.')
    version = payload['config_version']
    if type(version) is not int or not 1 <= version <= 2**31-1:
        raise ValueError('config_version은 1~2147483647의 정수여야 합니다.')
    groups = {
        'worker': {'max_concurrent_jobs': (1, 2)},
        'rclone': {'transfers': (1, 1), 'checkers': (1, 4), 'buffer_size_mib': (0, 16),
                   'multi_thread_streams': (0, 0), 'stats_interval_seconds': (1, 30)},
    }
    result = {'config_version': version}
    for group, fields in groups.items():
        values = payload[group]
        if not isinstance(values, dict) or set(values) != set(fields):
            raise ValueError(f'{group}에 허용된 전체 설정만 지정해주세요.')
        for key, (minimum, maximum) in fields.items():
            if type(values[key]) is not int or not minimum <= values[key] <= maximum:
                raise ValueError(f'{group}.{key}의 허용 범위는 정수 {minimum}~{maximum}입니다.')
        result[group] = dict(values)
    return result


class Configurations:
    def __init__(self, database, events):
        self.database, self.events = database, events

    @staticmethod
    def read(connection, version=None):
        if version is None:
            row = connection.execute("SELECT value FROM schema_meta WHERE key='active_config_version'").fetchone()
            if row is None:
                raise RuntimeError('활성 실행 설정이 없습니다.')
            version = int(row[0])
        row = connection.execute('SELECT payload FROM configurations WHERE version=?', (version,)).fetchone()
        if row is None:
            raise ValueError('등록되지 않은 설정 버전입니다.')
        payload = validate_configuration(json.loads(row[0]))
        if payload['config_version'] != version:
            raise RuntimeError('저장된 설정 버전이 일치하지 않습니다.')
        return payload

    def initialize(self, settings):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            if connection.execute('SELECT count(*) FROM configurations').fetchone()[0] == 0:
                payload = {'config_version': 1, 'worker': {'max_concurrent_jobs': settings.max_concurrent_jobs},
                    'rclone': {'transfers': 1, 'checkers': 2, 'buffer_size_mib': 8,
                               'multi_thread_streams': 0, 'stats_interval_seconds': 1}}
                self.insert(connection, payload)
            self.read(connection)
            connection.commit()

    @staticmethod
    def insert(connection, payload):
        version = payload['config_version']
        connection.execute('INSERT INTO configurations(version,payload,created_at) VALUES (?,?,?)',
            (version, json.dumps(payload, sort_keys=True, separators=(',', ':')), datetime.now(timezone.utc).isoformat()))
        connection.execute("INSERT OR REPLACE INTO schema_meta(key,value) VALUES ('active_config_version',?)", (str(version),))

    def get(self, version=None):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN')
            return self.read(connection, version)

    def update(self, payload):
        payload = validate_configuration(payload)
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            current = self.read(connection)
            if payload == current:
                return current
            if payload['config_version'] != current['config_version'] + 1:
                raise ConflictError('설정 버전은 현재 버전보다 정확히 1 커야 합니다.')
            if connection.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running') LIMIT 1").fetchone():
                raise ConflictError('대기 또는 실행 작업이 있어 설정을 변경할 수 없습니다.')
            # 이전 행은 변경하지 않는다. 새 버전과 활성 포인터·알림을 함께 커밋한다.
            self.insert(connection, payload)
            self.events.append(connection, 'worker.status', {'config_version': payload['config_version'],
                'active_jobs': 0, 'queued_jobs': 0, 'configuration_changed': True})
            connection.commit()
        self.events.notify()
        return payload
