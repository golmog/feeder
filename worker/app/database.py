"""작업 저장소를 준비하며 연결마다 잠금 대기 시간을 설정한다."""

from contextlib import closing
import os
from pathlib import Path
import sqlite3
import uuid


SCHEMA_VERSION = 7


class Database:
    def __init__(self, path: Path):
        self.path = path

    def connect(self) -> sqlite3.Connection:
        # 잘못된 경로에 빈 DB를 자동 생성하지 않도록 초기화 이후에는 rw로 연다.
        connection = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA busy_timeout = 5000')
        connection.execute('PRAGMA foreign_keys = ON')
        connection.execute('PRAGMA synchronous = NORMAL')
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        with closing(self.connect()) as connection:
            mode = connection.execute('PRAGMA journal_mode = WAL').fetchone()[0]
            if mode.lower() != 'wal':
                raise RuntimeError('SQLite WAL 모드를 활성화하지 못했습니다.')
            connection.execute('BEGIN IMMEDIATE')
            try:
                connection.execute('CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
                existing = connection.execute("SELECT value FROM schema_meta WHERE key = 'version'").fetchone()
                if existing is not None and int(existing['value']) not in range(1, SCHEMA_VERSION + 1):
                    raise RuntimeError('지원하지 않는 DB 스키마 버전입니다. 기존 DB를 보존하고 시작을 중단합니다.')
                connection.execute('''CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    bytes_transferred INTEGER NOT NULL DEFAULT 0 CHECK(bytes_transferred >= 0),
                    total_bytes INTEGER CHECK(total_bytes IS NULL OR total_bytes >= 0),
                    speed REAL NOT NULL DEFAULT 0 CHECK(speed >= 0),
                    eta REAL,
                    error_message TEXT,
                    rclone_exit_code INTEGER,
                    config_version INTEGER NOT NULL DEFAULT 1 CHECK(config_version >= 1)
                )''')
                connection.execute('CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs(status, created_at)')
                columns = {row['name'] for row in connection.execute('PRAGMA table_info(jobs)')}
                if 'cancel_requested' not in columns:
                    connection.execute('ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0')
                if 'exclude_patterns' not in columns:
                    connection.execute("ALTER TABLE jobs ADD COLUMN exclude_patterns TEXT NOT NULL DEFAULT '[]'")
                if 'drive_impersonate' not in columns:
                    connection.execute("ALTER TABLE jobs ADD COLUMN drive_impersonate TEXT NOT NULL DEFAULT ''")
                connection.execute('''CREATE TABLE IF NOT EXISTS job_logs (
                    id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id),
                    timestamp TEXT NOT NULL, message TEXT NOT NULL
                )''')
                connection.execute('CREATE INDEX IF NOT EXISTS job_logs_job ON job_logs(job_id, id)')
                connection.execute('''CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL, data TEXT NOT NULL, created_at TEXT NOT NULL
                )''')
                connection.execute("INSERT OR IGNORE INTO schema_meta(key,value) VALUES ('stream_id',?)", (uuid.uuid4().hex,))
                connection.execute('''CREATE TABLE IF NOT EXISTS configurations (
                    version INTEGER PRIMARY KEY CHECK(version > 0),
                    payload TEXT NOT NULL, created_at TEXT NOT NULL
                )''')
                connection.execute('''CREATE TABLE IF NOT EXISTS cancelled_job_ids (
                    job_id TEXT PRIMARY KEY NOT NULL, created_at TEXT NOT NULL
                )''')
                connection.execute("INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', ?)", (str(SCHEMA_VERSION),))
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def counts(self) -> dict[str, int]:
        with closing(self.connect()) as connection:
            rows = connection.execute("SELECT status, count(*) AS count FROM jobs WHERE status IN ('running', 'queued') GROUP BY status").fetchall()
        counts = {row['status']: row['count'] for row in rows}
        return {'active_jobs': counts.get('running', 0), 'queued_jobs': counts.get('queued', 0)}
