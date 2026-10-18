"""영속 이벤트 보관과 메모리 상한이 있는 SSE 구독을 제공한다."""

from contextlib import closing
from datetime import datetime, timezone
import json
import sqlite3
import threading
import time


def frame(event_type, data, event_id):
    return f'id: {event_id}\nevent: {event_type}\ndata: {json.dumps(data, ensure_ascii=True, allow_nan=False, separators=(",", ":"))}\n\n'


class StreamLimitError(RuntimeError):
    pass


class Lease:
    def __init__(self, broker):
        self.broker = broker
        self.released = False

    def release(self):
        with self.broker.condition:
            if not self.released:
                self.released = True
                self.broker.clients -= 1


class Events:
    def __init__(self, database, retention=2000, max_clients=1, heartbeat=15):
        if retention < 1 or max_clients < 1 or heartbeat <= 0:
            raise ValueError('이벤트 보관/연결/heartbeat 값은 양수여야 합니다.')
        self.database = database
        self.retention, self.max_clients, self.heartbeat = retention, max_clients, heartbeat
        self.condition = threading.Condition()
        self.clients = 0
        self.closed = False

    def append(self, connection, event_type, data):
        cursor = connection.execute('INSERT INTO events(event_type,data,created_at) VALUES (?,?,?)',
            (event_type, json.dumps(data, ensure_ascii=True, allow_nan=False, separators=(',', ':')),
             datetime.now(timezone.utc).isoformat()))
        connection.execute('DELETE FROM events WHERE event_id<=?', (cursor.lastrowid-self.retention,))
        return cursor.lastrowid

    def notify(self):
        with self.condition:
            self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    @staticmethod
    def _state(connection):
        latest = connection.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
        oldest = connection.execute('SELECT min(event_id) FROM events').fetchone()[0]
        counts = {row['status']: row['count'] for row in connection.execute(
            "SELECT status,count(*) AS count FROM jobs WHERE status IN ('running','queued') GROUP BY status")}
        return {
            'stream_id': connection.execute("SELECT value FROM schema_meta WHERE key='stream_id'").fetchone()[0],
            'latest_event_id': latest[0] if latest else 0, 'oldest_event_id': oldest,
            'active_jobs': counts.get('running', 0), 'queued_jobs': counts.get('queued', 0),
        }

    def state(self):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN')
            return self._state(connection)

    @staticmethod
    def reset_reason(cursor, stream_id, state):
        if stream_id is not None and stream_id != state['stream_id']:
            return 'stream_changed'
        if cursor is None:
            return 'initial_sync'
        if cursor > state['latest_event_id']:
            return 'cursor_ahead'
        oldest = state['oldest_event_id']
        if cursor < state['latest_event_id'] and (oldest is None or cursor < oldest-1):
            return 'history_expired'
        return None

    def page(self, cursor, stream_id):
        with closing(self.database.connect()) as connection:
            connection.execute('BEGIN')
            state = self._state(connection)
            reason = self.reset_reason(cursor, stream_id, state)
            rows = [] if reason else [dict(row) for row in connection.execute(
                'SELECT event_id,event_type,data FROM events WHERE event_id>? ORDER BY event_id LIMIT 100', (cursor,))]
        return state, reason, rows

    def acquire(self):
        with self.condition:
            if self.closed or self.clients >= self.max_clients:
                raise StreamLimitError('SSE 연결 상한에 도달했거나 서비스가 종료 중입니다.')
            self.clients += 1
            return Lease(self)

    def stream(self, lease, cursor, stream_id, state, disconnected=None):
        try:
            reason = self.reset_reason(cursor, stream_id, state)
            if reason:
                cursor = state['latest_event_id']
            # 접속 상태 프레임은 현재 커서를 전달하며 새 작업 이벤트 ID를 소비하지 않는다.
            yield 'retry: 3000\n\n'
            yield frame('worker.status', dict(state, resync_required=reason is not None,
                reason=reason or 'connected', replay_after=cursor), cursor)
            stream_id = state['stream_id']
            last_sent = time.monotonic()
            while not self.closed:
                if disconnected is not None and disconnected():
                    return
                try:
                    state, reason, rows = self.page(cursor, stream_id)
                except (sqlite3.Error, OSError):
                    yield frame('worker.status', {'resync_required': True, 'reason': 'storage_unavailable'}, cursor)
                    return
                if reason:
                    cursor = state['latest_event_id']
                    stream_id = state['stream_id']
                    yield frame('worker.status', dict(state, resync_required=True, reason=reason, replay_after=cursor), cursor)
                    last_sent = time.monotonic()
                elif rows:
                    for row in rows:
                        if self.closed:
                            return
                        cursor = row['event_id']
                        yield f'id: {cursor}\nevent: {row["event_type"]}\ndata: {row["data"]}\n\n'
                        last_sent = time.monotonic()
                    continue
                if time.monotonic()-last_sent >= self.heartbeat:
                    yield ': heartbeat\n\n'
                    last_sent = time.monotonic()
                # 알림을 놓쳐도 최대 0.5초 뒤 DB를 다시 확인한다.
                with self.condition:
                    if not self.closed:
                        self.condition.wait(timeout=min(.5, self.heartbeat))
        finally:
            lease.release()
