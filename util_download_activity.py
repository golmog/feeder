# -*- coding: utf-8 -*-
import os
import sqlite3
import time

from .setup import path_data


class DownloadActivity:
    """웹 서버와 Celery가 공유하는 다운로드 작업 활동 기록이다."""

    MAX_ENTRIES = 200

    @classmethod
    def _connect(cls):
        filepath = os.path.join(path_data, 'db', 'feeder_download_activity.db')
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        connection = sqlite3.connect(filepath, timeout=2)
        try:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('''CREATE TABLE IF NOT EXISTS activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                stage TEXT NOT NULL,
                message TEXT NOT NULL,
                level TEXT NOT NULL
            )''')
            return connection
        except Exception:
            connection.close()
            raise

    @classmethod
    def record(cls, stage, message, level='info'):
        """인증 정보나 원본 오류 대신 화면에 공개할 진행 설명만 전달한다."""
        try:
            connection = cls._connect()
            try:
                with connection:
                    connection.execute(
                        'INSERT INTO activity (timestamp, stage, message, level) VALUES (?, ?, ?, ?)',
                        (time.time(), str(stage)[:40], str(message).replace('\n', ' ')[:500],
                         level if level in ('info', 'warning', 'error') else 'info'))
                    connection.execute('''DELETE FROM activity WHERE id <= (
                        SELECT id FROM activity ORDER BY id DESC LIMIT 1 OFFSET ?
                    )''', (cls.MAX_ENTRIES,))
            finally:
                connection.close()
        except (OSError, sqlite3.Error):
            # 활동 표시 장애가 실제 다운로드 작업을 중단시키지 않도록 한다.
            pass

    @classmethod
    def read(cls, after_id=0):
        connection = cls._connect()
        try:
            connection.row_factory = sqlite3.Row
            rows = connection.execute('''SELECT * FROM (
                SELECT * FROM activity WHERE id > ? ORDER BY id DESC LIMIT ?
            ) ORDER BY id''', (max(0, int(after_id)), cls.MAX_ENTRIES)).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    @classmethod
    def track(cls, stage, items, label='항목', context='', report_start=True):
        """순회한 개수를 약 3초 간격으로 기록하고 정상 종료 시 최종 개수를 남긴다."""
        total = len(items)
        prefix = context + ' · ' if context else ''
        reported = report_start
        if report_start:
            cls.record(stage, f'{prefix}{label} 검토 0/{total}건')
        last_report = time.monotonic()
        for index, item in enumerate(items, 1):
            yield item
            if (index == total and reported) or time.monotonic() - last_report >= 3:
                cls.record(stage, f'{prefix}{label} 검토 {index}/{total}건')
                last_report = time.monotonic()
                reported = True
