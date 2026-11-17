"""Feeder가 제출한 작업을 실행하고 Rclone 생명주기를 관리한다."""

import fcntl
import json
import logging
import os
from pathlib import Path
import selectors
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque

from rclone_logging import redact_log, FileQuota

from .jobs import validate_payload
from .configuration import Configurations


logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, settings, store):
        self.settings, self.store = settings, store
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._threads = set()
        self._threads_lock = threading.Lock()
        self._dispatcher = None
        self._lock = None
        self.started = False
        self.failure = False
        self.failure_callback = None

    def start(self):
        if self.started:
            return
        descriptor = os.open(str(self.settings.database) + '.runner.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise RuntimeError('동일 DB의 워커가 이미 실행 중입니다.') from None
        self._lock = descriptor
        try:
            self.store.recover()
            self._stop.clear()
            self.failure = False
            self.started = True
            # 동시 전송 수는 Feeder가 제어하고 워커는 접수한 작업마다 실행 스레드를 만든다.
            self._dispatcher = threading.Thread(target=self._loop, name='worker-dispatcher', daemon=True)
            self._dispatcher.start()
            self.store.worker_status(True)
        except Exception:
            self.stop()
            raise

    def notify(self):
        self._wake.set()

    def stop(self):
        self._stop.set()
        self.notify()
        deadline = time.monotonic() + 15
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=max(0, deadline-time.monotonic()))
        with self._threads_lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(timeout=max(0, deadline-time.monotonic()))
        if (self._dispatcher is not None and self._dispatcher.is_alive()) or any(thread.is_alive() for thread in threads):
            # 스레드가 DB/프로세스를 소유하는 동안 실행 잠금을 해제하지 않는다.
            self.failure = True
            raise RuntimeError('실행 중인 워커가 종료되지 않았습니다.')
        self._threads.clear()
        self._dispatcher = None
        try:
            if self.started:
                self.store.worker_status(False, self.failure)
        finally:
            if self._lock is not None:
                os.close(self._lock)
                self._lock = None
            self.started = False

    def _loop(self):
        try:
            while not self._stop.is_set():
                try:
                    job = self.store.claim()
                except sqlite3.Error:
                    logger.warning('작업 저장소 조회 지연 · 다음 순회에서 재확인합니다.')
                    self._wake.wait(.5)
                    self._wake.clear()
                    continue
                if job is None:
                    self._wake.wait(.5)
                    self._wake.clear()
                    continue
                thread = threading.Thread(target=self._run_job, args=(job,), name='worker-job-' + job['job_id'], daemon=True)
                with self._threads_lock:
                    self._threads.add(thread)
                try:
                    thread.start()
                except Exception:
                    with self._threads_lock:
                        self._threads.discard(thread)
                    self.store.finish(job['job_id'], None, '작업 실행 스레드를 시작하지 못했습니다.')
                    raise
        except Exception:
            self._fail('작업 워커 중단')

    def _fail(self, message):
        """치명적인 실행기 오류만 기존 메인 종료 경로에 한 번 알린다."""
        with self._threads_lock:
            first_failure = not self.failure
            self.failure = True
        self._stop.set()
        self.notify()
        logger.exception(message)
        if first_failure and self.failure_callback is not None:
            self.failure_callback()
        try:
            self.store.worker_status(False, True)
        except Exception:
            logger.error('워커 중단 상태 이벤트를 기록하지 못했습니다.')

    def _run_job(self, job):
        try:
            self._execute(job)
        except sqlite3.Error:
            logger.warning('작업 %s 저장소 처리 지연 · 전송 상태 재확인이 필요합니다.', job['job_id'])
        except Exception:
            self._fail('작업 실행 중단')
        finally:
            with self._threads_lock:
                self._threads.discard(threading.current_thread())

    @staticmethod
    def _terminate(process, force=False):
        try:
            os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            pass

    def command(self, job, entry=None):
        options = Configurations(self.store.database, self.store.events).get(job['config_version'])['rclone']
        # 검증해 저장한 작업별 값만 적용하고 경로는 -- 뒤에 둔다.
        return [str(self.settings.rclone_binary), '--config', str(self.settings.rclone_config),
            'copyurl' if entry else 'copy', '--transfers', str(job.get('rclone_transfers', options['transfers'])),
            *job['rclone_extra_args'],
            '--stats', f"{options['stats_interval_seconds']}s", '--stats-log-level', 'INFO',
            '--use-json-log', '--log-level', 'INFO',
            *[value for pattern in job.get('exclude_patterns', []) for value in ('--exclude', pattern)],
            *(['--drive-impersonate', job['drive_impersonate']] if job.get('drive_impersonate') else []),
            *[argument for key, value in (entry or {}).get('headers', {}).items()
              for argument in (['--user-agent', value] if key.lower() == 'user-agent' else ['--header-download', key + ': ' + value])],
            '--', entry['url'] if entry else job['source'],
            job['destination'].rstrip('/') + '/' + entry['relative_path'] if entry else job['destination']]

    def _execute(self, job):
        process = None
        selector = selectors.DefaultSelector()
        error = None
        tail = bytearray()
        termination = None
        forced = False
        errors = deque(maxlen=8)
        quota = FileQuota()
        url_mode = job.get('source_kind', 'remote') == 'url_files'
        files = job.get('source_files', []) if url_mode else [None]
        completed_bytes = 0
        total_bytes = sum(entry['size'] for entry in files) if url_mode else None
        entry = None
        secrets = [self.settings.api_token]
        if url_mode:
            for file in files:
                secrets.extend([file['url'], *file['headers'].values()])

        def charged():
            amount = quota.charged_bytes()
            return completed_bytes + amount if url_mode and amount is not None else amount

        def remember(message):
            message = redact_log(message, secrets)
            if message and (not errors or errors[-1] != message):
                errors.append(message)

        def consume(line):
            try:
                try:
                    log = json.loads(line)
                except ValueError:
                    text = line.decode('utf-8', errors='replace') if isinstance(line, (bytes, bytearray)) else line
                    if any(word in text.lower() for word in ('error', 'failed', 'fatal')):
                        remember(text)
                    return
                if not isinstance(log, dict):
                    return
                if url_mode:
                    # copyurl의 로그 객체명 대신 CD2 메타데이터 경로로 파일별 정산한다.
                    if log.get('object'):
                        log['object'] = entry['relative_path']
                    if isinstance(log.get('stats'), dict):
                        log['stats'] = dict(log['stats'])
                        log['stats']['transferring'] = [dict(value, name=entry['relative_path'], size=entry['size'])
                            for value in log['stats'].get('transferring') or [] if isinstance(value, dict)]
                quota.observe(log)
                if isinstance(log.get('stats'), dict):
                    stats = dict(log['stats'])
                    if url_mode:
                        current_bytes = min(entry['size'], max(0, int(stats.get('bytes') or 0)))
                        # copyurl은 transferring 배열 없이 바이트 통계만 보낼 수도 있다.
                        if current_bytes:
                            quota._start(entry['relative_path'], entry['size'], current_bytes)
                        stats.update(bytes=completed_bytes + current_bytes, totalBytes=total_bytes)
                        speed = stats.get('speed') or 0
                        stats['eta'] = max(0, total_bytes - stats['bytes']) / speed if speed > 0 else None
                    self.store.accounting(job['job_id'], charged())
                    self.store.progress(job['job_id'], stats)
                    if log['stats'].get('lastError'):
                        remember(log['stats']['lastError'])
                if log.get('level') in ('error', 'critical'):
                    remember(' · '.join(str(log[key]) for key in ('object', 'msg', 'error') if log.get(key)))
            except (ValueError, TypeError, OverflowError):
                pass

        try:
            validate_payload({k: job[k] for k in ('job_id', 'source', 'destination', 'config_version', 'exclude_patterns', 'drive_impersonate', 'rclone_transfers', 'rclone_extra_args', 'session_id', 'source_kind', 'source_files') if k in job}, self.settings)
            current = self.store.get(job['job_id'])
            if self._stop.is_set() or current['cancel_requested']:
                self.store.finish(job['job_id'], None, '서비스 종료로 전송 시작이 중단됐습니다.')
                return
            # 전송 전 목록은 파일별 쿼터 계산에만 사용하며 실패해도 복사는 계속한다.
            quota = FileQuota() if url_mode else FileQuota.inspect_source([
                str(self.settings.rclone_binary), '--config', str(self.settings.rclone_config),
                'lsjson', '--recursive', '--files-only', '--no-modtime', '--no-mimetype',
                '--contimeout', '10s', '--timeout', '15s', '--retries', '1', '--low-level-retries', '1',
                *[value for pattern in job.get('exclude_patterns', []) for value in ('--exclude', pattern)],
                '--', job['source']], stopped=lambda: self._stop.is_set() or self.store.get(job['job_id'])['cancel_requested'])
            current = self.store.get(job['job_id'])
            if self._stop.is_set() or current['cancel_requested']:
                self.store.accounting(job['job_id'], 0)
                self.store.finish(job['job_id'], None, '전송 전 목록 조회 중 작업이 중단됐습니다.')
                return
            for entry in files:
                current = self.store.get(job['job_id'])
                if self._stop.is_set() or current['cancel_requested']:
                    error = '전송이 중단됐습니다.'
                    break
                if url_mode:
                    quota = FileQuota({entry['relative_path']: entry['size']})
                if process is not None:
                    self._terminate(process, force=True)
                    process.wait()
                    process.stderr.close()
                process = subprocess.Popen(
                    [sys.executable, '-B', '-m', 'app.child', str(os.getpid())] + (self.command(job, entry) if url_mode else self.command(job)),
                    cwd=Path(__file__).resolve().parents[1], stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True,
                    env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'),
                )
                os.set_blocking(process.stderr.fileno(), False)
                selector.register(process.stderr, selectors.EVENT_READ)
                eof = False
                tail.clear()
                while True:
                    current = self.store.get(job['job_id'])
                    if termination is None and (self._stop.is_set() or current['cancel_requested']):
                        termination = time.monotonic()
                        if self._stop.is_set() and not current['cancel_requested']:
                            error = '서비스 종료로 전송이 중단됐습니다.'
                        self._terminate(process)
                    if termination is not None and not forced and time.monotonic()-termination >= 5:
                        self._terminate(process, force=True)
                        forced = True
                    if not eof:
                        for key, _ in selector.select(timeout=.2):
                            data = os.read(key.fileobj.fileno(), 16384)
                            if not data:
                                selector.unregister(key.fileobj)
                                eof = True
                                break
                            tail.extend(data)
                            while b'\n' in tail:
                                line, _, remainder = tail.partition(b'\n')
                                tail[:] = remainder
                                if len(line) <= 65536:
                                    consume(line)
                            if len(tail) > 65536:
                                tail.clear()
                    else:
                        time.sleep(.05)
                    if process.poll() is not None and eof:
                        if tail:
                            consume(tail)
                        break
                if process.returncode and error is None:
                    error = f'Rclone 종료 코드 {process.returncode}'
                    if errors:
                        error += '\n' + '\n'.join(errors)
                if error or termination is not None:
                    break
                if url_mode:
                    completed_bytes += entry['size']
                    quota = FileQuota()
                    quota.stats_seen = True
                    self.store.progress(job['job_id'], {'bytes': completed_bytes, 'totalBytes': total_bytes, 'speed': 0})
        except Exception as exc:
            error = f'전송 실행 실패: {type(exc).__name__}'
        finally:
            if process is not None:
                # 정상 종료 후에도 같은 그룹에 남은 보조 프로세스를 정리한다.
                self._terminate(process, force=True)
                process.wait()
                process.stderr.close()
            selector.close()
        if error:
            logger.error('작업 %s: %s', job['job_id'], error)
        try:
            self.store.accounting(job['job_id'], charged() if process is not None else 0)
            self.store.finish(job['job_id'], process.returncode if process is not None else None, error)
        except Exception as exc:
            logger.error('작업 %s 결과 저장 미확인 (%s) · 기존 작업 ID를 유지합니다.', job['job_id'], type(exc).__name__)
