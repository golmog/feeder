"""Feeder가 제출한 작업을 실행하고 Rclone 생명주기를 관리한다."""

import fcntl
import json
import logging
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import threading
import time

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
                job = self.store.claim()
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
        except Exception as exc:
            self.failure = True
            self._stop.set()
            logger.error('작업 워커 중단: %s', type(exc).__name__)
            try:
                self.store.worker_status(False, True)
            except Exception:
                logger.error('워커 중단 상태 이벤트를 기록하지 못했습니다.')

    def _run_job(self, job):
        try:
            self._execute(job)
        except Exception as exc:
            self.failure = True
            self._stop.set()
            self.notify()
            logger.error('작업 실행 중단: %s', type(exc).__name__)
            try:
                self.store.worker_status(False, True)
            except Exception:
                logger.error('워커 중단 상태 이벤트를 기록하지 못했습니다.')
        finally:
            with self._threads_lock:
                self._threads.discard(threading.current_thread())

    @staticmethod
    def _terminate(process, force=False):
        try:
            os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            pass

    def command(self, job):
        options = Configurations(self.store.database, self.store.events).get(job['config_version'])['rclone']
        # 옵션은 고정하고 원본과 목적지는 -- 뒤에 넣어 옵션 해석을 차단한다.
        return [str(self.settings.rclone_binary), '--config', str(self.settings.rclone_config),
            'copy', '--transfers', '1', '--checkers', str(options['checkers']), '--buffer-size', f"{options['buffer_size_mib']}Mi",
            '--multi-thread-streams', '0', '--retries', '1', '--low-level-retries', '1',
            '--contimeout', '10s', '--timeout', '2m', '--stats', f"{options['stats_interval_seconds']}s", '--stats-log-level', 'INFO',
            '--use-json-log', '--log-level', 'INFO',
            *[value for pattern in job.get('exclude_patterns', []) for value in ('--exclude', pattern)],
            *(['--drive-impersonate', job['drive_impersonate']] if job.get('drive_impersonate') else []),
            '--', job['source'], job['destination']]

    def _execute(self, job):
        process = None
        selector = selectors.DefaultSelector()
        error = None
        tail = bytearray()
        termination = None
        forced = False
        error_flags = set()

        def consume(line):
            try:
                log = json.loads(line)
                if not isinstance(log, dict):
                    return
                if isinstance(log.get('stats'), dict):
                    self.store.progress(job['job_id'], log['stats'])
                if log.get('level') in ('error', 'critical'):
                    message = str(log.get('msg', '')).lower()
                    # 원문 로그에는 인증 URL이 섞일 수 있어 분류명만 보관한다.
                    for word in ('403', '429', 'quota', 'permission', 'timeout', 'not found'):
                        if word in message:
                            error_flags.add(word)
            except (ValueError, TypeError, OverflowError):
                pass

        try:
            validate_payload({k: job[k] for k in ('job_id', 'source', 'destination', 'config_version', 'exclude_patterns', 'drive_impersonate')}, self.settings)
            current = self.store.get(job['job_id'])
            if self._stop.is_set() or current['cancel_requested']:
                self.store.finish(job['job_id'], None, '서비스 종료로 전송 시작이 중단됐습니다.')
                return
            process = subprocess.Popen(
                [sys.executable, '-B', '-m', 'app.child', str(os.getpid())] + self.command(job),
                cwd=Path(__file__).resolve().parents[1], stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True,
                env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'),
            )
            os.set_blocking(process.stderr.fileno(), False)
            selector.register(process.stderr, selectors.EVENT_READ)
            eof = False
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
                if error_flags:
                    error += ' (' + ', '.join(sorted(error_flags)) + ')'
        except Exception as exc:
            error = f'전송 실행 실패: {type(exc).__name__}'
        finally:
            if process is not None:
                # 정상 종료 후에도 같은 그룹에 남은 보조 프로세스를 정리한다.
                self._terminate(process, force=True)
                process.wait()
                process.stderr.close()
            selector.close()
        self.store.finish(job['job_id'], process.returncode if process is not None else None, error)
