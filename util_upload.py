# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import json
import shutil
import subprocess
import threading
import uuid
from datetime import datetime, timedelta
import pytz
from sqlalchemy import func

from .setup import *
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil
from .util_download_activity import DownloadActivity
from .worker.rclone_logging import redact_log, classify_error, FileQuota


class UploadUtil:
    """Rclone 커맨드 실행, Google Drive SA 풀 쿼터 관리 및 업로드 핸들러 통합 관리자"""

    _CLEANUP_WORKERS = 4

    # --------------------------------------------------------------------------
    # 바이트 및 포맷 헬퍼
    # --------------------------------------------------------------------------
    @staticmethod
    def parse_size_bytes(size_val) -> int:
        if not size_val:
            return 0
        if isinstance(size_val, (int, float)):
            return int(size_val)
        size_str = str(size_val).strip().upper()
        units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4, "PB": 1024**5}
        m = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGTP]?I?B)$", size_str)
        if m:
            val, unit = m.groups()
            return int(float(val) * units[unit.replace('I', '')])
        return 0

    @staticmethod
    def format_bytes(size: int) -> str:
        if size is None or size <= 0:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if size < 1024.0 or unit == 'PB':
                break
            size /= 1024.0
        return f"{size:.2f} {unit}"

    @classmethod
    def _get_pid_file(cls, item_id) -> str:
        pids_dir = FeederUtil.get_tmp_dir('pids')
        return os.path.join(pids_dir, f"rclone_{item_id}.pid")

    @classmethod
    def register_rclone_pid(cls, item_id, pid: int):
        if not item_id:
            return
        try:
            pid_file = cls._get_pid_file(item_id)
            with open(pid_file, 'w', encoding='utf-8') as f:
                f.write(str(pid))
        except Exception:
            pass

    @classmethod
    def unregister_rclone_pid(cls, item_id):
        if not item_id:
            return
        try:
            pid_file = cls._get_pid_file(item_id)
            if os.path.exists(pid_file):
                os.remove(pid_file)
        except Exception:
            pass

    @classmethod
    def kill_item_rclone(cls, item_id):
        """특정 작업의 백그라운드 Rclone 프로세스를 즉시 강제 종료"""
        if not item_id:
            return
        pid_file = cls._get_pid_file(item_id)
        if not os.path.exists(pid_file):
            return
        try:
            with open(pid_file, 'r', encoding='utf-8') as f:
                pid_str = f.read().strip()
            if pid_str.isdigit():
                pid = int(pid_str)
                import signal
                try:
                    os.kill(pid, signal.SIGTERM)
                    time.sleep(0.5)
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except Exception as ex:
                    logger.debug(f"[UploadUtil] 프로세스 종료 예외: {ex}")
                logger.info(f"[UploadUtil] 중단된 작업({item_id})의 Rclone 프로세스(PID: {pid}) 강제 종료 완료")
        finally:
            cls.unregister_rclone_pid(item_id)
            FeederUtil.clear_rclone_progress(item_id)

    @staticmethod
    def run_rclone(cmd: list[str], description: str = "", log_output: bool = True, item_id=None, file_size=None, watchdog_timeout: int = None, execution_timeout: float = None, quota=None) -> tuple[bool, str]:
        """Rclone 서브프로세스 실행 및 실시간 출력/전송률 캡처"""
        env = os.environ.copy()
        safe_description = redact_log(description)

        def report_failure(message, lines=()):
            summary = f'작업 {item_id} · ' if item_id else ''
            summary += f'{safe_description}: {redact_log(message)}'
            logger.error('[Rclone] %s', summary)
            DownloadActivity.record('Rclone 오류', summary, 'error')
            for line in list(dict.fromkeys(lines))[-8:]:
                safe_line = redact_log(line)
                logger.error('[Rclone Error] %s: %s', safe_description, safe_line)
                DownloadActivity.record('Rclone 오류', f'작업 {item_id} · {safe_line}' if item_id else safe_line, 'error')

        if log_output and description:
            size_text = f" ({UploadUtil.format_bytes(file_size)})" if file_size else ""
            logger.info(f"[Rclone] 시작: {safe_description}{size_text}")

        deadline_timer = None
        execution_expired = threading.Event()
        try:
            if item_id:
                from .util_block import DownloadBlock
                if DownloadBlock.stopped(item_id):
                    return False, '차단 또는 취소된 작업입니다.'
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env
            )

            if execution_timeout is not None:
                def _expire_execution():
                    if proc.poll() is None:
                        execution_expired.set()
                        try:
                            proc.kill()
                        except ProcessLookupError:
                            pass
                deadline_timer = threading.Timer(max(0, execution_timeout), _expire_execution)
                deadline_timer.daemon = True
                deadline_timer.start()

            if item_id:
                UploadUtil.register_rclone_pid(item_id, proc.pid)
                if DownloadBlock.stopped(item_id):
                    proc.kill()
                    proc.communicate()
                    return False, '차단 또는 취소된 작업입니다.'

            # 무전송 감시 타이머 (로컬 스테이징 등 지정된 경우에만 가동, 대용량 업로드 해싱 지연 보호)
            transfer_started = threading.Event()
            killed_by_watchdog = [False]

            if watchdog_timeout and watchdog_timeout > 0:
                def _watchdog():
                    if not transfer_started.wait(timeout=watchdog_timeout):
                        if proc.poll() is None:
                            logger.warning(f"[Rclone] {description}: {watchdog_timeout}초간 데이터 전송 시작이 감지되지 않아 프로세스를 강제 종료합니다.")
                            killed_by_watchdog[0] = True
                            try:
                                proc.kill()
                                proc.wait(timeout=3)
                            except Exception:
                                pass

                watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
                watchdog_thread.start()

            output_lines = []
            for line in iter(proc.stdout.readline, ''):
                FeederUtil.cooperate()
                line = line.strip()
                if not line:
                    continue
                output_lines.append(line)
                if quota is not None:
                    try:
                        entry = json.loads(line)
                        if isinstance(entry, dict):
                            quota.observe(entry)
                            stats = entry.get('stats') or {}
                            if stats.get('bytes', 0) > 0:
                                transfer_started.set()
                            if item_id and stats:
                                transferred = stats.get('bytes', 0)
                                total = stats.get('totalBytes') or file_size or 0
                                FeederUtil.report_rclone_progress(item_id, {
                                    'progress': min(100, 100 * transferred / total) if total else 0,
                                    'downloaded_bytes': transferred, 'total_bytes': total,
                                    'download_speed': stats.get('speed') or 0,
                                    'speed_str': FeederUtil.format_bytes(stats.get('speed') or 0) + '/s',
                                    'eta': str(stats.get('eta') or '')})
                    except (ValueError, TypeError):
                        pass

                # 실제 전송용 실행 시에만 에러/경고 출력 (lsjson 등 단순 검사 시 오탐 차단)
                if log_output:
                    line_lower = line.lower()
                    has_error_level = bool(re.match(r'^\s*(?:error|fatal)\s*:', line_lower))
                    has_failure_phrase = any(phrase in line_lower for phrase in (
                        'failed to', 'permission denied', 'quota exceeded',
                        'rate limit', 'authentication failed', '403 forbidden'
                    ))
                    is_progress_line = bool(re.search(r'\d+(?:\.\d+)?\s*[kmgt]?ib\s*/', line_lower))
                    if (has_error_level or has_failure_phrase) and not is_progress_line and 'directory not found' not in line_lower:
                        logger.error('[Rclone Error] %s: %s', safe_description, redact_log(line))
                    elif re.match(r'^\s*(?:warning|notice)\s*:', line_lower):
                        logger.info('[Rclone Log] %s: %s', safe_description, redact_log(line))

                # Rclone --stats-one-line 실시간 전송률 정밀 파싱
                if "%" in line:
                    m = re.search(r'([0-9.]+\s*[a-zA-Z]+)\s*/\s*([0-9.]+\s*[a-zA-Z]+),\s*([0-9.]+)%,\s*([0-9.]+\s*[a-zA-Z/]+)(?:,\s*ETA\s*([^\s,]+))?', line)
                    if m:
                        trans_str = m.group(1).strip()
                        total_str = m.group(2).strip()
                        pct_val = float(m.group(3))
                        speed_val = m.group(4).strip()
                        eta_val = m.group(5) or ''

                        speed_clean = speed_val.lower().replace('/s', '').strip()
                        speed_bytes = UploadUtil.parse_size_bytes(speed_clean)
                        trans_bytes = UploadUtil.parse_size_bytes(trans_str)

                        if (pct_val > 0 or trans_bytes > 0) and not transfer_started.is_set():
                            transfer_started.set()

                        if item_id:
                            FeederUtil.report_rclone_progress(item_id, {
                                'progress': pct_val,
                                'speed_str': speed_val,
                                'download_speed': speed_bytes,
                                'downloaded_bytes': trans_bytes,
                                'total_bytes': UploadUtil.parse_size_bytes(total_str) if total_str else (file_size or 0),
                                'eta': eta_val
                            }, action='update')

            transfer_started.set()
            proc.stdout.close()
            proc.wait()
            full_output = '\n'.join(output_lines)

            if execution_expired.is_set():
                if log_output:
                    report_failure('원격 유지보수 실행 시간 만료', output_lines[-8:])
                return False, "원격 유지보수 실행 시간 만료 (다음 주기에 재확인)"

            if killed_by_watchdog[0]:
                err_msg = f"{watchdog_timeout}초간 데이터 전송 시작 불가 (무반응 타임아웃 강제 종료)"
                if log_output:
                    report_failure(err_msg, output_lines[-8:])
                return False, err_msg

            if proc.returncode == 0:
                if log_output and description:
                    logger.info(f"[Rclone] 성공: {safe_description}")
                return True, full_output

            filtered = [line for line in output_lines if re.search(
                r'\b(?:error|critical|fatal|notice|failed)\b', line, re.IGNORECASE)]
            if log_output:
                report_failure(f'종료 코드 {proc.returncode}', filtered or output_lines[-8:])
            return False, full_output
        except Exception as ex:
            message = redact_log(ex)
            if log_output:
                report_failure(f'실행 예외: {message}')
            return False, message
        finally:
            if deadline_timer is not None:
                deadline_timer.cancel()
            if item_id:
                UploadUtil.unregister_rclone_pid(item_id)
                FeederUtil.report_rclone_progress(item_id, {}, action='clear')

    @classmethod
    def get_remote_size(cls, remote_path: str, rclone_conf: str, impersonate_email: str = None) -> int:
        cmd = ["rclone", "size", remote_path, "--json", "--config", rclone_conf]
        if impersonate_email:
            cmd.extend(["--drive-impersonate", impersonate_email])
        suc, out = cls.run_rclone(cmd, log_output=False)
        if suc:
            try:
                return int(json.loads(out).get('bytes', -1))
            except Exception:
                return -1
        return -1

    # --------------------------------------------------------------------------
    # SA 계정 풀 매니저 싱글톤/인스턴스
    # --------------------------------------------------------------------------
    class AccountManager:
        def __init__(self):
            self.lock = threading.Lock()
            config_data = FeederUtil.load_yaml()
            self.rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or config_data.get('rclone', {}).get('conf_path', '')
            self.accounts = FeederUtil.get_gdrive_accounts()
            self.busy_accounts = set()

            # 쿼터 및 임계 수치 계층 판별: 전역 DB 설정 -> YAML default 설정 -> 시스템 기본값
            default_yaml = config_data.get('default', {})
            self.limit_user = UploadUtil.parse_size_bytes(P.ModelSetting.get('download_gdrive_upload_limit') or default_yaml.get('gdrive_upload_limit') or '700GB')
            self.limit_shared = UploadUtil.parse_size_bytes(P.ModelSetting.get('download_shared_drive_upload_limit') or default_yaml.get('shared_drive_upload_limit') or '3TB')
            self.threshold = UploadUtil.parse_size_bytes(P.ModelSetting.get('download_mydrive_upload_threshold') or default_yaml.get('mydrive_upload_threshold') or '14GB')
            self.reset_time_str = P.ModelSetting.get('download_shared_drive_quota_reset_time') or default_yaml.get('shared_drive_quota_reset_time') or '16:00'

            self.usage_map = self._get_24h_usage_summary()
            self.shared_usage = self.usage_map.get('SHARED_DRIVE_UPLOAD', 0)
            self.blocked_accounts = self._get_blocked_accounts()

        def _get_24h_usage_summary(self) -> dict[str, int]:
            cutoff = int(time.time()) - 86400
            rows = (
                db.session.query(
                    ModelDownloadStat.stat_key,
                    func.sum(ModelDownloadStat.stat_value).label('total')
                )
                .filter(
                    ModelDownloadStat.stat_type.in_(['gdrive_usage', 'gdrive_reservation']),
                    ModelDownloadStat.timestamp > cutoff
                )
                .group_by(ModelDownloadStat.stat_key)
                .all()
            )
            return {r[0]: int(r[1]) for r in rows if r[0]}

        def _get_blocked_accounts(self) -> dict[str, int]:
            now = int(time.time())
            rows = (
                db.session.query(ModelDownloadStat.stat_key, ModelDownloadStat.stat_value)
                .filter(
                    ModelDownloadStat.stat_type == 'account_block',
                    ModelDownloadStat.stat_value > now
                )
                .all()
            )
            return {r[0]: int(r[1]) for r in rows if r[0]}

        def allocate_account(self, file_size: int, use_pool: bool = True, exclude_accounts=None) -> tuple[str, dict | None, str]:
            with self.lock:
                # 공유 드라이브 403 쿼터 차단 및 일일 한도 체크
                if 'SHARED_DRIVE_UPLOAD' in self.blocked_accounts:
                    return 'skip', None, "공유 드라이브 403 쿼터 초과 차단 중"
                if self.shared_usage + file_size > self.limit_shared:
                    return 'skip', None, "공유 드라이브 일일 설정 한도 초과"

                if not use_pool:
                    pending_cleanup = db.session.query(ModelDownload.upload_state).filter(
                        ModelDownload.status == 'pending_upload', ModelDownload.upload_state.isnot(None)).all()
                    if any((state or {}).get('cleanup_pending') and not (state or {}).get('uses_pool')
                           for (state,) in pending_cleanup):
                        return 'wait', None, '기본 리모트의 실패 잔여 파일 회수 대기'
                    self.shared_usage += file_size
                    return 'shareddrive', None, "기본 리모트 공유 드라이브 직행"

                is_direct_shared = (file_size > self.threshold)
                excluded = set(exclude_accounts or ())
                # 통신 장애로 실행 스레드가 종료돼도 미확인 원격 작업의 계정은 재할당하지 않는다.
                for checkpoint, status in db.session.query(ModelDownload.upload_state, ModelDownload.status).filter(
                        ModelDownload.status != 'completed', ModelDownload.upload_state.isnot(None)).all():
                    FeederUtil.cooperate()
                    checkpoint = checkpoint or {}
                    worker = checkpoint.get('worker')
                    if checkpoint.get('uses_pool') and (checkpoint.get('cleanup_pending') or
                            (checkpoint.get('phase') == 'copying' and status in ('pending_upload', 'uploading')) or
                            (worker and (not worker.get('settled') or
                             checkpoint.get('phase') in ('uploaded', 'shared_incoming')))):
                        excluded.add(checkpoint.get('account'))

                # 계정 풀에서 24시간 사용량이 가장 적은 계정 선점 (직행 여부와 무관하게 항상 계정 할당)
                sorted_accs = sorted(self.accounts, key=lambda x: self.usage_map.get(x.get('username'), 0))
                for acc in sorted_accs:
                    uname = acc.get('username')
                    if uname in excluded:
                        continue
                    if uname in self.blocked_accounts:
                        continue
                    if uname not in self.busy_accounts:
                        current_u = self.usage_map.get(uname, 0)
                        if current_u + file_size < self.limit_user:
                            self.busy_accounts.add(uname)
                            self.usage_map[uname] = current_u + file_size
                            self.shared_usage += file_size
                            if is_direct_shared:
                                return 'shareddrive', acc, f"대용량 직행 ({uname})"
                            return 'mydrive', acc, f"내 드라이브 바이패스 ({uname})"

                for acc in self.accounts:
                    uname = acc.get('username')
                    if uname in excluded:
                        continue
                    if uname not in self.blocked_accounts and self.usage_map.get(uname, 0) + file_size < self.limit_user:
                        return 'wait', None, "모든 가용 계정 작업 중 (대기)"

                return 'skip', None, "모든 계정 일일 한도 초과 또는 차단 상태"

        def release_account(self, username: str):
            with self.lock:
                if username in self.busy_accounts:
                    self.busy_accounts.remove(username)

        def reserve_existing_account(self, username: str) -> bool:
            """재업로드 없이 이전 계정의 원격 이동을 재개하기 위한 실행권만 선점한다."""
            with self.lock:
                if username in self.busy_accounts:
                    return False
                self.busy_accounts.add(username)
                return True

        def add_usage(self, key: str, size: int):
            stat = ModelDownloadStat(
                stat_type='gdrive_usage',
                stat_key=key,
                stat_value=float(size),
                timestamp=int(time.time())
            )
            db.session.add(stat)
            db.session.commit()

        def block_account(self, username: str, is_rate_limit: bool = False):
            with self.lock:
                block_hours = 0.5 if is_rate_limit else 24.0
                unblock_time = int(time.time()) + int(block_hours * 3600)

                db.session.query(ModelDownloadStat).filter_by(
                    stat_type='account_block',
                    stat_key=username
                ).delete()

                stat = ModelDownloadStat(
                    stat_type='account_block',
                    stat_key=username,
                    stat_value=float(unblock_time),
                    timestamp=int(time.time())
                )
                db.session.add(stat)
                db.session.commit()

                self.blocked_accounts[username] = unblock_time
                if username in self.busy_accounts:
                    self.busy_accounts.remove(username)

                logger.warning(f"[UploadUtil] 계정 차단 등록: {username} ({block_hours}시간)")

        def block_shared_drive(self):
            with self.lock:
                now_dt = datetime.now(pytz.timezone('Asia/Seoul'))
                try:
                    h, m = map(int, self.reset_time_str.split(':'))
                    reset_dt = now_dt.replace(hour=h, minute=m, second=0, microsecond=0)
                    if reset_dt <= now_dt:
                        reset_dt += timedelta(days=1)
                    unblock_time = int(reset_dt.timestamp())
                except Exception:
                    unblock_time = int(time.time()) + 86400

                db.session.query(ModelDownloadStat).filter_by(
                    stat_type='account_block',
                    stat_key='SHARED_DRIVE_UPLOAD'
                ).delete()

                stat = ModelDownloadStat(
                    stat_type='account_block',
                    stat_key='SHARED_DRIVE_UPLOAD',
                    stat_value=float(unblock_time),
                    timestamp=int(time.time())
                )
                db.session.add(stat)
                db.session.commit()

                self.blocked_accounts['SHARED_DRIVE_UPLOAD'] = unblock_time
                logger.warning(f"[UploadUtil] 공유 드라이브 403 쿼터 초과 차단 등록 (리셋 시점: {self.reset_time_str})")

    @classmethod
    def get_account_manager(cls) -> AccountManager:
        return cls.AccountManager()

    @staticmethod
    def reserve_usage(account, size, download_id=None, attempt_id=None):
        """시도별 예약 행을 체크포인트와 같은 트랜잭션에 기록한다."""
        attempt_id = attempt_id or uuid.uuid4().hex
        rows = [ModelDownloadStat(stat_type='gdrive_reservation', stat_key=key,
                stat_value=float(size), timestamp=int(time.time()), download_id=download_id, attempt_id=attempt_id)
                for key in dict.fromkeys(filter(None, ('SHARED_DRIVE_UPLOAD', account)))]
        db.session.add_all(rows)
        db.session.flush()
        return {'stat_ids': [row.id for row in rows], 'attempt_id': attempt_id, 'reserved': int(size), 'settled': False}

    @staticmethod
    def settle_usage(item, manager, amount=None):
        """전송한 파일 전체 크기로 정산하며 과거의 출처 불명 사용량은 수정하지 않는다."""
        state = dict(item.upload_state or {})
        usage = dict(state.get('usage') or {})
        if not usage or usage.get('settled'):
            return
        reserved = int(usage['reserved'])
        charged = reserved if amount is None else max(0, int(amount))
        rows = db.session.query(ModelDownloadStat).filter(
            ModelDownloadStat.id.in_(usage['stat_ids']),
            ModelDownloadStat.stat_type == 'gdrive_reservation').all()
        adjustments = [(row.stat_key, charged - int(row.stat_value)) for row in rows]
        for row in rows:
            row.stat_type = 'gdrive_usage'
            row.stat_value = float(charged)
        usage.update(settled=True, charged=charged)
        state['usage'] = usage
        item.upload_state = state
        db.session.commit()
        if manager is not None:
            with manager.lock:
                for key, delta in adjustments:
                    if key == 'SHARED_DRIVE_UPLOAD':
                        manager.shared_usage = max(0, manager.shared_usage + delta)
                    else:
                        manager.usage_map[key] = max(0, manager.usage_map.get(key, 0) + delta)

    @classmethod
    def recover_copy_failure(cls, item_id):
        """복사가 중단된 작업의 완성 파일을 보존한 뒤에만 계정을 재사용한다."""
        item = db.session.get(ModelDownload, item_id)
        state = dict(item.upload_state or {}) if item else {}
        if not state.get('cleanup_pending'):
            return True
        if state.get('worker') and not state['worker'].get('settled'):
            return False
        folder, parent = state.get('folder_name'), state.get('dest_parent', '').rstrip('/')
        if not folder or '/' in folder or '\\' in folder or folder in ('.', '..') or not parent:
            return False
        final = parent + '/' + folder
        conf_args = ['--config', state['rclone_conf']] + state.get('impersonate_arg', [])
        db.session.remove()
        for source in dict.fromkeys(filter(None, (state.get('incoming'), state.get('shared_incoming')))):
            if (not source.endswith('/' + folder) or source == final or
                    source.startswith(parent + '/') or not re.fullmatch(r'[A-Za-z0-9_-]+:\{[A-Za-z0-9_-]+\}/.+', source)):
                return False
            exists = cls._remote_path_exists(source, conf_args, execution_timeout=60, item_id=item_id)
            if exists is None:
                return False
            if exists:
                success, _ = cls.run_rclone(['rclone', 'move', source, final,
                    '--delete-empty-src-dirs', '--drive-server-side-across-configs',
                    '--drive-use-trash=false'] + conf_args, '실패한 업로드 잔여 파일 최종 경로 복구',
                    item_id=item_id, execution_timeout=60)
                if not success:
                    return False
        item = db.session.get(ModelDownload, item_id)
        if not item or item.status in ('blocked', 'cancelled', 'completed'):
            return False
        state = dict(item.upload_state or {})
        state['cleanup_pending'] = False
        item.upload_state = state
        db.session.commit()
        return True

    @staticmethod
    def webdav_failure_hours(cfg):
        try:
            raw = cfg.get('webdav_failure_timeout_hours', 24)
            hours = int(raw)
            if str(raw).strip() != str(hours) or not 0 <= hours <= 8760:
                raise ValueError
        except (TypeError, ValueError):
            hours = 24
        return hours

    @staticmethod
    def expire_source_failure(item, confirmed=False):
        """새 소스 실패가 확정되고 유예가 지났을 때만 AD 작업을 전환한다."""
        if (item.status not in ('pending_upload', 'pending_local_staging') or
                item.transfer_error_type != 'SOURCE_UNAVAILABLE' or not item.source_unavailable_since):
            return False
        cfg = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
        is_ad = cfg.get('engine_type') == 'alldebrid'
        if is_ad:
            if not confirmed:
                return False
            hours = UploadUtil.webdav_failure_hours(cfg)
            started = item.source_unavailable_since
        else:
            # 다른 엔진의 기존 원본 실패 정책은 유지한다.
            try:
                hours = float(cfg.get('stalled_timeout_hours', 24))
            except (TypeError, ValueError):
                hours = 24
            started = item.engine_added_time or item.source_unavailable_since
        elapsed = max(0, (datetime.now() - started).total_seconds())
        logger.debug('[UploadUtil] 원본 실패 유예: 작업 %s · 최초 %s · 경과 %.2f시간 / 제한 %s시간 · 최신 소스 오류 확인=%s',
                     item.id, started.isoformat(timespec='seconds'), elapsed / 3600, hours or '무제한', confirmed)
        if hours <= 0 or (datetime.now() - started).total_seconds() < hours * 3600:
            return False
        state = item.upload_state or {}
        if state.get('cleanup_pending') or (state.get('worker') and not state['worker'].get('settled')):
            return False
        if state.get('usage') and not state['usage'].get('settled'):
            return False
        from .util_download import DownloadUtil
        engine = DownloadUtil.create_engine(cfg) if cfg else None
        if item.engine_task_id:
            deleted = False
            reason = '엔진 객체 없음'
            try:
                if engine:
                    deleted = engine.delete_task(item.engine_task_id) is True
                    reason = '삭제 성공 응답 미확인'
            except Exception as exc:
                reason = '삭제 예외: ' + type(exc).__name__
            if not deleted:
                logger.warning('[UploadUtil] 작업 %s · 엔진 %s: %s · %s', item.id,
                    item.current_engine_name, reason, 'ID와 유예 시각 보존' if is_ad else '기존 정책에 따라 엔진 전환 진행')
                if is_ad:
                    return False
        previous = item.current_engine_name
        item.current_engine_index = (item.current_engine_index or 0) + 1
        remaining = item.current_engine_index < len(item.priority_chain or [])
        item.status = 'pending' if remaining else 'failed'
        item.engine_task_id = None
        item.current_engine_name = None
        item.engine_added_time = None
        item.remote_missing_since = None
        item.local_path = None
        item.upload_state = None
        item.gdrive_account = None
        item.upload_retry_after = None
        item.source_unavailable_since = None
        detail = item.error_message or ''
        item.error_message = f'[{previous}] 원본 다운로드 불가 {hours:g}시간 지속 · ' + (
            '다음 엔진으로 전환' if remaining else '다음 엔진 없음: 최종 실패') + '\n' + detail
        db.session.commit()
        DownloadActivity.record('엔진 전환', f'작업 #{item.id} · {item.error_message.splitlines()[0]}', 'warning')
        return True

    @classmethod
    def transfer_failed(cls, item_id, message, manager=None, amount=None, retry_status='pending_upload', structured_type=None):
        """오류 분류·시도 정산·공간 회수·엔진 대기를 순서대로 적용한다."""
        item = db.session.get(ModelDownload, item_id)
        if not item or item.status in ('blocked', 'cancelled', 'completed'):
            return False
        cls.settle_usage(item, manager, amount)
        error_type = classify_error(message)
        if error_type == 'SOURCE_UNAVAILABLE':
            text = str(message or '').lower()
            network = re.search(r'timeout|timed out|connection reset|\beof\b|connection refused|network is unreachable', text)
            definite_source = re.search(r'(?:failed to open source object|error opening source)[^\n]*(?:\b404\b|not found|server not allowed)', text)
            definite_source = definite_source or ('alldebrid' in text and ('not found' in text or 'server not allowed' in text))
            if network and not definite_source:
                # 소스 열기라는 접두사만으로 일반 연결 단절을 AD 원본 불가로 확정하지 않는다.
                error_type = 'NETWORK_ERROR'
        valid_types = {'SOURCE_UNAVAILABLE', 'DRIVE_SERVER_ERROR', 'DRIVE_STORAGE_QUOTA',
                       'DRIVE_RATE_LIMIT', 'NETWORK_ERROR', 'UNKNOWN_ERROR'}
        if structured_type is not None and (not isinstance(structured_type, str) or structured_type not in valid_types or structured_type != error_type):
            safe_type = structured_type if isinstance(structured_type, str) and structured_type in valid_types else 'INVALID'
            logger.warning('[UploadUtil] 작업 %s: 구조화 오류 유형과 원문 분류가 다릅니다 (%s/%s). 보수적으로 보류합니다.', item_id, safe_type, error_type)
            error_type = 'UNKNOWN_ERROR'
        item.transfer_error_type = error_type
        item.error_message = f'[{error_type}] ' + redact_log(message, limit=3500)
        item.last_status_time = datetime.now()
        cfg = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
        if error_type == 'SOURCE_UNAVAILABLE':
            first_failure = not item.source_unavailable_since
            item.source_unavailable_since = item.source_unavailable_since or datetime.now()
            if cfg.get('engine_type') == 'alldebrid':
                first = item.source_unavailable_since
                item.error_message += '\nWebDAV 최초 소스 실패: ' + first.isoformat(timespec='seconds')
                hours = cls.webdav_failure_hours(cfg)
                elapsed = max(0, (datetime.now() - first).total_seconds()) / 3600
                deadline = (first + timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S') if hours else '무제한'
                item.error_message += f' · 경과 {elapsed:.2f}시간 · 제한 {hours}시간 · 기한 {deadline} · 다음 실행에서 실제 소스 오류 재확인'
                if first_failure:
                    DownloadActivity.record('AD WebDAV 첫 소스 실패', f'작업 #{item_id} · 최초 {first:%Y-%m-%d %H:%M:%S} · 유예 후에도 소스 실패가 재확인되어야 엔진을 전환합니다.', 'warning')
        elif cfg.get('engine_type') != 'alldebrid':
            item.source_unavailable_since = None
        state = dict(item.upload_state or {})
        state['cleanup_pending'] = bool(state.get('incoming'))
        item.upload_state = state
        item.status = retry_status
        try:
            minutes = max(1, P.ModelSetting.get_int('download_interval'))
        except (TypeError, ValueError):
            minutes = 5
        item.upload_retry_after = datetime.now() + timedelta(minutes=minutes)
        db.session.commit()
        if manager and error_type == 'DRIVE_RATE_LIMIT' and state.get('uses_pool'):
            manager.block_account(state['account'], is_rate_limit=True)
        DownloadActivity.record('업로드 오류', f'작업 #{item_id} · {error_type} · {redact_log(message)}', 'warning')
        if not cls.recover_copy_failure(item_id):
            DownloadActivity.record('업로드 복구', f'작업 #{item_id} · 잔여 파일 회수 대기: 해당 계정 재배정 보류', 'warning')
            return False
        item = db.session.get(ModelDownload, item_id)
        if item.status in ('blocked', 'cancelled', 'completed'):
            return False
        item.upload_state = None
        item.gdrive_account = None
        db.session.commit()
        cls.expire_source_failure(item, confirmed=error_type == 'SOURCE_UNAVAILABLE')
        return False

    # --------------------------------------------------------------------------
    # SA 내 드라이브 고아 파일 드레인 및 업로드 실행
    # --------------------------------------------------------------------------
    @staticmethod
    def _cleanup_results(worker, jobs, max_workers, label):
        """정리 명령만 제한된 슬롯에 배정하고 중단 후에는 다음 명령을 시작하지 않는다."""
        from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
        selected_id = FeederUtil.single_download_id()

        def guarded(job):
            with F.app.app_context(), FeederUtil.single_download_scope(selected_id):
                try:
                    if FeederUtil.is_download_paused():
                        return None
                    return worker(job)
                finally:
                    db.session.remove()

        remaining = iter(jobs)
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            active = {}
            exhausted = False
            while active or not exhausted:
                while not exhausted and len(active) < max_workers:
                    if FeederUtil.is_download_paused():
                        exhausted = True
                        break
                    try:
                        job = next(remaining)
                    except StopIteration:
                        exhausted = True
                        break
                    active[executor.submit(guarded, job)] = job
                if not active:
                    break
                done, _ = wait(active, timeout=3, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    completed += 1
                    job = active.pop(future)
                    DownloadActivity.record('사전 정리', f'{label} 종료 {completed}/{len(jobs)}건')
                    yield job, future

    @classmethod
    def drain_all_sa_mydrives(cls) -> bool:
        """새 업로드 전에 잔여 파일을 최종 경로로 복구하며 실패 시 업로드를 보류한다."""
        if FeederUtil.is_download_paused():
            return False
        recovery = db.session.query(ModelDownload).filter(ModelDownload.upload_state.isnot(None),
            ~ModelDownload.status.in_(['blocked', 'cancelled', 'completed'])).all()
        selected = FeederUtil.single_download_id()
        recovery_ids = [item.id for item in recovery if (item.upload_state or {}).get('cleanup_pending')
                        and (selected is None or item.id == selected)]
        db.session.remove()
        for item_id in recovery_ids:
            if not cls.recover_copy_failure(item_id):
                return False
            item = db.session.get(ModelDownload, item_id)
            item.upload_state = None
            item.gdrive_account = None
            db.session.commit()
            cls.expire_source_failure(item)
        failed_ids = [row_id for (row_id,) in db.session.query(ModelDownload.id).filter_by(status='move_failed').all()]
        db.session.remove()
        all_ok = True
        for item_id in DownloadActivity.track('사전 정리', failed_ids, '이동 실패 복구'):
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return False
            if not cls.recover_failed_remote_files(item_id):
                all_ok = False
        config = FeederUtil.load_yaml()
        rclone = config.get('rclone', {})
        conf = P.ModelSetting.get('download_rclone_conf_path') or rclone.get('conf_path', '')
        default_drive = P.ModelSetting.get('download_shared_drive_id') or rclone.get('shared_drive_id', '')
        if not conf:
            return all_ok
        accounts = list(FeederUtil.get_gdrive_accounts())
        accounts.append({'username': 'Default_Account'})
        protected = set()
        protected_shared_paths = set()
        for item in db.session.query(ModelDownload).filter(
            ModelDownload.status.in_(['uploading', 'pending_upload', 'blocked']) |
            ((ModelDownload.status != 'completed') & ModelDownload.upload_state.isnot(None))
        ).all():
            FeederUtil.cooperate()
            state = item.upload_state or {}
            worker = state.get('worker') or {}
            remote_pending = worker and (not worker.get('settled') or state.get('phase') in ('uploaded', 'shared_incoming'))
            if item.status not in ('uploading', 'pending_upload', 'blocked') and not remote_pending:
                continue
            if item.gdrive_account:
                protected.add(item.gdrive_account)
            if worker and state.get('incoming'):
                protected_shared_paths.add(state['incoming'].rstrip('/'))
            if state.get('shared_incoming'):
                protected_shared_paths.add(state['shared_incoming'].rstrip('/'))
            elif item.gdrive_upload_path:
                drive_id = item.gdrive_remote_id or default_drive
                protected_shared_paths.add(f":{{{drive_id}}}/{item.gdrive_upload_path.strip('/')}")
        db.session.remove()
        base_remote = P.ModelSetting.get('download_gdrive_mydrive_remote_name') or rclone.get('mydrive_remote_name') or 'gdrive_sa'
        default_remote = P.ModelSetting.get('download_rclone_upload_remote_name') or base_remote
        default_options = FeederUtil.get_upload_options(None)
        routes = [('incoming/default', 'uploads/default', default_drive,
                   default_options, base_remote if default_options['use_gdrive_pool'] else default_remote)] if default_drive else []
        for profile in DownloadActivity.track('사전 정리', FeederUtil.get_download_profiles(), '프로필'):
            FeederUtil.cooperate()
            for engine_name in [''] + list((profile.get('destination_by_engine') or {}).keys()):
                FeederUtil.cooperate()
                dest = FeederUtil.get_profile_destination(profile, engine_name)
                if dest.get('type') not in ('gdrive_rotation', 'remote_worker'):
                    continue
                options = FeederUtil.get_upload_options(profile, engine_name)
                if not options['use_gdrive_pool']:
                    route_remote = dest.get('remote_name') or default_remote
                else:
                    route_remote = dest.get('mydrive_remote_name') or (dest.get('remote_name') if dest.get('type') == 'remote_worker' else '') or base_remote
                routes.append(((dest.get('upload_path') or 'incoming/default').strip('/'),
                               (dest.get('complete_path') or 'uploads/default').strip('/'),
                               dest.get('shared_drive_id') or default_drive,
                               options, route_remote))
        drives = {}

        def add_route(remote, drive_id, conf_args, upload, target, preserve_root=False):
            root = f'{remote}:{{{drive_id}}}' if drive_id else f'{remote}:'
            # 명시적인 드라이브 ID가 있으면 인증 계정이 달라도 같은 드라이브다.
            key = (remote, drive_id, conf) if drive_id else (root, tuple(conf_args))
            entry = drives.setdefault(key, {'root': root, 'args': conf_args, 'routes': {}, 'preserve_roots': set()})
            source = f'{root}/{upload}'
            if source != target:
                entry['routes'].setdefault(upload, target)
                if preserve_root:
                    entry['preserve_roots'].add(upload)

        for upload, complete, drive, options, route_remote in DownloadActivity.track('사전 정리', routes, '경로 구성'):
            FeederUtil.cooperate()
            if not drive:
                continue
            for account in accounts:
                FeederUtil.cooperate()
                username = account.get('username')
                if not username or username in protected:
                    continue
                if username == 'Default_Account':
                    if options['use_gdrive_pool']:
                        continue
                    remote = route_remote
                    identity = []
                else:
                    use_identity = options['use_impersonate']
                    remote = route_remote if use_identity else account.get('remote_name') or route_remote
                    identity = ['--drive-impersonate', username] if use_identity else []
                conf_args = ['--config', conf] + identity
                target = f'{remote}:{{{drive}}}/{complete}'
                add_route(remote, account.get('mydrive_rclone_id'), conf_args, upload, target)
                add_route(remote, drive, conf_args, upload, target, preserve_root=True)

        drives = [entry for entry in drives.values() if entry['routes']]
        if not drives:
            return all_ok
        logger.info(f'[UploadUtil] 업로드 전 원격 잔여 파일 확인 시작 (중복 제거 후 드라이브: {len(drives)}개)')
        DownloadActivity.record('사전 정리', f'원격 잔여 파일 확인 · 드라이브 {len(drives)}개')
        cleanup_workers = cls._CLEANUP_WORKERS
        logger.info('[UploadUtil] 드라이브 목록 조회 및 빈 폴더 정리 병렬 수: %s', cleanup_workers)
        moved = cleaned = 0
        cleanups = []

        def list_drive(job):
            index, entry = job
            root = entry['root']
            parents = {upload.rpartition('/')[0] or upload for upload in entry['routes']}
            filters = []
            for parent in sorted(parents):
                filters += ['--include', f'/{parent}/**']
            logger.info(f'[UploadUtil] 잔여 파일 목록 조회 ({index}/{len(drives)}): {root}')
            DownloadActivity.record('사전 정리', f'드라이브 {index}/{len(drives)} 잔여 파일 조회 중')
            return cls.run_rclone(
                ['rclone', 'lsjson', root, '--recursive', '--disable', 'ListR', '--no-mimetype', '--no-modtime'] + filters + entry['args'],
                log_output=False, execution_timeout=60)

        listed = 0
        for (index, entry), future in cls._cleanup_results(list_drive, list(enumerate(drives, 1)), cleanup_workers, '드라이브 조회'):
            listed += 1
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return False
            root, conf_args = entry['root'], entry['args']
            blocked = set()
            for path in protected_shared_paths:
                # 같은 드라이브에 다른 계정으로 접근해도 보호 경로는 유지한다.
                suffix = path[path.index(':'):] if ':' in path else path
                root_suffix = root[root.index(':'):]
                if suffix.startswith(root_suffix + '/'):
                    blocked.add(suffix[len(root_suffix) + 1:])
            parents = {upload.rpartition('/')[0] or upload for upload in entry['routes']}
            try:
                result = future.result()
                if result is None:
                    all_ok = False
                    continue
                success, out = result
            except Exception:
                all_ok = False
                logger.exception('[UploadUtil] 드라이브 목록 조회 워커 오류: %s', root)
                continue
            if not success:
                if 'directory not found' not in out.lower():
                    all_ok = False
                    logger.warning(f'[UploadUtil] 잔여 파일 목록 조회 실패: {root}: {out[:300]}')
                    DownloadActivity.record('사전 정리', f'드라이브 {index}/{len(drives)} 목록 조회 실패 · 새 업로드를 보류합니다.', 'warning')
                continue
            try:
                rows = json.loads(out)
                if not isinstance(rows, list):
                    raise ValueError('원격 목록 형식 오류')
                files = {row.get('Path') or row['Name'] for row in rows if not row.get('IsDir')}
                directories = {row.get('Path') or row['Name'] for row in rows if row.get('IsDir')}
            except (TypeError, ValueError, KeyError) as ex:
                all_ok = False
                logger.warning(f'[UploadUtil] 잔여 파일 목록 해석 실패: {root}: {ex}')
                continue
            for upload, target in DownloadActivity.track('사전 정리', list(entry['routes'].items()), '이동 경로', f'드라이브 {index}/{len(drives)}'):
                FeederUtil.cooperate()
                if FeederUtil.is_download_paused():
                    return False
                matched = {path for path in files if path.startswith(upload + '/')}
                if not matched:
                    continue
                if any(path == upload or path.startswith(upload + '/') or upload.startswith(path + '/') for path in blocked):
                    logger.info(f'[UploadUtil] 대기/진행 중인 업로드 경로 보호: {root}/{upload}')
                    continue
                source = f'{root}/{upload}'
                # 공유 드라이브의 프로필 폴더 자체는 이동하거나 삭제하지 않는다.
                move_options = ['--disable', 'DirMove'] if upload in entry['preserve_roots'] else ['--delete-empty-src-dirs']
                logger.info(f'[UploadUtil] 잔여 파일 최종 경로 이동: {source}')
                DownloadActivity.record('사전 정리', f'드라이브 {index}/{len(drives)} 잔여 파일 {len(matched)}개를 최종 경로로 이동 중')
                success, out = cls.run_rclone(
                    ['rclone', 'move', source, target] + move_options + [
                     '--drive-server-side-across-configs', '--drive-use-trash=false'] + conf_args,
                    '리모트 잔여 파일 최종 경로 복구', execution_timeout=60)
                if not success:
                    all_ok = False
                    logger.warning(f'[UploadUtil] 원격 잔여 파일 이동 미완료: {source}: {out[:300]}')
                else:
                    moved += 1
                    files.difference_update(matched)
            preserve_roots = entry['preserve_roots']
            cleanup_paths = [(upload, True) for upload in sorted(preserve_roots)]
            for parent in sorted(parents):
                # MyDrive는 최상위 임시 경로를 정리하되 공유 드라이브의 프로필 경로는 보호한다.
                if any(parent.startswith(other + '/') for other in parents) or any(
                        path == parent or path.startswith(parent + '/') or parent.startswith(path + '/')
                        for path in preserve_roots):
                    continue
                cleanup_paths.append((parent, False))
            for parent, leave_root in cleanup_paths:
                # 중첩된 프로필 경로가 있으면 상위 경로의 일괄 정리를 피한다.
                if any(path.startswith(parent + '/') for path in preserve_roots):
                    continue
                if any(path == parent or path.startswith(parent + '/') or parent.startswith(path + '/') for path in blocked):
                    continue
                if any(path == parent or path.startswith(parent + '/') for path in files):
                    continue
                if any(path.startswith(parent + '/') or (not leave_root and path == parent) for path in directories):
                    cleanups.append((f'{root}/{parent}', conf_args, leave_root))

        if listed != len(drives):
            return False
        # 이동이 모두 끝난 뒤 rmdirs로 빈 폴더만 제거한다.
        def clean_empty(job):
            parent, conf_args, leave_root = job
            logger.info(f'[UploadUtil] Drive 임시 업로드 경로의 빈 폴더 정리: {parent}')
            return cls.run_rclone(
                ['rclone', 'rmdirs', parent, '--drive-use-trash=false'] + (['--leave-root'] if leave_root else []) + conf_args,
                'Drive 임시 업로드 경로의 빈 폴더 정리', execution_timeout=60)

        checked = 0
        for (parent, conf_args, leave_root), future in cls._cleanup_results(clean_empty, cleanups, cleanup_workers, '빈 폴더 정리'):
            checked += 1
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return False
            try:
                result = future.result()
                if result is None:
                    all_ok = False
                    continue
                success, out = result
            except Exception:
                all_ok = False
                logger.exception('[UploadUtil] 빈 폴더 정리 워커 오류: %s', parent)
                continue
            if not success and 'directory not found' not in out.lower():
                all_ok = False
                logger.warning(f'[UploadUtil] Drive 임시 업로드 경로의 빈 폴더 정리 실패: {parent}: {out[:300]}')
            else:
                cleaned += 1
        if checked != len(cleanups):
            return False
        logger.info(f'[UploadUtil] 업로드 전 원격 복구 종료 (드라이브: {len(drives)}개, 이동: {moved}개, 빈 경로 정리: {cleaned}개, 완료: {all_ok})')
        DownloadActivity.record('사전 정리', f'원격 복구 {"완료" if all_ok else "미완료"} · 이동 경로 {moved}개, 빈 경로 정리 {cleaned}개', 'info' if all_ok else 'warning')
        return all_ok

    @classmethod
    def _remote_path_exists(cls, path: str, conf_args: list, execution_timeout: float = None, item_id=None) -> bool | None:
        success, out = cls.run_rclone(['rclone', 'lsjson', path, '--stat'] + conf_args, log_output=False, execution_timeout=execution_timeout, item_id=item_id)
        if success:
            try:
                return bool(json.loads(out))
            except (TypeError, ValueError):
                return None
        if any(word in out.lower() for word in ['not found', 'does not exist', '404']):
            return False
        return None

    @classmethod
    def recover_failed_remote_files(cls, item_id) -> bool:
        """move_failed의 잔여 원격 파일을 삭제하지 않고 해당 작업의 최종 경로로 이동한다."""
        item = db.session.query(ModelDownload).filter_by(id=item_id, status='move_failed').first()
        if not item:
            return True
        state = dict(item.upload_state or {})
        if state.get('remote_recovered'):
            db.session.remove()
            return True
        profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest = FeederUtil.get_profile_destination(profile, item.current_engine_name)
        options = FeederUtil.get_upload_options(profile, item.current_engine_name)
        cfg = FeederUtil.load_yaml().get('rclone', {})
        account = next((a for a in FeederUtil.get_gdrive_accounts() if a.get('username') == item.gdrive_account), {})
        base_remote = dest.get('mydrive_remote_name') or P.ModelSetting.get('download_gdrive_mydrive_remote_name') or cfg.get('mydrive_remote_name') or 'gdrive_sa'
        if item.gdrive_account == 'Default_Account' or not state.get('uses_pool', options['use_gdrive_pool']):
            selected_remote = P.ModelSetting.get('download_rclone_upload_remote_name') or base_remote
        elif options['use_impersonate']:
            selected_remote = base_remote
        else:
            selected_remote = account.get('remote_name') or base_remote
        remote = state.get('remote') or selected_remote
        drive = item.gdrive_remote_id or dest.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or cfg.get('shared_drive_id')
        folder = state.get('folder_name') or item.output_folder_name or item.file_name or item.title
        up = (item.gdrive_upload_path or dest.get('upload_path') or 'incoming/default').strip('/')
        complete = (item.gdrive_complete_path or dest.get('complete_path') or 'uploads/default').strip('/')
        parent = state.get('dest_parent') or (f'{remote}:{{{drive}}}/{complete}' if drive else '')
        if not parent or not folder:
            db.session.remove()
            return False
        conf = state.get('rclone_conf') or P.ModelSetting.get('download_rclone_conf_path') or cfg.get('conf_path', '')
        impersonate = state.get('impersonate_arg')
        if impersonate is None:
            impersonate = ['--drive-impersonate', item.gdrive_account] if options['use_impersonate'] and item.gdrive_account and item.gdrive_account != 'Default_Account' else []
        conf_args = ['--config', conf] + impersonate
        candidates = [state.get('incoming'), state.get('shared_incoming')]
        if account.get('mydrive_rclone_id'):
            candidates.append(f"{remote}:{{{account['mydrive_rclone_id']}}}/{up}/{folder}")
        if drive:
            candidates.append(f'{remote}:{{{drive}}}/{up}/{folder}')
        final_path = f'{parent}/{folder}'
        db.session.remove()
        all_ok = True
        moved_any = False
        for source in dict.fromkeys(path for path in candidates if path and path != final_path):
            FeederUtil.cooperate()
            exists = cls._remote_path_exists(source, conf_args, execution_timeout=60, item_id=item_id)
            if exists is None:
                all_ok = False
                continue
            if not exists:
                continue
            cmd = ['rclone', 'move', source, final_path, '--delete-empty-src-dirs',
                   '--drive-server-side-across-configs', '--drive-use-trash=false'] + conf_args
            success, out = cls.run_rclone(cmd, f'이동 실패 잔여 파일 최종 경로 복구: {folder}', execution_timeout=60, item_id=item_id)
            if not success:
                logger.warning(f'[UploadUtil] 원격 잔여 파일 복구 실패: {out[:200]}')
                all_ok = False
            else:
                moved_any = True
        if moved_any:
            item = db.session.query(ModelDownload).filter_by(id=item_id, status='move_failed').first()
            if item:
                state['remote_recovered'] = all_ok
                item.upload_state = state
                db.session.commit()
            db.session.remove()
        return all_ok

    @classmethod
    def retry_failed_upload(cls, item, manager: AccountManager) -> bool:
        """소스가 남으면 재업로드, 없으면 move_failed 이력을 보존하고 성공 종료한다."""
        item_id = item.id
        state = dict(item.upload_state or {})
        source = item.local_path or state.get('local_path') or ''
        is_remote = bool(source and ':' in source and not source.startswith('/'))
        if is_remote:
            conf = state.get('rclone_conf') or P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
            db.session.remove()
            exists = cls._remote_path_exists(source, ['--config', conf])
            if exists is None:
                return False
        else:
            exists = bool(source and os.path.exists(source))
        item = db.session.query(ModelDownload).filter_by(id=item_id, status='move_failed').first()
        if not item:
            db.session.remove()
            return False
        if not exists:
            item.completed_time = item.completed_time or datetime.now()
            item.error_message = '소스 파일 없음: move_failed 이력을 보존하고 성공 종료. 원격 잔여 파일은 최종 경로로 복구합니다.'
            db.session.commit()
            db.session.remove()
            return True
        # 재업로드 전에 이전 원격 잔여 파일부터 최종 경로로 보존한다.
        if not cls.recover_failed_remote_files(item_id):
            return False
        item = db.session.query(ModelDownload).filter_by(id=item_id, status='move_failed').first()
        if not item:
            return False
        item.local_path = source
        item.upload_state = None
        item.status = 'pending_upload'
        item.completed_time = None
        item.error_message = None
        db.session.commit()
        return cls.execute_upload(item, manager)

    @classmethod
    def execute_upload(cls, item: ModelDownload, manager: AccountManager) -> bool:
        """소스를 업로드하고, 영속화한 체크포인트부터 원격 이동과 정리를 수행한다."""
        if item.destination_type == 'remote_worker' or (item.upload_state or {}).get('worker'):
            from .util_remote_worker import RemoteWorker
            return RemoteWorker.execute(item, manager)
        if item.status == 'completed':
            return True
        if item.status in ['cancelled', 'blocked']:
            return False
        if item.status == 'move_failed':
            return cls.retry_failed_upload(item, manager)
        if (item.upload_state or {}).get('phase') in ['uploaded', 'shared_incoming', 'moved']:
            return cls.resume_upload(item, manager)
        if (item.upload_state or {}).get('cleanup_pending'):
            return False
        if (item.upload_state or {}).get('phase') == 'copying':
            return cls.transfer_failed(item.id, '이전 업로드 실행이 중단되어 잔여 파일을 먼저 복구합니다.', manager)

        local_path = item.local_path
        is_remote_source = bool(local_path and not os.path.exists(local_path) and (':' in local_path or local_path.startswith(('http://', 'https://'))))
        if not local_path or (not is_remote_source and not os.path.exists(local_path)):
            item.status = 'failed'
            item.error_message = '소스 경로 없음'
            db.session.commit()
            return False

        from .util_download import DownloadUtil
        folder_name = DownloadUtil.reserve_output_folder(item)
        item.file_name = folder_name
        rclone_cfg = FeederUtil.load_yaml().get('rclone', {})
        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or rclone_cfg.get('conf_path', '')
        if is_remote_source:
            fsize = item.file_size or cls.get_remote_size(local_path, rclone_conf)
        elif os.path.isfile(local_path):
            fsize = os.path.getsize(local_path)
        else:
            fsize = item.file_size or FeederUtil.directory_size(local_path)
        item.file_size = max(0, fsize)
        fsize = item.file_size

        profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name)
        options = FeederUtil.get_upload_options(profile, item.current_engine_name)
        use_pool = options['use_gdrive_pool']
        remote_sa = dest_cfg.get('mydrive_remote_name') or P.ModelSetting.get('download_gdrive_mydrive_remote_name') or rclone_cfg.get('mydrive_remote_name') or 'gdrive_sa'
        target_drive_id = dest_cfg.get('shared_drive_id') or item.gdrive_remote_id or P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')
        if not target_drive_id:
            item.status = 'failed'
            item.error_message = '공유 드라이브 ID 누락'
            db.session.commit()
            return False

        comp_path = (dest_cfg.get('complete_path') or item.gdrive_complete_path or 'uploads/default').strip('/')
        up_path = (dest_cfg.get('upload_path') or item.gdrive_upload_path or 'incoming/default').strip('/')
        mode, acc, reason = manager.allocate_account(fsize, use_pool=use_pool)
        if mode in ['wait', 'skip']:
            logger.info(f"[UploadUtil] 업로드 보류 ({reason}): {folder_name}")
            return False
        acc_name = acc.get('username') if acc else 'Default_Account'

        try:
            use_impersonate = options['use_impersonate']
            if not use_pool:
                effective_remote = P.ModelSetting.get('download_rclone_upload_remote_name') or remote_sa
                impersonate_arg = []
            elif use_impersonate:
                effective_remote = remote_sa
                impersonate_arg = ['--drive-impersonate', acc_name]
            else:
                effective_remote = acc.get('remote_name') or remote_sa
                impersonate_arg = []

            shared_incoming = f"{effective_remote}:{{{target_drive_id}}}/{up_path}/{folder_name}"
            dest_incoming = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{up_path}/{folder_name}" if mode == 'mydrive' else shared_incoming
            dst_parent = f"{effective_remote}:{{{target_drive_id}}}/{comp_path}"
            state = {
                'phase': 'copying', 'mode': mode, 'uses_pool': use_pool,
                'account': acc_name, 'remote': effective_remote,
                'rclone_conf': rclone_conf, 'impersonate_arg': impersonate_arg,
                'incoming': dest_incoming, 'shared_incoming': shared_incoming,
                'dest_parent': dst_parent, 'folder_name': folder_name,
                'local_path': local_path, 'is_remote_source': is_remote_source,
                'engine_name': item.current_engine_name, 'engine_task_id': item.engine_task_id,
            }
            item_id = item.id
            item.status = 'uploading'
            item.gdrive_account = acc_name
            item.upload_state = dict(state)
            state['usage'] = cls.reserve_usage(acc_name if use_pool else None, fsize, item_id)
            item.upload_state = dict(state)
            item.upload_retry_after = None
            db.session.commit()
            db.session.remove()

            clean_chk = ['rclone', 'lsjson', dest_incoming, '--stat', '--config', rclone_conf] + impersonate_arg
            csuc, cout = cls.run_rclone(clean_chk, log_output=False, item_id=item_id)
            if not csuc and not any(word in cout.lower() for word in ('directory not found', 'object not found', "doesn't exist")):
                return cls.transfer_failed(item_id, f'incoming 확인 실패: {cout}', manager, 0)
            if csuc and cout.strip() and cout.strip() not in ['{}', '[]']:
                clean_cmd = ['rclone', 'move', dest_incoming, f'{dst_parent}/{folder_name}', '--delete-empty-src-dirs', '--drive-server-side-across-configs', '--drive-use-trash=false', '--config', rclone_conf] + impersonate_arg
                cleaned, clean_error = cls.run_rclone(clean_cmd, log_output=False, item_id=item_id)
                if not cleaned:
                    return cls.transfer_failed(item_id, f'incoming 초기화 실패: {clean_error}', manager, 0)

            chunk_size = P.ModelSetting.get('download_rclone_chunk_size') or '256M'
            cmd = ['rclone', 'copy', local_path, dest_incoming, '--config', rclone_conf,
                   '--stats', '1s', '--use-json-log', '--log-level', 'INFO', '--retries', '1',
                   '--drive-chunk-size', chunk_size] + impersonate_arg
            cmd.extend(FeederUtil.get_rclone_exclude_options())
            cmd.extend(FeederUtil.get_rclone_transfer_options())
            from .util_block import DownloadBlock
            quota = FileQuota.inspect_source(['rclone', 'lsjson', local_path, '--recursive', '--files-only',
                '--no-modtime', '--no-mimetype', '--config', rclone_conf,
                '--retries', '1', '--low-level-retries', '1'] + FeederUtil.get_rclone_exclude_options(),
                stopped=lambda: DownloadBlock.stopped(item_id))
            success, out = cls.run_rclone(cmd, f'업로드 {folder_name}', item_id=item_id, file_size=fsize,
                                          watchdog_timeout=300, quota=quota)
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if item:
                cls.settle_usage(item, manager, quota.charged_bytes())
            if not item or item.status != 'uploading':
                return False

            if not success:
                return cls.transfer_failed(item_id, out, manager, quota.charged_bytes())

            state = dict(item.upload_state or {})
            state['phase'] = 'uploaded'
            item.source_unavailable_since = None
            item.transfer_error_type = None
            item.upload_state = state
            db.session.commit()
            db.session.remove()
            return cls.resume_upload(item_id, manager, account_reserved=bool(acc))
        finally:
            if acc:
                manager.release_account(acc_name)
            db.session.remove()

    @classmethod
    def _mark_move_failure(cls, item_id, message: str, status: str = 'move_failed'):
        item = db.session.query(ModelDownload).filter_by(id=item_id).first()
        if item and item.status not in ['cancelled', 'completed', 'blocked']:
            item.status = status
            item.error_message = redact_log(message, limit=3600)
            item.last_move_attempt_time = datetime.now()
            item.last_status_time = datetime.now()
            db.session.commit()
        db.session.remove()

    @classmethod
    def resume_upload(cls, item, manager: AccountManager, account_reserved: bool = False) -> bool:
        """완료된 업로드는 다시 복사/과금하지 않고 원격 이동 또는 원본 정리만 재개한다."""
        item_id = item if isinstance(item, int) else item.id
        item = db.session.query(ModelDownload).filter_by(id=item_id).first()
        if not item or item.status in ['cancelled', 'failed', 'blocked']:
            db.session.remove()
            return False
        if item.status == 'completed':
            db.session.remove()
            return True
        state = dict(item.upload_state or {})
        if state.get('phase') not in ['uploaded', 'shared_incoming', 'moved']:
            cls._mark_move_failure(item_id, '이전 업로드의 단계/경로 기록이 없어 자동 이동을 재개할 수 없습니다. 원격 파일 확인이 필요합니다.')
            return False
        account = state['account']
        reserved_here = bool(state.get('uses_pool') and not account_reserved)
        if reserved_here and not manager.reserve_existing_account(account):
            db.session.remove()
            return False
        db.session.remove()

        def save_phase(phase):
            target = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not target or target.status in ['cancelled', 'failed', 'completed', 'blocked']:
                db.session.remove()
                return False
            state['phase'] = phase
            target.upload_state = dict(state)
            db.session.commit()
            db.session.remove()
            return True

        try:
            conf_args = ['--config', state['rclone_conf']] + state['impersonate_arg']
            if state['phase'] != 'moved':
                time.sleep(3)
                moved = False
                for attempt in range(1, 4):
                    if state['phase'] == 'uploaded' and state['mode'] == 'mydrive':
                        move_cmd = ['rclone', 'move', state['incoming'], state['shared_incoming'],
                                    '--delete-empty-src-dirs', '--drive-server-side-across-configs',
                                    '--drive-use-trash=false', '--stats-one-line', '--log-level', 'NOTICE'] + conf_args
                        success, out = cls.run_rclone(move_cmd, f"MyDrive 이동 {state['folder_name']}", item_id=item_id)
                        if not success:
                            if attempt < 3:
                                time.sleep(attempt * 3)
                            continue
                        if not save_phase('shared_incoming'):
                            return False
                    mkdir_cmd = ['rclone', 'mkdir', state['dest_parent']] + conf_args
                    cls.run_rclone(mkdir_cmd, log_output=False, item_id=item_id)
                    chpar_cmd = ['rclone', 'backend', 'chpar', state['shared_incoming'], state['dest_parent'], '--log-level', 'NOTICE'] + conf_args
                    moved, out = cls.run_rclone(chpar_cmd, f"원자적 부모폴더 변경 {state['folder_name']}", item_id=item_id)
                    if moved:
                        if not save_phase('moved'):
                            return False
                        break
                    if attempt < 3:
                        time.sleep(attempt * 3)
                if not moved:
                    cls._mark_move_failure(item_id, f'서버사이드 이동 실패: {out.strip()[:150]}')
                    return False

            # moved 체크포인트는 원본 정리 실패 시에도 보존하므로 재시도 때 이동을 반복하지 않는다.
            local_path = state['local_path']
            if not state['is_remote_source'] and os.path.exists(local_path):
                try:
                    if os.path.isdir(local_path):
                        shutil.rmtree(local_path)
                    else:
                        os.remove(local_path)
                except Exception as ex:
                    cls._mark_move_failure(item_id, f'업로드 완료 후 원본 정리 실패: {ex}')
                    return False

            downloader_cfg = FeederUtil.get_downloader_by_name(state['engine_name'])
            if downloader_cfg and state['engine_task_id']:
                try:
                    from .util_download import DownloadUtil
                    engine = DownloadUtil.create_engine(downloader_cfg)
                    if engine:
                        engine.delete_task(state['engine_task_id'])
                except Exception as ex:
                    logger.debug(f'[UploadUtil] 다운로더 히스토리 삭제 실패 (무시): {ex}')
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item or item.status in ['cancelled', 'failed', 'completed', 'blocked']:
                return bool(item and item.status == 'completed')
            item.status = 'completed'
            item.completed_time = datetime.now()
            item.error_message = None
            item.transfer_error_type = None
            item.source_unavailable_since = None
            item.upload_retry_after = None
            item.upload_state = None
            db.session.commit()
            logger.info(f"[UploadUtil] 구글 드라이브 최종 완료: {state['folder_name']}")
            return True
        except Exception as ex:
            db.session.rollback()
            cls._mark_move_failure(item_id, f'업로드 후처리 예외: {ex}')
            logger.error(f'[UploadUtil] 업로드 후처리 예외: {ex}')
            return False
        finally:
            if reserved_here:
                manager.release_account(account)
            db.session.remove()
