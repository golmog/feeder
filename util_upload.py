# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import json
import shutil
import subprocess
import threading
from datetime import datetime, timedelta
import pytz
from sqlalchemy import func

from .setup import *
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil


class UploadUtil:
    """Rclone 커맨드 실행, Google Drive SA 풀 쿼터 관리 및 업로드 핸들러 통합 관리자"""

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
    def run_rclone(cmd: list[str], description: str = "", log_output: bool = True, item_id=None, file_size=None, watchdog_timeout: int = None, execution_timeout: float = None) -> tuple[bool, str]:
        """Rclone 서브프로세스 실행 및 실시간 출력/전송률 캡처"""
        env = os.environ.copy()

        if log_output and description:
            size_text = f" ({UploadUtil.format_bytes(file_size)})" if file_size else ""
            logger.info(f"[Rclone] 시작: {description}{size_text}")

        deadline_timer = None
        execution_expired = threading.Event()
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
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
                        logger.error(f"[Rclone Error] {description}: {line}")
                    elif re.match(r'^\s*(?:warning|notice)\s*:', line_lower):
                        logger.info(f"[Rclone Log] {description}: {line}")

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
                return False, "원격 유지보수 실행 시간 만료 (다음 주기에 재확인)"

            if killed_by_watchdog[0]:
                err_msg = f"{watchdog_timeout}초간 데이터 전송 시작 불가 (무반응 타임아웃 강제 종료)"
                if log_output:
                    logger.error(f"[Rclone] 실패 ({description}): {err_msg}")
                return False, err_msg

            if proc.returncode == 0:
                if log_output and description:
                    logger.info(f"[Rclone] 성공: {description}")
                return True, full_output

            filtered = [l for l in output_lines if not ("Transferred:" in l or "ETA" in l or "%" in l)]
            err_msg = '\n'.join(filtered) if filtered else "알 수 없는 Rclone 오류"
            if log_output:
                logger.error(f"[Rclone] 실패 ({description}):\n{err_msg}")
            return False, full_output
        except Exception as ex:
            logger.error(f"[Rclone] 실행 예외 ({description}): {ex}")
            return False, str(ex)
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
                    ModelDownloadStat.stat_type == 'gdrive_usage',
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

        def allocate_account(self, file_size: int, use_pool: bool = True) -> tuple[str, dict | None, str]:
            with self.lock:
                # 공유 드라이브 403 쿼터 차단 및 일일 한도 체크
                if 'SHARED_DRIVE_UPLOAD' in self.blocked_accounts:
                    return 'skip', None, "공유 드라이브 403 쿼터 초과 차단 중"
                if self.shared_usage + file_size > self.limit_shared:
                    return 'skip', None, "공유 드라이브 일일 설정 한도 초과"

                if not use_pool:
                    self.shared_usage += file_size
                    return 'shareddrive', None, "기본 리모트 공유 드라이브 직행"

                is_direct_shared = (file_size > self.threshold)

                # 계정 풀에서 24시간 사용량이 가장 적은 계정 선점 (직행 여부와 무관하게 항상 계정 할당)
                sorted_accs = sorted(self.accounts, key=lambda x: self.usage_map.get(x.get('username'), 0))
                for acc in sorted_accs:
                    uname = acc.get('username')
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

    # --------------------------------------------------------------------------
    # SA 내 드라이브 고아 파일 드레인 및 업로드 실행
    # --------------------------------------------------------------------------
    @classmethod
    def drain_all_sa_mydrives(cls) -> bool:
        """새 업로드 전에 잔여 파일을 최종 경로로 복구하며 실패 시 업로드를 보류한다."""
        failed_ids = [row_id for (row_id,) in db.session.query(ModelDownload.id).filter_by(status='move_failed').all()]
        db.session.remove()
        all_ok = True
        for item_id in failed_ids:
            FeederUtil.cooperate()
            if not cls.recover_failed_remote_files(item_id):
                all_ok = False
        config = FeederUtil.load_yaml()
        rclone = config.get('rclone', {})
        conf = P.ModelSetting.get('download_rclone_conf_path') or rclone.get('conf_path', '')
        default_drive = P.ModelSetting.get('download_shared_drive_id') or rclone.get('shared_drive_id', '')
        if not conf or not default_drive:
            return all_ok
        accounts = list(FeederUtil.get_gdrive_accounts())
        if not P.ModelSetting.get_bool('download_use_gdrive_pool'):
            default_remote = P.ModelSetting.get('download_rclone_upload_remote_name') or P.ModelSetting.get('download_gdrive_mydrive_remote_name') or rclone.get('mydrive_remote_name') or 'gdrive_sa'
            accounts.append({'username': 'Default_Account', 'remote_name': default_remote})
        protected = {account for (account,) in db.session.query(ModelDownload.gdrive_account).filter(
            ModelDownload.status.in_(['uploading', 'pending_upload']),
            ModelDownload.gdrive_account.isnot(None)
        ).all()}
        protected_shared_paths = set()
        for item in db.session.query(ModelDownload).filter(
            ModelDownload.status.in_(['uploading', 'pending_upload']),
            ModelDownload.gdrive_account.isnot(None)
        ).all():
            FeederUtil.cooperate()
            state = item.upload_state or {}
            if state.get('shared_incoming'):
                protected_shared_paths.add(state['shared_incoming'].rstrip('/'))
            elif item.gdrive_upload_path:
                drive_id = item.gdrive_remote_id or default_drive
                protected_shared_paths.add(f":{{{drive_id}}}/{item.gdrive_upload_path.strip('/')}")
        db.session.remove()
        routes = [('incoming/default', 'uploads/default', default_drive)]
        for profile in FeederUtil.get_download_profiles():
            FeederUtil.cooperate()
            for engine_name in [''] + list((profile.get('destination_by_engine') or {}).keys()):
                FeederUtil.cooperate()
                dest = FeederUtil.get_profile_destination(profile, engine_name)
                if dest.get('type') != 'gdrive_rotation':
                    continue
                routes.append(((dest.get('upload_path') or 'incoming/default').strip('/'),
                               (dest.get('complete_path') or 'uploads/default').strip('/'),
                               dest.get('shared_drive_id') or default_drive))
        base_remote = P.ModelSetting.get('download_gdrive_mydrive_remote_name') or rclone.get('mydrive_remote_name') or 'gdrive_sa'
        impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')
        drives = {}

        def add_route(remote, drive_id, conf_args, upload, target):
            root = f'{remote}:{{{drive_id}}}' if drive_id else f'{remote}:'
            # 명시적인 드라이브 ID가 있으면 인증 계정이 달라도 같은 드라이브다.
            key = (remote, drive_id, conf) if drive_id else (root, tuple(conf_args))
            entry = drives.setdefault(key, {'root': root, 'args': conf_args, 'routes': {}})
            source = f'{root}/{upload}'
            if source != target:
                entry['routes'].setdefault(upload, target)

        for account in accounts:
            FeederUtil.cooperate()
            username = account.get('username')
            if not username or username in protected:
                continue
            use_identity = impersonate and username != 'Default_Account'
            remote = base_remote if use_identity else account.get('remote_name') or base_remote
            identity = ['--drive-impersonate', username] if use_identity else []
            conf_args = ['--config', conf] + identity
            for upload, complete, drive in routes:
                FeederUtil.cooperate()
                target = f'{remote}:{{{drive}}}/{complete}'
                add_route(remote, account.get('mydrive_rclone_id'), conf_args, upload, target)
                add_route(remote, drive, conf_args, upload, target)

        drives = [entry for entry in drives.values() if entry['routes']]
        if not drives:
            return all_ok
        logger.info(f'[UploadUtil] 업로드 전 원격 잔여 파일 확인 시작 (중복 제거 후 드라이브: {len(drives)}개)')
        moved = cleaned = 0
        cleanups = []
        for index, entry in enumerate(drives, 1):
            FeederUtil.cooperate()
            root, conf_args = entry['root'], entry['args']
            blocked = set()
            for path in protected_shared_paths:
                # 같은 드라이브에 다른 계정으로 접근해도 보호 경로는 유지한다.
                suffix = path[path.index(':'):] if ':' in path else path
                root_suffix = root[root.index(':'):]
                if suffix.startswith(root_suffix + '/'):
                    blocked.add(suffix[len(root_suffix) + 1:])
            parents = {upload.rpartition('/')[0] or upload for upload in entry['routes']}
            # 같은 드라이브의 여러 스테이징 경로를 한 명령에서 조회한다.
            # 필터로 완료 경로 등 스테이징 외 영역의 재귀 탐색을 제외한다.
            filters = []
            for parent in sorted(parents):
                filters += ['--include', f'/{parent}/**']
            logger.info(f'[UploadUtil] 잔여 파일 목록 조회 ({index}/{len(drives)}): {root}')
            success, out = cls.run_rclone(
                ['rclone', 'lsjson', root, '--recursive', '--disable', 'ListR', '--no-mimetype', '--no-modtime'] + filters + conf_args,
                log_output=False, execution_timeout=60)
            if not success:
                if 'directory not found' not in out.lower():
                    all_ok = False
                    logger.warning(f'[UploadUtil] 잔여 파일 목록 조회 실패: {root}: {out[:300]}')
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
            for upload, target in entry['routes'].items():
                FeederUtil.cooperate()
                matched = {path for path in files if path.startswith(upload + '/')}
                if not matched:
                    continue
                if any(path == upload or path.startswith(upload + '/') or upload.startswith(path + '/') for path in blocked):
                    logger.info(f'[UploadUtil] 대기/진행 중인 업로드 경로 보호: {root}/{upload}')
                    continue
                source = f'{root}/{upload}'
                logger.info(f'[UploadUtil] 잔여 파일 최종 경로 이동: {source}')
                success, out = cls.run_rclone(
                    ['rclone', 'move', source, target, '--delete-empty-src-dirs',
                     '--drive-server-side-across-configs', '--drive-use-trash=false'] + conf_args,
                    '리모트 잔여 파일 최종 경로 복구', execution_timeout=60)
                if not success:
                    all_ok = False
                    logger.warning(f'[UploadUtil] 원격 잔여 파일 이동 미완료: {source}: {out[:300]}')
                else:
                    moved += 1
                    files.difference_update(matched)
            for parent in sorted(parents):
                # 최상위 스테이징 경로만 한 번 정리한다.
                if any(parent.startswith(other + '/') for other in parents):
                    continue
                if any(path == parent or path.startswith(parent + '/') or parent.startswith(path + '/') for path in blocked):
                    continue
                if any(path == parent or path.startswith(parent + '/') for path in files):
                    continue
                if any(path == parent or path.startswith(parent + '/') for path in directories):
                    cleanups.append((f'{root}/{parent}', conf_args))

        # 이동이 모두 끝난 뒤 rmdirs로 빈 폴더만 제거한다.
        for parent, conf_args in cleanups:
            FeederUtil.cooperate()
            logger.info(f'[UploadUtil] 원격 빈 폴더 정리: {parent}')
            success, out = cls.run_rclone(
                ['rclone', 'rmdirs', parent, '--drive-use-trash=false'] + conf_args,
                '원격 스테이징 빈 폴더 정리', execution_timeout=60)
            if not success and 'directory not found' not in out.lower():
                all_ok = False
                logger.warning(f'[UploadUtil] 원격 빈 폴더 정리 실패: {parent}: {out[:300]}')
            else:
                cleaned += 1
        logger.info(f'[UploadUtil] 업로드 전 원격 복구 종료 (드라이브: {len(drives)}개, 이동: {moved}개, 빈 경로 정리: {cleaned}개, 완료: {all_ok})')
        return all_ok

    @classmethod
    def _remote_path_exists(cls, path: str, conf_args: list, execution_timeout: float = None) -> bool | None:
        success, out = cls.run_rclone(['rclone', 'lsjson', path, '--stat'] + conf_args, log_output=False, execution_timeout=execution_timeout)
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
        cfg = FeederUtil.load_yaml().get('rclone', {})
        account = next((a for a in FeederUtil.get_gdrive_accounts() if a.get('username') == item.gdrive_account), {})
        base_remote = dest.get('mydrive_remote_name') or P.ModelSetting.get('download_gdrive_mydrive_remote_name') or cfg.get('mydrive_remote_name') or 'gdrive_sa'
        if item.gdrive_account == 'Default_Account' or not P.ModelSetting.get_bool('download_use_gdrive_pool'):
            selected_remote = P.ModelSetting.get('download_rclone_upload_remote_name') or base_remote
        elif P.ModelSetting.get_bool('download_gdrive_use_impersonate'):
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
            impersonate = ['--drive-impersonate', item.gdrive_account] if P.ModelSetting.get_bool('download_gdrive_use_impersonate') and item.gdrive_account and item.gdrive_account != 'Default_Account' else []
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
            exists = cls._remote_path_exists(source, conf_args, execution_timeout=60)
            if exists is None:
                all_ok = False
                continue
            if not exists:
                continue
            cmd = ['rclone', 'move', source, final_path, '--delete-empty-src-dirs',
                   '--drive-server-side-across-configs', '--drive-use-trash=false'] + conf_args
            success, out = cls.run_rclone(cmd, f'이동 실패 잔여 파일 최종 경로 복구: {folder}', execution_timeout=60)
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
        if item.status == 'completed':
            return True
        if item.status == 'cancelled':
            return False
        if item.status == 'move_failed':
            return cls.retry_failed_upload(item, manager)
        if (item.upload_state or {}).get('phase') in ['uploaded', 'shared_incoming', 'moved']:
            return cls.resume_upload(item, manager)

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
        use_pool = P.ModelSetting.get_bool('download_use_gdrive_pool')
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
            use_impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')
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
                'phase': 'uploaded', 'mode': mode, 'uses_pool': use_pool,
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
            db.session.commit()
            manager.add_usage(acc_name, fsize)
            db.session.remove()

            clean_chk = ['rclone', 'lsjson', dest_incoming, '--stat', '--config', rclone_conf] + impersonate_arg
            csuc, cout = cls.run_rclone(clean_chk, log_output=False)
            if csuc and cout.strip() and cout.strip() not in ['{}', '[]']:
                clean_cmd = ['rclone', 'move', dest_incoming, f'{dst_parent}/{folder_name}', '--delete-empty-src-dirs', '--drive-server-side-across-configs', '--drive-use-trash=false', '--config', rclone_conf] + impersonate_arg
                cleaned, clean_error = cls.run_rclone(clean_cmd, log_output=False)
                if not cleaned:
                    cls._mark_move_failure(item_id, f'incoming 초기화 실패: {clean_error}', status='pending_upload')
                    return False

            chunk_size = P.ModelSetting.get('download_rclone_chunk_size') or '256M'
            cmd = ['rclone', 'copy', local_path, dest_incoming, '--config', rclone_conf,
                   '--stats', '1s', '--stats-one-line', '--log-level', 'INFO',
                   '--drive-chunk-size', chunk_size] + impersonate_arg
            cmd.extend(FeederUtil.get_rclone_exclude_options())
            cmd.extend(FeederUtil.get_rclone_extra_options())
            success, out = cls.run_rclone(cmd, f'업로드 {folder_name}', item_id=item_id, file_size=fsize, watchdog_timeout=300)
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item or item.status != 'uploading':
                return False

            if not success:
                db.session.remove()
                out_lower = out.lower()
                is_quota = 'storagequotaexceeded' in out_lower or 'upload limit' in out_lower
                is_rate = 'userratelimitexceeded' in out_lower or 'rate limit' in out_lower or '403' in out_lower
                if is_quota:
                    if mode == 'mydrive':
                        manager.block_account(acc_name, is_rate_limit=False)
                    else:
                        manager.block_shared_drive()
                elif is_rate and mode == 'mydrive':
                    manager.block_account(acc_name, is_rate_limit=True)
                cls._mark_move_failure(item_id, f'업로드 실패 (다음 주기에 재시도): {out.strip()[:150]}', status='pending_upload')
                return False

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
        if item and item.status not in ['cancelled', 'completed']:
            item.status = status
            item.error_message = message
            item.last_move_attempt_time = datetime.now()
            item.last_status_time = datetime.now()
            db.session.commit()
        db.session.remove()

    @classmethod
    def resume_upload(cls, item, manager: AccountManager, account_reserved: bool = False) -> bool:
        """완료된 업로드는 다시 복사/과금하지 않고 원격 이동 또는 원본 정리만 재개한다."""
        item_id = item if isinstance(item, int) else item.id
        item = db.session.query(ModelDownload).filter_by(id=item_id).first()
        if not item or item.status in ['cancelled', 'failed']:
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
            if not target or target.status in ['cancelled', 'failed', 'completed']:
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
                        success, out = cls.run_rclone(move_cmd, f"MyDrive 이동 {state['folder_name']}")
                        if not success:
                            if attempt < 3:
                                time.sleep(attempt * 3)
                            continue
                        if not save_phase('shared_incoming'):
                            return False
                    mkdir_cmd = ['rclone', 'mkdir', state['dest_parent']] + conf_args
                    cls.run_rclone(mkdir_cmd, log_output=False)
                    chpar_cmd = ['rclone', 'backend', 'chpar', state['shared_incoming'], state['dest_parent'], '--log-level', 'NOTICE'] + conf_args
                    moved, out = cls.run_rclone(chpar_cmd, f"원자적 부모폴더 변경 {state['folder_name']}")
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
            if state['mode'] == 'mydrive':
                cls.run_rclone(['rclone', 'cleanup', f"{state['remote']}:", '--log-level', 'NOTICE'] + conf_args, log_output=False)

            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item or item.status in ['cancelled', 'failed', 'completed']:
                return bool(item and item.status == 'completed')
            item.status = 'completed'
            item.completed_time = datetime.now()
            item.error_message = None
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
