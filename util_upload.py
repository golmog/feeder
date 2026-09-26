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
        units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        m = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B)$", size_str)
        if m:
            val, unit = m.groups()
            return int(float(val) * units[unit])
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

    @staticmethod
    def run_rclone(cmd: list[str], description: str = "", log_output: bool = True) -> tuple[bool, str]:
        """Rclone 서브프로세스 실행 및 실시간 출력 캡처"""
        rclone_cfg = FeederUtil.load_yaml().get('rclone', {})
        env = os.environ.copy()
        if rclone_cfg.get('bind_ip'):
            env['RCLONE_BIND_ADDR'] = rclone_cfg['bind_ip']

        if log_output and description:
            logger.info(f"[Rclone] 시작: {description}")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                env=env
            )
            output_lines = []
            for line in iter(proc.stdout.readline, ''):
                line = line.strip()
                if not line:
                    continue
                output_lines.append(line)
                if "Transferred:" in line or "ETA" in line or "%" in line:
                    logger.debug(f"[Rclone Progress] {line}")

            proc.stdout.close()
            proc.wait()
            full_output = '\n'.join(output_lines)

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

            self.limit_user = UploadUtil.parse_size_bytes(P.ModelSetting.get('download_gdrive_upload_limit', '700GB'))
            self.limit_shared = UploadUtil.parse_size_bytes(P.ModelSetting.get('download_shared_drive_upload_limit', '3TB'))
            self.threshold = UploadUtil.parse_size_bytes(P.ModelSetting.get('download_mydrive_upload_threshold', '14GB'))
            self.reset_time_str = P.ModelSetting.get('download_shared_drive_quota_reset_time', '16:00')

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

        def allocate_account(self, file_size: int) -> tuple[str, dict | None, str]:
            with self.lock:
                if file_size > self.threshold:
                    if 'SHARED_DRIVE_UPLOAD' in self.blocked_accounts:
                        return 'skip', None, "공유 드라이브 403 쿼터 초과 차단 중"
                    if self.shared_usage + file_size > self.limit_shared:
                        return 'skip', None, "공유 드라이브 일일 설정 한도(3TB) 초과"
                    self.shared_usage += file_size
                    return 'shareddrive', None, "대용량 파일 (공유 드라이브 직행)"

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
                            return 'mydrive', acc, f"내 드라이브 바이패스 ({uname})"

                for acc in self.accounts:
                    uname = acc.get('username')
                    if uname not in self.blocked_accounts and self.usage_map.get(uname, 0) + file_size < self.limit_user:
                        return 'wait', None, "모든 가용 SA 계정 작업 중 (잠시 대기)"

                return 'skip', None, "모든 개인 계정 일일 한도 초과 또는 차단 상태"

        def release_account(self, username: str):
            with self.lock:
                if username in self.busy_accounts:
                    self.busy_accounts.remove(username)

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
    def drain_all_sa_mydrives(cls):
        """모든 SA 계정의 내 드라이브 잔여 파일을 공유 드라이브로 밀어내고 휴지통 비우기"""
        config_data = FeederUtil.load_yaml()
        rclone_cfg = config_data.get('rclone', {})
        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or rclone_cfg.get('conf_path', '')
        base_remote = P.ModelSetting.get('download_rclone_remote_name') or rclone_cfg.get('remote_name', 'gdrive_sa')
        dst_drive_id = P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')

        if not rclone_conf or not dst_drive_id:
            logger.debug("[UploadUtil] Rclone 설정 파일 경로 또는 공유 드라이브 ID가 설정되지 않아 SA 정리 건너뜀")
            return

        target_root = f"{base_remote}:{{{dst_drive_id}}}/"
        accounts = FeederUtil.get_gdrive_accounts()
        use_impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')

        for acc in accounts:
            email = acc.get('username')
            if not email:
                continue

            current_remote = base_remote if use_impersonate else (acc.get('remote_name') or base_remote)
            try:
                cmd_move = [
                    "rclone", "move", f"{current_remote}:", target_root,
                    "--config", rclone_conf,
                    "--drive-server-side-across-configs",
                    "--drive-use-trash=false",
                    "--delete-empty-src-dirs",
                    "--log-level", "ERROR"
                ]
                if use_impersonate:
                    cmd_move.extend(["--drive-impersonate", email])
                cls.run_rclone(cmd_move, log_output=False)

                cmd_cleanup = [
                    "rclone", "cleanup", f"{current_remote}:",
                    "--config", rclone_conf,
                    "--log-level", "ERROR"
                ]
                if use_impersonate:
                    cmd_cleanup.extend(["--drive-impersonate", email])
                cls.run_rclone(cmd_cleanup, log_output=False)
            except Exception as ex:
                logger.debug(f"[UploadUtil] {email} 내 드라이브 정리 중 예외 (무시): {ex}")

    @classmethod
    def execute_upload(cls, item: ModelDownload, manager: AccountManager) -> bool:
        """구글 드라이브 업로드 및 2단계 서버사이드 원자적 이동 실행"""
        local_path = item.local_path
        if not local_path or not os.path.exists(local_path):
            logger.error(f"[UploadUtil] 업로드할 로컬 폴더가 존재하지 않습니다: {local_path}")
            item.status = 'failed'
            item.error_message = "로컬 소스 폴더 없음"
            db.session.commit()
            return False

        folder_name = item.file_name or os.path.basename(local_path)
        fsize = item.file_size or sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(local_path) for f in fs)
        item.file_size = fsize

        rclone_cfg = FeederUtil.load_yaml().get('rclone', {})
        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or rclone_cfg.get('conf_path', '')
        remote_net = P.ModelSetting.get('download_rclone_remote_name') or rclone_cfg.get('remote_name', 'gdrive_sa')
        remote_shared = P.ModelSetting.get('download_rclone_shared_remote_name') or rclone_cfg.get('shared_remote_name', 'gdrive_shared')

        target_drive_id = item.gdrive_remote_id or P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')
        if not target_drive_id:
            logger.error("[UploadUtil] 목적지 공유 드라이브 ID(shared_drive_id)가 설정되지 않았습니다.")
            item.status = 'failed'
            item.error_message = "공유 드라이브 ID 누락"
            db.session.commit()
            return False

        mode, acc, reason = manager.allocate_account(fsize)
        if mode in ['wait', 'skip']:
            logger.info(f"[UploadUtil] 업로드 보류 ({reason}): {folder_name}")
            return False

        acc_name = acc.get('username') if acc else 'Default_Shared'
        usage_key = acc_name if mode == 'mydrive' else 'SHARED_DRIVE_UPLOAD'

        comp_path = (item.gdrive_complete_path or 'uploads/default').strip('/')
        up_path = (item.gdrive_upload_path or 'incoming/default').strip('/')
        use_impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')

        if mode == 'mydrive':
            effective_remote = remote_net if use_impersonate else (acc.get('remote_name') or remote_net)
            impersonate_arg = ["--drive-impersonate", acc_name] if use_impersonate else []
            dest_incoming = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{comp_path}/{folder_name}"
        else:
            effective_remote = remote_shared
            impersonate_arg = []
            dest_incoming = f"{remote_shared}:{{{target_drive_id}}}/{up_path}/{folder_name}"

        clean_chk = ["rclone", "lsjson", dest_incoming, "--stat", "--config", rclone_conf] + impersonate_arg
        csuc, cout = cls.run_rclone(clean_chk, log_output=False)
        if csuc and cout.strip() and cout.strip() not in ["{}", "[]"]:
            logger.warning(f"[UploadUtil] 이전 세션 비정상 중단 찌꺼기 발견 -> incoming 폴더 즉시 초기화: {dest_incoming}")
            clean_cmd = ["rclone", "purge", dest_incoming, "--config", rclone_conf] + impersonate_arg
            cls.run_rclone(clean_cmd, log_output=False)

        item.status = 'uploading'
        item.gdrive_account = acc_name
        db.session.commit()

        logger.info(f"[UploadUtil] 업로드 시작: {folder_name} [{cls.format_bytes(fsize)}] -> {mode} ({acc_name}, 리모트: {effective_remote})")
        manager.add_usage(usage_key, fsize)

        chunk_size = P.ModelSetting.get('download_rclone_chunk_size') or '256M'
        cmd = [
            "rclone", "copy", local_path, dest_incoming,
            "--config", rclone_conf,
            "--stats", "10s", "--stats-one-line", "--log-level", "NOTICE",
            "--drive-chunk-size", chunk_size
        ] + impersonate_arg

        # 유저 설정 Rclone 확장 옵션 결합
        cmd.extend(FeederUtil.get_rclone_extra_options())

        success, out = cls.run_rclone(cmd, f"업로드 {folder_name}")

        if not success:
            logger.error(f"[UploadUtil] 업로드 실패: {folder_name} -> incoming 불완전 찌꺼기 즉시 파기")
            purge_cmd = ["rclone", "purge", dest_incoming, "--config", rclone_conf]
            if mode == 'mydrive':
                purge_cmd.extend(["--drive-impersonate", acc_name])
            cls.run_rclone(purge_cmd, log_output=False)

            out_lower = out.lower()
            is_quota = "storagequotaexceeded" in out_lower or "upload limit" in out_lower
            is_rate = "userratelimitexceeded" in out_lower or "rate limit" in out_lower or "403" in out_lower

            if is_quota:
                if mode == 'mydrive':
                    manager.block_account(acc_name, is_rate_limit=False)
                else:
                    manager.block_shared_drive()
            elif is_rate and mode == 'mydrive':
                manager.block_account(acc_name, is_rate_limit=True)

            if mode == 'mydrive':
                manager.release_account(acc_name)

            item.status = 'pending_upload'
            item.error_message = f"업로드 실패 (찌꺼기 파기 완료: {out[:100]})"
            db.session.commit()
            return False

        move_success = False
        base_move_opts = [
            "--config", rclone_conf,
            "--drive-server-side-across-configs",
            "--drive-use-trash=false",
            "--stats-one-line", "--log-level", "NOTICE"
        ]

        if mode == 'mydrive':
            src_m = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{comp_path}/{folder_name}"
            dst_m = f"{remote_net}:{{{target_drive_id}}}/{comp_path}/{folder_name}"
            move_cmd = ["rclone", "move", src_m, dst_m, "--delete-empty-src-dirs"] + impersonate_arg + base_move_opts
            move_success, _ = cls.run_rclone(move_cmd, f"서버사이드 폴더 이동(Across) {folder_name}")
        else:
            dst_parent = f"{remote_shared}:{{{target_drive_id}}}/{comp_path}"
            chpar_cmd = ["rclone", "backend", "chpar", dest_incoming, dst_parent, "--config", rclone_conf, "--log-level", "NOTICE"]
            move_success, _ = cls.run_rclone(chpar_cmd, f"원자적 부모폴더 변경(chpar) {folder_name}")

        if move_success:
            try:
                shutil.rmtree(local_path, ignore_errors=True)
                logger.info(f"[UploadUtil] 로컬 작업 완료 폴더 정리 완료: {folder_name}")
            except Exception:
                pass

        if mode == 'mydrive':
            cleanup_cmd = ["rclone", "cleanup", f"{effective_remote}:", "--config", rclone_conf, "--log-level", "NOTICE"] + impersonate_arg
            cls.run_rclone(cleanup_cmd, log_output=False)
            manager.release_account(acc_name)

        if move_success:
            item.status = 'completed'
            item.completed_time = datetime.now()
            item.error_message = None
            logger.info(f"[UploadUtil] 구글 드라이브 최종 완료(completed): {folder_name}")
        else:
            item.status = 'move_failed'
            item.last_move_attempt_time = datetime.now()
            item.error_message = "서버사이드 chpar/move 실패"
            logger.warning(f"[UploadUtil] 최종 이동 실패 (move_failed로 보존): {folder_name}")

        db.session.commit()
        return True
