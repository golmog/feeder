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
from .util_crawl import get_tmp_dir
from .util_feed import FeedConfigUtil


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


def format_bytes(size: int) -> str:
    if size is None or size <= 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.2f} {unit}"


def run_rclone(cmd: list[str], description: str = "", log_output: bool = True) -> tuple[bool, str]:
    """Rclone 커맨드 실행 및 결과/로그 반환"""
    rclone_cfg = FeedConfigUtil.load_yaml().get('rclone', {})
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


def get_remote_size(remote_path: str, rclone_conf: str, impersonate_email: str = None) -> int:
    cmd = ["rclone", "size", remote_path, "--json", "--config", rclone_conf]
    if impersonate_email:
        cmd.extend(["--drive-impersonate", impersonate_email])
    suc, out = run_rclone(cmd, log_output=False)
    if suc:
        try:
            return int(json.loads(out).get('bytes', -1))
        except Exception:
            return -1
    return -1


class GDriveAccountManager:
    """구글 드라이브 계정 풀 24시간 쿼터 및 차단 관리자"""

    def __init__(self):
        self.lock = threading.Lock()
        config_data = FeedConfigUtil.load_yaml()
        global_cfg = config_data.get('GLOBAL', {})

        self.rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or config_data.get('rclone', {}).get('conf_path', '')
        self.accounts = FeedConfigUtil.get_gdrive_accounts()
        self.busy_accounts = set()

        self.limit_user = parse_size_bytes(P.ModelSetting.get('download_gdrive_upload_limit', '700GB'))
        self.limit_shared = parse_size_bytes(P.ModelSetting.get('download_shared_drive_upload_limit', '3TB'))
        self.threshold = parse_size_bytes(P.ModelSetting.get('download_mydrive_upload_threshold', '14GB'))
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
            # 임계값 초과 파일: 공유 드라이브 직접 업로드 분기
            if file_size > self.threshold:
                if 'SHARED_DRIVE_UPLOAD' in self.blocked_accounts:
                    return 'skip', None, "공유 드라이브 403 쿼터 초과 차단 중"
                if self.shared_usage + file_size > self.limit_shared:
                    return 'skip', None, "공유 드라이브 일일 설정 한도(3TB) 초과"
                self.shared_usage += file_size
                return 'shareddrive', None, "대용량 파일 (공유 드라이브 직행)"

            # 임계값 이하 파일: 24시간 사용량이 가장 적은 계정 선별
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

            # 모든 가용 계정이 작업 중인 경우 대기
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

            logger.warning(f"[GDriveAccount] 계정 차단 등록: {username} ({block_hours}시간)")

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
            logger.warning(f"[GDriveAccount] 공유 드라이브 403 쿼터 초과 차단 등록 (리셋 시점: {self.reset_time_str})")

    @classmethod
    def drain_all_sa_mydrives(cls):
        """모든 SA 계정의 내 드라이브 잔여 고아 파일을 목적지 공유 드라이브로 밀어내고 휴지통 비우기"""
        config_data = FeedConfigUtil.load_yaml()
        rclone_cfg = config_data.get('rclone', {})
        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or rclone_cfg.get('conf_path', '')
        base_remote = P.ModelSetting.get('download_rclone_remote_name') or rclone_cfg.get('remote_name', 'net')
        dst_drive_id = P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')

        if not rclone_conf or not dst_drive_id:
            logger.debug("[GDriveDrain] Rclone 설정 파일 경로 또는 공유 드라이브 ID가 설정되지 않아 SA 정리 건너뜀")
            return

        target_root = f"{base_remote}:{{{dst_drive_id}}}/"
        accounts = FeedConfigUtil.get_gdrive_accounts()
        use_impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')

        for acc in accounts:
            email = acc.get('username')
            if not email:
                continue

            current_remote = base_remote if use_impersonate else (acc.get('remote_name') or base_remote)
            try:
                # SA 내 드라이브 잔여 파일을 공유 드라이브로 서버사이드 이동
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
                run_rclone(cmd_move, log_output=False)

                # 내 드라이브 휴지통 영구 삭제 (15GB 공간 완전 회복)
                cmd_cleanup = [
                    "rclone", "cleanup", f"{current_remote}:",
                    "--config", rclone_conf,
                    "--log-level", "ERROR"
                ]
                if use_impersonate:
                    cmd_cleanup.extend(["--drive-impersonate", email])
                run_rclone(cmd_cleanup, log_output=False)
            except Exception as ex:
                logger.debug(f"[GDriveDrain] {email} 내 드라이브 정리 중 예외 (무시): {ex}")


class GDriveUploadHandler:
    """구글 드라이브 업로드 및 2단계 서버사이드 원자적 이동 핸들러"""

    @classmethod
    def execute_upload(cls, item: ModelDownload, manager: GDriveAccountManager) -> bool:
        local_path = item.local_path
        if not local_path or not os.path.exists(local_path):
            logger.error(f"[GDriveUpload] 업로드할 로컬 폴더가 존재하지 않습니다: {local_path}")
            item.status = 'failed'
            item.error_message = "로컬 소스 폴더 없음"
            db.session.commit()
            return False

        # 파이프라인 표준화에 의해 local_path는 항상 고유 폴더 구조임: {이름}_[{hash}]
        folder_name = item.file_name or os.path.basename(local_path)
        fsize = item.file_size or sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(local_path) for f in fs)
        item.file_size = fsize

        rclone_cfg = FeedConfigUtil.load_yaml().get('rclone', {})
        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or rclone_cfg.get('conf_path', '')
        remote_net = P.ModelSetting.get('download_rclone_remote_name') or rclone_cfg.get('remote_name', 'net')
        remote_shared = P.ModelSetting.get('download_rclone_shared_remote_name') or rclone_cfg.get('shared_remote_name', 'gf')

        target_drive_id = item.gdrive_remote_id or P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')
        if not target_drive_id:
            logger.error("[GDriveUpload] 목적지 공유 드라이브 ID(shared_drive_id)가 설정되지 않았습니다.")
            item.status = 'failed'
            item.error_message = "공유 드라이브 ID 누락"
            db.session.commit()
            return False

        # 구글 드라이브 사전 쿼터 계산 (한도 초과 시 403을 만나기 전에 차단)
        mode, acc, reason = manager.allocate_account(fsize)
        if mode in ['wait', 'skip']:
            logger.info(f"[GDriveUpload] 업로드 보류 ({reason}): {folder_name}")
            return False

        acc_name = acc.get('username') if acc else 'Default_Shared'
        usage_key = acc_name if mode == 'mydrive' else 'SHARED_DRIVE_UPLOAD'

        comp_path = (item.gdrive_complete_path or 'uploads/default').strip('/')
        up_path = (item.gdrive_upload_path or 'incoming/default').strip('/')
        use_impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')

        # 1차 업로드 목적지 incoming 경로 구성 (위임 여부에 따른 리모트 결정)
        if mode == 'mydrive':
            effective_remote = remote_net if use_impersonate else (acc.get('remote_name') or remote_net)
            impersonate_arg = ["--drive-impersonate", acc_name] if use_impersonate else []
            dest_incoming = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{comp_path}/{folder_name}"
        else:
            effective_remote = remote_shared
            impersonate_arg = []
            dest_incoming = f"{remote_shared}:{{{target_drive_id}}}/{up_path}/{folder_name}"

        # 이전 비정상 종료 찌꺼기 선삭제(Fail-Clean)
        clean_chk = ["rclone", "lsjson", dest_incoming, "--stat", "--config", rclone_conf] + impersonate_arg
        csuc, cout = run_rclone(clean_chk, log_output=False)
        if csuc and cout.strip() and cout.strip() not in ["{}", "[]"]:
            logger.warning(f"[GDriveUpload] 이전 세션 비정상 중단 찌꺼기 발견 -> incoming 폴더 즉시 초기화: {dest_incoming}")
            clean_cmd = ["rclone", "purge", dest_incoming, "--config", rclone_conf] + impersonate_arg
            run_rclone(clean_cmd, log_output=False)

        item.status = 'uploading'
        item.gdrive_account = acc_name
        db.session.commit()

        logger.info(f"[GDriveUpload] 업로드 시작: {folder_name} [{format_bytes(fsize)}] -> {mode} ({acc_name}, 리모트: {effective_remote})")
        manager.add_usage(usage_key, fsize)

        cmd = [
            "rclone", "copy", local_path, dest_incoming,
            "--config", rclone_conf,
            "--stats", "10s", "--stats-one-line", "--log-level", "NOTICE",
            "--drive-chunk-size", "256M",
            "--retries", "1", "--timeout", "30m", "--contimeout", "30s"
        ] + impersonate_arg

        success, out = run_rclone(cmd, f"업로드 {folder_name}")

        # 업로드 도중 에러 발생 시 incoming 찌꺼기 즉시 파기 및 계정 차단 처리
        if not success:
            logger.error(f"[GDriveUpload] 업로드 실패: {folder_name} -> incoming 불완전 찌꺼기 즉시 파기")
            purge_cmd = ["rclone", "purge", dest_incoming, "--config", rclone_conf]
            if mode == 'mydrive':
                purge_cmd.extend(["--drive-impersonate", acc_name])
            run_rclone(purge_cmd, log_output=False)

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

        # [2단계 최종 이동] 모든 대상이 폴더이므로 rclone backend chpar 및 서버사이드 폴더 이동으로 0.1초 완료
        move_success = False
        base_move_opts = [
            "--config", rclone_conf,
            "--drive-server-side-across-configs",
            "--drive-use-trash=false",
            "--stats-one-line", "--log-level", "NOTICE"
        ]

        if mode == 'mydrive':
            # 내 드라이브 -> 공유 드라이브 최종 위치로 서버사이드 이동
            src_m = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{comp_path}/{folder_name}"
            dst_m = f"{remote_net}:{{{target_drive_id}}}/{comp_path}/{folder_name}"
            move_cmd = ["rclone", "move", src_m, dst_m, "--delete-empty-src-dirs"] + impersonate_arg + base_move_opts
            move_success, _ = run_rclone(move_cmd, f"서버사이드 폴더 이동(Across) {folder_name}")
        else:
            # 공유 드라이브 내부: incoming 폴더에서 uploads 부모 폴더 ID로 원자적 chpar 변경
            dst_parent = f"{remote_shared}:{{{target_drive_id}}}/{comp_path}"
            chpar_cmd = ["rclone", "backend", "chpar", dest_incoming, dst_parent, "--config", rclone_conf, "--log-level", "NOTICE"]
            move_success, _ = run_rclone(chpar_cmd, f"원자적 부모폴더 변경(chpar) {folder_name}")

        # 이동 완료 시 로컬 격리 폴더 및 내 드라이브 휴지통 완전 삭제
        if move_success:
            try:
                shutil.rmtree(local_path, ignore_errors=True)
                logger.info(f"[GDriveUpload] 로컬 작업 완료 폴더 정리 완료: {folder_name}")
            except Exception:
                pass

        if mode == 'mydrive':
            cleanup_cmd = ["rclone", "cleanup", f"{effective_remote}:", "--config", rclone_conf, "--log-level", "NOTICE"] + impersonate_arg
            run_rclone(cleanup_cmd, log_output=False)
            manager.release_account(acc_name)

        if move_success:
            item.status = 'completed'
            item.completed_time = datetime.now()
            item.error_message = None
            logger.info(f"[GDriveUpload] 구글 드라이브 최종 완료(completed): {folder_name}")
        else:
            item.status = 'move_failed'
            item.last_move_attempt_time = datetime.now()
            item.error_message = "서버사이드 chpar/move 실패"
            logger.warning(f"[GDriveUpload] 최종 이동 실패 (move_failed로 보존): {folder_name}")

        db.session.commit()
        return True
