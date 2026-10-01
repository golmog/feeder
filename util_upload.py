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
    def run_rclone(cmd: list[str], description: str = "", log_output: bool = True, item_id=None, file_size=None, watchdog_timeout: int = None) -> tuple[bool, str]:
        """Rclone 서브프로세스 실행 및 실시간 출력/전송률 캡처"""
        env = os.environ.copy()

        if log_output and description:
            size_text = f" ({UploadUtil.format_bytes(file_size)})" if file_size else ""
            logger.info(f"[Rclone] 시작: {description}{size_text}")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                env=env
            )

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
            if item_id:
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
        base_remote = (
            P.ModelSetting.get('download_gdrive_mydrive_remote_name')
            or P.ModelSetting.get('download_rclone_remote_name')
            or rclone_cfg.get('mydrive_remote_name')
            or rclone_cfg.get('remote_name', 'gdrive_sa')
        )
        dst_drive_id = P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')

        if not rclone_conf or not dst_drive_id:
            logger.debug("[UploadUtil] Rclone 설정 파일 경로 또는 공유 드라이브 ID가 설정되지 않아 SA 정리 건너뜀")
            return

        accounts = FeederUtil.get_gdrive_accounts()
        if not accounts:
            logger.debug("[UploadUtil] 등록된 SA 계정이 없어 내 드라이브 정리 건너뜀")
            return

        target_root = f"{base_remote}:{{{dst_drive_id}}}/"
        use_impersonate = P.ModelSetting.get_bool('download_gdrive_use_impersonate')

        logger.info(f"[UploadUtil] Google Drive SA 내 드라이브 고아 파일 정리 시작 (대상: {len(accounts)}개 계정, 목적지: {target_root})")

        processed_count = 0
        for acc in accounts:
            email = acc.get('username')
            if not email:
                continue

            current_remote = base_remote if use_impersonate else (acc.get('remote_name') or base_remote)
            logger.debug(f"[UploadUtil] SA 계정 잔여 파일 정리 중: {email} (리모트: {current_remote})")

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
                processed_count += 1
            except Exception as ex:
                logger.debug(f"[UploadUtil] {email} 내 드라이브 정리 중 예외 (무시): {ex}")

        logger.info(f"[UploadUtil] Google Drive SA 내 드라이브 고아 파일 정리 완료 (처리: {processed_count}/{len(accounts)}개 계정)")

    @classmethod
    def execute_upload(cls, item: ModelDownload, manager: AccountManager) -> bool:
        """구글 드라이브 업로드 및 2단계 서버사이드 원자적 이동 실행 (Zero-Lock Rclone 전송)"""
        local_path = item.local_path

        # 원격 소스(AllDebrid 등) 다이렉트 스트리밍 여부 판별
        is_remote_source = bool(local_path and not os.path.exists(local_path) and (':' in local_path or local_path.startswith(('http://', 'https://'))))

        if not local_path or (not is_remote_source and not os.path.exists(local_path)):
            logger.error(f"[UploadUtil] 업로드할 소스 경로가 존재하지 않습니다: {local_path}")
            item.status = 'failed'
            item.error_message = "소스 경로 없음"
            db.session.commit()
            return False

        raw_name = item.file_name or os.path.basename(local_path)
        base_name, ext = os.path.splitext(raw_name)
        # 단일 파일일 때도 확장자를 제외한 폴더명을 생성하여 항상 폴더 단위로 수신 및 chpar 처리
        folder_name = base_name if (ext and not raw_name.endswith(('/', '\\'))) else raw_name
        item.file_name = folder_name

        if is_remote_source:
            fsize = item.file_size or 0
        else:
            fsize = item.file_size or sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(local_path) for f in fs)
        item.file_size = fsize

        rclone_cfg = FeederUtil.load_yaml().get('rclone', {})
        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or rclone_cfg.get('conf_path', '')

        # 프로필의 최신 목적지 설정 동적 조회 (과거 DB 고정값 오버라이드)
        profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name)

        # 리모트명 계층 판별: 프로필 목적지 리모트 -> 전역 DB 설정 -> YAML rclone -> 기본값
        remote_net = (
            dest_cfg.get('mydrive_remote_name')
            or P.ModelSetting.get('download_gdrive_mydrive_remote_name')
            or P.ModelSetting.get('download_rclone_remote_name')
            or rclone_cfg.get('mydrive_remote_name')
            or rclone_cfg.get('remote_name', 'gdrive_sa')
        )
        remote_shared = (
            dest_cfg.get('shared_remote_name')
            or P.ModelSetting.get('download_rclone_shared_remote_name')
            or rclone_cfg.get('shared_remote_name', 'gdrive_shared')
        )

        # 공유 드라이브 ID 계층 판별: 프로필 설정 -> DB 기본 설정 -> YAML 설정
        target_drive_id = dest_cfg.get('shared_drive_id') or item.gdrive_remote_id or P.ModelSetting.get('download_shared_drive_id') or rclone_cfg.get('shared_drive_id', '')
        if not target_drive_id:
            logger.error("[UploadUtil] 목적지 공유 드라이브 ID(shared_drive_id)가 설정되지 않았습니다.")
            item.status = 'failed'
            item.error_message = "공유 드라이브 ID 누락"
            db.session.commit()
            return False

        # 최신 목적지 서브 경로 동적 갱신
        comp_path = (dest_cfg.get('complete_path') or item.gdrive_complete_path or 'uploads/default').strip('/')
        up_path = (dest_cfg.get('upload_path') or item.gdrive_upload_path or 'incoming/default').strip('/')

        mode, acc, reason = manager.allocate_account(fsize)
        if mode in ['wait', 'skip']:
            logger.info(f"[UploadUtil] 업로드 보류 ({reason}): {folder_name}")
            return False

        acc_name = acc.get('username') if acc else 'Default_Shared'
        usage_key = acc_name if mode == 'mydrive' else 'SHARED_DRIVE_UPLOAD'
        use_impersonate = FeederUtil.resolve_setting(
            'use_impersonate',
            dest_cfg,
            {},
            P.ModelSetting.get_bool('download_gdrive_use_impersonate'),
            False
        )
        use_impersonate = str(use_impersonate).lower() in ('true', 'on', '1')

        # 계정 락 영구 누수 방지를 위한 try-finally 보장 블록
        try:
            if mode == 'mydrive':
                effective_remote = remote_net if use_impersonate else (acc.get('remote_name') or remote_net)
                impersonate_arg = ["--drive-impersonate", acc_name] if use_impersonate else []
                dest_incoming = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{up_path}/{folder_name}"
            else:
                effective_remote = remote_shared
                impersonate_arg = []
                dest_incoming = f"{remote_shared}:{{{target_drive_id}}}/{up_path}/{folder_name}"

            clean_chk = ["rclone", "lsjson", dest_incoming, "--stat", "--config", rclone_conf] + impersonate_arg
            csuc, cout = cls.run_rclone(clean_chk, log_output=False, watchdog_timeout=None)
            if csuc and cout.strip() and cout.strip() not in ["{}", "[]"]:
                logger.warning(f"[UploadUtil] 이전 세션 비정상 중단 찌꺼기 발견 -> incoming 폴더 즉시 초기화: {dest_incoming}")
                clean_cmd = ["rclone", "purge", dest_incoming, "--config", rclone_conf] + impersonate_arg
                cls.run_rclone(clean_cmd, log_output=False, watchdog_timeout=None)

            item.status = 'uploading'
            item.gdrive_account = acc_name
            db.session.commit()

            logger.info(f"[UploadUtil] 업로드 시작: {folder_name} [{cls.format_bytes(fsize)}] -> {mode} ({acc_name}, 리모트: {effective_remote})")
            manager.add_usage(usage_key, fsize)

            chunk_size = P.ModelSetting.get('download_rclone_chunk_size') or '256M'
            cmd = [
                "rclone", "copy", local_path, dest_incoming,
                "--config", rclone_conf,
                "--stats", "1s", "--stats-one-line", "--log-level", "INFO",
                "--drive-chunk-size", chunk_size
            ] + impersonate_arg

            cmd.extend(FeederUtil.get_rclone_exclude_options())
            cmd.extend(FeederUtil.get_rclone_extra_options())

            item_id = item.id
            current_engine_name = item.current_engine_name
            engine_task_id = item.engine_task_id

            # 대용량 Rclone 업로드 중 DB 락을 원천 차단하기 위해 세션을 완전히 반환
            db.session.remove()

            success, out = cls.run_rclone(cmd, f"업로드 {folder_name}", item_id=item_id, file_size=fsize, watchdog_timeout=300)

            # Rclone 전송 종료 후 다시 세션을 열어 최종 상태 갱신
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item:
                return False

            if not success:
                logger.error(f"[UploadUtil] 업로드 실패: {folder_name} -> incoming 불완전 찌꺼기 즉시 파기")
                purge_cmd = ["rclone", "purge", dest_incoming, "--config", rclone_conf]
                if mode == 'mydrive':
                    purge_cmd.extend(["--drive-impersonate", acc_name])
                cls.run_rclone(purge_cmd, log_output=False, watchdog_timeout=None)

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

                # 실패 시에도 완료된 로컬 파일은 보존하고 pending_upload 상태 유지
                err_summary = out.strip()[:150] if out else "5분간 전송 시작 불가 또는 네트워크 오류"
                item.status = 'pending_upload'
                item.error_message = f"업로드 실패 (다음 주기에 재시도): {err_summary}"
                item.last_status_time = datetime.now()
                db.session.commit()
                db.session.remove()
                return False

            is_dir = os.path.isdir(local_path)
            move_success = False
            base_move_opts = [
                "--config", rclone_conf,
                "--drive-server-side-across-configs",
                "--drive-use-trash=false",
                "--stats-one-line", "--log-level", "NOTICE"
            ]

            if mode == 'mydrive':
                src_m = f"{effective_remote}:{{{acc.get('mydrive_rclone_id')}}}/{comp_path}/{folder_name}"
                dst_m = f"{remote_shared}:{{{target_drive_id}}}/{comp_path}/{folder_name}"
                cmd_action = "move" if (is_dir or is_remote_source) else "moveto"
                move_cmd = ["rclone", cmd_action, src_m, dst_m] + impersonate_arg + base_move_opts
                if is_dir or is_remote_source:
                    move_cmd.append("--delete-empty-src-dirs")
                move_success, _ = cls.run_rclone(move_cmd, f"서버사이드 이동(Across/{cmd_action}) {folder_name}", log_output=True, watchdog_timeout=None)
            else:
                dst_parent = f"{remote_shared}:{{{target_drive_id}}}/{comp_path}"
                move_cmd = ["rclone", "backend", "chpar", dest_incoming, dst_parent, "--config", rclone_conf, "--log-level", "NOTICE"]
                move_success, _ = cls.run_rclone(move_cmd, f"원자적 부모폴더 변경(chpar) {folder_name}", log_output=True, watchdog_timeout=None)

            if move_success:
                downloader_cfg = FeederUtil.get_downloader_by_name(current_engine_name)
                is_cd2_source = bool(
                    downloader_cfg and
                    str(downloader_cfg.get('engine_type', '')).lower() == 'cd2'
                )

                # CD2 -> Google Drive는 복사가 아니라 이동이므로 마운트 원본도 제거
                if is_cd2_source and local_path and os.path.exists(local_path):
                    try:
                        if os.path.isdir(local_path):
                            shutil.rmtree(local_path)
                        else:
                            os.remove(local_path)
                        logger.info(f"[UploadUtil] CD2 마운트 원본 삭제 완료: {local_path}")
                    except Exception as cleanup_ex:
                        item.status = 'move_failed'
                        item.last_move_attempt_time = datetime.now()
                        item.error_message = f"CD2 마운트 원본 삭제 실패: {cleanup_ex}"
                        db.session.commit()
                        logger.error(f"[UploadUtil] CD2 마운트 원본 삭제 실패: {local_path} ({cleanup_ex})")
                        db.session.remove()
                        return False

                # 최종 업로드 완료 시 다운로더 엔진의 원본 작업 히스토리 자동 삭제
                if downloader_cfg and engine_task_id:
                    try:
                        from .util_download import DownloadUtil
                        engine = DownloadUtil.create_engine(downloader_cfg)
                        if engine:
                            engine.delete_task(engine_task_id)
                            logger.info(f"[UploadUtil] 다운로더 작업 히스토리 정리 완료: [{current_engine_name}] {engine_task_id}")
                    except Exception as del_ex:
                        logger.debug(f"[UploadUtil] 다운로더 작업 삭제 실패 (무시): {del_ex}")

                # 로컬 임시 스테이징 폴더인 경우 정리 (CD2 마운트 원본은 위에서 별도 처리)
                if not is_remote_source and not is_cd2_source:
                    try:
                        if os.path.exists(local_path):
                            shutil.rmtree(local_path, ignore_errors=True)
                            logger.info(f"[UploadUtil] 로컬 작업 완료 폴더 정리 완료: {folder_name}")
                    except Exception:
                        pass

            if mode == 'mydrive':
                cleanup_cmd = ["rclone", "cleanup", f"{effective_remote}:", "--config", rclone_conf, "--log-level", "NOTICE"] + impersonate_arg
                cls.run_rclone(cleanup_cmd, log_output=False, watchdog_timeout=None)

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
            db.session.remove()
            return move_success
        finally:
            if mode == 'mydrive' and acc_name:
                manager.release_account(acc_name)
