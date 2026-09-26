# -*- coding: utf-8 -*-
import os
import glob
import shutil
import requests
import subprocess
import importlib.util

from .setup import *
from .util_base import FeederUtil
from .util_upload import UploadUtil


# ==============================================================================
# 커스텀 확장용 베이스 인터페이스 (Base Interfaces)
# ==============================================================================
class BaseDownloadEngine:
    """모든 다운로더 엔진의 표준 인터페이스"""
    ENGINE_ID = "base"
    ENGINE_NAME = "Base Engine"
    OUTPUT_TYPE = "local"
    SUPPORTED_PROTOCOLS = ["magnet"]
    CONFIG_SCHEMA = []

    def __init__(self, config: dict):
        self.config = config or {}
        self.name = self.config.get('name', self.ENGINE_ID)
        self.enabled = bool(self.config.get('enabled', True))

    def add_magnet(self, link: str, title: str = None) -> tuple[bool, str, str]:
        raise NotImplementedError

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        raise NotImplementedError

    def delete_task(self, task_id: str) -> bool:
        raise NotImplementedError

    def restart_task(self, task_id: str) -> bool:
        return False

    def test_connection(self) -> tuple[bool, str]:
        return True, "연결 테스트가 정의되지 않은 엔진입니다."


class BaseTransporter:
    """모든 이송/후처리 핸들러의 표준 인터페이스"""
    TRANSPORTER_ID = "base"
    TRANSPORTER_NAME = "Base Transporter"
    CONFIG_SCHEMA = []

    def __init__(self, config: dict = None):
        self.config = config or {}

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        raise NotImplementedError


# ==============================================================================
# 기본 내장 엔진 및 트랜스포터 (Built-in Implementations)
# ==============================================================================
class QBittorrentEngine(BaseDownloadEngine):
    ENGINE_ID = "qbittorrent"
    ENGINE_NAME = "qBittorrent (로컬 다운로더)"
    OUTPUT_TYPE = "local"
    SUPPORTED_PROTOCOLS = ["magnet"]

    CONFIG_SCHEMA = [
        {"name": "url", "label": "Web UI 주소", "type": "text", "default": "http://127.0.0.1:8080", "required": True},
        {"name": "username", "label": "사용자명", "type": "text", "default": "admin"},
        {"name": "password", "label": "비밀번호", "type": "password"},
        {"name": "save_path", "label": "다운로드 완료 경로", "type": "text", "default": "/data/downloads", "placeholder": "예: /data/downloads"},
        {"name": "category", "label": "카테고리", "type": "text", "default": "feeder"},
        {"name": "stalled_timeout_hours", "label": "지연 타임아웃 (시간)", "type": "number", "default": 24}
    ]

    def __init__(self, config: dict):
        super(QBittorrentEngine, self).__init__(config)
        self.url = self.config.get('url', 'http://127.0.0.1:8080').rstrip('/')
        self.username = self.config.get('username', 'admin')
        self.password = self.config.get('password', '')
        self.save_path = self.config.get('save_path', '').strip()
        self.category = self.config.get('category', 'feeder').strip()
        self.session = None

    def _get_session(self) -> requests.Session | None:
        if self.session:
            return self.session

        s = requests.Session()
        login_url = f"{self.url}/api/v2/auth/login"
        try:
            res = s.post(login_url, data={'username': self.username, 'password': self.password}, timeout=10)
            if res.status_code in [200, 204] and 'Fails' not in res.text:
                self.session = s
                return s
        except Exception as e:
            logger.debug(f"[QBittorrentEngine] 로그인 예외: {e}")
        return None

    def add_magnet(self, link: str, title: str = None) -> tuple[bool, str, str]:
        s = self._get_session()
        if not s:
            return False, "", "qBittorrent 로그인 실패"
        if str(link).lower().startswith('ed2k://'):
            return False, "", "ed2k 프로토콜 미지원 (115 등 지원 엔진 필요)"

        add_url = f"{self.url}/api/v2/torrents/add"
        payload = {
            'urls': link,
            'category': self.category,
            'autoTMM': 'false'
        }
        if self.save_path:
            payload['savepath'] = self.save_path

        info_hash = FeederUtil.extract_info_hash(link)

        try:
            res = s.post(add_url, data=payload, timeout=15)
            if res.status_code in [200, 204]:
                task_id = info_hash or link[:40]
                logger.info(f"[QBittorrentEngine] 작업 추가 성공: {task_id} ({title or link[:30]})")
                return True, task_id, ""
            return False, "", f"추가 실패: HTTP {res.status_code}"
        except Exception as e:
            return False, "", f"추가 통신 예외: {str(e)}"

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        s = self._get_session()
        if not s:
            return [], "qBittorrent 세션 없음"

        info_url = f"{self.url}/api/v2/torrents/info"
        try:
            res = s.get(info_url, params={'category': self.category}, timeout=15)
            if res.status_code != 200:
                return [], f"조회 실패: HTTP {res.status_code}"

            torrents = res.json()
            ret = []
            for t in torrents:
                t_hash = t.get('hash', '').lower()
                progress = t.get('progress', 0.0)
                state = t.get('state', '')

                if progress >= 1.0 or state in ['uploading', 'pausedUP', 'queuedUP']:
                    std_status = 'completed'
                elif state in ['error', 'missingFiles']:
                    std_status = 'error'
                else:
                    std_status = 'downloading'

                save_dir = t.get('save_path') or self.save_path
                fname = t.get('name') or ''
                full_local_path = os.path.join(save_dir, fname) if save_dir and fname else save_dir

                ret.append({
                    'task_id': t_hash,
                    'hash': t_hash,
                    'status': std_status,
                    'filename': fname,
                    'file_size': t.get('total_size', 0),
                    'source_path': full_local_path
                })
            return ret, ""
        except Exception as e:
            return [], f"조회 통신 예외: {str(e)}"

    def delete_task(self, task_id: str) -> bool:
        s = self._get_session()
        if not s or not task_id:
            return False

        del_url = f"{self.url}/api/v2/torrents/delete"
        try:
            res = s.post(del_url, data={'hashes': task_id, 'deleteFiles': 'false'}, timeout=10)
            return res.status_code in [200, 204]
        except Exception:
            return False

    def test_connection(self) -> tuple[bool, str]:
        s = self._get_session()
        if not s:
            return False, "qBittorrent 로그인 실패 (URL, 사용자명, 비밀번호 확인 필요)"
        try:
            res = s.get(f"{self.url}/api/v2/app/version", timeout=10)
            if res.status_code == 200:
                return True, f"qBittorrent 연결 성공 (버전: {res.text.strip()})"
            return False, f"qBittorrent 응답 오류: HTTP {res.status_code}"
        except Exception as e:
            return False, f"qBittorrent 통신 예외: {str(e)}"


class LocalTransporter(BaseTransporter):
    TRANSPORTER_ID = "local"
    TRANSPORTER_NAME = "로컬 디스크 보존 (단순 완료)"

    CONFIG_SCHEMA = [
        {"name": "target_folder", "label": "최종 이동 경로", "type": "text", "placeholder": "비워두면 수득 완료 위치 그대로 보존", "desc": "로컬 디스크 내 완료 파일이 이동될 최종 디렉터리 경로"}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        target_folder = (dest_config.get('target_folder') or '').strip()
        folder_name = item.file_name or os.path.basename(source_path)

        if not source_path or not os.path.exists(source_path):
            logger.error(f"[LocalTransporter] 로컬 소스 경로가 존재하지 않음: {source_path}")
            return False, "failed", f"로컬 파일 없음: {source_path}"

        final_path = source_path
        if target_folder and os.path.abspath(source_path) != os.path.abspath(target_folder):
            try:
                os.makedirs(target_folder, exist_ok=True)
                dest_file = os.path.join(target_folder, folder_name)
                if os.path.exists(dest_file):
                    short_hash = (item.infohash[:6] if item.infohash else "dup")
                    n, e = os.path.splitext(folder_name)
                    dest_file = os.path.join(target_folder, f"{n}_{short_hash}{e}" if e else f"{folder_name}_{short_hash}")

                shutil.move(source_path, dest_file)
                final_path = dest_file
                logger.info(f"[LocalTransporter] 로컬 최종 경로 이동 완료: {final_path}")
            except Exception as e:
                logger.error(f"[LocalTransporter] 로컬 파일 이동 실패: {e}")
                return False, "failed", f"로컬 이동 실패: {str(e)}"

        item.local_path = final_path
        return True, "completed", f"로컬 보존 완료 ({final_path})"


class RcloneSimpleTransporter(BaseTransporter):
    TRANSPORTER_ID = "rclone_simple"
    TRANSPORTER_NAME = "일반 Rclone 리모트 단순 업로드"

    CONFIG_SCHEMA = [
        {"name": "remote_path", "label": "Rclone 목적지 경로", "type": "text", "required": True, "placeholder": "예: onedrive:media/movies 또는 my_gdrive:incoming"},
        {"name": "chunk_size", "label": "업로드 청크 크기", "type": "text", "default": "128M"}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        remote_dest = (dest_config.get('remote_path') or '').strip()
        if not remote_dest:
            return False, "failed", "Rclone 목적지 경로(remote_path) 미설정"

        if not source_path or not os.path.exists(source_path):
            return False, "failed", f"업로드 대상 소스 파일 없음: {source_path}"

        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        folder_name = item.file_name or os.path.basename(source_path)
        dest_full = f"{remote_dest.rstrip('/')}/{folder_name}"
        chunk_size = dest_config.get('chunk_size', '128M')

        cmd = [
            "rclone", "copy", source_path, dest_full,
            "--stats", "10s", "--stats-one-line", "--log-level", "NOTICE",
            "--drive-chunk-size", chunk_size
        ]
        if rclone_conf:
            cmd.extend(["--config", rclone_conf])

        cmd.extend(FeederUtil.get_rclone_extra_options())

        logger.info(f"[RcloneTransporter] 업로드 시작: {folder_name} -> {dest_full}")
        try:
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3600)
            if res.returncode == 0:
                logger.info(f"[RcloneTransporter] 업로드 완료: {dest_full}")
                if os.path.isdir(source_path):
                    shutil.rmtree(source_path, ignore_errors=True)
                elif os.path.isfile(source_path):
                    os.remove(source_path)
                return True, "completed", f"Rclone 업로드 완료 ({dest_full})"
            else:
                err = res.stderr.strip() or "알 수 없는 Rclone 오류"
                logger.error(f"[RcloneTransporter] Rclone 오류: {err}")
                return False, "failed", f"Rclone 실패: {err[:120]}"
        except Exception as ex:
            logger.error(f"[RcloneTransporter] Rclone 실행 예외: {ex}")
            return False, "failed", f"Rclone 실행 예외: {str(ex)}"


class GDrivePoolTransporter(BaseTransporter):
    TRANSPORTER_ID = "gdrive_rotation"
    TRANSPORTER_NAME = "Google Drive 계정 풀 로테이션 (MyDrive 경유용)"

    CONFIG_SCHEMA = [
        {"name": "upload_path", "label": "임시 수신 경로 (Incoming)", "type": "text", "default": "incoming/default"},
        {"name": "complete_path", "label": "최종 라이브러리 경로 (Complete)", "type": "text", "default": "uploads/default"},
        {"name": "shared_drive_id", "label": "공유 드라이브 ID (Shared Drive ID)", "type": "text", "placeholder": "미입력 시 기본 설정값 사용"}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        item.destination_type = "gdrive_rotation"
        item.gdrive_upload_path = dest_config.get('upload_path', 'incoming/default')
        item.gdrive_complete_path = dest_config.get('complete_path', 'uploads/default')
        item.gdrive_remote_id = dest_config.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
        item.local_path = source_path

        manager = UploadUtil.get_account_manager()
        success = UploadUtil.execute_upload(item, manager)
        if success:
            return True, "completed", "구글 드라이브 최종 완료"
        else:
            return False, item.status, f"구글 드라이브 업로드 보류 또는 실패 ({item.error_message})"


# ==============================================================================
# 단일 통합 관리자: DownloadUtil
# ==============================================================================
class DownloadUtil:
    """다운로더 엔진 및 이송 핸들러 통합 관리자"""

    _engine_classes = {}
    _transporter_classes = {}

    # 엔진 관리
    @classmethod
    def load_engines(cls):
        FeederUtil.ensure_custom_dirs()
        cls._engine_classes = {
            QBittorrentEngine.ENGINE_ID: QBittorrentEngine,
        }

        py_files = glob.glob(os.path.join(FeederUtil.ENGINES_DIR, "*.py"))
        for fpath in py_files:
            fname = os.path.basename(fpath)
            if fname.startswith("__") or fname in ["engine_qbittorrent.py", "dl_qbittorrent.py"]:
                continue
            module_name = f"feeder_engine_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    obj = getattr(mod, attr_name)
                    if isinstance(obj, type) and issubclass(obj, BaseDownloadEngine) and obj is not BaseDownloadEngine:
                        e_id = getattr(obj, 'ENGINE_ID', '').lower()
                        if e_id:
                            cls._engine_classes[e_id] = obj
            except Exception as e:
                logger.error(f"[DownloadUtil] 엔진 스크립트 로드 실패 ({fname}): {e}")

    @classmethod
    def get_engine_schemas(cls) -> list[dict]:
        if not cls._engine_classes:
            cls.load_engines()

        schemas = []
        for e_id, e_cls in cls._engine_classes.items():
            schemas.append({
                'engine_id': e_id,
                'engine_name': getattr(e_cls, 'ENGINE_NAME', e_id),
                'output_type': getattr(e_cls, 'OUTPUT_TYPE', 'local'),
                'config_schema': getattr(e_cls, 'CONFIG_SCHEMA', [])
            })
        return schemas

    @classmethod
    def create_engine(cls, downloader_cfg: dict) -> BaseDownloadEngine | None:
        if not cls._engine_classes:
            cls.load_engines()

        e_type = downloader_cfg.get('engine_type', '').lower()
        engine_cls = cls._engine_classes.get(e_type)
        if not engine_cls:
            logger.warning(f"[DownloadUtil] 등록되지 않은 다운로더 엔진 타입: '{e_type}'")
            return None

        try:
            return engine_cls(downloader_cfg)
        except Exception as e:
            logger.error(f"[DownloadUtil] 엔진 인스턴스 생성 오류 ({downloader_cfg.get('name')}): {e}")
            return None

    # 트랜스포터 관리
    @classmethod
    def load_transporters(cls):
        FeederUtil.ensure_custom_dirs()
        cls._transporter_classes = {
            LocalTransporter.TRANSPORTER_ID: LocalTransporter,
            RcloneSimpleTransporter.TRANSPORTER_ID: RcloneSimpleTransporter,
            GDrivePoolTransporter.TRANSPORTER_ID: GDrivePoolTransporter,
        }

        py_files = glob.glob(os.path.join(FeederUtil.TRANSPORTERS_DIR, "*.py"))
        for fpath in py_files:
            fname = os.path.basename(fpath)
            if fname.startswith("__") or fname.endswith("_worker.py") or "worker" in fname:
                continue
            if fname in ["trans_local.py", "trans_rclone.py", "trans_gdrive_pool.py"]:
                continue
            module_name = f"feeder_trans_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    obj = getattr(mod, attr_name)
                    if isinstance(obj, type) and issubclass(obj, BaseTransporter) and obj is not BaseTransporter:
                        t_id = getattr(obj, 'TRANSPORTER_ID', '').lower()
                        if t_id:
                            cls._transporter_classes[t_id] = obj
            except (Exception, SystemExit) as e:
                logger.error(f"[DownloadUtil] 이송 스크립트 로드 실패 ({fname}): {e}")

    @classmethod
    def get_transporter_schemas(cls) -> list[dict]:
        if not cls._transporter_classes:
            cls.load_transporters()

        schemas = []
        for t_id, t_cls in cls._transporter_classes.items():
            schemas.append({
                'transporter_id': t_id,
                'transporter_name': getattr(t_cls, 'TRANSPORTER_NAME', t_id),
                'config_schema': getattr(t_cls, 'CONFIG_SCHEMA', [])
            })
        return schemas

    @classmethod
    def get_transporter(cls, transporter_id: str) -> BaseTransporter | None:
        if not cls._transporter_classes:
            cls.load_transporters()

        t_cls = cls._transporter_classes.get(transporter_id.lower()) if transporter_id else None
        if not t_cls:
            logger.warning(f"[DownloadUtil] 등록되지 않은 이송 핸들러 타입: '{transporter_id}'")
            return None
        try:
            return t_cls()
        except Exception as e:
            logger.error(f"[DownloadUtil] 이송 핸들러 생성 오류 ({transporter_id}): {e}")
            return None
