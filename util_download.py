# -*- coding: utf-8 -*-

import os
import glob
import shutil
import importlib.util

from .setup import *

# 커스텀 스크립트 격리 디렉터리 체계
CUSTOM_DIR = os.path.join(path_data, 'db', 'feeder_custom')
ENGINES_DIR = os.path.join(CUSTOM_DIR, 'engines')
TRANSPORTERS_DIR = os.path.join(CUSTOM_DIR, 'transporters')
SITES_DIR = os.path.join(CUSTOM_DIR, 'sites')


def ensure_custom_dirs():
    """하위 디렉터리 생성 및 기존 루트 파일 자동 마이그레이션"""
    for d in [CUSTOM_DIR, ENGINES_DIR, TRANSPORTERS_DIR, SITES_DIR]:
        os.makedirs(d, exist_ok=True)

    # 기존 루트에 있던 스크립트 자동 하위 이동 (하위 호환성 유지)
    try:
        for fpath in glob.glob(os.path.join(CUSTOM_DIR, "*.py")):
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue
            if fname.startswith("site_"):
                shutil.move(fpath, os.path.join(SITES_DIR, fname))
            elif fname.startswith(("engine_", "dl_")):
                shutil.move(fpath, os.path.join(ENGINES_DIR, fname))
            elif fname.startswith("trans_"):
                shutil.move(fpath, os.path.join(TRANSPORTERS_DIR, fname))
    except Exception as e:
        logger.debug(f"[DownloaderManager] 기존 파일 마이그레이션 예외: {e}")


ensure_custom_dirs()


class BaseDownloadEngine:
    """
    모든 다운로더 엔진(AllDebrid, 115, qBit, PikPak 등)이 상속받을 표준 인터페이스
    """
    ENGINE_ID = "base"
    ENGINE_NAME = "Base Engine"
    OUTPUT_TYPE = "local"  # "local" (로컬 파일/폴더) 또는 "remote_cloud" (ad:..., 115:... 형태)

    # UI 모달 폼을 동적으로 자동 생성하기 위한 설정 스키마 정의
    # 형식: [{"name": "변수명", "label": "라벨명", "type": "text|number|password|checkbox", "default": 기본값, "placeholder": 안내문구, "desc": 설명}]
    CONFIG_SCHEMA = []

    def __init__(self, config: dict):
        self.config = config or {}
        self.name = self.config.get('name', self.ENGINE_ID)
        self.enabled = bool(self.config.get('enabled', True))

    def add_magnet(self, link: str, title: str = None) -> tuple[bool, str, str]:
        """토렌트/마그넷 작업 추가 (성공여부, 엔진 작업 ID, 오류메시지 반환)"""
        raise NotImplementedError

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        """다운로드 상태 목록 조회 (표준 작업 딕셔너리 리스트, 오류메시지 반환)"""
        raise NotImplementedError

    def delete_task(self, task_id: str) -> bool:
        """엔진 작업 삭제 및 원격 캐시 정리"""
        raise NotImplementedError

    def restart_task(self, task_id: str) -> bool:
        """지연 또는 실패 작업 재시작"""
        return False

    def test_connection(self) -> tuple[bool, str]:
        """입력된 설정값으로 외부 API 통신 및 인증 상태 검증 (성공여부, 메시지 반환)"""
        return True, "연결 테스트가 정의되지 않은 엔진입니다."


class DownloaderManager:
    """data/db/feeder_custom/engines/*.py 스크립트를 동적으로 로드하고 관리"""
    _engine_classes = {}

    @classmethod
    def load_engines(cls):
        ensure_custom_dirs()
        cls._engine_classes = {}

        # engines 폴더 내 모든 파이썬 스크립트 스캔
        py_files = glob.glob(os.path.join(ENGINES_DIR, "*.py"))

        for fpath in py_files:
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue
            module_name = f"feeder_engine_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    obj = getattr(mod, attr_name)
                    if (
                        isinstance(obj, type)
                        and issubclass(obj, BaseDownloadEngine)
                        and obj is not BaseDownloadEngine
                    ):
                        e_id = getattr(obj, 'ENGINE_ID', '').lower()
                        if e_id:
                            cls._engine_classes[e_id] = obj
                            # logger.debug(f"[DownloaderManager] 다운로드 엔진 로드 완료: '{e_id}' ({fname})")
            except Exception as e:
                logger.error(f"[DownloaderManager] 다운로드 엔진 로드 실패 ({fname}): {e}")

    @classmethod
    def get_engine_schemas(cls) -> list[dict]:
        """UI 모달 동적 생성을 위해 등록된 모든 엔진의 식별자, 이름, 스키마 목록 반환"""
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
    def get_engine_class(cls, engine_id: str):
        if not cls._engine_classes:
            cls.load_engines()
        return cls._engine_classes.get(engine_id.lower()) if engine_id else None

    @classmethod
    def create_instance(cls, downloader_cfg: dict) -> BaseDownloadEngine | None:
        if not cls._engine_classes:
            cls.load_engines()

        e_type = downloader_cfg.get('engine_type', '').lower()
        engine_cls = cls._engine_classes.get(e_type)
        if not engine_cls:
            logger.warning(f"[DownloaderManager] 등록되지 않은 다운로더 엔진 타입: '{e_type}'")
            return None

        try:
            return engine_cls(downloader_cfg)
        except Exception as e:
            logger.error(f"[DownloaderManager] 다운로더 인스턴스 생성 오류 ({downloader_cfg.get('name')}): {e}")
            return None


class BaseTransporter:
    """
    모든 이송/후처리 핸들러(로컬 보관, Rclone 단순 전송, GDrive SA 풀, 코랩 릴레이 등)의 표준 인터페이스
    """
    TRANSPORTER_ID = "base"
    TRANSPORTER_NAME = "Base Transporter"

    # 프로필의 목적지 설정 모달을 동적으로 자동 렌더링하기 위한 스키마 정의
    # 형식: [{"name": "변수명", "label": "라벨명", "type": "text|number|password|checkbox", "default": 기본값, "placeholder": 안내, "desc": 설명}]
    CONFIG_SCHEMA = []

    def __init__(self, config: dict = None):
        self.config = config or {}

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        """
        이송/후처리 실행
        매개변수:
          - item: ModelDownload DB 엔티티
          - source_path: 수득 엔진이 넘겨준 산출물 경로 (로컬 경로 또는 ad:magnets/... 등 원격 경로)
          - dest_config: 프로필에 지정된 목적지 상세 설정 딕셔너리
        반환:
          (성공여부: bool, 전이할_DB상태: str ('completed', 'pending_colab', 'failed' 등), 메시지/오류: str)
        """
        raise NotImplementedError


class TransporterManager:
    """data/db/feeder_custom/transporters/*.py 스크립트를 동적으로 로드하고 관리"""
    _transporter_classes = {}

    @classmethod
    def load_transporters(cls):
        ensure_custom_dirs()
        cls._transporter_classes = {}

        py_files = glob.glob(os.path.join(TRANSPORTERS_DIR, "*.py"))
        for fpath in py_files:
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue
            module_name = f"feeder_trans_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    obj = getattr(mod, attr_name)
                    if (
                        isinstance(obj, type)
                        and issubclass(obj, BaseTransporter)
                        and obj is not BaseTransporter
                    ):
                        t_id = getattr(obj, 'TRANSPORTER_ID', '').lower()
                        if t_id:
                            cls._transporter_classes[t_id] = obj
                            # logger.debug(f"[TransporterManager] 이송 핸들러 로드 완료: '{t_id}' ({fname})")
            except Exception as e:
                logger.error(f"[TransporterManager] 이송 핸들러 로드 실패 ({fname}): {e}")

    @classmethod
    def get_transporter_schemas(cls) -> list[dict]:
        """프로필 설정 UI의 목적지 드롭다운 및 동적 필드 구성을 위한 스키마 목록 반환"""
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
            logger.warning(f"[TransporterManager] 등록되지 않은 이송 핸들러 타입: '{transporter_id}'")
            return None
        try:
            return t_cls()
        except Exception as e:
            logger.error(f"[TransporterManager] 이송 핸들러 인스턴스 생성 오류 ({transporter_id}): {e}")
            return None
