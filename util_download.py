# -*- coding: utf-8 -*-

import os
import glob
import importlib.util

from .setup import *
from .util_feed import CUSTOM_DIR


class BaseDownloaderEngine:
    """모든 다운로더 엔진(AllDebrid, 115, qBit 등)이 구현해야 할 표준 인터페이스"""
    ENGINE_TYPE = "base"
    ENGINE_NAME = "base"

    def __init__(self, config: dict):
        self.config = config or {}
        self.name = self.config.get('name', self.ENGINE_NAME)
        self.enabled = bool(self.config.get('enabled', True))

    def add_magnet(self, link: str, title: str = None) -> tuple[bool, str, str]:
        """
        토렌트/마그넷 추가
        반환: (성공여부, 엔진 작업 ID, 오류메시지)
        """
        raise NotImplementedError

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        """
        다운로드 상태 목록 조회
        반환: (표준 작업상태 딕셔너리 리스트, 오류메시지)
        """
        raise NotImplementedError

    def delete_task(self, task_id: str) -> bool:
        """작업 삭제 및 취소"""
        raise NotImplementedError

    def restart_task(self, task_id: str) -> bool:
        """오류 또는 지연된 작업 재시작"""
        return False


class DownloaderManager:
    """data/db/feeder_custom/dl_*.py 스크립트를 동적으로 로드하고 엔진 인스턴스를 관리하는 매니저"""
    _engine_classes = {}

    @classmethod
    def load_engines(cls):
        cls._engine_classes = {}
        os.makedirs(CUSTOM_DIR, exist_ok=True)
        py_files = glob.glob(os.path.join(CUSTOM_DIR, "dl_*.py"))

        for fpath in py_files:
            fname = os.path.basename(fpath)
            module_name = f"feeder_custom_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    obj = getattr(mod, attr_name)
                    if (
                        isinstance(obj, type)
                        and issubclass(obj, BaseDownloaderEngine)
                        and obj is not BaseDownloaderEngine
                    ):
                        engine_type = getattr(obj, 'ENGINE_TYPE', '').lower()
                        if engine_type:
                            cls._engine_classes[engine_type] = obj
                            logger.debug(f"[DownloaderManager] 다운로더 플러그인 로드: '{engine_type}' ({fname})")
            except Exception as e:
                logger.error(f"[DownloaderManager] 다운로더 플러그인 로드 실패 ({fname}): {e}")

    @classmethod
    def get_engine_class(cls, engine_type: str):
        if not cls._engine_classes:
            cls.load_engines()
        return cls._engine_classes.get(engine_type.lower()) if engine_type else None

    @classmethod
    def create_instance(cls, downloader_cfg: dict) -> BaseDownloaderEngine | None:
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
