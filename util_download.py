# -*- coding: utf-8 -*-
import os
import glob
import shutil
import requests
import subprocess
import importlib.util
import inspect

import sys

from .setup import *
from .util_base import FeederUtil
from .util_upload import UploadUtil

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
if PLUGIN_DIR not in sys.path:
    sys.path.insert(0, PLUGIN_DIR)

# CD2 gRPC 모듈 안전 로드
CD2_GRPC_AVAILABLE = False
CloudDrive_pb2 = None
CloudDrive_pb2_grpc = None

# Protobuf Gencode/Runtime 버전 불일치 강제 검증(VersionError) 완화 패치
try:
    import google.protobuf.runtime_version as _proto_runtime
    _proto_runtime.ValidateProtobufRuntimeVersion = lambda *args, **kwargs: None
except Exception:
    pass

try:
    import grpc
    import clouddrive_pb2 as CloudDrive_pb2
    import clouddrive_pb2_grpc as CloudDrive_pb2_grpc
    CD2_GRPC_AVAILABLE = True
except Exception:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "grpcio", "protobuf"])
        import grpc
        import clouddrive_pb2 as CloudDrive_pb2
        import clouddrive_pb2_grpc as CloudDrive_pb2_grpc
        CD2_GRPC_AVAILABLE = True
    except Exception as ex:
        logger.debug(f"[DownloadUtil] CD2 gRPC 모듈 대기 (미설치/파일 없음): {ex}")
        CD2_GRPC_AVAILABLE = False


# ==============================================================================
# 커스텀 확장용 베이스 인터페이스 (Base Interfaces)
# ==============================================================================
class BaseDownloadEngine:
    """모든 다운로더 엔진의 표준 인터페이스"""
    ENGINE_ID = "base"
    status_list_complete = True
    ENGINE_NAME = "Base Engine"
    OUTPUT_TYPE = "local"
    SUPPORTED_PROTOCOLS = ["magnet"]
    CONFIG_SCHEMA = []

    def __init__(self, config: dict):
        self.config = config or {}
        self.name = self.config.get('name', self.ENGINE_ID)
        self.enabled = bool(self.config.get('enabled', True))

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        raise NotImplementedError

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        raise NotImplementedError

    def delete_task(self, task_id: str) -> bool:
        raise NotImplementedError

    def delete_tasks(self, task_ids: list[str]) -> bool:
        """여러 작업을 일괄 삭제 (기본 구현: delete_task 순회 호출)"""
        if not task_ids:
            return True
        success = True
        for tid in task_ids:
            FeederUtil.cooperate()
            if not self.delete_task(tid):
                success = False
        return success

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

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        """qBittorrent에 마그넷 등록 (동적 하위 저장 경로 지원)"""
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

        # 기본 저장 경로와 프로필 지정 하위 수신/완료 경로 결합
        effective_save_path = self.save_path
        if upload_path and str(upload_path).strip():
            clean_sub = str(upload_path).strip().strip('/\\')
            if effective_save_path:
                effective_save_path = os.path.join(effective_save_path, clean_sub)
            else:
                effective_save_path = clean_sub

        if effective_save_path:
            payload['savepath'] = effective_save_path

        info_hash = FeederUtil.extract_info_hash(link)

        try:
            res = s.post(add_url, data=payload, timeout=15)
            if res.status_code in [200, 204]:
                task_id = info_hash or link[:40]
                logger.info(f"[QBittorrentEngine] 작업 추가 성공: {task_id} ({title or link[:30]}) -> 저장경로: {effective_save_path or '기본'}")
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
                FeederUtil.cooperate()
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

                progress_val = round(float(t.get('progress', 0.0)) * 100, 1)
                ret.append({
                    'task_id': t_hash,
                    'hash': t_hash,
                    'status': std_status,
                    'filename': fname,
                    'file_size': t.get('total_size', 0),
                    'downloaded_bytes': t.get('completed', 0),
                    'download_speed': t.get('dlspeed', 0),
                    'progress': progress_val,
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


class CloudDrive2Engine(BaseDownloadEngine):
    """CloudDrive2 (CD2 gRPC 연동) 공식 내장 다운로더 엔진"""
    ENGINE_ID = "cd2"
    ENGINE_NAME = "CloudDrive2 (CD2)"
    OUTPUT_TYPE = "cloud_storage"
    SUPPORTED_PROTOCOLS = ["magnet", "ed2k"]

    CONFIG_SCHEMA = [
        {"name": "cd2_addr", "label": "CD2 주소", "type": "text", "default": "127.0.0.1", "required": True},
        {"name": "cd2_port", "label": "CD2 포트 (gRPC)", "type": "number", "default": 19798},
        {"name": "cd2_token", "label": "CD2 인증 토큰", "type": "password", "placeholder": "CloudDrive2 JWT 토큰"},
        {"name": "cd2_mount_path", "label": "CD2 로컬 마운트 경로", "type": "text", "placeholder": "예: /mnt/cd2", "desc": "로컬 시스템에 CD2가 마운트된 FUSE 루트 디렉터리 (예: /mnt/cd2)"},
        {"name": "cd2_virtual_path", "label": "마그넷 저장 가상 경로", "type": "text", "default": "115open/云下载", "placeholder": "예: 115open/云下载", "desc": "CD2 내부에서 오프라인 다운로드가 저장될 클라우드 상대 경로. 로컬 실물 탐색 시 [마운트 경로] + [가상 경로]가 자동 결합됩니다."},
        {"name": "use_local_staging", "label": "로컬 스테이징 기본 사용", "type": "checkbox", "default": False, "desc": "CD2 마운트는 기본적으로 직접 전송합니다. 다운로드 프로필에서 명시하면 프로필 설정이 우선합니다."},
        {"name": "cd2_max_pages", "label": "작업 목록 최대 탐색 페이지 수", "type": "number", "default": 1, "desc": "오프라인 작업 목록 조회 시 최대 탐색할 페이지 수 (기본값: 1페이지)"},
        {"name": "cd2_timeout", "label": "gRPC 타임아웃 (초)", "type": "number", "default": 30, "desc": "gRPC 요청 제한 시간(초) (기본값: 30초)"},
        {"name": "stalled_timeout_hours", "label": "지연 타임아웃 (시간)", "type": "number", "default": 24, "desc": "해당 시간 동안 미완료 시 다음 엔진으로 자동 폴백 (0 입력 시 무제한 대기)"}
    ]

    def __init__(self, config: dict):
        super(CloudDrive2Engine, self).__init__(config)
        self.addr = self.config.get('cd2_addr', '127.0.0.1')
        self.port = int(self.config.get('cd2_port', 19798))
        self.token = self.config.get('cd2_token', '').strip()
        raw_vpath = str(self.config.get('cd2_virtual_path') or '').strip()
        self.vpath = ("/" + raw_vpath.strip('/')) if raw_vpath else ""
        self.mount_path = str(self.config.get('cd2_mount_path') or '').strip().rstrip('/\\')

        try:
            self.max_pages = int(self.config.get('cd2_max_pages') or 1)
        except Exception:
            self.max_pages = 1
        try:
            self.grpc_timeout = int(self.config.get('cd2_timeout') or 30)
        except Exception:
            self.grpc_timeout = 30

    def get_local_mount_path(self) -> str:
        """로컬 파일시스템에서 마그넷 다운로드 폴더의 실제 절대 경로 반환 (자동 결합)"""
        if not self.mount_path:
            return ''
        if not self.vpath:
            return self.mount_path
        rel_vpath = self.vpath.strip('/')
        if self.mount_path.replace('\\', '/').endswith(rel_vpath):
            return self.mount_path
        return os.path.join(self.mount_path, rel_vpath)

    def _get_grpc_stub(self):
        """gRPC 채널 재사용 및 스텁 생성 (소켓 낭비 방지)"""
        if not CD2_GRPC_AVAILABLE or not CloudDrive_pb2_grpc:
            return None, None
        if not hasattr(self, '_channel') or self._channel is None:
            self._channel = grpc.insecure_channel(f"{self.addr}:{self.port}")
            self._stub = CloudDrive_pb2_grpc.CloudDriveFileSrvStub(self._channel)
        metadata = [('authorization', f"Bearer {self.token}")] if self.token else []
        return self._stub, metadata

    def refresh_cache(self, path: str = None) -> list[str]:
        """CD2 가상경로 디렉터리 캐시 갱신"""
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return []

        target_path = "/" + (path or self.vpath or '').strip().strip('/')
        # 대상 경로가 비어있거나 전체 루트('/')인 경우 CD2 전체 동기화 방지를 위해 스킵
        if not target_path or target_path == "/":
            return []

        timeout_sec = min(20, max(10, self.grpc_timeout))
        found_subfiles = []

        try:
            req = CloudDrive_pb2.ListSubFileRequest(path=target_path, forceRefresh=True)
            response_stream = stub.GetSubFiles(req, metadata=metadata, timeout=timeout_sec)

            for response in response_stream:
                FeederUtil.cooperate()
                if hasattr(response, 'subFiles') and response.subFiles:
                    for item in response.subFiles:
                        FeederUtil.cooperate()
                        fname = getattr(item, 'name', None)
                        if fname:
                            found_subfiles.append(fname)

            if not found_subfiles:
                logger.warning(f"[CloudDrive2Engine] CD2 조회 결과가 비어 있습니다: {target_path}")
            else:
                logger.info(f"[CloudDrive2Engine] CD2 캐시 갱신 완료: {target_path} (총 {len(found_subfiles)}개 요소 발견)")
            return found_subfiles

        except grpc.RpcError as e:
            logger.warning(f"[CloudDrive2Engine] CD2 gRPC 갱신 실패/타임아웃 ({timeout_sec}s): 코드={e.code()} | 상세={e.details()}")
            return []
        except Exception as e:
            logger.error(f"[CloudDrive2Engine] CD2 처리 중 예외: {e}")
            return []

    def add_magnet(self, link: str, title: str = None, upload_path: str = None) -> tuple[bool, str, str]:
        """CD2 gRPC를 통한 오프라인 다운로드 추가 (수신 경로 하위 폴더 자동 생성 및 결합)"""
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return False, "", "CD2 gRPC 모듈(clouddrive_pb2)이 준비되지 않았습니다."

        try:
            target_vpath = self.vpath
            if upload_path and str(upload_path).strip():
                clean_sub = str(upload_path).strip().strip('/')
                target_vpath = f"{self.vpath}/{clean_sub}"

            try:
                parent_dir = os.path.dirname(target_vpath)
                folder_name = os.path.basename(target_vpath)
                req_cf = CloudDrive_pb2.CreateFolderRequest(parentPath=parent_dir, folderName=folder_name)
                stub.CreateFolder(req_cf, metadata=metadata, timeout=5)
            except Exception:
                pass

            req = CloudDrive_pb2.AddOfflineFileRequest(urls=link, toFolder=target_vpath)
            res = stub.AddOfflineFiles(req, metadata=metadata, timeout=20)

            info_hash = FeederUtil.extract_info_hash(link)
            task_id = info_hash or link[:40]

            if hasattr(res, 'success') and not res.success:
                err_msg = getattr(res, 'errorMessage', 'API Task Add Failed')
                if "10008" in err_msg or "任务已存在" in err_msg:
                    logger.info(f"[CloudDrive2Engine] CD2에 이미 존재하는 작업입니다 (정상 수용): {title or link[:30]}")
                    return True, task_id, ""
                return False, "", f"CD2 작업 추가 실패: {err_msg}"

            logger.info(f"[CloudDrive2Engine] CD2 작업 추가 성공: {title or link[:30]}")
            return True, task_id, ""

        except Exception as e:
            err_str = str(e)
            if "10008" in err_str or "任务已存在" in err_str:
                logger.info(f"[CloudDrive2Engine] CD2에 이미 존재하는 작업입니다 (정상 수용): {title or link[:30]}")
                return True, link[:40], ""
            logger.error(f"[CloudDrive2Engine] CD2 gRPC 추가 예외: {err_str}")
            return False, "", f"CD2 gRPC 예외: {err_str}"

    def get_status(self, task_ids: list[str] = None) -> tuple[list[dict], str]:
        """CD2 gRPC 오프라인 작업 목록 조회 및 로컬 마운트 경로 매핑"""
        self.status_list_complete = False
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return [], "CD2 gRPC 모듈 미로드"

        try:
            ret = []
            max_pages = max(1, self.max_pages)
            for page_num in range(1, max_pages + 1):
                FeederUtil.cooperate()
                try:
                    req = CloudDrive_pb2.OfflineFileListAllRequest(path=self.vpath, page=page_num)
                    res = stub.ListAllOfflineFiles(req, metadata=metadata, timeout=self.grpc_timeout)
                    if not res.offlineFiles:
                        self.status_list_complete = True
                        break

                    def _get_pb_attr(pb_obj, *attr_names, default=None):
                        for name in attr_names:
                            FeederUtil.cooperate()
                            if hasattr(pb_obj, name):
                                val = getattr(pb_obj, name)
                                if val is not None:
                                    return val
                        return default

                    for t in res.offlineFiles:
                        FeederUtil.cooperate()
                        infohash = _get_pb_attr(t, 'infoHash', 'info_hash', 'infohash', 'hash', default='')
                        infohash = str(infohash).strip().lower()
                        original_url = getattr(t, 'url', '')
                        if not infohash and original_url:
                            infohash = (FeederUtil.extract_info_hash(original_url) or '').lower()

                        fname = _get_pb_attr(t, 'name', 'fileName', 'file_name', default='')
                        fsize = int(_get_pb_attr(t, 'size', 'totalSize', 'total_size', default=0) or 0)
                        status_code = int(_get_pb_attr(t, 'status', default=0) or 0)

                        # CD2 작업 상태 코드: 2 완료(FINISHED), 3 에러(ERROR), 그 외 다운로드 중
                        if status_code == 2:
                            std_status = 'completed'
                            progress_val = 100.0
                            dl_bytes = fsize
                        elif status_code == 3:
                            std_status = 'error'
                            progress_val = 0.0
                            dl_bytes = 0
                        else:
                            std_status = 'downloading'
                            raw_pct = _get_pb_attr(t, 'percent', 'percentage', 'progress', 'percentDone', default=None)
                            if raw_pct is not None:
                                pct = float(raw_pct)
                                progress_val = round(pct * 100.0, 1) if (0.0 < pct <= 1.0) else round(pct, 1)
                            else:
                                progress_val = 0.0

                            raw_bytes = _get_pb_attr(t, 'downloadedBytes', 'downloaded_bytes', 'downloaded', 'downloadedSize', default=None)
                            if raw_bytes is not None:
                                dl_bytes = int(raw_bytes)
                            elif fsize > 0 and progress_val > 0:
                                dl_bytes = int(fsize * (progress_val / 100.0))
                            else:
                                dl_bytes = 0

                        raw_speed = _get_pb_attr(t, 'speed', 'downloadSpeed', 'download_speed', 'rate', default=0)
                        dl_speed = int(raw_speed) if raw_speed else 0

                        ret.append({
                            'task_id': infohash or original_url[:40],
                            'hash': infohash,
                            'status': std_status,
                            'statusCode': status_code,
                            'filename': fname,
                            'file_size': fsize,
                            'downloaded_bytes': dl_bytes,
                            'download_speed': dl_speed,
                            'progress': progress_val,
                            'source_path': ''
                        })

                    page_count = getattr(res, 'pageCount', 0)
                    if page_count > 0 and page_num >= page_count:
                        self.status_list_complete = True
                        break
                except Exception as page_err:
                    logger.debug(f"[CloudDrive2Engine] 오프라인 작업 목록 조회 예외 ({page_num}p): {page_err}")
                    return ret, f'CD2 작업 목록 조회 실패 ({page_num}p): {page_err}'

            return ret, ""

        except Exception as e:
            return [], f"CD2 gRPC 상태 조회 예외: {str(e)}"

    def delete_task(self, task_id: str) -> bool:
        """CD2 오프라인 다운로드 히스토리에서 해당 작업 삭제"""
        return self.delete_tasks([task_id])

    def delete_tasks(self, task_ids: list[str]) -> bool:
        """CD2 오프라인 다운로드 히스토리에서 작업들 일괄 삭제 (115 실물 파일은 안전하게 보존)"""
        if not task_ids:
            return True
        stub, metadata = self._get_grpc_stub()
        if not stub:
            return False

        # stub에서 오프라인 삭제 RPC 메서드 탐색
        method_name = None
        for name in ['DeleteOfflineFiles', 'RemoveOfflineFiles', 'DeleteOfflineFile', 'RemoveOfflineFile', 'CancelOfflineFiles']:
            FeederUtil.cooperate()
            if hasattr(stub, name):
                method_name = name
                break

        if not method_name:
            logger.debug(f"[CloudDrive2Engine] CD2 gRPC 삭제 메서드 부재: {[m for m in dir(stub) if 'offline' in m.lower()]}")
            return False

        # CloudDrive_pb2에서 요청 메시지 클래스 탐색
        req_cls = None
        for name in [f"{method_name}Request", 'DeleteOfflineFilesRequest', 'RemoveOfflineFilesRequest', 'DeleteOfflineFileRequest']:
            FeederUtil.cooperate()
            if hasattr(CloudDrive_pb2, name):
                req_cls = getattr(CloudDrive_pb2, name)
                break

        if not req_cls:
            logger.debug("[CloudDrive2Engine] CD2 gRPC 삭제 요청 클래스 부재")
            return False

        # 요청 파라미터 동적 구성 (115 원본 파일 삭제 방지를 위해 deleteFiles는 무조건 False 지정)
        field_names = req_cls.DESCRIPTOR.fields_by_name.keys()
        kwargs = {}
        if 'path' in field_names:
            kwargs['path'] = self.vpath
        if 'deleteFiles' in field_names:
            kwargs['deleteFiles'] = False
        elif 'delete_files' in field_names:
            kwargs['delete_files'] = False

        # 40자리 infoHash 및 식별자 추출
        hashes = [tid.lower() for tid in task_ids if len(tid) == 40]
        if not hashes:
            hashes = task_ids

        if 'infoHashes' in field_names:
            kwargs['infoHashes'] = hashes
        elif 'info_hashes' in field_names:
            kwargs['info_hashes'] = hashes
        elif 'infoHash' in field_names and len(hashes) == 1:
            kwargs['infoHash'] = hashes[0]
        elif 'taskIds' in field_names:
            kwargs['taskIds'] = task_ids
        elif 'urls' in field_names:
            kwargs['urls'] = task_ids

        try:
            req = req_cls(**kwargs)
            method = getattr(stub, method_name)
            method(req, metadata=metadata, timeout=15)
            logger.info(f"[CloudDrive2Engine] CD2 오프라인 히스토리 레코드 삭제 완료: {len(task_ids)}건")
            return True
        except Exception as ex:
            logger.warning(f"[CloudDrive2Engine] CD2 오프라인 히스토리 삭제 실패 ({len(task_ids)}건): {ex}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        """CloudDrive2 gRPC 통신 및 토큰 유효성 검증"""
        if not CD2_GRPC_AVAILABLE or not CloudDrive_pb2:
            return False, "clouddrive_pb2 모듈 또는 grpc 패키지가 준비되지 않았습니다."

        stub, metadata = self._get_grpc_stub()
        if not stub:
            return False, "gRPC 스텁 생성 실패"

        try:
            req = CloudDrive_pb2.OfflineFileListAllRequest(path=self.vpath, page=1)
            res = stub.ListAllOfflineFiles(req, metadata=metadata, timeout=10)
            task_count = len(res.offlineFiles) if res.offlineFiles else 0
            return True, f"CloudDrive2 (CD2) gRPC 연결 성공! (오프라인 작업 {task_count}개 감지됨)"
        except grpc.RpcError as rpc_err:
            code = rpc_err.code()
            details = rpc_err.details()
            return False, f"CD2 gRPC 인증/연결 실패: [{code.name}] {details}"
        except Exception as e:
            return False, f"CD2 gRPC 통신 예외: {str(e)}"


class LocalTransporter(BaseTransporter):
    TRANSPORTER_ID = "local"
    TRANSPORTER_NAME = "단순 경로 이동"

    CONFIG_SCHEMA = [
        {"name": "complete_path", "label": "최종 완료 이동 경로", "type": "text", "placeholder": "예: /mnt/mergerfs/uploads 또는 uploads/프로필명", "desc": "'/'로 시작하면 시스템 절대경로(mergerfs 마운트 등)로 직접 이동하며, 상대경로로 입력하면 엔진 설정 루트 하위 경로로 자동 결합됩니다."}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        target_folder = (dest_config.get('complete_path') or dest_config.get('target_folder') or '').strip()
        folder_name = getattr(item, 'output_folder_name', None) or item.file_name or os.path.basename(source_path or '')
        if source_path and os.path.isfile(source_path) and getattr(item, 'output_folder_name', None):
            folder_name += os.path.splitext(os.path.basename(source_path))[1]

        if not source_path or not os.path.exists(source_path):
            logger.error(f"[LocalTransporter] 로컬 소스 경로가 존재하지 않음: {source_path}")
            return False, "failed", f"로컬 파일 없음: {source_path}"

        final_path = source_path
        if not target_folder and getattr(item, 'output_folder_name', None):
            target_folder = os.path.dirname(os.path.abspath(source_path))
        if target_folder:
            try:
                os.makedirs(target_folder, exist_ok=True)
                dest_file = os.path.join(target_folder, folder_name)
                if os.path.exists(dest_file) and os.path.abspath(source_path) != os.path.abspath(dest_file):
                    short_hash = (item.infohash[:6] if item.infohash else f"id_{item.id}")
                    if os.path.isdir(source_path):
                        candidate_name = f"{folder_name}_[{short_hash}]"
                    else:
                        n, e = os.path.splitext(folder_name)
                        candidate_name = f"{n}_{short_hash}{e}"
                    dest_file = os.path.join(target_folder, candidate_name)

                    # 동일 hash의 기존 항목까지 있으면 추가 suffix를 붙여 덮어쓰기를 방지한다.
                    suffix = 2
                    while os.path.exists(dest_file):
                        if os.path.isdir(source_path):
                            candidate_name = f"{folder_name}_[{short_hash}]_{suffix}"
                        else:
                            n, e = os.path.splitext(folder_name)
                            candidate_name = f"{n}_{short_hash}_{suffix}{e}"
                        dest_file = os.path.join(target_folder, candidate_name)
                        suffix += 1

                if os.path.abspath(source_path) == os.path.abspath(dest_file):
                    final_path = dest_file
                else:
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
        {"name": "use_local_staging", "label": "로컬 스테이징 사용", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "사용"}, {"value": "false", "label": "사용 안 함"}],
         "desc": "사용: 원격 소스를 로컬 임시 경로에 먼저 수신합니다. 사용 안 함: 로컬 디스크를 거치지 않고 직접 전송합니다."},
        {"name": "staging_path", "label": "임시 수신 경로", "type": "text", "placeholder": "예: profile-name", "desc": "전역 로컬 스테이징 루트 아래에 생성할 작업별 임시 수신 경로입니다."},
        {"name": "upload_path", "label": "임시 업로드 경로", "type": "text", "placeholder": "예: incoming/프로필명", "desc": "Rclone 리모트 안에서 업로드가 먼저 완료되는 임시 경로입니다."},
        {"name": "complete_path", "label": "최종 완료 이동 경로", "type": "text", "placeholder": "예: uploads/프로필명", "desc": "임시 업로드가 성공한 뒤 최종적으로 이동할 Rclone 리모트 경로입니다."},
        {"name": "remote_name", "label": "일반 업로드 리모트명", "type": "text", "placeholder": "미입력 시 기본 설정값 사용", "desc": "비워두면 기본 설정의 일반 업로드 리모트를 사용합니다."}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        upload_path = (dest_config.get('upload_path') or '').strip().strip('/')
        complete_path = (dest_config.get('complete_path') or '').strip().strip('/')
        if not upload_path or not complete_path:
            return False, "failed", "Rclone 임시 업로드 경로(upload_path)와 최종 완료 이동 경로(complete_path)가 모두 필요합니다."

        is_remote_source = bool(source_path and not os.path.exists(source_path) and (':' in source_path or source_path.startswith(('http://', 'https://'))))
        if not source_path or (not is_remote_source and not os.path.exists(source_path)):
            return False, "failed", f"업로드 대상 소스 파일 없음: {source_path}"

        rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        rclone_cfg = FeederUtil.load_yaml().get('rclone', {})
        remote_name = (
            dest_config.get('remote_name')
            or P.ModelSetting.get('download_rclone_upload_remote_name')
            or P.ModelSetting.get('download_rclone_remote_name')
            or rclone_cfg.get('upload_remote_name')
            or rclone_cfg.get('remote_name')
            or ''
        ).strip()
        if not remote_name:
            return False, "failed", "일반 업로드 Rclone 리모트명이 설정되지 않았습니다."
        raw_name = item.file_name or os.path.basename(source_path)
        base_name, ext = os.path.splitext(raw_name)
        folder_name = getattr(item, 'output_folder_name', None) or (base_name if DownloadUtil.is_single_file(raw_name, source_path) else raw_name)
        temp_full = f"{remote_name}:{upload_path}/{folder_name}"
        final_full = f"{remote_name}:{complete_path}/{folder_name}"
        chunk_size = P.ModelSetting.get('download_rclone_chunk_size') or '256M'

        cmd = [
            "rclone", "copy", source_path, temp_full,
            "--stats", "10s", "--stats-one-line", "--log-level", "NOTICE",
            "--drive-chunk-size", chunk_size
        ]
        if rclone_conf:
            cmd.extend(["--config", rclone_conf])

        cmd.extend(FeederUtil.get_rclone_extra_options())

        logger.info(f"[RcloneTransporter] 임시 업로드 시작 ({'원격 다이렉트' if is_remote_source else '로컬'}): {folder_name} -> {temp_full}")
        try:
            from .util_upload import UploadUtil
            from .util_block import DownloadBlock
            target = db.session.get(ModelDownload, item.id)
            if not target or DownloadBlock.stopped(item.id):
                return False, 'blocked', '차단 또는 취소된 작업입니다.'
            target.upload_state = dict(target.upload_state or {}, incoming=temp_full,
                dest_parent=f'{remote_name}:{complete_path}', folder_name=folder_name,
                rclone_conf=rclone_conf, impersonate_arg=[])
            db.session.commit()
            db.session.remove()
            success, output = UploadUtil.run_rclone(cmd, f'업로드 {folder_name}',
                item_id=item.id, execution_timeout=3600)
            if success:
                move_cmd = ["rclone", "move", temp_full, final_full, "--delete-empty-src-dirs"]
                if rclone_conf:
                    move_cmd.extend(["--config", rclone_conf])
                move_cmd.extend(FeederUtil.get_rclone_extra_options())
                moved, output = UploadUtil.run_rclone(move_cmd, f'최종 이동 {folder_name}',
                    item_id=item.id, execution_timeout=3600)
                if not moved:
                    err = output.strip() or "최종 완료 경로 이동 실패"
                    logger.error(f"[RcloneTransporter] 최종 이동 오류: {err}")
                    return False, "failed", f"Rclone 최종 이동 실패: {err[:120]}"

                logger.info(f"[RcloneTransporter] 최종 이동 완료: {final_full}")
                if DownloadBlock.stopped(item.id):
                    return False, 'blocked', '차단 또는 취소된 작업입니다.'
                if is_remote_source:
                    downloader_cfg = FeederUtil.get_downloader_by_name(item.current_engine_name)
                    if downloader_cfg and item.engine_task_id:
                        try:
                            engine = DownloadUtil.create_engine(downloader_cfg)
                            if engine:
                                engine.delete_task(item.engine_task_id)
                        except Exception:
                            pass
                else:
                    if os.path.isdir(source_path):
                        shutil.rmtree(source_path, ignore_errors=True)
                    elif os.path.isfile(source_path):
                        os.remove(source_path)
                return True, "completed", f"Rclone 업로드 및 최종 이동 완료 ({final_full})"
            else:
                err = output.strip() or "알 수 없는 Rclone 오류"
                logger.error(f"[RcloneTransporter] Rclone 오류: {err}")
                return False, "failed", f"Rclone 실패: {err[:120]}"
        except Exception as ex:
            logger.error(f"[RcloneTransporter] Rclone 실행 예외: {ex}")
            return False, "failed", f"Rclone 실행 예외: {str(ex)}"


class GDrivePoolTransporter(BaseTransporter):
    TRANSPORTER_ID = "gdrive_rotation"
    TRANSPORTER_NAME = "Google Drive 계정 풀 로테이션 (MyDrive 경유용)"

    CONFIG_SCHEMA = [
        {"name": "use_impersonate", "label": "Google Workspace 위임", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "위임 사용"}, {"value": "false", "label": "위임 사용 안 함"}],
         "desc": "계정 풀 사용 시 적용합니다. 위임 사용: SA 리모트에서 계정 이메일로 위임합니다. 사용 안 함: 계정별 독립 리모트를 사용합니다."},
        {"name": "use_local_staging", "label": "로컬 스테이징 사용", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "사용"}, {"value": "false", "label": "사용 안 함"}],
         "desc": "사용: 원격 소스를 로컬 임시 경로에 먼저 수신합니다. 사용 안 함: 로컬 디스크를 거치지 않고 직접 전송합니다."},
        {"name": "staging_path", "label": "임시 수신 경로", "type": "text", "placeholder": "예: profile-name", "desc": "전역 로컬 스테이징 루트 아래에 생성할 작업별 임시 수신 경로입니다."},
        {"name": "upload_path", "label": "임시 업로드 경로", "type": "text", "placeholder": "예: incoming/프로필명", "desc": "Google Drive에서 업로드가 먼저 완료되는 임시 경로입니다."},
        {"name": "mydrive_remote_name", "label": "내 드라이브 SA 리모트명", "type": "text", "placeholder": "미입력 시 기본 설정값 사용"},
        {"name": "shared_remote_name", "label": "목적지 공유 드라이브 리모트명", "type": "text", "placeholder": "미입력 시 기본 설정값 사용"},
        {"name": "shared_drive_id", "label": "공유 드라이브 ID", "type": "text", "placeholder": "미입력 시 기본 설정값 사용", "desc": "최종 라이브러리가 위치할 대상 공유 드라이브 ID"},
        {"name": "complete_path", "label": "최종 완료 이동 경로", "type": "text", "placeholder": "예: uploads/프로필명", "desc": "지정한 공유 드라이브 내 최종 보관 폴더 경로"}
    ]

    def transport(self, item, source_path: str, dest_config: dict) -> tuple[bool, str, str]:
        item.destination_type = "gdrive_rotation"
        item.gdrive_upload_path = dest_config.get('upload_path', 'incoming/default')
        item.gdrive_complete_path = dest_config.get('complete_path', 'uploads/default')
        item.gdrive_remote_id = dest_config.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
        item.local_path = source_path
        item.status = "pending_upload"
        return True, "pending_upload", "구글 드라이브 업로드 대기열 등록 완료"


# ==============================================================================
# 단일 통합 관리자: DownloadUtil
# ==============================================================================
class RemoteWorkerTransporter(BaseTransporter):
    TRANSPORTER_ID = 'remote_worker'
    TRANSPORTER_NAME = '원격 워커'
    FORCE_REMOTE_SOURCE = True
    CONFIG_SCHEMA = [
        {"name": "use_gdrive_pool", "label": "계정 풀 로테이션", "type": "radio", "allow_inherit": True,
         "options": [{"value": "", "label": "상위 설정 사용"}, {"value": "true", "label": "계정 풀 사용"}, {"value": "false", "label": "기본 리모트 사용"}],
         "desc": "계정 풀을 사용하면 등록된 계정을 순환합니다. 계정 접근 방식은 아래 위임 설정에서 선택합니다."},
        dict(GDrivePoolTransporter.CONFIG_SCHEMA[0], depends_on='use_gdrive_pool'),
        {'name': 'remote_name', 'label': 'Drive 리모트명', 'type': 'text', 'placeholder': '비워두면 기본 업로드 리모트를 사용합니다',
         'desc': '기본 리모트 방식에서 사용하며, 비워두면 전역 기본 업로드 리모트를 사용합니다. Feeder가 작업별로 리모트명을 전달하므로 원격 서버와 FF의 Rclone에 같은 이름으로 설정하세요.'},
        {'name': 'mydrive_remote_name', 'label': '위임용 SA 리모트명', 'type': 'text',
         'placeholder': '비워두면 Drive 리모트명 또는 전역 위임용 리모트를 사용합니다',
         'desc': 'Workspace Impersonate 방식에서 사용합니다. 비우면 위 Drive 리모트명 또는 전역 위임용 리모트를 사용합니다. 워커와 FF 양쪽에 동일하게 설정하세요.'},
        {'name': 'shared_drive_id', 'label': '목적지 드라이브 ID', 'type': 'text',
         'desc': '미입력 시 기본 목적지 공유 드라이브 ID를 사용합니다.'},
        {'name': 'upload_path', 'label': '임시 업로드 경로', 'type': 'text', 'placeholder': '예: incoming/프로필명',
         'desc': '설정한 리모트 내 임시 업로드 경로를 사용합니다. 로컬 스테이징을 사용하지 않습니다.'},
        {'name': 'complete_path', 'label': '최종 완료 경로', 'type': 'text', 'placeholder': 'uploads/my-profile',
         'desc': '원격 서버에서 복사를 완료한 뒤 Feeder가 서버 측 폴더 이동으로 정리합니다.'},
    ]

    def transport(self, item, source_path, dest_config):
        item.destination_type = self.TRANSPORTER_ID
        item.local_path = source_path
        return True, 'pending_upload', '원격 워커 업로드 대기열에 등록했습니다.'


class DownloadUtil:
    @staticmethod
    def is_single_file(name: str, source_path: str = '') -> bool:
        if source_path and os.path.exists(source_path):
            return os.path.isfile(source_path)
        return os.path.splitext(name)[1].lower() in {
            '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.ts', '.m2ts', '.webm',
            '.mp3', '.flac', '.zip', '.rar', '.7z', '.torrent', '.iso', '.srt', '.ass'
        }

    @classmethod
    def reserve_output_folder(cls, item, source_name: str = '') -> str:
        """실물 존재 여부와 무관하게 DB의 이전 이름 이력을 기준으로 출력 폴더명을 확정한다."""
        if item.output_folder_name:
            return item.output_folder_name
        from .model_download import ModelDownload
        raw_name = source_name or item.source_file_name or item.file_name or item.title or f'item_{item.id}'
        base = os.path.splitext(raw_name)[0] if cls.is_single_file(raw_name, item.local_path or '') else raw_name
        rows = db.session.query(ModelDownload.id, ModelDownload.source_file_name,
                                ModelDownload.file_name, ModelDownload.output_folder_name,
                                ModelDownload.title).filter(ModelDownload.id != item.id).all()
        used = set()
        duplicate = False
        for row_id, original, file_name, output, title in rows:
            FeederUtil.cooperate()
            historical = original or file_name or title or ''
            historical_base = os.path.splitext(historical)[0] if cls.is_single_file(historical) else historical
            used.add((output or historical_base).casefold())
            if row_id < item.id and (historical_base.casefold() == base.casefold() or (output or '').casefold() == base.casefold()):
                duplicate = True
        result = base
        if duplicate or base.casefold() in used:
            short_hash = item.infohash[:6] if item.infohash else f'id_{item.id}'
            result = f'{base}_[{short_hash}]'
            if result.casefold() in used:
                result = f'{result}_{item.id}'
        item.source_file_name = raw_name
        item.output_folder_name = result
        db.session.commit()
        return result

    @staticmethod
    def get_queue_limit(config: dict) -> int:
        """Feeder의 엔진 큐 개수 제한은 AllDebrid에만 적용한다."""
        if str(config.get('engine_type', '')).lower() != 'alldebrid':
            return 0
        try:
            return max(0, int(config.get('max_active_tasks') or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def get_local_root(config: dict | None) -> str:
        """Return the configured filesystem root for an engine's local output."""
        config = config or {}
        if config.get('engine_type') == 'cd2':
            mount = str(config.get('cd2_mount_path') or '').strip().rstrip('/\\')
            vpath = str(config.get('cd2_virtual_path') or '').strip().strip('/')
            if mount and vpath:
                if mount.replace('\\', '/').endswith(vpath):
                    return mount
                return os.path.join(mount, vpath)
            return mount or vpath

    @staticmethod
    def add_magnet(engine, link: str, title: str = None, upload_path: str = None):
        """Call engines with the optional upload_path argument only when supported."""
        signature = inspect.signature(engine.add_magnet)
        accepts_upload_path = (
            'upload_path' in signature.parameters
            or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values())
        )
        if accepts_upload_path:
            return engine.add_magnet(link, title=title, upload_path=upload_path)
        return engine.add_magnet(link, title=title)

    """다운로더 엔진 및 이송 핸들러 통합 관리자"""

    _engine_classes = {}
    _transporter_classes = {}

    # 엔진 관리
    @classmethod
    def load_engines(cls):
        FeederUtil.ensure_custom_dirs()
        cls._engine_classes = {
            QBittorrentEngine.ENGINE_ID: QBittorrentEngine,
            CloudDrive2Engine.ENGINE_ID: CloudDrive2Engine,
        }

        py_files = glob.glob(os.path.join(FeederUtil.ENGINES_DIR, "*.py"))
        for fpath in py_files:
            FeederUtil.cooperate()
            fname = os.path.basename(fpath)
            if fname.startswith("__"):
                continue

            module_name = f"feeder_engine_{os.path.splitext(fname)[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    FeederUtil.cooperate()
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
            config_schema = list(getattr(e_cls, 'CONFIG_SCHEMA', []))
            if not any(field.get('name') == 'missing_timeout_hours' for field in config_schema):
                config_schema.append({
                    'name': 'missing_timeout_hours', 'label': '원격 목록 누락 유예 (시간)',
                    'type': 'number', 'default': 24,
                    'desc': '전체 목록 조회 성공 후에도 계속 보이지 않는 작업의 실패 처리 유예 시간. 0은 자동 실패 안 함. CD2 페이지 제한으로 일부 목록만 조회한 경우에는 판정하지 않습니다.'
                })
            schemas.append({
                'engine_id': e_id,
                'engine_name': getattr(e_cls, 'ENGINE_NAME', e_id),
                'output_type': getattr(e_cls, 'OUTPUT_TYPE', 'local'),
                'config_schema': config_schema
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
            RemoteWorkerTransporter.TRANSPORTER_ID: RemoteWorkerTransporter,
        }

        py_files = glob.glob(os.path.join(FeederUtil.TRANSPORTERS_DIR, "*.py"))
        for fpath in py_files:
            FeederUtil.cooperate()
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
                    FeederUtil.cooperate()
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

    @classmethod
    def delete_remote_tasks(cls, item, strict=False) -> None:
        """작업 취소/삭제 시 연동된 모든 다운로더 엔진에서 해당 원격 작업을 확실히 선제 삭제"""
        target_hash = (item.infohash or FeederUtil.extract_info_hash(item.magnet) or '').lower()
        target_tid = str(item.engine_task_id or '')

        engines_to_check = set()
        if item.current_engine_name:
            engines_to_check.add(item.current_engine_name)
        if item.priority_chain:
            for eng in item.priority_chain:
                engines_to_check.add(eng)
        if not engines_to_check:
            for d in FeederUtil.get_downloaders():
                if d.get('enabled', True):
                    engines_to_check.add(d.get('name'))

        for engine_name in engines_to_check:
            downloader_cfg = FeederUtil.get_downloader_by_name(engine_name)
            if not downloader_cfg:
                if strict and engine_name == item.current_engine_name and target_tid:
                    raise RuntimeError(f'{engine_name}: 다운로더 설정이 없어 원격 작업을 중단하지 못했습니다.')
                continue
            engine = cls.create_engine(downloader_cfg)
            if not engine:
                if strict and engine_name == item.current_engine_name and target_tid:
                    raise RuntimeError(f'{engine_name}: 다운로더를 로드하지 못했습니다.')
                continue

            try:
                # 작업 ID로 직접 삭제 시도
                if target_tid and str(item.current_engine_name) == str(engine_name):
                    deleted = engine.delete_task(target_tid)
                    if strict and not deleted:
                        statuses, error = engine.get_status()
                        if error or any(str(s.get('task_id') or s.get('id')) == target_tid for s in statuses):
                            raise RuntimeError(f'{engine_name}: 원격 작업 중단 실패')

                # 원격 엔진 목록을 조회하여 동일 해시를 가진 잔여 작업 모두 삭제
                if target_hash and hasattr(engine, 'get_status'):
                    status_list, err = engine.get_status()
                    if strict and err:
                        raise RuntimeError(f'{engine_name}: 원격 작업 목록 조회 실패: {err}')
                    if not err and status_list:
                        for s in status_list:
                            s_hash = (s.get('hash') or '').lower()
                            s_tid = str(s.get('task_id') or s.get('id') or '')
                            if (s_hash and s_hash == target_hash) or (s_tid and s_tid == target_tid):
                                deleted = engine.delete_task(s_tid)
                                if strict and not deleted:
                                    raise RuntimeError(f'{engine_name}: 원격 작업 삭제 실패 ({s_tid})')
                                logger.info(f"[DownloadUtil] [{engine_name}] 원격 소스 작업 삭제 완료: ID={s_tid}, Title={item.title[:25]}")
            except Exception as ex:
                if strict:
                    raise
                logger.debug(f"[DownloadUtil] [{engine_name}] 원격 소스 삭제 중 예외: {ex}")
