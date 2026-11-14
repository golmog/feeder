# -*- coding: utf-8 -*-
import io
import os
import re
import sys
import glob
import time
import math
import yaml
import json
import base64
import hashlib
import requests
import subprocess
import unicodedata
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from sqlalchemy import func

from .setup import *
from .util_runtime import cooperate


class FeederUtil:
    """플러그인 공통 유틸리티 및 YAML 설정 통합 관리자"""

    CONFIG_FILEPATH = os.path.join(path_data, 'db', 'feeder_settings.yaml')
    CUSTOM_DIR = os.path.join(path_data, 'db', 'feeder_custom')
    ENGINES_DIR = os.path.join(CUSTOM_DIR, 'engines')
    TRANSPORTERS_DIR = os.path.join(CUSTOM_DIR, 'transporters')
    SITES_DIR = os.path.join(CUSTOM_DIR, 'sites')

    cooperate = staticmethod(cooperate)
    _download_execution = threading.local()

    @classmethod
    def single_download_id(cls):
        return getattr(cls._download_execution, 'item_id', None)

    @classmethod
    @contextmanager
    def single_download_scope(cls, item_id):
        """현재 실행 스레드의 지정 항목에만 중단 예외를 적용한다."""
        previous = cls.single_download_id()
        cls._download_execution.item_id = item_id
        try:
            yield
        finally:
            cls._download_execution.item_id = previous

    @classmethod
    def directory_size(cls, path: str) -> int:
        """Measure a directory while allowing other work to run during large scans."""
        total = 0
        for root, _, files in os.walk(path):
            cls.cooperate()
            for filename in files:
                cls.cooperate()
                total += os.path.getsize(os.path.join(root, filename))
        return total

    # --------------------------------------------------------------------------
    # 경로 및 시스템 헬퍼
    # --------------------------------------------------------------------------
    @staticmethod
    def format_bytes(size: int) -> str:
        """바이트 수를 사람이 읽을 수 있는 단위로 포맷"""
        if size is None or size <= 0:
            return "0 B"
        value = float(size)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if value < 1024.0 or unit == 'PB':
                return f"{value:.2f} {unit}"
            value /= 1024.0
        return f"{value:.2f} PB"

    @staticmethod
    def resolve_setting(key: str, profile_config: dict | None = None,
                        engine_config: dict | None = None,
                        global_value=None, default=None):
        """프로필 > 엔진 > 플러그인 DB > 코드 기본값 순서로 설정을 해석"""
        for config in (profile_config or {}, engine_config or {}):
            if key in config and config[key] is not None:
                return config[key]
        if global_value is not None:
            return global_value
        return default

    @classmethod
    def ensure_custom_dirs(cls):
        """커스텀 스크립트 디렉터리 준비"""
        for d in [cls.CUSTOM_DIR, cls.ENGINES_DIR, cls.TRANSPORTERS_DIR, cls.SITES_DIR]:
            os.makedirs(d, exist_ok=True)

    @classmethod
    def get_tmp_dir(cls, sub_path: str = '') -> str:
        """플러그인 전용 임시 디렉터리 반환"""
        base_dir = os.path.join(path_data, 'tmp', P.package_name)
        target_dir = os.path.join(base_dir, sub_path) if sub_path else base_dir
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    @classmethod
    def get_ddns(cls) -> str:
        """플랫폼 DDNS 주소 반환"""
        try:
            if hasattr(F, 'SystemModelSetting') and F.SystemModelSetting:
                return F.SystemModelSetting.get('ddns') or ''
        except Exception:
            pass
        try:
            if SystemModelSetting:
                return SystemModelSetting.get('ddns') or ''
        except Exception:
            pass
        try:
            return F.config.get('ddns', '')
        except Exception:
            return ''

    @classmethod
    def get_system_apikey(cls) -> str:
        """플랫폼 API Key 반환"""
        try:
            if hasattr(F, 'SystemModelSetting') and F.SystemModelSetting:
                return F.SystemModelSetting.get('auth_apikey') or F.SystemModelSetting.get('apikey') or ''
        except Exception:
            pass
        try:
            if SystemModelSetting:
                return SystemModelSetting.get('auth_apikey') or SystemModelSetting.get('apikey') or ''
        except Exception:
            pass
        return ''

    # --------------------------------------------------------------------------
    # 마그넷, 해시 및 텍스트 파싱 헬퍼
    # --------------------------------------------------------------------------
    @classmethod
    def extract_info_hash(cls, magnet_uri: str) -> str | None:
        """마그넷 또는 ed2k 주소로부터 40/32자리 고유 해시 추출"""
        if not magnet_uri:
            return None

        if magnet_uri.startswith('magnet:'):
            match = re.search(r'xt=urn:btih:([a-zA-Z0-9]+)', magnet_uri, re.IGNORECASE)
            if match:
                raw_hash = match.group(1).lower()
                if len(raw_hash) == 40:
                    return raw_hash
                elif len(raw_hash) == 32:
                    try:
                        decoded = base64.b32decode(raw_hash.upper())
                        return decoded.hex().lower()
                    except Exception:
                        return raw_hash

        elif magnet_uri.startswith('ed2k://'):
            match = re.search(r'ed2k://\|file\|[^|]+\|[0-9]+\|([a-fA-F0-9]{32})', magnet_uri, re.IGNORECASE)
            if match:
                return match.group(1).lower()

        return None

    @classmethod
    def extract_info_hash_from_torrent(cls, torrent_bytes: bytes) -> str | None:
        """bencode 파싱을 통해 .torrent 바이너리에서 info hash 직접 추출"""
        if not torrent_bytes:
            return None
        try:
            info_idx = torrent_bytes.find(b'4:info')
            if info_idx == -1:
                return None

            start = info_idx + 6
            if start >= len(torrent_bytes) or torrent_bytes[start:start+1] != b'd':
                return None

            pos = start
            depth = 0
            while pos < len(torrent_bytes):
                char = torrent_bytes[pos:pos+1]
                if char in (b'd', b'l'):
                    depth += 1
                    pos += 1
                elif char == b'e':
                    depth -= 1
                    pos += 1
                    if depth == 0:
                        break
                elif char == b'i':
                    end_int = torrent_bytes.find(b'e', pos + 1)
                    if end_int == -1:
                        break
                    pos = end_int + 1
                elif b'0' <= char <= b'9':
                    colon = torrent_bytes.find(b':', pos)
                    if colon == -1:
                        break
                    str_len = int(torrent_bytes[pos:colon])
                    pos = colon + 1 + str_len
                else:
                    pos += 1

            if depth == 0 and pos > start:
                raw_info = torrent_bytes[start:pos]
                return hashlib.sha1(raw_info).hexdigest().lower()
        except Exception as ex:
            logger.debug(f"[FeederUtil] .torrent 마그넷 해시 추출 예외: {ex}")
        return None

    @classmethod
    def parse_board_info(cls, board: str, subcat: str = None) -> tuple[str, str, str]:
        """게시판 ID 및 서브카테고리 문자열 정규화 파싱 (board_id, subcat_id, full_key)"""
        board_str = str(board).strip() if board else ''
        subcat_str = str(subcat).strip() if subcat else ''

        if 'fid=' in board_str and 'typeid=' in board_str:
            m_fid = re.search(r'fid=(?P<fid>\d+)', board_str)
            m_type = re.search(r'typeid=(?P<typeid>\d+)', board_str)
            if m_fid:
                board_str = m_fid.group('fid')
            if m_type:
                subcat_str = m_type.group('typeid')
        elif not subcat_str and ':' in board_str:
            parts = board_str.split(':', 1)
            board_str, subcat_str = parts[0].strip(), parts[1].strip()

        full_key = f"{board_str}:{subcat_str}" if subcat_str else board_str
        return board_str, subcat_str, full_key

    @classmethod
    def split_magnets(cls, magnet_str: str) -> list[str]:
        """문자열 내 복수 마그넷 및 ed2k 링크 분리"""
        if not magnet_str:
            return []
        raw = str(magnet_str).strip()
        if not raw:
            return []

        matches = list(re.finditer(r'(?:magnet:\?|ed2k://)', raw, re.IGNORECASE))
        if not matches:
            sep = '\n' if '\n' in raw else '|'
            return [m.strip() for m in raw.split(sep) if m.strip()]

        result = []
        for idx, match in enumerate(matches):
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
            part = raw[start:end].strip().strip('|').strip()
            if part:
                result.append(part)
        return result

    @classmethod
    def clean_xml_string(cls, value: str) -> str:
        """XML 특수문자 이스케이프"""
        if not value:
            return ''
        return (
            value.replace('&', '&amp;')
                 .replace('<', '&lt;')
                 .replace('>', '&gt;')
                 .replace('"', '&quot;')
                 .replace("'", '&apos;')
        )

    @classmethod
    def get_paging_info(cls, count, current_page, page_size):
        """UI 페이징 블록 계산 헬퍼"""
        total_page = math.ceil(count / page_size) if page_size > 0 else 1
        if total_page == 0:
            total_page = 1
        current_page = max(1, min(int(current_page or 1), total_page))

        page_block = 10
        start_page = ((current_page - 1) // page_block) * page_block + 1
        end_page = min(start_page + page_block - 1, total_page)
        page_list = list(range(start_page, end_page + 1))

        prev_p = start_page - 1 if start_page > 1 else 0
        next_p = end_page + 1 if end_page < total_page else 0

        return {
            'page': current_page,
            'current_page': current_page,
            'page_size': page_size,
            'list_step': page_size,
            'total_page': total_page,
            'total_count': count,
            'count': count,
            'start_page': start_page,
            'end_page': end_page,
            'last_page': end_page,
            'page_list': page_list,
            'prev_page': prev_p,
            'next_page': next_p,
        }

    @classmethod
    def db_vacuum(cls):
        """SQLite DB VACUUM 공간 회수 공통 실행"""
        try:
            with F.app.app_context():
                try:
                    engine = db.get_engine(bind=P.package_name)
                except Exception:
                    engine = db.engine

                if engine.dialect.name == 'sqlite':
                    raw_conn = engine.raw_connection()
                    try:
                        raw_conn.isolation_level = None
                        cursor = raw_conn.cursor()
                        cursor.execute("VACUUM")
                        cursor.close()
                        logger.info(f"[FeederUtil] SQLite DB VACUUM 정리 완료 ({P.package_name}.db)")
                    finally:
                        raw_conn.close()
        except Exception as e:
            logger.error(f"[FeederUtil] DB VACUUM 실행 오류: {e}")

    @classmethod
    def db_checkpoint(cls, mode: str = 'TRUNCATE'):
        """SQLite WAL 파일의 커밋된 데이터를 메인 DB로 안전하게 병합(체크포인트) 및 파일 비우기"""
        try:
            with F.app.app_context():
                try:
                    engine = db.get_engine(bind=P.package_name)
                except Exception:
                    engine = db.engine

                if engine.dialect.name == 'sqlite':
                    db.session.remove()
                    raw_conn = engine.raw_connection()
                    try:
                        raw_conn.isolation_level = None
                        cursor = raw_conn.cursor()
                        cursor.execute(f"PRAGMA wal_checkpoint({mode});")
                        result = cursor.fetchone()
                        cursor.close()
                        logger.debug(f"[FeederUtil] SQLite WAL 체크포인트({mode}) 완료: {result} ({P.package_name}.db)")
                    finally:
                        raw_conn.close()
        except Exception as e:
            logger.debug(f"[FeederUtil] SQLite WAL 체크포인트 중 예외 (정상 스킵): {e}")

    @classmethod
    def get_rclone_transfers(cls) -> int:
        """업로드·스테이징 작업 내부의 파일 전송 수를 반환한다."""
        try:
            value = int(P.ModelSetting.get('download_rclone_transfers') or 2)
            return value if 1 <= value <= 2**31 - 1 else 2
        except (ValueError, TypeError):
            return 2

    @classmethod
    def get_rclone_transfer_options(cls) -> list[str]:
        """추가 옵션의 중복 지정을 제거하고 전용 설정을 마지막에 적용한다."""
        options = cls.get_rclone_extra_options()
        result = []
        index = 0
        while index < len(options):
            option = options[index]
            if option == '--transfers':
                index += 2
                continue
            if not option.startswith('--transfers='):
                result.append(option)
            index += 1
        return result + ['--transfers', str(cls.get_rclone_transfers())]

    @classmethod
    def migrate_rclone_options(cls):
        """구형 청크 설정을 한 번만 합치고 이후 사용자 편집은 그대로 둔다."""
        import shlex
        if P.ModelSetting.get_bool('download_rclone_options_migrated'):
            return
        raw = P.ModelSetting.get('download_rclone_extra_options') or ''
        try:
            arguments = shlex.split(raw)
        except ValueError:
            logger.warning('[FeederUtil] Rclone 추가 옵션의 따옴표를 확인해주세요. 청크 설정 통합을 보류합니다.')
            return
        if not any(value == '--drive-chunk-size' or value.startswith('--drive-chunk-size=') for value in arguments):
            chunk = P.ModelSetting.get('download_rclone_chunk_size') or '256M'
            extra = '--timeout 60m' if not raw.strip() or raw == '--timeout 30m' else raw
            P.ModelSetting.set('download_rclone_extra_options', '--drive-chunk-size ' + shlex.quote(chunk) + ' ' + extra)
        P.ModelSetting.set('download_rclone_options_migrated', 'True')

    @classmethod
    def get_rclone_extra_options(cls) -> list[str]:
        """설정된 Rclone 확장 옵션 문자열을 안전하게 분리하여 리스트로 반환"""
        import shlex
        raw_opt = P.ModelSetting.get('download_rclone_extra_options') if P.ModelSetting else ''
        if not raw_opt or not raw_opt.strip():
            raw_opt = '--drive-chunk-size 256M --timeout 60m'
        try:
            return shlex.split(raw_opt.strip())
        except Exception as ex:
            logger.debug(f"[FeederUtil] Rclone 옵션 shlex 파싱 예외 (단순 분리 사용): {ex}")
            return [x.strip() for x in raw_opt.split() if x.strip()]

    @classmethod
    def get_rclone_exclude_options(cls) -> list[str]:
        """DB에 설정된 전송 제외 파일 패턴을 Rclone --exclude 인자 리스트로 변환"""
        raw_patterns = P.ModelSetting.get('download_exclude_pattern') if P.ModelSetting else ''
        if not raw_patterns:
            # YAML fallback
            yaml_rclone = cls.load_yaml().get('rclone', {})
            raw_patterns = yaml_rclone.get('exclude_patterns', [])
            if isinstance(raw_patterns, list):
                res = []
                for p in raw_patterns:
                    if p and str(p).strip():
                        res.extend(['--exclude', str(p).strip()])
                return res

        options = []
        for line in str(raw_patterns).splitlines():
            pattern = line.strip()
            if pattern and not pattern.startswith('#'):
                options.extend(['--exclude', pattern])
        return options

    _rclone_live_stats = {}
    _rclone_stats_lock = threading.Lock()
    _rclone_stats_stale_after = 600

    @classmethod
    def set_rclone_progress(cls, item_id, progress_dict: dict):
        """웹 프로세스 메모리에 실시간 전송률 저장"""
        if not item_id:
            return
        with cls._rclone_stats_lock:
            value = dict(progress_dict or {})
            value['_updated_at'] = time.time()
            cls._rclone_live_stats[str(item_id)] = value

    @classmethod
    def get_rclone_progress(cls, item_id) -> dict:
        """웹 프로세스 메모리에서 실시간 전송률 조회"""
        if not item_id:
            return {}
        with cls._rclone_stats_lock:
            now = time.time()
            stale_ids = [
                key for key, value in cls._rclone_live_stats.items()
                if now - value.get('_updated_at', now) > cls._rclone_stats_stale_after
            ]
            for key in stale_ids:
                cls._rclone_live_stats.pop(key, None)
            value = cls._rclone_live_stats.get(str(item_id), {})
            return dict(value)

    @classmethod
    def clear_rclone_progress(cls, item_id):
        """전송 완료 시 웹 프로세스 메모리에서 제거"""
        if not item_id:
            return
        with cls._rclone_stats_lock:
            cls._rclone_live_stats.pop(str(item_id), None)

    @classmethod
    def report_rclone_progress(cls, item_id, progress_dict: dict, action: str = 'update'):
        """Celery 워커에서 Web API(progress_update)로 실시간 전송률을 HTTP POST 발송"""
        if not item_id:
            return
        try:
            port = F.config.get('port', 9999) if (hasattr(F, 'config') and F.config) else 9999
            api_url = f"http://127.0.0.1:{port}/{P.package_name}/api/download/progress_update"
            apikey = cls.get_system_apikey()
            payload = {
                'item_id': str(item_id),
                'data': progress_dict,
                'action': action,
                'apikey': apikey
            }
            requests.post(api_url, json=payload, timeout=1.5)
        except Exception:
            pass

    @classmethod
    def apply_download_status_filter(cls, model_cls, query, status_filter: str):
        """메모리 로딩 0 byte, 순수 DB B-Tree 인덱스 조인을 통한 0.002초 다운로드 상태 필터링"""
        if status_filter not in ['download_completed', 'download_active', 'not_downloaded']:
            return query

        from .model_download import ModelDownload
        from sqlalchemy import or_

        if status_filter == 'download_completed':
            target_statuses = ['completed']
        elif status_filter == 'download_active':
            target_statuses = [
                'downloading', 'pending', 'downloaded', 'waiting_transporter', 'local_staging', 'uploading',
                'pending_local_staging', 'pending_upload', 'pending_relay',
                'relay_transferring'
            ]
        else:
            target_statuses = [
                'completed', 'downloading', 'pending', 'downloaded', 'waiting_transporter', 'local_staging', 'uploading',
                'pending_local_staging', 'pending_upload', 'pending_relay',
                'relay_transferring'
            ]

        status_condition = ModelDownload.status.in_(target_statuses)
        if status_filter != 'download_active':
            status_condition = or_(status_condition, ModelDownload.success_filter())
        subq = (
            db.session.query(ModelDownload.infohash)
            .filter(
                status_condition,
                ModelDownload.infohash.isnot(None)
            )
            .subquery()
        )

        if status_filter in ['download_completed', 'download_active']:
            return query.filter(model_cls.infohash.in_(subq))
        else:
            return query.filter(or_(model_cls.infohash.is_(None), ~model_cls.infohash.in_(subq)))

    @classmethod
    def migrate_db(cls):
        """기존 사용자 DB 스키마 자동 검사 및 infohash 인덱스 무중단 자동 승격 (1회 실행)"""
        with F.app.app_context():
            try:
                try:
                    engine = db.get_engine(bind=P.package_name)
                except Exception:
                    engine = db.engine

                from sqlalchemy import inspect
                inspector = inspect(engine)

                download_table = f'{P.package_name}_download'
                if download_table in inspector.get_table_names():
                    columns = {c['name'] for c in inspector.get_columns(download_table)}
                    with engine.begin() as conn:
                        for name, sql_type in [('upload_state', 'JSON'), ('video_manifest', 'JSON'), ('source_file_name', 'VARCHAR'),
                                               ('output_folder_name', 'VARCHAR'), ('remote_missing_since', 'DATETIME'),
                                               ('transfer_error_type', 'VARCHAR'), ('source_unavailable_since', 'DATETIME'),
                                               ('upload_retry_after', 'DATETIME')]:
                            FeederUtil.cooperate()
                            if name not in columns:
                                conn.execute(db.text(f"ALTER TABLE {download_table} ADD COLUMN {name} {sql_type}"))
                        conn.execute(db.text(f"CREATE INDEX IF NOT EXISTS ix_{download_table}_output_folder_name ON {download_table} (output_folder_name)"))

                stat_table = f'{P.package_name}_download_stat'
                if stat_table in inspector.get_table_names():
                    columns = {c['name'] for c in inspector.get_columns(stat_table)}
                    with engine.begin() as conn:
                        for name, sql_type in [('download_id', 'INTEGER'), ('attempt_id', 'VARCHAR')]:
                            if name not in columns:
                                conn.execute(db.text(f'ALTER TABLE {stat_table} ADD COLUMN {name} {sql_type}'))
                            conn.execute(db.text(f'CREATE INDEX IF NOT EXISTS ix_{stat_table}_{name} ON {stat_table} ({name})'))

                # feeder_bbs 테이블 infohash 컬럼 확인 및 자동 추가
                if f'{P.package_name}_bbs' in inspector.get_table_names():
                    columns = [c['name'] for c in inspector.get_columns(f'{P.package_name}_bbs')]
                    if 'infohash' not in columns:
                        with engine.connect() as conn:
                            conn.execute(db.text(f"ALTER TABLE {P.package_name}_bbs ADD COLUMN infohash VARCHAR(40)"))
                            conn.execute(db.text(f"CREATE INDEX ix_{P.package_name}_bbs_infohash ON {P.package_name}_bbs (infohash)"))
                            conn.commit()
                        logger.info("[FeederUtil] feeder_bbs 테이블 infohash 인덱스 컬럼 자동 마이그레이션 완료")

                        # 기존 레코드 infohash 일괄 채움
                        from .model_crawl import ModelCrawlItem
                        rows = db.session.query(ModelCrawlItem).filter(ModelCrawlItem.infohash.is_(None), ModelCrawlItem.magnet.isnot(None)).all()
                        for r in rows:
                            FeederUtil.cooperate()
                            r.infohash = cls.extract_info_hash(r.magnet)
                        db.session.commit()
                        logger.info(f"[FeederUtil] feeder_bbs 기존 레코드 {len(rows)}건 infohash 자동 채움 완료")

                # feeder_feed 테이블 infohash 컬럼 확인 및 자동 추가
                if f'{P.package_name}_feed' in inspector.get_table_names():
                    columns = [c['name'] for c in inspector.get_columns(f'{P.package_name}_feed')]
                    if 'infohash' not in columns:
                        with engine.connect() as conn:
                            conn.execute(db.text(f"ALTER TABLE {P.package_name}_feed ADD COLUMN infohash VARCHAR(40)"))
                            conn.execute(db.text(f"CREATE INDEX ix_{P.package_name}_feed_infohash ON {P.package_name}_feed (infohash)"))
                            conn.commit()
                        logger.info("[FeederUtil] feeder_feed 테이블 infohash 인덱스 컬럼 자동 마이그레이션 완료")

                        from .model_feed import ModelFeedItem
                        rows = db.session.query(ModelFeedItem).filter(ModelFeedItem.infohash.is_(None), ModelFeedItem.magnet.isnot(None)).all()
                        for r in rows:
                            FeederUtil.cooperate()
                            r.infohash = cls.extract_info_hash(r.magnet)
                        db.session.commit()
                        logger.info(f"[FeederUtil] feeder_feed 기존 레코드 {len(rows)}건 infohash 자동 채움 완료")

            except Exception as e:
                logger.error(f"[FeederUtil] DB 자동 마이그레이션 예외: {e}")
                try:
                    db.session.rollback()
                except Exception:
                    pass

    @classmethod
    def init_sqlite_concurrency(cls):
        """SQLite 엔진에 WAL 저널 모드 및 60초 busy_timeout을 강제 적용하여 동시성 락 차단"""
        try:
            from sqlalchemy import event
            try:
                engine = db.get_engine(bind=P.package_name)
            except Exception:
                engine = db.engine

            if engine.dialect.name == 'sqlite':
                @event.listens_for(engine, "connect")
                def _set_sqlite_pragma(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    cursor.execute("PRAGMA journal_mode=WAL;")
                    cursor.execute("PRAGMA busy_timeout=60000;")
                    cursor.execute("PRAGMA synchronous=NORMAL;")
                    cursor.close()
                # logger.info(f"[FeederUtil] SQLite WAL 모드 및 60초 busy_timeout 동시성 최적화 완료 ({P.package_name}.db)")
        except Exception as ex:
            logger.debug(f"[FeederUtil] SQLite PRAGMA 설정 예외 (무시): {ex}")

    @classmethod
    def init_runtime_locks(cls):
        """공유 DB에서 Celery 워커 간 실행권을 원자적으로 관리하는 락 테이블을 준비한다."""
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        with engine.begin() as conn:
            conn.execute(db.text(f"""
                CREATE TABLE IF NOT EXISTS {P.package_name}_runtime_lock (
                    lock_name VARCHAR(100) PRIMARY KEY,
                    owner VARCHAR(100) NOT NULL,
                    acquired_at REAL NOT NULL
                )
            """))

    @classmethod
    def acquire_runtime_lock(cls, lock_name: str, owner: str, stale_after: int | None = 86400) -> bool:
        """None이면 자동 만료 없이, 그 외에는 만료된 실행권만 교체한다."""
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        now = time.time()
        conflict = 'DO NOTHING' if stale_after is None else f"""DO UPDATE SET
            owner = excluded.owner,
            acquired_at = excluded.acquired_at
            WHERE {P.package_name}_runtime_lock.acquired_at < :expired_at"""
        values = {'lock_name': lock_name, 'owner': owner, 'acquired_at': now}
        if stale_after is not None:
            values['expired_at'] = now - stale_after
        with engine.begin() as conn:
            result = conn.execute(db.text(f"""
                INSERT INTO {P.package_name}_runtime_lock (lock_name, owner, acquired_at)
                VALUES (:lock_name, :owner, :acquired_at)
                ON CONFLICT(lock_name) {conflict}
            """), values)
            return result.rowcount == 1

    @classmethod
    def release_runtime_lock(cls, lock_name: str, owner: str) -> bool:
        """소유자가 바뀐 경우에는 새 소유자의 락을 건드리지 않는다."""
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        with engine.begin() as conn:
            result = conn.execute(db.text(f"""
                DELETE FROM {P.package_name}_runtime_lock
                WHERE lock_name = :lock_name AND owner = :owner
            """), {'lock_name': lock_name, 'owner': owner})
            return result.rowcount == 1

    @classmethod
    def reset_runtime_lock(cls, lock_name: str) -> None:
        """플러그인 로드 시 이전 프로세스의 런타임 락을 초기화한다."""
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        with engine.begin() as conn:
            conn.execute(db.text(f"""
                DELETE FROM {P.package_name}_runtime_lock
                WHERE lock_name = :lock_name
            """), {'lock_name': lock_name})

    @classmethod
    def is_download_paused(cls) -> bool:
        """FF/Celery 사이에서 캐시 없이 영속적인 중단 요청을 확인한다."""
        if cls.single_download_id() is not None:
            return False
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        with engine.connect() as conn:
            return conn.execute(db.text(f"""
                SELECT 1 FROM {P.package_name}_runtime_lock
                WHERE lock_name = 'download_pause'
            """)).first() is not None

    @classmethod
    def set_download_paused(cls, paused: bool) -> None:
        cls.init_runtime_locks()
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        with engine.begin() as conn:
            if paused:
                conn.execute(db.text(f"""
                    INSERT INTO {P.package_name}_runtime_lock (lock_name, owner, acquired_at)
                    VALUES ('download_pause', 'user', :now)
                    ON CONFLICT(lock_name) DO NOTHING
                """), {'now': time.time()})
            else:
                conn.execute(db.text(f"""
                    DELETE FROM {P.package_name}_runtime_lock WHERE lock_name = 'download_pause'
                """))

    @classmethod
    def get_download_control_state(cls) -> dict:
        try:
            engine = db.get_engine(bind=P.package_name)
        except Exception:
            engine = db.engine
        with engine.connect() as conn:
            locks = {row[0]: row[1] for row in conn.execute(db.text(f"""
                SELECT lock_name, owner FROM {P.package_name}_runtime_lock
                WHERE lock_name IN ('download_pause', 'download_pipeline')
            """))}
            active_transfers = conn.execute(db.text(f"""
                SELECT COUNT(*) FROM {P.package_name}_download
                WHERE status IN ('uploading', 'local_staging')
            """)).scalar()
        paused = 'download_pause' in locks
        running = 'download_pipeline' in locks or bool(active_transfers)
        pipeline_owner = locks.get('download_pipeline', '')
        single_match = re.fullmatch(r'single:(\d+):[a-f0-9]+', pipeline_owner)
        return {
            'pause_requested': paused,
            'running': running,
            'state': ('stopping' if running else 'paused') if paused else ('running' if running else 'idle'),
            'active_transfers': active_transfers,
            'pipeline_locked': 'download_pipeline' in locks,
            'pipeline_owner': pipeline_owner,
            'single_item_id': int(single_match[1]) if single_match else None,
            'observed_at': time.time(),
        }

    @classmethod
    def attach_download_info(cls, items: list) -> list[dict]:
        """수집/피드 목록 아이템에 다운로드 상태(download_info) 매핑 주입 공통 로직"""
        if not items:
            return []

        from .model_download import ModelDownload

        all_page_hashes = {}
        for item_obj in items:
            FeederUtil.cooperate()
            m_list = cls.split_magnets(item_obj.magnet)
            for m_str in m_list:
                FeederUtil.cooperate()
                h = cls.extract_info_hash(m_str)
                if h:
                    all_page_hashes[h] = item_obj.id

        dl_map = {}
        if all_page_hashes:
            dl_records = (
                db.session.query(ModelDownload.infohash, ModelDownload.status, ModelDownload.id)
                .filter(ModelDownload.infohash.in_(list(all_page_hashes.keys())))
                .all()
            )
            for d_hash, d_status, d_id in dl_records:
                FeederUtil.cooperate()
                dl_map[d_hash] = {'status': d_status, 'id': d_id}

        item_dicts = []
        for item_obj in items:
            FeederUtil.cooperate()
            d = item_obj.as_dict()
            d_links = d.get('magnet', [])
            matched_dl = None
            for m_str in d_links:
                FeederUtil.cooperate()
                h = cls.extract_info_hash(m_str)
                if h and h in dl_map:
                    matched_dl = dl_map[h]
                    break
            d['download_info'] = matched_dl
            item_dicts.append(d)

        return item_dicts

    # --------------------------------------------------------------------------
    # feeder_settings.yaml 단일 설정 관리 메서드
    # --------------------------------------------------------------------------
    @classmethod
    def get_filepath(cls) -> str:
        """feeder_settings.yaml 파일 경로 반환"""
        return cls.CONFIG_FILEPATH

    @classmethod
    def load_yaml(cls) -> dict:
        if not os.path.exists(cls.CONFIG_FILEPATH):
            default_data = {
                'GLOBAL': {},
                'CRAWLERS': [],
                'FEEDS': [],
                'DOWNLOADERS': [],
                'DOWNLOAD_PROFILES': [],
                'GDRIVE_ACCOUNTS': []
            }
            cls.save_yaml(default_data)
            return default_data

        try:
            with open(cls.CONFIG_FILEPATH, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}

            for sec in ['CRAWLERS', 'FEEDS', 'DOWNLOADERS', 'DOWNLOAD_PROFILES', 'GDRIVE_ACCOUNTS']:
                if sec not in data or not isinstance(data[sec], list):
                    data[sec] = []
            if 'GLOBAL' not in data or not isinstance(data['GLOBAL'], dict):
                data['GLOBAL'] = {}

            for c in data['CRAWLERS']:
                if 'boards' not in c or not isinstance(c['boards'], list):
                    c['boards'] = []
                for b in c['boards']:
                    b_val = b.get('board', '')
                    s_val = b.get('subcat', '')
                    _, _, f_key = cls.parse_board_info(b_val, s_val)
                    b['full_board_key'] = f_key

            for f in data['FEEDS']:
                if 'sources' not in f or not isinstance(f['sources'], list):
                    f['sources'] = []
                for src in f['sources']:
                    b_val = src.get('board', '')
                    s_val = src.get('subcat', '')
                    _, _, f_key = cls.parse_board_info(b_val, s_val)
                    src['full_board_key'] = f_key

            return data
        except Exception as e:
            logger.error(f"[FeederUtil] YAML 로드 실패: {e}")
            return {'GLOBAL': {}, 'CRAWLERS': [], 'FEEDS': [], 'DOWNLOADERS': [], 'DOWNLOAD_PROFILES': [], 'GDRIVE_ACCOUNTS': []}

    @classmethod
    def save_yaml(cls, data: dict) -> bool:
        try:
            os.makedirs(os.path.dirname(cls.CONFIG_FILEPATH), exist_ok=True)
            raw_yaml = yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
            formatted_yaml = re.sub(r'\n([A-Z0-9_]+:)', r'\n\n\1', raw_yaml).lstrip('\n')
            with open(cls.CONFIG_FILEPATH, 'w', encoding='utf-8') as f:
                f.write(formatted_yaml)
            return True
        except Exception as e:
            logger.error(f"[FeederUtil] YAML 저장 실패: {e}")
            return False

    # Crawlers
    @classmethod
    def get_crawlers(cls) -> list[dict]:
        return cls.load_yaml().get('CRAWLERS', [])

    @classmethod
    def get_crawler(cls, crawler_id):
        for c in cls.get_crawlers():
            if str(c.get('id')) == str(crawler_id):
                return c
        return None

    @classmethod
    def get_crawler_by_board(cls, site_name: str, board_id: str, subcat_id: str = None):
        _, _, full_key = cls.parse_board_info(board_id, subcat_id)
        for c in cls.get_crawlers():
            if c.get('site') == site_name:
                for b in c.get('boards', []):
                    b_key = b.get('full_board_key') or b.get('board')
                    if b_key == full_key:
                        return c
        return None

    @classmethod
    def save_crawler(cls, item: dict) -> str:
        item = dict(item)
        if not str(item.get('site') or '').strip() or not isinstance(item.get('boards'), list):
            return 'invalid_crawler'
        if any(not isinstance(b, dict) or not str(b.get('board') if b.get('board') is not None else '').strip() for b in item['boards']):
            return 'invalid_boards'
        try:
            target_id = int(item.get('id') or 0)
        except (TypeError, ValueError):
            return 'invalid_id'
        item['id'] = target_id
        data = cls.load_yaml()
        crawlers = data.get('CRAWLERS', [])
        target_id = item.get('id')

        boards = item.get('boards', [])
        normalized_boards = []
        for b in boards:
            b_val = b.get('board', '')
            s_val = b.get('subcat', '')
            _, _, f_key = cls.parse_board_info(b_val, s_val)
            normalized_boards.append({
                'board': str(b_val),
                'subcat': str(s_val) if s_val else '',
                'full_board_key': f_key
            })
        item['boards'] = normalized_boards

        if target_id is not None and int(target_id) > 0:
            for idx, c in enumerate(crawlers):
                if str(c.get('id')) == str(target_id):
                    crawlers[idx] = item
                    data['CRAWLERS'] = crawlers
                    if not cls.save_yaml(data):
                        return 'save_failed'
                    logger.info(f"[FeederUtil] 수집기 설정 수정 완료: ID={target_id}, Site={item.get('site')}")
                    return 'success_update'
            return 'not_found'

        for c in crawlers:
            if c.get('site') == item.get('site'):
                logger.warning(f"[FeederUtil] 동일 사이트 수집기 이미 존재: {item.get('site')}")
                return 'already_exist'

        max_id = max([int(c.get('id', 0)) for c in crawlers], default=0)
        item['id'] = max_id + 1
        crawlers.append(item)
        data['CRAWLERS'] = crawlers
        if not cls.save_yaml(data):
            return 'save_failed'
        logger.info(f"[FeederUtil] 신규 수집기 추가 완료: ID={item['id']}, Site={item.get('site')}")
        return 'success'

    @classmethod
    def delete_crawler(cls, target_id) -> bool:
        data = cls.load_yaml()
        crawlers = data.get('CRAWLERS', [])
        data['CRAWLERS'] = [c for c in crawlers if str(c.get('id')) != str(target_id)]
        cls.save_yaml(data)
        logger.info(f"[FeederUtil] 수집기 삭제 완료: ID={target_id}")
        return True

    # Feeds
    @classmethod
    def get_feeds(cls) -> list[dict]:
        return cls.load_yaml().get('FEEDS', [])

    @classmethod
    def get_feed(cls, feed_id):
        for f in cls.get_feeds():
            if str(f.get('id')) == str(feed_id):
                return f
        return None

    @classmethod
    def get_feed_by_name(cls, feed_name: str):
        target_name = unicodedata.normalize('NFC', str(feed_name).strip()).lower()
        for f in cls.get_feeds():
            current_name = unicodedata.normalize('NFC', str(f.get('name', '')).strip()).lower()
            if current_name == target_name:
                return f
        return None

    @classmethod
    def save_feed(cls, item: dict) -> str:
        item = dict(item)
        if not str(item.get('name') or '').strip() or not isinstance(item.get('sources'), list):
            return 'invalid_feed'
        if any(not isinstance(src, dict) or not (src.get('board') or src.get('url')) for src in item['sources']):
            return 'invalid_sources'
        try:
            target_id = int(item.get('id') or 0)
        except (TypeError, ValueError):
            return 'invalid_id'
        item['id'] = target_id
        data = cls.load_yaml()
        feeds = data.get('FEEDS', [])
        target_id = item.get('id')

        sources = item.get('sources', [])
        normalized_sources = []
        for src in sources:
            b_val = src.get('board', '')
            s_val = src.get('subcat', '')
            _, _, f_key = cls.parse_board_info(b_val, s_val)

            src_type = src.get('type') or ('rss' if str(b_val).startswith(('http://', 'https://')) else 'crawl')
            src_url = src.get('url') or (str(b_val) if src_type == 'rss' else '')

            normalized_sources.append({
                'type': src_type,
                'site': src.get('site', '외부RSS' if src_type == 'rss' else ''),
                'board': str(b_val),
                'url': src_url,
                'subcat': str(s_val) if s_val else '',
                'full_board_key': f_key
            })
        item['sources'] = normalized_sources

        if target_id is not None and int(target_id) > 0:
            for idx, f in enumerate(feeds):
                if str(f.get('id')) == str(target_id):
                    feeds[idx] = item
                    data['FEEDS'] = feeds
                    if not cls.save_yaml(data):
                        return 'save_failed'
                    logger.info(f"[FeederUtil] 피드 설정 수정 완료: ID={target_id}, Name={item.get('name')}")
                    return 'success_update'
            return 'not_found'

        target_name = str(item.get('name', '')).strip().lower()
        for f in feeds:
            if str(f.get('name', '')).strip().lower() == target_name:
                logger.warning(f"[FeederUtil] 동일 이름 피드 중복: {item.get('name')}")
                return 'already_exist'

        max_id = max([int(f.get('id', 0)) for f in feeds], default=0)
        item['id'] = max_id + 1
        feeds.append(item)
        data['FEEDS'] = feeds
        if not cls.save_yaml(data):
            return 'save_failed'
        logger.info(f"[FeederUtil] 신규 피드 추가 완료: ID={item['id']}, Name={item.get('name')}")
        return 'success'

    @classmethod
    def delete_feed(cls, target_id) -> bool:
        data = cls.load_yaml()
        feeds = data.get('FEEDS', [])
        data['FEEDS'] = [f for f in feeds if str(f.get('id')) != str(target_id)]
        cls.save_yaml(data)
        logger.info(f"[FeederUtil] 피드 삭제 완료: ID={target_id}")
        return True

    # Global
    @classmethod
    def get_global(cls) -> dict:
        return cls.load_yaml().get('GLOBAL', {})

    # Downloaders
    @classmethod
    def get_downloaders(cls) -> list[dict]:
        return cls.load_yaml().get('DOWNLOADERS', [])

    @classmethod
    def get_downloader_by_name(cls, name: str) -> dict | None:
        target = str(name).strip().lower()
        for d in cls.get_downloaders():
            if str(d.get('name', '')).strip().lower() == target:
                return d
        return None

    @classmethod
    def save_downloader(cls, item: dict) -> str:
        item = dict(item)
        if not str(item.get('name') or '').strip():
            return 'empty_name'
        if not item.get('engine_type'):
            return 'empty_engine_type'
        for key in ('upload_root', 'complete_root'):
            if key in item and cls.relative_path_error(item[key]):
                return cls.relative_path_error(item[key])
        if 'default_destination' in item:
            item['default_destination'] = cls.destination_settings(item['default_destination'])
            effective = cls.resolve_destination_paths(item['default_destination'], item, {})
            error = cls.validate_destination(effective)
            if error:
                return error
        for profile in cls.get_download_profiles():
            if str(item['name']).strip().lower() not in [str(name).strip().lower() for name in profile.get('priority_chain') or []]:
                continue
            error = cls.get_engine_destination(item, profile).get('_path_error', '')
            if error:
                return error
        if item.get('engine_type') == 'alldebrid' and 'webdav_failure_timeout_hours' in item:
            raw = item['webdav_failure_timeout_hours']
            try:
                hours = int(raw)
                if str(raw).strip() != str(hours) or not 0 <= hours <= 8760:
                    raise ValueError
            except (TypeError, ValueError):
                return 'WebDAV 실패 유예는 0~8760 사이의 정수 시간이어야 합니다.'
            item = dict(item, webdav_failure_timeout_hours=hours)
        data = cls.load_yaml()
        items = data.get('DOWNLOADERS', [])
        target_name = item.get('name', '').strip()
        if not target_name:
            return 'empty_name'

        for idx, d in enumerate(items):
            if str(d.get('name', '')).strip().lower() == target_name.lower():
                items[idx] = item
                data['DOWNLOADERS'] = items
                if not cls.save_yaml(data):
                    return 'save_failed'
                logger.info(f"[FeederUtil] 다운로더 설정 갱신: {target_name}")
                return 'success_update'

        items.append(item)
        data['DOWNLOADERS'] = items
        if not cls.save_yaml(data):
            return 'save_failed'
        logger.info(f"[FeederUtil] 신규 다운로더 추가: {target_name}")
        return 'success'

    @classmethod
    def delete_downloader(cls, name: str) -> bool:
        data = cls.load_yaml()
        items = data.get('DOWNLOADERS', [])
        target = str(name).strip().lower()
        data['DOWNLOADERS'] = [d for d in items if str(d.get('name', '')).strip().lower() != target]
        cls.save_yaml(data)
        logger.info(f"[FeederUtil] 다운로더 삭제: {name}")
        return True

    # Download Profiles
    @classmethod
    def get_download_profiles(cls) -> list[dict]:
        return cls.load_yaml().get('DOWNLOAD_PROFILES', [])

    @classmethod
    def get_download_profile_by_feed(cls, feed_name: str) -> dict | None:
        target = str(feed_name).strip().lower()
        for p in cls.get_download_profiles():
            feeds = [str(f).strip().lower() for f in p.get('feeds', [])]
            if target in feeds or '*' in feeds:
                return p
        return None

    @classmethod
    def get_upload_roots(cls):
        roots = {}
        for key, default in (('upload_root', 'incoming'), ('complete_root', 'uploads')):
            value = P.ModelSetting.get('download_' + key)
            roots[key] = default if value is None else value
        return roots

    @staticmethod
    def relative_path_error(value, single=False):
        message = '다운로드 상위 폴더는 유효한 한 개의 폴더명으로 지정해주세요.' if single else '업로드 루트는 유효한 비어있지 않은 상대 경로로 지정해주세요.'
        if (not isinstance(value, str) or not value.strip() or ':' in value or '\\' in value or
                any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value)):
            return message
        parts = value.split('/')
        if (single and len(parts) != 1) or any(not p.strip() or p.strip() in ('.', '..') for p in parts):
            return message
        return ''

    @staticmethod
    def destination_settings(destination):
        """내장 원격 목적지의 계산 경로는 YAML에 복제하지 않는다."""
        if not isinstance(destination, dict):
            return destination
        result = dict(destination)
        if result.get('type') in ('rclone_simple', 'gdrive_rotation', 'remote_worker'):
            result.pop('upload_path', None)
            result.pop('complete_path', None)
            result.pop('staging_path', None)
        return result

    @staticmethod
    def get_profile_folder(profile):
        folder = profile.get('destination_subfolder')
        return profile.get('name', 'direct') if folder is None or (isinstance(folder, str) and not folder.strip()) else folder

    @classmethod
    def resolve_download_subpath(cls, engine, profile):
        """다운로드 원본과 원격 임시 경로가 공유하는 상대 계층을 계산한다."""
        upload = engine.get('upload_root', cls.get_upload_roots()['upload_root'])
        folder = cls.get_profile_folder(profile)
        error = cls.relative_path_error(upload) or cls.relative_path_error(folder, single=True)
        if error:
            raise ValueError(error)
        return upload + '/' + folder

    @classmethod
    def get_download_subpath(cls, profile=None, engine_name='', item=None):
        profile = profile or {}
        chain = (item.priority_chain if item is not None else None) or profile.get('priority_chain') or []
        index = (item.current_engine_index or 0) if item is not None else 0
        engine_name = engine_name or (item.current_engine_name if item is not None else '') or (chain[index] if 0 <= index < len(chain) else '')
        engine = cls.get_downloader_by_name(engine_name) or {}
        destination = cls.get_profile_destination(profile, engine_name, item)
        state = (item.upload_state or {}) if item is not None else {}
        explicit = state.get('destination_snapshot') or (state.get('routing') or {}).get('destination')
        custom = destination.get('type') not in ('local', 'rclone_simple', 'gdrive_rotation', 'remote_worker', '')
        if (explicit or custom) and 'upload_path' in destination:
            path = destination['upload_path']
            error = cls.relative_path_error(path)
            if error:
                raise ValueError(error)
            return path
        return cls.resolve_download_subpath(engine, profile)

    @classmethod
    def get_local_staging_root(cls):
        return os.path.abspath(P.ModelSetting.get('download_local_staging_path') or
            cls.get_global().get('local_staging_path') or os.path.join(cls.get_tmp_dir(), 'staging'))

    @classmethod
    def get_local_staging_path(cls, item, profile=None, root=None):
        """등록한 원본 계층을 사용하고 디스크 루트 밖이나 심볼릭 링크에는 쓰지 않는다."""
        root = os.path.abspath(root or cls.get_local_staging_root())
        saved = (item.upload_state or {}).get('staging_local_path')
        if saved:
            path = saved
        else:
            profile = cls.get_item_profile(item) if profile is None else profile
            sub = cls.get_download_source_subpath(item, None)
            if sub is None:
                sub = cls.get_download_subpath(profile, item=item)
            folder = item.output_folder_name or item.file_name
            error = cls.relative_path_error(sub) or cls.relative_path_error(folder, single=True)
            if error:
                raise ValueError(error)
            path = os.path.join(root, sub, folder)
        absolute = os.path.abspath(path)
        actual = os.path.realpath(path)
        real_root = os.path.realpath(root)
        if (absolute == root or os.path.commonpath([root, absolute]) != root or
                actual == real_root or os.path.commonpath([real_root, actual]) != real_root):
            raise ValueError('로컬 스테이징 경로가 저장 루트 밖에 있습니다.')
        if item.local_path and os.path.exists(item.local_path) and os.path.commonpath([actual, os.path.realpath(item.local_path)]) == actual:
            raise ValueError('로컬 스테이징 경로가 다운로드 원본을 포함합니다.')
        cursor = absolute
        while cursor != root:
            if os.path.islink(cursor):
                raise ValueError('로컬 스테이징 경로에 심볼릭 링크를 사용할 수 없습니다.')
            cursor = os.path.dirname(cursor)
        return path

    @classmethod
    def resolve_destination_paths(cls, destination, engine, profile):
        if not isinstance(destination, dict):
            return destination
        result = dict(destination)
        if result.get('type') not in ('rclone_simple', 'gdrive_rotation', 'remote_worker'):
            return result
        defaults = cls.get_upload_roots()
        complete = engine['complete_root'] if 'complete_root' in engine else defaults['complete_root']
        try:
            upload = cls.resolve_download_subpath(engine, profile)
            error = cls.relative_path_error(complete)
        except ValueError as exc:
            error = str(exc)
        if error:
            result.update(upload_path='', complete_path='', _path_error=error)
        else:
            result.update(upload_path=upload, complete_path=complete + '/' + cls.get_profile_folder(profile))
            up, final = result['upload_path'], result['complete_path']
            if up == final or up.startswith(final + '/') or final.startswith(up + '/'):
                result['_path_error'] = '임시 업로드 경로와 최종 이동 경로가 같거나 서로 포함되지 않도록 설정해주세요.'
        return result

    @staticmethod
    def get_download_source_subpath(item, fallback=''):
        source = (item.upload_state or {}).get('download_source') or {}
        if (source and source.get('engine_name') == item.current_engine_name and
                str(source.get('task_id')) == str(item.engine_task_id)):
            return source.get('upload_path', '')
        return item.gdrive_upload_path if item.gdrive_upload_path is not None else fallback

    @classmethod
    def get_profile_destination(cls, profile: dict | None, engine_name: str = '', item=None, engine_config: dict | None = None) -> dict:
        """전송 스냅샷·단건 지정 목적지 또는 현재 엔진 기본값과 오버라이드를 해석한다."""
        if item is not None:
            saved = (item.upload_state or {}).get('destination_snapshot')
            if isinstance(saved, dict):
                return dict(saved)
            routing = (item.upload_state or {}).get('routing') or {}
            if isinstance(routing.get('destination'), dict):
                destination = dict(routing['destination'])
                destination.setdefault('type', '')
                return destination
        profile = profile or {}
        chain = (item.priority_chain if item is not None else None) or profile.get('priority_chain') or []
        index = (item.current_engine_index or 0) if item is not None else 0
        engine_name = engine_name or (item.current_engine_name if item is not None else '') or (chain[index] if 0 <= index < len(chain) else '')
        engine = (cls.get_downloader_by_name(engine_name) or {}) if engine_config is None else engine_config
        return cls.get_engine_destination(engine, profile, engine_name)

    @classmethod
    def get_engine_destination(cls, engine, profile, engine_name=''):
        """저장 전 후보 엔진도 런타임과 같은 규칙으로 해석한다."""
        destination = dict(engine.get('default_destination') or {})
        overrides = profile.get('destination_by_engine') or {}
        target = str(engine_name or engine.get('name') or '').strip().lower()
        for name, override in overrides.items():
            if isinstance(override, str):
                override = {'type': override}
            if str(name).strip().lower() == target and isinstance(override, dict):
                if override.get('type', destination.get('type')) != destination.get('type'):
                    destination = {}
                destination.update(override)
                break
        destination.setdefault('type', '')
        return cls.resolve_destination_paths(destination, engine, profile)

    @classmethod
    def get_upload_options(cls, profile: dict | None, engine_name: str = '', item=None) -> dict:
        """중앙 목적지 해석 결과를 사용해 전송 스위치의 상속을 적용한다."""
        if item is not None and (item.upload_state or {}).get('upload_options_snapshot'):
            return dict(item.upload_state['upload_options_snapshot'])
        profile = profile or {}
        chain = (item.priority_chain if item is not None else None) or profile.get('priority_chain') or []
        index = (item.current_engine_index or 0) if item is not None else 0
        engine_name = engine_name or (item.current_engine_name if item is not None else '') or (chain[index] if 0 <= index < len(chain) else '')
        engine = cls.get_downloader_by_name(engine_name) or {}
        keys = {'use_gdrive_pool': 'download_use_gdrive_pool',
                'use_impersonate': 'download_gdrive_use_impersonate',
                'use_local_staging': 'download_use_local_staging'}
        options = {key: P.ModelSetting.get_bool(global_key) for key, global_key in keys.items()}
        modes = {'direct': (False, False), 'rotation': (True, False), 'impersonate': (True, True)}
        destination = cls.get_profile_destination(profile, engine_name, item)
        for layer in (engine, destination):
            if layer.get('gdrive_mode') in modes:
                options['use_gdrive_pool'], options['use_impersonate'] = modes[layer['gdrive_mode']]
            for key in keys:
                if layer.get(key) is not None:
                    options[key] = str(layer[key]).lower() in ('true', 'on', '1')
            if layer.get('type') == 'gdrive_rotation':
                options['use_gdrive_pool'] = True
        if destination.get('type') == 'gdrive_rotation':
            options['use_gdrive_pool'] = True
        if destination.get('type') == 'remote_worker':
            options['use_local_staging'] = False
        return options

    @classmethod
    def validate_destination(cls, destination):
        from .util_download import DownloadUtil
        if isinstance(destination, dict) and destination.get('_path_error'):
            return destination['_path_error']
        if not isinstance(destination, dict) or not destination.get('type'):
            return '목적지 유형이 미설정입니다. 엔진의 기본 목적지 또는 엔진별 오버라이드를 설정해주세요.'
        schema = next((s for s in DownloadUtil.get_transporter_schemas()
                       if s['transporter_id'] == destination['type']), None)
        if schema is None:
            return '등록되지 않은 이송 핸들러입니다. 스크립트와 목적지 설정을 확인해주세요.'
        required = ['upload_path', 'complete_path'] if destination['type'] in ('rclone_simple', 'gdrive_rotation', 'remote_worker') else []
        if any(not isinstance(destination.get(key), str) for key in required):
            return '목적지 필수 경로 또는 필수 설정이 누락되었습니다.'
        if required:
            error = cls.relative_path_error(destination['upload_path']) or cls.relative_path_error(destination['complete_path'])
            if error:
                return error
            upload, complete = destination['upload_path'], destination['complete_path']
            if upload == complete or upload.startswith(complete + '/') or complete.startswith(upload + '/'):
                return '임시 업로드 경로와 최종 이동 경로가 같거나 서로 포함되지 않도록 설정해주세요.'
        if destination['type'] == 'local' and destination.get('complete_path') is not None and not isinstance(destination['complete_path'], str):
            return '목적지 필수 경로 또는 필수 설정이 누락되었습니다.'
        required += [f['name'] for f in schema.get('config_schema', []) if f.get('required')]
        if any(destination.get(key) is None or (isinstance(destination.get(key), str) and not destination[key].strip()) for key in required):
            return '목적지 필수 경로 또는 필수 설정이 누락되었습니다.'
        return ''

    @classmethod
    def get_item_profile(cls, item):
        routing = (item.upload_state or {}).get('routing') or {}
        if 'profile_name' in routing:
            if not routing['profile_name']:
                return {'priority_chain': item.priority_chain or []}
            return next((p for p in cls.get_download_profiles() if p.get('name') == routing['profile_name']), {})
        return cls.get_download_profile_by_feed(item.feed_name) or {}

    @classmethod
    def pin_transfer_destination(cls, item, profile=None, destination=None, options=None):
        """전송 시작 전 경로와 스위치를 체크포인트에 고정한다."""
        if (item.upload_state or {}).get('destination_snapshot'):
            return
        profile = cls.get_item_profile(item) if profile is None else profile
        if destination is None:
            destination = cls.get_profile_destination(profile, item.current_engine_name, item)
        if options is None:
            options = cls.get_upload_options(profile, item.current_engine_name, item)
        item.upload_state = dict(item.upload_state or {}, destination_snapshot=destination, upload_options_snapshot=options)

    @staticmethod
    def clear_transfer_state(item):
        routing = (item.upload_state or {}).get('routing')
        item.upload_state = {'routing': routing} if routing else None

    @classmethod
    def save_download_profile(cls, item: dict) -> str:
        item = dict(item)
        item.pop('destination', None)
        item.pop('destination_mode', None)
        folder = item.get('destination_subfolder')
        if folder is None or (isinstance(folder, str) and not folder.strip()):
            item.pop('destination_subfolder', None)
        error = cls.relative_path_error(cls.get_profile_folder(item), single=True)
        if error:
            return error
        overrides = {}
        for engine_name, value in (item.get('destination_by_engine') or {}).items():
            if isinstance(value, dict):
                value = dict(value)
                effective_type = value.get('type', ((cls.get_downloader_by_name(engine_name) or {}).get('default_destination') or {}).get('type'))
                if effective_type in ('rclone_simple', 'gdrive_rotation', 'remote_worker'):
                    value.pop('upload_path', None)
                    value.pop('complete_path', None)
                    value.pop('staging_path', None)
            overrides[engine_name] = value
        item['destination_by_engine'] = overrides
        if not item.get('priority_chain'):
            return '우선순위 엔진을 설정해주세요.'
        for engine_name in item.get('priority_chain') or []:
            error = cls.validate_destination(cls.get_profile_destination(item, engine_name))
            if error:
                return error
        data = cls.load_yaml()
        profiles = data.get('DOWNLOAD_PROFILES', [])
        name = item.get('name', '').strip()
        if not name:
            return 'empty_name'

        for idx, p in enumerate(profiles):
            if str(p.get('name', '')).strip().lower() == name.lower():
                profiles[idx] = item
                data['DOWNLOAD_PROFILES'] = profiles
                if not cls.save_yaml(data):
                    return 'save_failed'
                logger.info(f"[FeederUtil] 다운로드 프로필 갱신: {name}")
                return 'success_update'

        profiles.append(item)
        data['DOWNLOAD_PROFILES'] = profiles
        if not cls.save_yaml(data):
            return 'save_failed'
        logger.info(f"[FeederUtil] 신규 다운로드 프로필 추가: {name}")
        return 'success'

    @classmethod
    def delete_download_profile(cls, name: str) -> bool:
        data = cls.load_yaml()
        profiles = data.get('DOWNLOAD_PROFILES', [])
        target = str(name).strip().lower()
        data['DOWNLOAD_PROFILES'] = [p for p in profiles if str(p.get('name', '')).strip().lower() != target]
        cls.save_yaml(data)
        logger.info(f"[FeederUtil] 다운로드 프로필 삭제: {name}")
        return True

    @classmethod
    def add_direct_download(cls, title: str, magnet: str, profile_name: str = '', feed_name: str = 'DIRECT', caller_name: str = 'feeder', custom_chain: list = None, custom_dest_type: str = '', custom_dest_config: dict = None) -> dict:
        """다운로드 큐 직접 추가 공통 처리 로직 (프로필 프리셋 및 커스텀 옵션 지원)"""
        if not magnet:
            return {'ret': 'fail', 'msg': '마그넷/ed2k 링크가 누락되었습니다.'}

        from .model_download import ModelDownload

        infohash = cls.extract_info_hash(magnet)
        if ModelDownload.is_blocked(infohash, magnet):
            return {'ret': 'fail', 'msg': '차단된 콘텐츠입니다. 차단 목록에서 먼저 해제해주세요.'}
        existing = ModelDownload.get_by_infohash(infohash) if infohash else ModelDownload.get_by_magnet(magnet)
        if existing:
            return {'ret': 'exist', 'msg': f'이미 다운로드 큐에 등록된 작업입니다 (상태: {existing.status}).'}

        dl_item = ModelDownload(
            feed_name=feed_name,
            title=title or (infohash or magnet[:30]),
            magnet=magnet,
            infohash=infohash
        )

        selected_profile = next((p for p in cls.get_download_profiles() if p.get('name') == profile_name), None) if profile_name else {}
        if profile_name and selected_profile is None:
            return {'ret': 'fail', 'msg': '선택한 다운로드 프로필을 찾을 수 없습니다.'}
        selected_profile = selected_profile or {}
        dl_item.priority_chain = list(custom_chain if custom_chain is not None else selected_profile.get('priority_chain') or
                                      [d['name'] for d in cls.get_downloaders() if d.get('enabled', True)])
        routing = {'profile_name': profile_name}
        if custom_chain is not None:
            routing['priority_chain'] = list(dl_item.priority_chain)
        if custom_dest_type:
            routing['destination'] = dict(custom_dest_config or {}, type=custom_dest_type)
        if routing:
            dl_item.upload_state = {'routing': routing}
        dest = cls.get_profile_destination(selected_profile, item=dl_item)
        error = cls.validate_destination(dest)
        if error:
            return {'ret': 'fail', 'msg': error}
        dl_item.destination_type = dest['type']
        dl_item.gdrive_upload_path = dest.get('upload_path', '')
        dl_item.gdrive_complete_path = dest.get('complete_path', '')
        dl_item.gdrive_remote_id = dest.get('shared_drive_id', '')

        dl_item.status = 'pending'
        db.session.add(dl_item)
        db.session.commit()
        chain_desc = ' -> '.join(dl_item.priority_chain) if dl_item.priority_chain else '기본'
        logger.info(f"[{caller_name}] 다운로드 큐 직접 추가: {title} (체인: {chain_desc}, 목적지: {dl_item.destination_type})")
        return {'ret': 'success', 'msg': '다운로드 큐에 성공적으로 등록되었습니다.'}

    # GDrive Accounts
    @classmethod
    def get_gdrive_accounts(cls) -> list[dict]:
        return cls.load_yaml().get('GDRIVE_ACCOUNTS', [])

    @classmethod
    def save_gdrive_accounts(cls, accounts: list[dict]) -> bool:
        data = cls.load_yaml()
        data['GDRIVE_ACCOUNTS'] = accounts
        return cls.save_yaml(data)


FeederUtil.ensure_custom_dirs()
