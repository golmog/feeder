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
import requests
import subprocess
import unicodedata
from datetime import datetime, timedelta
from sqlalchemy import func


from .setup import *


class FeederUtil:
    """플러그인 공통 유틸리티 및 YAML 설정 통합 관리자"""

    CONFIG_FILEPATH = os.path.join(path_data, 'db', 'feeder_settings.yaml')
    CUSTOM_DIR = os.path.join(path_data, 'db', 'feeder_custom')
    ENGINES_DIR = os.path.join(CUSTOM_DIR, 'engines')
    TRANSPORTERS_DIR = os.path.join(CUSTOM_DIR, 'transporters')
    SITES_DIR = os.path.join(CUSTOM_DIR, 'sites')

    # --------------------------------------------------------------------------
    # 경로 및 시스템 헬퍼
    # --------------------------------------------------------------------------
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
    def get_rclone_extra_options(cls) -> list[str]:
        """설정된 Rclone 확장 옵션 문자열을 안전하게 분리하여 리스트로 반환"""
        import shlex
        raw_opt = P.ModelSetting.get('download_rclone_extra_options') if P.ModelSetting else ''
        if not raw_opt:
            raw_opt = '--timeout 30m'
        try:
            return shlex.split(raw_opt.strip())
        except Exception as ex:
            logger.debug(f"[FeederUtil] Rclone 옵션 shlex 파싱 예외 (단순 분리 사용): {ex}")
            return [x.strip() for x in raw_opt.split() if x.strip()]

    @classmethod
    def apply_download_status_filter(cls, query, magnet_column, status_filter: str):
        """마그넷 컬럼과 ModelDownload 상태 간의 서브쿼리 필터 공통 적용"""
        if status_filter not in ['download_completed', 'download_active', 'not_downloaded']:
            return query

        from .model_download import ModelDownload

        if status_filter == 'download_completed':
            target_statuses = ['completed']
        elif status_filter == 'download_active':
            target_statuses = ['downloading', 'pending', 'local_staging', 'uploading', 'colab_transferring']
        else:
            target_statuses = None

        if target_statuses:
            subq = db.session.query(ModelDownload.id).filter(
                ModelDownload.status.in_(target_statuses),
                ModelDownload.infohash.isnot(None),
                magnet_column.like(func.concat('%', ModelDownload.infohash, '%'))
            ).exists()
            return query.filter(subq)
        else:
            subq = db.session.query(ModelDownload.id).filter(
                ModelDownload.infohash.isnot(None),
                magnet_column.like(func.concat('%', ModelDownload.infohash, '%'))
            ).exists()
            return query.filter(~subq)

    @classmethod
    def attach_download_info(cls, items: list) -> list[dict]:
        """수집/피드 목록 아이템에 다운로드 상태(download_info) 매핑 주입 공통 로직"""
        if not items:
            return []

        from .model_download import ModelDownload

        all_page_hashes = {}
        for item_obj in items:
            m_list = cls.split_magnets(item_obj.magnet)
            for m_str in m_list:
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
                dl_map[d_hash] = {'status': d_status, 'id': d_id}

        item_dicts = []
        for item_obj in items:
            d = item_obj.as_dict()
            d_links = d.get('magnet', [])
            matched_dl = None
            for m_str in d_links:
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
                    crawlers[idx].update(item)
                    data['CRAWLERS'] = crawlers
                    cls.save_yaml(data)
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
        cls.save_yaml(data)
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
        data = cls.load_yaml()
        feeds = data.get('FEEDS', [])
        target_id = item.get('id')

        sources = item.get('sources', [])
        normalized_sources = []
        for src in sources:
            b_val = src.get('board', '')
            s_val = src.get('subcat', '')
            _, _, f_key = cls.parse_board_info(b_val, s_val)
            normalized_sources.append({
                'site': src.get('site', ''),
                'board': str(b_val),
                'subcat': str(s_val) if s_val else '',
                'full_board_key': f_key
            })
        item['sources'] = normalized_sources

        if target_id is not None and int(target_id) > 0:
            for idx, f in enumerate(feeds):
                if str(f.get('id')) == str(target_id):
                    feeds[idx].update(item)
                    data['FEEDS'] = feeds
                    cls.save_yaml(data)
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
        cls.save_yaml(data)
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
        data = cls.load_yaml()
        items = data.get('DOWNLOADERS', [])
        target_name = item.get('name', '').strip()
        if not target_name:
            return 'empty_name'

        for idx, d in enumerate(items):
            if str(d.get('name', '')).strip().lower() == target_name.lower():
                items[idx].update(item)
                data['DOWNLOADERS'] = items
                cls.save_yaml(data)
                logger.info(f"[FeederUtil] 다운로더 설정 갱신: {target_name}")
                return 'success_update'

        items.append(item)
        data['DOWNLOADERS'] = items
        cls.save_yaml(data)
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
    def save_download_profile(cls, item: dict) -> str:
        data = cls.load_yaml()
        profiles = data.get('DOWNLOAD_PROFILES', [])
        name = item.get('name', '').strip()
        if not name:
            return 'empty_name'

        for idx, p in enumerate(profiles):
            if str(p.get('name', '')).strip().lower() == name.lower():
                profiles[idx].update(item)
                data['DOWNLOAD_PROFILES'] = profiles
                cls.save_yaml(data)
                logger.info(f"[FeederUtil] 다운로드 프로필 갱신: {name}")
                return 'success_update'

        profiles.append(item)
        data['DOWNLOAD_PROFILES'] = profiles
        cls.save_yaml(data)
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
    def add_direct_download(cls, title: str, magnet: str, profile_name: str = '', feed_name: str = 'DIRECT', caller_name: str = 'feeder') -> dict:
        """다운로드 큐 직접 추가 공통 처리 로직"""
        if not magnet:
            return {'ret': 'fail', 'msg': '마그넷/ed2k 링크가 누락되었습니다.'}

        from .model_download import ModelDownload

        infohash = cls.extract_info_hash(magnet)
        existing = ModelDownload.get_by_infohash(infohash) if infohash else ModelDownload.get_by_magnet(magnet)
        if existing:
            return {'ret': 'exist', 'msg': f'이미 다운로드 큐에 등록된 작업입니다 (상태: {existing.status}).'}

        dl_item = ModelDownload(
            feed_name=feed_name,
            title=title or (infohash or magnet[:30]),
            magnet=magnet,
            infohash=infohash
        )

        selected_profile = None
        if profile_name:
            for p in cls.get_download_profiles():
                if p.get('name') == profile_name:
                    selected_profile = p
                    break

        if selected_profile:
            dl_item.priority_chain = list(selected_profile.get('priority_chain', []))
            dest = selected_profile.get('destination', {})
            dl_item.destination_type = dest.get('type', 'local')
            dl_item.gdrive_upload_path = dest.get('upload_path', '')
            dl_item.gdrive_complete_path = dest.get('complete_path', '')
            dl_item.gdrive_remote_id = dest.get('shared_drive_id', '')
        else:
            enabled_downloaders = [d['name'] for d in cls.get_downloaders() if d.get('enabled', True)]
            dl_item.priority_chain = enabled_downloaders
            dl_item.destination_type = 'local'

        dl_item.status = 'pending'
        db.session.add(dl_item)
        db.session.commit()
        chain_desc = ' -> '.join(dl_item.priority_chain) if dl_item.priority_chain else '기본'
        logger.info(f"[{caller_name}] 다운로드 큐 직접 추가: {title} (체인: {chain_desc})")
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
