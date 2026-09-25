# -*- coding: utf-8 -*-
import io
import os
import re
import sys
import time
import math
import yaml
import json
import base64
from datetime import datetime, timedelta

from .setup import *
from .util_crawl import (
    get_ddns, get_system_apikey, clean_xml_string,
    extract_info_hash, split_magnets, get_paging_info,
    get_tmp_dir
)

CONFIG_FILEPATH = os.path.join(path_data, 'db', 'feeder_settings.yaml')


class FeedConfigUtil:

    @classmethod
    def get_filepath(cls) -> str:
        return CONFIG_FILEPATH

    @classmethod
    def load_yaml(cls) -> dict:
        if not os.path.exists(CONFIG_FILEPATH):
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
            with open(CONFIG_FILEPATH, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}

            if 'FEEDS' not in data or not isinstance(data['FEEDS'], list):
                data['FEEDS'] = []
            if 'GLOBAL' not in data or not isinstance(data['GLOBAL'], dict):
                data['GLOBAL'] = {}
            if 'CRAWLERS' not in data or not isinstance(data['CRAWLERS'], list):
                data['CRAWLERS'] = []
            if 'DOWNLOADERS' not in data or not isinstance(data['DOWNLOADERS'], list):
                data['DOWNLOADERS'] = []
            if 'DOWNLOAD_PROFILES' not in data or not isinstance(data['DOWNLOAD_PROFILES'], list):
                data['DOWNLOAD_PROFILES'] = []
            if 'GDRIVE_ACCOUNTS' not in data or not isinstance(data['GDRIVE_ACCOUNTS'], list):
                data['GDRIVE_ACCOUNTS'] = []

            from .task_crawl import TaskCrawl
            for f in data['FEEDS']:
                if 'sources' not in f or not isinstance(f['sources'], list):
                    f['sources'] = []
                for src in f['sources']:
                    b_val = src.get('board', '')
                    s_val = src.get('subcat', '')
                    _, _, f_key = TaskCrawl.parse_board_info(b_val, s_val)
                    src['full_board_key'] = f_key

            return data
        except Exception as e:
            logger.error(f"[FeedConfig] YAML 로드 실패: {e}")
            return {'GLOBAL': {}, 'CRAWLERS': [], 'FEEDS': [], 'DOWNLOADERS': [], 'DOWNLOAD_PROFILES': [], 'GDRIVE_ACCOUNTS': []}

    @classmethod
    def save_yaml(cls, data: dict) -> bool:
        try:
            os.makedirs(os.path.dirname(CONFIG_FILEPATH), exist_ok=True)
            raw_yaml = yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
            formatted_yaml = re.sub(r'\n([A-Z0-9_]+:)', r'\n\n\1', raw_yaml).lstrip('\n')
            with open(CONFIG_FILEPATH, 'w', encoding='utf-8') as f:
                f.write(formatted_yaml)
            return True
        except Exception as e:
            logger.error(f"[FeedConfig] YAML 저장 실패: {e}")
            return False

    @classmethod
    def get_feeds(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('FEEDS', [])

    @classmethod
    def get_feed(cls, feed_id):
        for f in cls.get_feeds():
            if str(f.get('id')) == str(feed_id):
                return f
        return None

    @classmethod
    def get_feed_by_name(cls, feed_name: str):
        import unicodedata
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

        from .task_crawl import TaskCrawl
        sources = item.get('sources', [])
        normalized_sources = []
        for src in sources:
            b_val = src.get('board', '')
            s_val = src.get('subcat', '')
            _, _, f_key = TaskCrawl.parse_board_info(b_val, s_val)
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
                    logger.info(f"[FeedConfig] 피드 수정 완료: ID={target_id}, Name={item.get('name')}")
                    return 'success_update'
            return 'not_found'

        target_name = str(item.get('name', '')).strip().lower()
        for f in feeds:
            if str(f.get('name', '')).strip().lower() == target_name:
                logger.warning(f"[FeedConfig] 동일 이름의 피드 중복: {item.get('name')}")
                return 'already_exist'

        max_id = max([int(f.get('id', 0)) for f in feeds], default=0)
        item['id'] = max_id + 1
        feeds.append(item)
        data['FEEDS'] = feeds
        cls.save_yaml(data)
        logger.info(f"[FeedConfig] 신규 피드 추가 완료: ID={item['id']}, Name={item.get('name')}")
        return 'success'

    @classmethod
    def delete_feed(cls, target_id) -> bool:
        data = cls.load_yaml()
        feeds = data.get('FEEDS', [])
        data['FEEDS'] = [f for f in feeds if str(f.get('id')) != str(target_id)]
        cls.save_yaml(data)
        logger.info(f"[FeedConfig] 피드 삭제 완료: ID={target_id}")
        return True

    @classmethod
    def get_global(cls) -> dict:
        data = cls.load_yaml()
        return data.get('GLOBAL', {})

    # 다운로더 엔진 (DOWNLOADERS) 설정 관리
    @classmethod
    def get_downloaders(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('DOWNLOADERS', [])

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
                logger.info(f"[FeedConfig] 다운로더 설정 갱신: {target_name}")
                return 'success_update'

        items.append(item)
        data['DOWNLOADERS'] = items
        cls.save_yaml(data)
        logger.info(f"[FeedConfig] 신규 다운로더 추가: {target_name}")
        return 'success'

    @classmethod
    def delete_downloader(cls, name: str) -> bool:
        data = cls.load_yaml()
        items = data.get('DOWNLOADERS', [])
        target = str(name).strip().lower()
        data['DOWNLOADERS'] = [d for d in items if str(d.get('name', '')).strip().lower() != target]
        cls.save_yaml(data)
        logger.info(f"[FeedConfig] 다운로더 삭제: {name}")
        return True

    # 다운로드 라우팅 프로필 (DOWNLOAD_PROFILES) 설정 관리
    @classmethod
    def get_download_profiles(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('DOWNLOAD_PROFILES', [])

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
                logger.info(f"[FeedConfig] 다운로드 프로필 갱신: {name}")
                return 'success_update'

        profiles.append(item)
        data['DOWNLOAD_PROFILES'] = profiles
        cls.save_yaml(data)
        logger.info(f"[FeedConfig] 신규 다운로드 프로필 추가: {name}")
        return 'success'

    # 구글 드라이브 계정 풀 (GDRIVE_ACCOUNTS) 설정 관리
    @classmethod
    def get_gdrive_accounts(cls) -> list[dict]:
        data = cls.load_yaml()
        return data.get('GDRIVE_ACCOUNTS', [])

    @classmethod
    def save_gdrive_accounts(cls, accounts: list[dict]) -> bool:
        data = cls.load_yaml()
        data['GDRIVE_ACCOUNTS'] = accounts
        return cls.save_yaml(data)


class FeedFilter:

    RESOLUTION_MAP = {
        '4320p': 4320, '8k': 4320,
        '2160p': 2160, '4k': 2160, 'uhd': 2160,
        '1080p': 1080, '1080i': 1080, 'fhd': 1080,
        '720p': 720, 'hd': 720,
        '576p': 576, '480p': 480, 'sd': 480,
        '360p': 360,
    }

    @classmethod
    def detect_resolution(cls, item: dict) -> int | None:
        texts = [item.get('title', '')]
        if item.get('files'):
            for f in item['files']:
                if isinstance(f, (list, tuple)) and len(f) > 1 and f[1]:
                    texts.append(str(f[1]))
                elif isinstance(f, str):
                    texts.append(f)

        t_info = item.get('torrent_info')
        if isinstance(t_info, str):
            try:
                t_info = json.loads(t_info)
            except Exception:
                t_info = None
        if isinstance(t_info, list):
            for t in t_info:
                if isinstance(t, dict) and t.get('name'):
                    texts.append(str(t['name']))

        if item.get('download'):
            for d in item['download']:
                if isinstance(d, dict) and d.get('filename'):
                    texts.append(d['filename'])

        combined_text = " ".join([t for t in texts if t])
        if not combined_text:
            return None

        if re.search(r'\b(4320p|8k)\b', combined_text, re.IGNORECASE):
            return 4320
        if re.search(r'\b(2160p|4k|uhd)\b', combined_text, re.IGNORECASE):
            return 2160
        if re.search(r'\b(1080p|1080i|fhd)\b', combined_text, re.IGNORECASE):
            return 1080
        if re.search(r'\b(720p)\b', combined_text, re.IGNORECASE):
            return 720
        if re.search(r'\b(576p|480p|sd)\b', combined_text, re.IGNORECASE):
            return 480
        if re.search(r'\b(360p)\b', combined_text, re.IGNORECASE):
            return 360

        m = re.search(r'\b\d{3,4}x(?P<res>\d{3,4})\b', combined_text, re.IGNORECASE)
        if m:
            res_val = int(m.group('res'))
            if res_val >= 2100: return 2160
            if res_val >= 1050: return 1080
            if res_val >= 700: return 720
            if res_val >= 450: return 480

        return None

    @classmethod
    def matches_quality(cls, item_res: int | None, requirement: str) -> bool:
        if item_res is None:
            return False

        req = str(requirement).strip().lower()

        if req.endswith('+'):
            base_res = cls.RESOLUTION_MAP.get(req[:-1].strip())
            if base_res:
                return item_res >= base_res

        m_op = re.match(r'^(>=|<=|>|<)(.+)$', req)
        if m_op:
            op, target = m_op.group(1), m_op.group(2).strip()
            base_res = cls.RESOLUTION_MAP.get(target)
            if base_res:
                if op == '>=': return item_res >= base_res
                if op == '<=': return item_res <= base_res
                if op == '>': return item_res > base_res
                if op == '<': return item_res < base_res

        if '-' in req:
            parts = req.split('-', 1)
            r_min = cls.RESOLUTION_MAP.get(parts[0].strip())
            r_max = cls.RESOLUTION_MAP.get(parts[1].strip())
            if r_min and r_max:
                return min(r_min, r_max) <= item_res <= max(r_min, r_max)

        base_res = cls.RESOLUTION_MAP.get(req)
        if base_res:
            return item_res == base_res

        return False

    @classmethod
    def parse_rule(cls, rule_item) -> tuple[re.Pattern | None, list[str]]:
        pattern = None
        raw_from = 'title'

        if isinstance(rule_item, str):
            pattern = rule_item.strip()
        elif isinstance(rule_item, dict):
            if 'pattern' in rule_item or 'regexp' in rule_item:
                pattern = rule_item.get('pattern') or rule_item.get('regexp')
                raw_from = rule_item.get('from', 'title')
            else:
                pattern = list(rule_item.keys())[0]
                val = rule_item[pattern]
                raw_from = val.get('from', 'title') if isinstance(val, dict) else 'title'
        else:
            return None, []

        if not pattern:
            return None, []

        fields = [str(f).strip().lower() for f in raw_from] if isinstance(raw_from, list) else [str(raw_from).strip().lower()]

        try:
            compiled = re.compile(str(pattern), re.IGNORECASE)
            return compiled, fields
        except re.error as e:
            logger.warning(f"[FeedFilter] 올바르지 않은 정규식 패턴 '{pattern}': {e}")
            return None, []

    @classmethod
    def get_field_values(cls, item: dict, field_name: str) -> list[str]:
        values = []
        target = field_name.lower()

        if target in ['title', 'all', 'filename', 'file']:
            if item.get('title'):
                values.append(str(item['title']))
            if item.get('files'):
                for f in item['files']:
                    if isinstance(f, (list, tuple)) and len(f) > 1 and f[1]:
                        values.append(str(f[1]))
            t_info = item.get('torrent_info')
            if isinstance(t_info, str):
                try:
                    t_info = json.loads(t_info)
                except Exception:
                    t_info = None
            if isinstance(t_info, list):
                for t in t_info:
                    if isinstance(t, dict) and t.get('name'):
                        values.append(str(t['name']))

        if target in ['link', 'url', 'magnet', 'all']:
            if item.get('url'):
                values.append(str(item['url']))
            if item.get('magnet'):
                mags = item['magnet'] if isinstance(item['magnet'], list) else [item['magnet']]
                values.extend([str(m) for m in mags if m])
            if item.get('files'):
                for f in item['files']:
                    if isinstance(f, (list, tuple)) and len(f) > 0 and f[0]:
                        values.append(str(f[0]))
            if item.get('download'):
                downs = item['download'] if isinstance(item['download'], list) else [item['download']]
                for d in downs:
                    if isinstance(d, dict) and d.get('link'):
                        values.append(str(d['link']))
                    elif isinstance(d, (list, tuple)) and len(d) > 0:
                        values.append(str(d[0]))
        return values

    @classmethod
    def check_match(cls, item: dict, regex: re.Pattern, fields: list[str]) -> bool:
        for f in fields:
            vals = cls.get_field_values(item, f)
            for v in vals:
                if regex.search(v):
                    return True
        return False

    @classmethod
    def evaluate(cls, item: dict, feed_cfg: dict = None, global_cfg: dict = None) -> tuple[bool, str]:
        feed_dict = feed_cfg if isinstance(feed_cfg, dict) else (vars(feed_cfg) if feed_cfg else {})
        glob_dict = global_cfg if global_cfg is not None else FeedConfigUtil.get_global()

        glob_regexp = glob_dict.get('regexp', {}) if isinstance(glob_dict, dict) else {}
        for r in (glob_regexp.get('reject') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return False, f"GLOBAL reject: '{comp.pattern}'"

        feed_regexp = feed_dict.get('regexp', {}) if isinstance(feed_dict.get('regexp'), dict) else {}
        for r in (feed_regexp.get('reject') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return False, f"FEED reject: '{comp.pattern}'"

        target_quality = feed_dict.get('quality')
        if target_quality is None and isinstance(glob_dict, dict):
            target_quality = glob_dict.get('quality')

        if target_quality:
            req_list = target_quality if isinstance(target_quality, list) else [target_quality]
            item_res = cls.detect_resolution(item)
            if item_res is None:
                return False, f"화질 식별 불가 (요구: {target_quality})"
            matched_quality = any(cls.matches_quality(item_res, req) for req in req_list)
            if not matched_quality:
                return False, f"화질 조건 불일치 ({item_res}p != {target_quality})"

        glob_re_ex = glob_regexp.get('reject_excluding') or []
        if glob_re_ex:
            matched_any = False
            for r in glob_re_ex:
                comp, fields = cls.parse_rule(r)
                if comp and cls.check_match(item, comp, fields):
                    matched_any = True
                    break
            if not matched_any:
                return False, "GLOBAL reject_excluding 조건 불일치"

        feed_re_ex = feed_regexp.get('reject_excluding') or []
        if feed_re_ex:
            matched_any = False
            for r in feed_re_ex:
                comp, fields = cls.parse_rule(r)
                if comp and cls.check_match(item, comp, fields):
                    matched_any = True
                    break
            if not matched_any:
                return False, "FEED reject_excluding 조건 불일치"

        for r in (glob_regexp.get('accept') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return True, f"GLOBAL accept: '{comp.pattern}'"

        for r in (feed_regexp.get('accept') or []):
            comp, fields = cls.parse_rule(r)
            if comp and cls.check_match(item, comp, fields):
                return True, f"FEED accept: '{comp.pattern}'"

        accept_all_flag = feed_dict.get('accept_all')
        if accept_all_flag is None and isinstance(glob_dict, dict):
            accept_all_flag = glob_dict.get('accept_all')
        if str(accept_all_flag).lower() in ['true', 'yes', '1', 'on']:
            return True, "accept_all 허용"

        has_accept_rules = bool(glob_regexp.get('accept') or feed_regexp.get('accept'))
        if has_accept_rules:
            return False, "accept 조건 미충족"

        return True, "기본 허용 (미거부 항목)"


class FeedRssFileWriter:

    @classmethod
    def save_rss_file(cls, feed_cfg: dict) -> bool:
        try:
            feed = feed_cfg if isinstance(feed_cfg, dict) else (vars(feed_cfg) if feed_cfg else {})

            global_make = P.ModelSetting.get_bool('feed_make_rss_file') if P.ModelSetting else False
            feed_use = str(feed.get('use_rss_file', False)).lower() in ['true', 'on', '1']
            if not (global_make and feed_use):
                return False

            save_dir = feed.get('rss_file_path') or (P.ModelSetting.get('feed_rss_file_path') if P.ModelSetting else '')
            save_dir = save_dir.strip() if save_dir else ''
            if not save_dir:
                logger.warning(f"[FeedRssFile] [{feed.get('name')}] RSS 파일 저장 경로 미설정")
                return False

            default_filename = f"{feed.get('name', 'feed')}.xml"
            filename = (feed.get('rss_file') or default_filename).strip()
            if not filename.lower().endswith('.xml'):
                filename += '.xml'

            try:
                days_val = int(feed.get('rss_file_days') or P.ModelSetting.get('feed_rss_file_days') or 14)
            except Exception:
                days_val = 14

            try:
                items_val = int(feed.get('rss_file_items') or P.ModelSetting.get('feed_rss_file_items') or P.ModelSetting.get('feed_feed_count') or 100)
            except Exception:
                items_val = 100

            sources = feed.get('sources', [])
            if not sources:
                return False

            from .model_crawl import ModelCrawlItem
            from sqlalchemy import and_, or_
            source_conditions = []
            for src in sources:
                s_name = src.get('site')
                b_name = src.get('full_board_key') or src.get('board')
                if s_name and b_name:
                    source_conditions.append(and_(ModelCrawlItem.site == s_name, ModelCrawlItem.board == b_name))

            if not source_conditions:
                return False

            query = db.session.query(ModelCrawlItem).filter(or_(*source_conditions))
            if days_val > 0:
                limit_date = datetime.now() - timedelta(days=days_val)
                query = query.filter(ModelCrawlItem.created_time >= limit_date)

            query_limit = max(items_val * 4, 400)
            candidates = query.order_by(ModelCrawlItem.id.desc()).limit(query_limit).all()

            global_cfg = FeedConfigUtil.get_global()
            seen_magnets = set()
            filtered_records = []
            for bbs in candidates:
                bbs_dict = bbs.as_dict()

                if not bbs_dict.get('magnet') and not bbs_dict.get('files'):
                    continue

                item_hashes = []
                for m in bbs_dict.get('magnet', []):
                    h = extract_info_hash(m) or m
                    if h:
                        item_hashes.append(h)

                if item_hashes and any(h in seen_magnets for h in item_hashes):
                    continue

                is_pass, _ = FeedFilter.evaluate(bbs_dict, feed, global_cfg)
                if is_pass:
                    for h in item_hashes:
                        seen_magnets.add(h)
                    filtered_records.append(bbs)
                    if len(filtered_records) >= items_val:
                        break

            feed_mod = P.get_module('feed')
            if not feed_mod:
                return False

            feed_title = f"{feed.get('name', 'Feeder')} (Shared Feed)"
            xml_content = feed_mod.generate_rss_feed(feed_title, filtered_records, include_apikey=False)

            os.makedirs(save_dir, exist_ok=True)
            target_filepath = os.path.join(save_dir, filename)
            with open(target_filepath, 'w', encoding='utf-8') as f:
                f.write(xml_content)

            logger.info(f"[FeedRssFile] 공유 RSS 파일 생성 완료: {target_filepath} ({len(filtered_records)}개 항목)")
            return True
        except Exception as e:
            logger.error(f"[FeedRssFile] 피드 RSS 파일 생성 실패 ({feed_cfg.get('name')}): {e}")
            return False

    @classmethod
    def save_all_rss_files(cls) -> int:
        count = 0
        for f in FeedConfigUtil.get_feeds():
            if str(f.get('use_rss_file', False)).lower() in ['true', 'on', '1']:
                if cls.save_rss_file(f):
                    count += 1
        return count
