# -*- coding: utf-8 -*-
import io
import os
import re
import sys
import time
import json
from datetime import datetime, timedelta

from xml.etree import ElementTree as ET
import requests
from sqlalchemy import and_, or_

from .setup import *
from .model_crawl import ModelCrawlItem
from .model_feed import ModelFeedItem
from .util_base import FeederUtil


class FeedUtil:
    """피드 필터링(Flexget 규격), 외부/내부 소스 동기화 및 공유 RSS XML 파일 생성 통합 관리자"""

    RESOLUTION_MAP = {
        '4320p': 4320, '8k': 4320,
        '2160p': 2160, '4k': 2160, 'uhd': 2160,
        '1080p': 1080, '1080i': 1080, 'fhd': 1080,
        '720p': 720, 'hd': 720,
        '576p': 576, '480p': 480, 'sd': 480,
        '360p': 360,
    }

    # --------------------------------------------------------------------------
    # 화질 및 정규식 필터링 엔진
    # --------------------------------------------------------------------------
    @classmethod
    def detect_resolution(cls, item: dict) -> int | None:
        """게시글 제목 및 첨부 파일명 기반 해상도 숫자(p) 감지"""
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
        """화질 조건식(1080p+, >=720p, 720p-1080p 등) 부합 여부 판정"""
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
        """정규식 필터 룰 파싱"""
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
            logger.warning(f"[FeedUtil] 올바르지 않은 정규식 패턴 '{pattern}': {e}")
            return None, []

    @classmethod
    def get_field_values(cls, item: dict, field_name: str) -> list[str]:
        """필터 검사용 필드별 값 추출"""
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
        """아이템이 피드 및 전역 필터 조건을 통과하는지 평가"""
        feed_dict = feed_cfg if isinstance(feed_cfg, dict) else (vars(feed_cfg) if feed_cfg else {})
        glob_dict = global_cfg if global_cfg is not None else FeederUtil.get_global()

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

    # --------------------------------------------------------------------------
    # 소스 동기화 (크롤러 DB 및 외부 RSS)
    # --------------------------------------------------------------------------
    @classmethod
    def fetch_remote_rss_items(cls, rss_url: str) -> list[dict]:
        """외부 원격 RSS 피드 파싱"""
        items = []
        try:
            res = requests.get(rss_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20, verify=False)
            if res.status_code != 200 or not res.text:
                logger.warning(f"[FeedUtil] 외부 RSS 응답 실패 ({res.status_code}): {rss_url}")
                return items

            root = ET.fromstring(res.text)
            for item_elem in root.findall('.//item'):
                title = (item_elem.findtext('title') or '').strip()
                link = (item_elem.findtext('link') or '').strip()
                guid = (item_elem.findtext('guid') or '').strip()

                target_url = link or guid
                magnets = []
                if target_url.startswith(('magnet:', 'ed2k://')):
                    magnets.append(target_url)

                enclosure = item_elem.find('enclosure')
                files = []
                if enclosure is not None:
                    enc_url = enclosure.attrib.get('url', '')
                    if enc_url.startswith(('magnet:', 'ed2k://')):
                        magnets.append(enc_url)
                    elif enc_url:
                        enc_name = target_url.split('/')[-1] if target_url else 'attachment'
                        files.append([enc_url, enc_name, 'NONE'])

                if title and (magnets or target_url or files):
                    items.append({
                        'title': title,
                        'url': target_url,
                        'magnet': magnets,
                        'files': files,
                        'torrent_info': None
                    })
        except Exception as e:
            logger.error(f"[FeedUtil] 외부 RSS 파싱 에러 ({rss_url}): {e}")
        return items

    @classmethod
    def sync_feed(cls, feed: dict) -> int:
        """개별 피드에 소속된 크롤러 및 외부 RSS 데이터 동기화"""
        feed_name = feed.get('name')
        if not feed_name:
            return 0

        global_cfg = FeederUtil.get_global()
        sources = feed.get('sources', [])
        added_count = 0


        try:
            # 내부 크롤러 게시판 소스 동기화
            internal_conds = []
            for src in sources:
                s_type = src.get('type') or 'crawl'
                if s_type == 'crawl':
                    s_name = src.get('site')
                    b_name = src.get('full_board_key') or src.get('board')
                    if s_name and b_name:
                        internal_conds.append(and_(ModelCrawlItem.site == s_name, ModelCrawlItem.board == b_name))

            if internal_conds:
                crawl_candidates = db.session.query(ModelCrawlItem).filter(or_(*internal_conds)).order_by(ModelCrawlItem.id.desc()).limit(300).all()
                for bbs in crawl_candidates:
                    b_dict = bbs.as_dict()
                    if not b_dict.get('magnet') and not b_dict.get('files'):
                        continue

                    exists = db.session.query(ModelFeedItem.id).filter_by(feed_name=feed_name, url=bbs.url).first()
                    if exists:
                        continue

                    is_pass, reason = cls.evaluate(b_dict, feed, global_cfg)
                    if is_pass:
                        new_feed_item = ModelFeedItem(
                            feed_name=feed_name,
                            source_type='crawl',
                            source_name=f"{bbs.site}:{bbs.board}"
                        )
                        new_feed_item.title = bbs.title
                        new_feed_item.url = bbs.url
                        new_feed_item.magnet_count = bbs.magnet_count
                        new_feed_item.file_count = bbs.file_count
                        new_feed_item.magnet = bbs.magnet
                        new_feed_item.files = bbs.files
                        new_feed_item.torrent_info = bbs.torrent_info
                        new_feed_item.broadcast_status = bbs.broadcast_status
                        db.session.add(new_feed_item)
                        added_count += 1
                        logger.debug(f"[FeedUtil] [{feed_name}] 크롤러 아이템 적재: '{bbs.title[:35]}'")

            # 외부 RSS 피드 소스 동기화
            for src in sources:
                s_type = src.get('type') or ('rss' if src.get('url') and not src.get('site') else 'crawl')
                if s_type == 'rss' and src.get('url'):
                    rss_url = src.get('url')
                    remote_items = cls.fetch_remote_rss_items(rss_url)
                    for r_item in remote_items:
                        exists = db.session.query(ModelFeedItem.id).filter_by(feed_name=feed_name, url=r_item['url']).first()
                        if exists:
                            continue

                        is_pass, reason = cls.evaluate(r_item, feed, global_cfg)
                        if is_pass:
                            new_feed_item = ModelFeedItem(
                                feed_name=feed_name,
                                source_type='rss',
                                source_name=rss_url
                            )
                            new_feed_item.title = r_item['title']
                            new_feed_item.url = r_item['url']
                            new_feed_item.magnet_count = len(r_item.get('magnet', []))
                            new_feed_item.file_count = len(r_item.get('files', []))
                            new_feed_item.magnet = '\n'.join(r_item.get('magnet', []))
                            if r_item.get('files'):
                                new_feed_item.files = '||'.join(f"{x[0]}|{x[1]}|NONE" for x in r_item['files'])
                            db.session.add(new_feed_item)
                            added_count += 1
                            logger.debug(f"[FeedUtil] [{feed_name}] 외부 RSS 아이템 적재: '{r_item['title'][:35]}'")

            if added_count > 0:
                db.session.commit()
                logger.info(f"[FeedUtil] 피드 [{feed_name}] 동기화 완료: 신규 {added_count}건 DB 적재")
        except Exception as e:
            db.session.rollback()
            logger.error(f"[FeedUtil] 피드 [{feed_name}] 동기화 중 오류: {e}")
        finally:
            try:
                db.session.remove()
            except Exception:
                pass

        return added_count

    @classmethod
    def sync_all_feeds(cls) -> int:
        """모든 활성 피드 동기화 수행"""
        total_added = 0
        for f in FeederUtil.get_feeds():
            total_added += cls.sync_feed(f)
        return total_added

    # --------------------------------------------------------------------------
    # 공유용 RSS XML 파일 생성 및 보관주기(Retention) 관리
    # --------------------------------------------------------------------------
    @classmethod
    def save_rss_file(cls, feed_cfg: dict) -> bool:
        """피드 전용 정적 RSS XML 파일 생성"""
        try:
            feed = feed_cfg if isinstance(feed_cfg, dict) else (vars(feed_cfg) if feed_cfg else {})

            global_make = P.ModelSetting.get_bool('feed_make_rss_file') if P.ModelSetting else False
            feed_use = str(feed.get('use_rss_file', False)).lower() in ['true', 'on', '1']
            if not (global_make and feed_use):
                return False

            feed_name = feed.get('name')
            if not feed_name:
                return False

            # 파일 저장 전 최신 DB 데이터 동기화 선행 실행
            cls.sync_feed(feed)

            save_dir = feed.get('rss_file_path') or (P.ModelSetting.get('feed_rss_file_path') if P.ModelSetting else '')
            save_dir = save_dir.strip() if save_dir else ''
            if not save_dir:
                logger.warning(f"[FeedUtil] [{feed_name}] RSS 파일 저장 경로 미설정")
                return False

            default_filename = f"{feed_name}.xml"
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

            query = db.session.query(ModelFeedItem).filter_by(feed_name=feed_name)
            if days_val > 0:
                limit_date = datetime.now() - timedelta(days=days_val)
                query = query.filter(ModelFeedItem.created_time >= limit_date)

            feed_records = query.order_by(ModelFeedItem.id.desc()).limit(items_val).all()

            feed_mod = P.get_module('feed')
            if not feed_mod:
                return False

            feed_title = f"{feed_name} (Shared Feed)"
            xml_content = feed_mod.generate_rss_feed(feed_title, feed_records, include_apikey=False)

            os.makedirs(save_dir, exist_ok=True)
            target_filepath = os.path.join(save_dir, filename)
            with open(target_filepath, 'w', encoding='utf-8') as f:
                f.write(xml_content)

            logger.info(f"[FeedUtil] 공유 RSS 파일 생성 완료: {target_filepath} ({len(feed_records)}개 항목)")
            return True
        except Exception as e:
            logger.error(f"[FeedUtil] 피드 RSS 파일 생성 실패 ({feed_cfg.get('name')}): {e}")
            return False
        finally:
            try:
                db.session.remove()
            except Exception:
                pass

    @classmethod
    def save_all_rss_files(cls) -> int:
        """설정된 모든 피드의 RSS 파일 일괄 갱신"""
        count = 0
        for f in FeederUtil.get_feeds():
            if str(f.get('use_rss_file', False)).lower() in ['true', 'on', '1']:
                if cls.save_rss_file(f):
                    count += 1
        return count
