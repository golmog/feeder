# -*- coding: utf-8 -*-
import os
import json
import yaml
import traceback
from datetime import datetime, timedelta
from flask import Response, request, jsonify, render_template
from sqlalchemy import and_, or_, desc

from .setup import *
from .model_crawl import ModelCrawlItem
from .model_download import ModelDownload
from .util_crawl import get_ddns, get_system_apikey, clean_xml_string, extract_info_hash, split_magnets
from .util_feed import FeedConfigUtil, FeedFilter, FeedRssFileWriter
from .task_feed import TaskFeedBase

name = 'feed'


class ModuleFeed(PluginModuleBase):

    def __init__(self, P):
        super(ModuleFeed, self).__init__(P, 'setting', name=name, scheduler_desc="Feeder - 피드 관리 및 RSS 발행")
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "10",
            f"{self.name}_feed_count": "100",
            f"{self.name}_make_rss_file": "False",
            f"{self.name}_rss_file_path": "",
            f"{self.name}_rss_file_days": "14",
            f"{self.name}_rss_file_items": "100",
            f"{self.name}_use_proxy": "False",
            f"{self.name}_proxy_url": "",
        }
        self.web_list_model = ModelCrawlItem

    def process_menu(self, page_name, req):
        try:
            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['sub'] = self.name
            arg['current_page'] = page_name
            arg['ddns'] = get_ddns()
            arg['apikey'] = get_system_apikey()
            arg['feeds'] = FeedConfigUtil.get_feeds()
            arg['profiles'] = FeedConfigUtil.get_download_profiles()

            if page_name == 'setting':
                arg['yaml_filepath'] = FeedConfigUtil.get_filepath()
                arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
                arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
                return render_template(f'{P.package_name}_{self.name}_setting.html', arg=arg)

            return render_template(f'{P.package_name}_{self.name}_list.html', arg=arg)
        except Exception as e:
            logger.error(f"[{self.name}] process_menu 에러: {e}")
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def process_ajax(self, sub, req):
        try:
            if sub == 'setting_save':
                ret = super(ModuleFeed, self).process_ajax(sub, req)
                try:
                    if F.scheduler.is_include(self.get_scheduler_name()):
                        F.scheduler.remove_job(self.get_scheduler_name())
                        F.scheduler.add_job_instance(self)
                        logger.info(f"[{self.name}] 설정 저장에 따른 스케줄러 주기 갱신 완료 ({P.ModelSetting.get(f'{self.name}_interval')}분)")
                except Exception as ex:
                    logger.debug(f"[{self.name}] 스케줄러 갱신 예외: {ex}")
                return ret

            if sub == 'web_list':
                res_data = self.feed_web_list(req)
                return jsonify(res_data)

            command = req.form.get('command') or sub
            if command:
                res = self.process_command(command, req.form.get('arg1', ''), req.form.get('arg2', ''), req.form.get('arg3', ''), req)
                if res is not None:
                    return res

            return super(ModuleFeed, self).process_ajax(sub, req)
        except Exception as e:
            logger.error(f"[{self.name}] process_ajax 에러: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'error', 'msg': str(e)})

    def feed_web_list(self, req):
        try:
            import unicodedata
            raw_feed_name = req.form.get('feed_select') or req.values.get('feed_select') or ''
            feed_name = unicodedata.normalize('NFC', raw_feed_name.strip())

            page = int(req.form.get('page', 1))
            page_size = int(req.form.get('page_size', 25))
            search_word = req.form.get('search_word', '').strip().lower()
            status_filter = req.form.get('status_filter', 'all')

            from .util_crawl import get_paging_info

            feed = FeedConfigUtil.get_feed_by_name(feed_name) if feed_name else None
            if not feed:
                feeds = FeedConfigUtil.get_feeds()
                if feeds:
                    feed = feeds[0]
                    if feed_name:
                        logger.warning(f"[{self.name}] 요청된 피드 '{feed_name}'를 찾지 못해 기본 피드 '{feed.get('name')}'로 대체합니다.")
                else:
                    feed = None

            if not feed:
                logger.debug(f"[{self.name}] 등록된 피드가 없어 빈 목록을 반환합니다.")
                return {'success': True, 'list': [], 'paging': get_paging_info(0, 1, page_size)}

            current_feed_name = feed.get('name')
            logger.info(f"[{self.name}] 피드 목록 조회 시작: '{current_feed_name}' (화질 요구: {feed.get('quality') or '무관'}, 정규식 필터: {'사용' if feed.get('regexp') else '미사용'})")

            sources = feed.get('sources', [])
            source_conds = []
            for src in sources:
                s_name = src.get('site')
                b_name = src.get('full_board_key') or src.get('board')
                if s_name and b_name:
                    source_conds.append(and_(ModelCrawlItem.site == s_name, ModelCrawlItem.board == b_name))

            if not source_conds:
                logger.info(f"[{self.name}] 피드 '{current_feed_name}'에 지정된 소스 게시판이 없습니다.")
                return {'success': True, 'list': [], 'paging': get_paging_info(0, 1, page_size)}

            candidates = db.session.query(ModelCrawlItem).filter(or_(*source_conds)).order_by(ModelCrawlItem.id.desc()).all()
            global_cfg = FeedConfigUtil.get_global()

            matched_items = []
            all_hashes = []
            excluded_count = 0

            for bbs in candidates:
                b_dict = bbs.as_dict()
                if not b_dict.get('magnet') and not b_dict.get('files'):
                    continue
                if search_word and search_word not in b_dict.get('title', '').lower():
                    continue

                is_pass, filter_reason = FeedFilter.evaluate(b_dict, feed, global_cfg)
                if is_pass:
                    for m_str in b_dict.get('magnet', []):
                        h = extract_info_hash(m_str)
                        if h:
                            all_hashes.append(h)
                    matched_items.append(b_dict)
                else:
                    excluded_count += 1
                    logger.debug(f"[{self.name}] [{current_feed_name}] 필터 제외: '{bbs.title[:35]}' (사유: {filter_reason})")

            dl_map = {}
            if all_hashes:
                records = db.session.query(ModelDownload.infohash, ModelDownload.status, ModelDownload.id).filter(ModelDownload.infohash.in_(all_hashes)).all()
                for h, st, did in records:
                    dl_map[h] = {'status': st, 'id': did}

            filtered_by_status = []
            for item_dict in matched_items:
                matched_dl = None
                for m_str in item_dict.get('magnet', []):
                    h = extract_info_hash(m_str)
                    if h and h in dl_map:
                        matched_dl = dl_map[h]
                        break
                item_dict['download_info'] = matched_dl

                if status_filter == 'magnet' and not any(m.startswith('magnet:') for m in item_dict.get('magnet', [])):
                    continue
                if status_filter == 'ed2k' and not any(m.startswith('ed2k://') for m in item_dict.get('magnet', [])):
                    continue
                if status_filter == 'download_completed' and (not matched_dl or matched_dl.get('status') != 'completed'):
                    continue
                if status_filter == 'download_active' and (not matched_dl or matched_dl.get('status') not in ['downloading', 'pending', 'local_staging', 'uploading', 'colab_transferring']):
                    continue
                if status_filter == 'not_downloaded' and matched_dl:
                    continue

                filtered_by_status.append(item_dict)

            total_count = len(filtered_by_status)
            paging = get_paging_info(total_count, page, page_size)
            actual_page = paging['current_page']

            start_idx = (actual_page - 1) * page_size
            paged_list = filtered_by_status[start_idx:start_idx + page_size]

            logger.info(f"[{self.name}] 피드 '{current_feed_name}' 필터링 완료: 소스 글 {len(candidates)}건 중 {total_count}건 매칭 (제외: {excluded_count}건, 검색어: '{search_word or '없음'}', 상태: '{status_filter}')")

            return {
                'success': True,
                'list': paged_list,
                'paging': paging
            }
        except Exception as e:
            logger.error(f"[{self.name}] feed_web_list 에러: {e}")
            logger.error(traceback.format_exc())
            return {'success': False, 'list': [], 'paging': None}

    def process_command(self, command, arg1, arg2, arg3, req):
        if command == 'load_feeds':
            feeds = FeedConfigUtil.get_feeds()
            ddns = get_ddns().rstrip('/')
            apikey = get_system_apikey()
            feed_list = []
            for f in feeds:
                info = dict(f)
                info['api'] = f"{ddns}/{P.package_name}/api/feed/rss?name={f.get('name')}&apikey={apikey}"
                info['use_rss_file'] = str(f.get('use_rss_file', False)).lower() in ['true', 'on', '1']
                feed_list.append(info)
            return jsonify({'feeds': feed_list})

        elif command == 'add_feed':
            feed_id = req.form.get('modal_feed_id', '-1').strip()
            name_val = req.form.get('feed_name', '').strip()
            sources_json = req.form.get('sources_json', '[]')
            try:
                sources = json.loads(sources_json)
            except Exception:
                sources = []

            quality = req.form.get('quality', '').strip()
            use_feed_filter = req.form.get('use_feed_filter') in ['True', 'on', 'true', True]
            accept_all = req.form.get('accept_all') in ['True', 'on', 'true', True]
            filter_reject = req.form.get('filter_reject', '').strip()
            filter_accept = req.form.get('filter_accept', '').strip()
            filter_reject_excluding = req.form.get('filter_reject_excluding', '').strip()

            use_rss_file = req.form.get('use_rss_file') in ['True', 'on', 'true', True]
            rss_file = req.form.get('rss_file', '').strip()
            rss_file_path = req.form.get('rss_file_path', '').strip()
            rss_file_days = req.form.get('rss_file_days', '').strip()
            rss_file_items = req.form.get('rss_file_items', '').strip()

            def parse_filter_lines(text: str) -> list:
                res = []
                if not text:
                    return res
                for raw_line in text.splitlines():
                    line = raw_line.strip().lstrip('-').strip()
                    if not line or line.startswith('#'):
                        continue
                    try:
                        loaded = yaml.safe_load(line)
                        if isinstance(loaded, (str, dict)):
                            res.append(loaded)
                        else:
                            res.append(line)
                    except Exception:
                        res.append(line)
                return res

            item_data = {
                'id': int(feed_id) if (feed_id and feed_id != '-1') else -1,
                'name': name_val or f"feed_{feed_id}",
                'sources': sources,
                'quality': quality,
                'use_rss_file': use_rss_file,
                'rss_file': rss_file,
                'rss_file_path': rss_file_path,
                'rss_file_days': rss_file_days,
                'rss_file_items': rss_file_items,
            }

            if use_feed_filter:
                item_data['accept_all'] = accept_all
                regexp_data = {}
                rej = parse_filter_lines(filter_reject)
                if rej:
                    regexp_data['reject'] = rej
                acc = parse_filter_lines(filter_accept)
                if acc:
                    regexp_data['accept'] = acc
                rex = parse_filter_lines(filter_reject_excluding)
                if rex:
                    regexp_data['reject_excluding'] = rex
                if regexp_data:
                    item_data['regexp'] = regexp_data
            else:
                item_data['accept_all'] = False

            ret = FeedConfigUtil.save_feed(item_data)
            return jsonify({'ret': ret, 'feeds': FeedConfigUtil.get_feeds()})

        elif command == 'remove_feed':
            target_id = req.form.get('target_id')
            ret = 'success' if FeedConfigUtil.delete_feed(target_id) else 'fail'
            return jsonify({'ret': ret, 'feeds': FeedConfigUtil.get_feeds()})

        elif command == 'generate_feed_file':
            target_id = req.form.get('target_id')
            feed = FeedConfigUtil.get_feed(target_id)
            if feed:
                ok = FeedRssFileWriter.save_rss_file(feed)
                return jsonify({'ret': 'success' if ok else 'fail'})
            return jsonify({'ret': 'not_exist'})

        elif command == 'direct_download':
            title = req.form.get('title', '').strip()
            magnet = req.form.get('magnet', '').strip()
            profile_name = req.form.get('profile_name', '').strip()
            feed_name = req.form.get('feed_name', 'FEED_DIRECT')

            if not magnet:
                return jsonify({'ret': 'fail', 'msg': '마그넷/ed2k 링크가 누락되었습니다.'})

            infohash = extract_info_hash(magnet)
            existing = ModelDownload.get_by_infohash(infohash) if infohash else ModelDownload.get_by_magnet(magnet)
            if existing:
                return jsonify({'ret': 'exist', 'msg': f'이미 다운로드 큐에 등록된 작업입니다 (상태: {existing.status}).'})

            dl_item = ModelDownload(
                feed_name=feed_name,
                title=title or (infohash or magnet[:30]),
                magnet=magnet,
                infohash=infohash
            )

            selected_profile = None
            if profile_name:
                for p in FeedConfigUtil.get_download_profiles():
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
                yaml_data = FeedConfigUtil.load_yaml()
                enabled_downloaders = [d['name'] for d in yaml_data.get('DOWNLOADERS', []) if d.get('enabled', True)]
                dl_item.priority_chain = enabled_downloaders
                dl_item.destination_type = 'local'

            dl_item.status = 'pending'
            db.session.add(dl_item)
            db.session.commit()
            logger.info(f"[{self.name}] 다운로드 큐 직접 추가: {title} (체인: {' -> '.join(dl_item.priority_chain)})")
            return jsonify({'ret': 'success', 'msg': '다운로드 큐에 성공적으로 등록되었습니다.'})

        return super(ModuleFeed, self).process_command(command, arg1, arg2, arg3, req)

    def process_api(self, sub, req):
        try:
            if sub in ['feed', 'rss']:
                feed_id = req.args.get('id')
                feed_name = req.args.get('name')
                if feed_id or feed_name:
                    logger.debug(f"[FeedAPI] RSS 발행 요청 수신: sub='{sub}', id='{feed_id}', name='{feed_name}'")
                    return self._handle_feed_rss(feed_id=feed_id, feed_name=feed_name)
                logger.warning(f"[FeedAPI] 피드 식별자 누락: {req.args}")
                return jsonify({'ret': 'fail', 'msg': '피드 식별자 누락'}), 400
            return jsonify({'ret': 'fail', 'msg': f'알 수 없는 API 명령: {sub}'}), 404
        except Exception as e:
            logger.error(f"[FeedAPI] process_api 에러 ({sub}): {e}")
            return jsonify({'ret': 'error', 'msg': str(e)}), 500

    def _handle_feed_rss(self, feed_id=None, feed_name=None):
        try:
            feed = FeedConfigUtil.get_feed(feed_id) if feed_id else FeedConfigUtil.get_feed_by_name(feed_name)
            if not feed:
                return jsonify({'ret': 'not_exist', 'msg': '피드를 찾을 수 없습니다.'}), 404

            sources = feed.get('sources', [])
            if not sources:
                xml_empty = self.generate_rss_feed(feed.get('name', 'Feed'), [])
                return Response(xml_empty, mimetype='application/xml; charset=utf-8')

            source_conditions = []
            for src in sources:
                s_name = src.get('site')
                b_name = src.get('full_board_key') or src.get('board')
                if s_name and b_name:
                    source_conditions.append(and_(ModelCrawlItem.site == s_name, ModelCrawlItem.board == b_name))

            if not source_conditions:
                xml_empty = self.generate_rss_feed(feed.get('name', 'Feed'), [])
                return Response(xml_empty, mimetype='application/xml; charset=utf-8')

            feed_count = P.ModelSetting.get_int(f"{self.name}_feed_count") if P.ModelSetting else 100
            query_limit = max(feed_count * 4, 400)
            candidates = db.session.query(ModelCrawlItem).filter(or_(*source_conditions)).order_by(ModelCrawlItem.id.desc()).limit(query_limit).all()

            global_cfg = FeedConfigUtil.get_global()
            seen_magnets = set()
            filtered_items = []
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
                    filtered_items.append(bbs)
                    if len(filtered_items) >= feed_count:
                        break

            feed_title = f"FEED: {feed.get('name', 'Feeder')}"
            xml_content = self.generate_rss_feed(feed_title, filtered_items)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[FeedAPI] _handle_feed_rss 에러: {e}")
            return Response('Internal Error', status=500)

    def generate_rss_feed(self, title: str, items: list, include_apikey: bool = True) -> str:
        ddns = get_ddns().rstrip('/')
        apikey = get_system_apikey() if include_apikey else ''

        xml = '<?xml version="1.0" encoding="utf-8"?>\n'
        xml += '<rss version="2.0" xmlns:showrss="http://showrss.info/">\n'
        xml += '  <channel>\n'
        xml += f'    <title>{clean_xml_string(title)}</title>\n'
        xml += f'    <link>{ddns}</link>\n'
        xml += '    <description>Feeder Generated RSS Feed</description>\n'

        for bbs in items:
            date_str = bbs.created_time.strftime('%a, %d %b %Y %H:%M:%S +0900') if bbs.created_time else ''
            bbs_dict = bbs.as_dict()
            has_magnet = False

            if bbs.torrent_info:
                for t_info in bbs.torrent_info:
                    has_magnet = True
                    target_name = t_info.get('name', bbs.title)
                    target_hash = t_info.get('info_hash', '')
                    magnet_link = f"magnet:?xt=urn:btih:{target_hash}"

                    xml += '    <item>\n'
                    xml += f'      <title>{clean_xml_string(target_name)}</title>\n'
                    xml += f'      <link>{magnet_link}</link>\n'
                    xml += f'      <pubDate>{date_str}</pubDate>\n'
                    xml += '    </item>\n'
            elif bbs_dict.get('magnet'):
                for mag in bbs_dict['magnet']:
                    has_magnet = True
                    xml += '    <item>\n'
                    xml += f'      <title>{clean_xml_string(bbs.title)}</title>\n'
                    xml += f'      <link>{clean_xml_string(mag)}</link>\n'
                    xml += f'      <pubDate>{date_str}</pubDate>\n'
                    xml += '    </item>\n'

            if bbs_dict.get('files'):
                for f_idx, file_item in enumerate(bbs_dict['files']):
                    filename = file_item[1]
                    if has_magnet and filename.lower().endswith('.torrent'):
                        continue

                    apikey_param = f"&apikey={apikey}" if include_apikey and apikey else ""
                    download_proxy_url = f"{ddns}/{P.package_name}/api/crawl/download?id={bbs.id}_{f_idx}{apikey_param}"

                    xml += '    <item>\n'
                    xml += f'      <title>{clean_xml_string(filename)}</title>\n'
                    xml += f'      <link>{clean_xml_string(download_proxy_url)}</link>\n'
                    xml += f'      <pubDate>{date_str}</pubDate>\n'
                    xml += '    </item>\n'

        xml += '  </channel>\n'
        xml += '</rss>'
        return xml

    def scheduler_function(self):
        logger.info(f"[{self.name}] 정기 피드 XML 파일 동기화 스케줄러 실행")
        self.start_celery(TaskFeedBase.start, None, "scheduler")
