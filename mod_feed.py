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
from .model_feed import ModelFeedItem
from .util_base import FeederUtil
from .util_feed import FeedUtil
from .task_feed import TaskFeedBase

name = 'feed'


class ModuleFeed(PluginModuleBase):

    def __init__(self, P):
        super(ModuleFeed, self).__init__(P, 'setting', name=name, scheduler_desc="Feeder - 피드 관리 및 RSS 발행")
        self.web_list_model = ModelFeedItem
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "10",
            f"{self.name}_db_delete_day": "30",
            f"{self.name}_db_auto_delete": "False",
            f"{self.name}_feed_count": "100",
            f"{self.name}_make_rss_file": "False",
            f"{self.name}_rss_file_path": "",
            f"{self.name}_rss_file_days": "14",
            f"{self.name}_rss_file_items": "100",
            f"{self.name}_use_proxy": "False",
            f"{self.name}_proxy_url": "",
        }

    def process_menu(self, page_name, req):
        try:
            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['sub'] = self.name
            arg['current_page'] = page_name
            arg['ddns'] = FeederUtil.get_ddns()
            arg['apikey'] = FeederUtil.get_system_apikey()
            arg['feeds'] = FeederUtil.get_feeds()
            arg['profiles'] = FeederUtil.get_download_profiles()

            if page_name == 'setting':
                arg['yaml_filepath'] = FeederUtil.get_filepath()
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

            command = req.form.get('command') or req.values.get('command') or sub
            if sub in ['web_list', 'list'] or command in ['web_list', 'list']:
                return jsonify(self.web_list_model.web_list(req))

            if command:
                res = self.process_command(command, req.form.get('arg1', ''), req.form.get('arg2', ''), req.form.get('arg3', ''), req)
                if res is not None:
                    return res

            return super(ModuleFeed, self).process_ajax(sub, req)
        except Exception as e:
            logger.error(f"[{self.name}] process_ajax 에러: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'error', 'msg': str(e)})

    def process_command(self, command, arg1, arg2, arg3, req):
        if command == 'clear_feed_db':
            feed_name = req.form.get('feed_name', '').strip()
            if not feed_name:
                return jsonify({'ret': 'fail', 'msg': '피드 이름 누락'})
            try:
                deleted = db.session.query(ModelFeedItem).filter_by(feed_name=feed_name).delete()
                db.session.commit()
                logger.info(f"[{self.name}] 피드 [{feed_name}] DB 비우기 완료: {deleted}건 삭제됨")
                return jsonify({'ret': 'success', 'msg': f'[{feed_name}] 피드 DB가 초기화되었습니다 ({deleted}건 삭제).'})
            except Exception as ex:
                db.session.rollback()
                logger.error(f"[{self.name}] clear_feed_db 에러: {ex}")
                return jsonify({'ret': 'fail', 'msg': str(ex)})

        elif command in ['db_delete', 'reset_db']:
            try:
                deleted = db.session.query(ModelFeedItem).delete()
                db.session.commit()
                logger.info(f"[{self.name}] 피드 전체 DB 초기화 완료: {deleted}건 삭제")
                return jsonify(True)
            except Exception as e:
                db.session.rollback()
                logger.error(f"[{self.name}] db_delete 에러: {e}")
                return jsonify(False)

        elif command == 'db_delete_day':
            try:
                day_val = arg1 or P.ModelSetting.get(f"{self.name}_db_delete_day")
                day = int(day_val)
                target_date = datetime.now() - timedelta(days=day)
                deleted = db.session.query(ModelFeedItem).filter(ModelFeedItem.created_time < target_date).delete()
                db.session.commit()
                logger.info(f"[{self.name}] {day}일 경과 피드 DB 레코드 삭제 완료: {deleted}건 삭제")
                return jsonify(True)
            except Exception as e:
                db.session.rollback()
                logger.error(f"[{self.name}] db_delete_day 에러: {e}")
                return jsonify(False)

        elif command == 'load_feeds':
            return jsonify({'feeds': self.get_feed_list()})

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

            item_data['accept_all'] = accept_all

            if use_feed_filter:
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

            ret = FeederUtil.save_feed(item_data)
            return jsonify({'ret': ret, 'feeds': self.get_feed_list()})

        elif command == 'remove_feed':
            target_id = req.form.get('target_id')
            ret = 'success' if FeederUtil.delete_feed(target_id) else 'fail'
            return jsonify({'ret': ret, 'feeds': self.get_feed_list()})

        elif command == 'generate_feed_file':
            target_id = req.form.get('target_id')
            feed = FeederUtil.get_feed(target_id)
            if feed:
                FeedUtil.sync_feed(feed)
                ok = FeedUtil.save_rss_file(feed)
                return jsonify({'ret': 'success' if ok else 'fail'})
            return jsonify({'ret': 'not_exist'})

        elif command == 'direct_download':
            title = req.form.get('title', '').strip()
            magnet = req.form.get('magnet', '').strip()
            profile_name = req.form.get('profile_name', '').strip()
            feed_name = req.form.get('feed_name', 'FEED_DIRECT')
            res = FeederUtil.add_direct_download(
                title=title,
                magnet=magnet,
                profile_name=profile_name,
                feed_name=feed_name,
                caller_name=self.name
            )
            return jsonify(res)

    def get_feed_list(self) -> list[dict]:
        """웹 UI 렌더링에 필요한 API URL 등이 포함된 피드 목록 반환"""
        feeds = FeederUtil.get_feeds()
        ddns = FeederUtil.get_ddns().rstrip('/')
        apikey = FeederUtil.get_system_apikey()
        feed_list = []
        for f in feeds:
            info = dict(f)
            info['api'] = f"{ddns}/{P.package_name}/api/feed/rss?name={f.get('name')}&apikey={apikey}"
            info['use_rss_file'] = str(f.get('use_rss_file', False)).lower() in ['true', 'on', '1']
            feed_list.append(info)
        return feed_list

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
            feed = FeederUtil.get_feed(feed_id) if feed_id else FeederUtil.get_feed_by_name(feed_name)
            if not feed:
                return jsonify({'ret': 'not_exist', 'msg': '피드를 찾을 수 없습니다.'}), 404

            current_feed_name = feed.get('name')
            feed_count = P.ModelSetting.get_int(f"{self.name}_feed_count") if P.ModelSetting else 100

            feed_records = (
                db.session.query(ModelFeedItem)
                .filter_by(feed_name=current_feed_name)
                .order_by(ModelFeedItem.id.desc())
                .limit(feed_count)
                .all()
            )

            feed_title = f"FEED: {current_feed_name}"
            xml_content = self.generate_rss_feed(feed_title, feed_records)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[FeedAPI] _handle_feed_rss 에러: {e}")
            return Response('Internal Error', status=500)
        finally:
            try:
                db.session.remove()
            except Exception:
                pass

    def generate_rss_feed(self, title: str, items: list, include_apikey: bool = True) -> str:
        ddns = FeederUtil.get_ddns().rstrip('/')
        apikey = FeederUtil.get_system_apikey() if include_apikey else ''

        xml = '<?xml version="1.0" encoding="utf-8"?>\n'
        xml += '<rss version="2.0" xmlns:showrss="http://showrss.info/">\n'
        xml += '  <channel>\n'
        xml += f'    <title>{FeederUtil.clean_xml_string(title)}</title>\n'
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
                    xml += f'      <title>{FeederUtil.clean_xml_string(target_name)}</title>\n'
                    xml += f'      <link>{magnet_link}</link>\n'
                    xml += f'      <pubDate>{date_str}</pubDate>\n'
                    xml += '    </item>\n'
            elif bbs_dict.get('magnet'):
                for mag in bbs_dict['magnet']:
                    has_magnet = True
                    xml += '    <item>\n'
                    xml += f'      <title>{FeederUtil.clean_xml_string(bbs.title)}</title>\n'
                    xml += f'      <link>{FeederUtil.clean_xml_string(mag)}</link>\n'
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
                    xml += f'      <title>{FeederUtil.clean_xml_string(filename)}</title>\n'
                    xml += f'      <link>{FeederUtil.clean_xml_string(download_proxy_url)}</link>\n'
                    xml += f'      <pubDate>{date_str}</pubDate>\n'
                    xml += '    </item>\n'

        xml += '  </channel>\n'
        xml += '</rss>'
        return xml

    def scheduler_function(self):
        if P.ModelSetting.get_bool(f"{self.name}_db_auto_delete"):
            try:
                day = P.ModelSetting.get_int(f"{self.name}_db_delete_day")
                if day > 0:
                    target_date = datetime.now() - timedelta(days=day)
                    deleted = db.session.query(ModelFeedItem).filter(ModelFeedItem.created_time < target_date).delete()
                    db.session.commit()
                    if deleted > 0:
                        logger.debug(f"[{self.name}] {day}일 경과 피드 DB 자동 정리 완료 (삭제: {deleted}건)")
            except Exception as e:
                logger.error(f"[{self.name}] feed db auto delete 에러: {e}")
                db.session.rollback()

        logger.info(f"[{self.name}] 정기 피드 동기화 및 XML 파일 갱신 스케줄러 실행")
        self.start_celery(TaskFeedBase.start, None, "scheduler")
