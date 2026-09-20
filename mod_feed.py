# -*- coding: utf-8 -*-
import os
import json
import yaml
import base64
import traceback
from datetime import datetime, timedelta
from types import SimpleNamespace
from flask import Response, send_file, abort, request, jsonify
from sqlalchemy import and_, or_, func, desc

from .setup import *
from .model_feed import ModelFeedSite, ModelFeedBbs
from .util_feed import (
    get_ddns, get_system_apikey, clean_xml_string,
    FeedConfigUtil, FeedCustomManager, FeedScraper, FeedTorrentInfo, FeedFilter
)
from .task_feed import TaskBase, Task

name = 'feed'


class ModuleFeed(PluginModuleBase):

    def __init__(self, P):
        super(ModuleFeed, self).__init__(P, 'setting', name=name, scheduler_desc="Feeder - 토렌트 수집 및 피드 생성")
        self.web_list_model = ModelFeedBbs
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "10",
            f"{self.name}_db_delete_day": "30",
            f"{self.name}_db_auto_delete": "False",
            # 수집 및 일반 설정
            f"{self.name}_feed_count": "100",
            f"{self.name}_web_page_size": "25",
            f"{self.name}_max_page": "5",
            f"{self.name}_test_count": "3",
            f"{self.name}_crawler_delay": "2.0",
            f"{self.name}_allow_duplicate_magnet": "False",
            f"{self.name}_scheduler_count": "0",
            # 프록시 및 보안 우회 설정
            f"{self.name}_use_proxy": "False",
            f"{self.name}_proxy_url": "",
            f"{self.name}_use_flaresolverr": "False",
            f"{self.name}_flaresolverr_url": "",
            f"{self.name}_use_selenium": "False",
            f"{self.name}_selenium_remote_url": "",
            f"{self.name}_selenium_timeout": "20",
            # 토렌트 정보 취득 설정
            f"{self.name}_use_torrent_info": "False",
            f"{self.name}_torrent_info_method": "plugin",
            f"{self.name}_qb_url": "",
            f"{self.name}_qb_username": "",
            f"{self.name}_qb_password": "",
            f"{self.name}_qb_temp_category": "feeder_temp",
            # 공유용 RSS 파일 별도 생성 설정
            f"{self.name}_make_rss_file": "False",
            f"{self.name}_rss_file_path": "",
            f"{self.name}_rss_file_days": "14",
            f"{self.name}_rss_file_items": "100",
        }

    def process_menu(self, page_name, req):
        try:
            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['sub'] = self.name
            arg['current_page'] = page_name
            arg['ddns'] = get_ddns()
            arg['apikey'] = get_system_apikey()

            if page_name == 'setting':
                arg['yaml_filepath'] = FeedConfigUtil.get_filepath()
                arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
                arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
                return render_template(f'{P.package_name}_{self.name}_setting.html', arg=arg)

            # 기본값: 'list' (토렌트 리스트 페이지)
            arg['is_torrent_info_installed'] = True
            return render_template(f'{P.package_name}_{self.name}_list.html', arg=arg)
        except Exception as e:
            logger.error(f"[{self.name}] process_menu 에러: {e}")
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def process_ajax(self, sub, req):
        try:
            command = req.form.get('command') or sub

            # 웹 리스트 검색 요청 처리
            if sub == 'web_list' or command == 'web_list':
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
        # 수동 1회 실행 명령
        if command == 'one_execute':
            logger.info(f"[{self.name}] 1회 실행 명령 수신 -> Celery 워커 전달")
            self.start_celery(TaskBase.start, None, "manual")
            return jsonify({'ret': 'success', 'msg': '수집 작업을 Celery 워커에서 시작했습니다.'})

        # 사이트 관리 명령
        elif command == 'load_site':
            FeedCustomManager.sync_default_site_info()
            sites = ModelFeedSite.get_list(by_dict=True)
            return jsonify({'site': sites})

        elif command == 'site_toggle_option':
            site_id = req.form.get('site_id')
            option_name = req.form.get('option')
            site = ModelFeedSite.get(site_id=site_id)
            if not site:
                return jsonify({'ret': 'fail', 'log': '사이트가 존재하지 않습니다.'})

            info = dict(site.info or {})
            key_map = {
                'use_proxy': 'USE_PROXY',
                'use_flaresolverr': 'USE_FLARESOLVERR',
                'use_selenium': 'USE_SELENIUM',
                'use_torrent_info': 'USE_TORRENT_INFO',
            }
            target_key = key_map.get(option_name)
            if target_key:
                current_val = site.get_options().get(option_name, False)
                new_val = not current_val
                info[target_key] = new_val

                extra = list(info.get('EXTRA', []))
                if new_val:
                    if target_key not in extra:
                        extra.append(target_key)
                else:
                    if target_key in extra:
                        extra.remove(target_key)
                info['EXTRA'] = extra

                site.info = info
                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(site, 'info')
                db.session.commit()
                logger.info(f"[{self.name}] 사이트 [{site.name}] 기본 옵션 변경: {option_name} -> {new_val}")
                return jsonify({'ret': 'success', 'site': ModelFeedSite.get_list(by_dict=True)})

            return jsonify({'ret': 'fail', 'log': '알 수 없는 옵션 항목입니다.'})

        elif command == 'test':
            site_id = req.form.get('site_id')
            board_input = req.form.get('board_id', '').strip()
            site = ModelFeedSite.get(site_id=site_id)
            if not site or not board_input:
                logger.warning(f"[{self.name}] 수집 테스트 실패: 사이트 또는 게시판 ID가 올바르지 않음 (site_id={site_id}, board_id={board_input})")
                return jsonify({'ret': 'fail', 'log': '사이트 또는 게시판 ID가 올바르지 않습니다.'})

            board_id, subcat_id, full_board_key = Task.parse_board_info(board_input)

            try:
                test_count = P.ModelSetting.get_int(f"{self.name}_test_count")
            except Exception:
                test_count = 3

            site_opts = site.get_options()
            global_proxy = P.ModelSetting.get_bool(f"{self.name}_use_proxy")
            global_fs = P.ModelSetting.get_bool(f"{self.name}_use_flaresolverr")
            global_selenium = P.ModelSetting.get_bool(f"{self.name}_use_selenium")
            global_torrent_info = P.ModelSetting.get_bool(f"{self.name}_use_torrent_info")

            test_target_cfg = SimpleNamespace(
                subcat_id=subcat_id,
                use_proxy=global_proxy and site_opts.get('use_proxy', False),
                proxy_url=P.ModelSetting.get(f"{self.name}_proxy_url") or '',
                use_flaresolverr=global_fs and site_opts.get('use_flaresolverr', False),
                use_selenium=global_selenium and site_opts.get('use_selenium', False),
                use_torrent_info=global_torrent_info and site_opts.get('use_torrent_info', False)
            )

            subcat_log = f", 서브카테고리='{subcat_id}'" if subcat_id else ""
            logger.info(f"[{self.name}] ========================================================")
            logger.info(f"[{self.name}] [수집 테스트 시작] 사이트='{site.name}', 게시판='{board_id}'{subcat_log}, 개수={test_count}")
            logger.info(f"[{self.name}] ========================================================")

            test_results = Task.execute_board_crawl(site.info, board_id, is_test=True, max_count=test_count, target_cfg=test_target_cfg)
            if test_results is None:
                test_results = []

            detailed_count = sum(1 for x in test_results if x.get('magnet') or x.get('download'))
            logger.info(f"[{self.name}] ========================================================")
            logger.info(f"[{self.name}] [수집 테스트 완료] 사이트='{site.name}', 게시판='{full_board_key}', 전체 항목={len(test_results)}개 (상세 파싱={detailed_count}개)")
            logger.info(f"[{self.name}] ========================================================")
            return jsonify(test_results)

        elif command == 'site_delete':
            site_id = req.form.get('site_id')
            ret = ModelFeedSite.delete(site_id)
            sites = ModelFeedSite.get_list(by_dict=True)
            return jsonify({'ret': ret, 'site': sites})

        elif command == 'site_edit':
            site_id = req.form.get('modal_site_id', '').strip()
            modal_site_json = req.form.get('modal_site_json', '').strip()
            try:
                info = json.loads(modal_site_json)
            except Exception:
                return jsonify({'ret': 'exception', 'log': '올바른 JSON 포맷이 아닙니다.'})

            site_name = info.get('NAME')
            if not site_name:
                return jsonify({'ret': 'exception', 'log': "JSON에 'NAME' 속성이 필요합니다."})

            is_edit_mode = bool(site_id and site_id != '-1' and int(site_id) > 0)
            entity = ModelFeedSite.get(site_id=int(site_id)) if is_edit_mode else None

            if entity:
                entity.name = site_name
                entity.info = info
                db.session.commit()
                return jsonify({'ret': 'edit_success', 'site': ModelFeedSite.get_list(by_dict=True)})
            else:
                if ModelFeedSite.get(name=site_name):
                    return jsonify({'ret': 'exist', 'log': f"동일 이름('{site_name}')의 사이트가 이미 존재합니다."})
                new_site = ModelFeedSite('my', info, '직접 입력')
                db.session.add(new_site)
                db.session.commit()
                return jsonify({'ret': 'add_success', 'site': ModelFeedSite.get_list(by_dict=True)})

        # 커스텀 스크립트 훅 관리 명령
        elif command == 'custom_script_list':
            custom_dir = FeedCustomManager.get_custom_dir()
            files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
            files.sort()
            return jsonify({'ret': 'success', 'files': files})

        elif command == 'custom_script_read':
            filename = os.path.basename(req.form.get('filename', '').strip())
            custom_dir = FeedCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            if not filename or not os.path.exists(fpath):
                return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    content = f.read()
                return jsonify({'ret': 'success', 'filename': filename, 'content': content})
            except Exception as e:
                logger.error(f"[{self.name}] custom_script_read 에러: {e}")
                return jsonify({'ret': 'error', 'log': str(e)})

        elif command == 'custom_script_save':
            filename = os.path.basename(req.form.get('filename', '').strip())
            content = req.form.get('content', '')
            if not filename:
                return jsonify({'ret': 'empty_filename', 'log': '파일명이 올바르지 않습니다.'})
            if not filename.endswith('.py'):
                filename += '.py'

            custom_dir = FeedCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            try:
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"[{self.name}] 커스텀 스크립트 저장 완료: {filename}")
                FeedCustomManager.load_hooks()
                files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({
                    'ret': 'success',
                    'filename': filename,
                    'files': files,
                    'site': ModelFeedSite.get_list(by_dict=True)
                })
            except Exception as e:
                logger.error(f"[{self.name}] custom_script_save 에러: {e}")
                return jsonify({'ret': 'error', 'log': str(e)})

        elif command == 'custom_script_delete':
            filename = os.path.basename(req.form.get('filename', '').strip())
            custom_dir = FeedCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            if not filename or not os.path.exists(fpath):
                return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
            try:
                os.remove(fpath)
                logger.info(f"[{self.name}] 커스텀 스크립트 삭제 완료: {filename}")
                FeedCustomManager.load_hooks()
                files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({'ret': 'success', 'files': files})
            except Exception as e:
                logger.error(f"[{self.name}] custom_script_delete 에러: {e}")
                return jsonify({'ret': 'error', 'log': str(e)})

        elif command == 'custom_script_upload':
            file_obj = req.files.get('file')
            if not file_obj or not file_obj.filename:
                return jsonify({'ret': 'empty_file', 'log': '업로드할 파일이 없습니다.'})

            filename = os.path.basename(file_obj.filename)
            if not filename.endswith('.py'):
                return jsonify({'ret': 'invalid_ext', 'log': '.py 파일만 업로드할 수 있습니다.'})

            custom_dir = FeedCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            try:
                file_obj.save(fpath)
                logger.info(f"[{self.name}] 커스텀 스크립트 업로드 완료: {filename}")
                FeedCustomManager.load_hooks()
                files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({
                    'ret': 'success',
                    'filename': filename,
                    'files': files,
                    'site': ModelFeedSite.get_list(by_dict=True)
                })
            except Exception as e:
                logger.error(f"[{self.name}] custom_script_upload 에러: {e}")
                return jsonify({'ret': 'error', 'log': str(e)})

        # YAML 수집 작업(TASKS) 관리 명령
        elif command == 'load_tasks':
            sites = ModelFeedSite.get_list(by_dict=True)
            tasks = self.get_task_list()
            return jsonify({'site': sites, 'tasks': tasks})

        elif command == 'add_task':
            task_id = req.form.get('modal_scheduler_id', '-1').strip()
            task_name = req.form.get('task_name', '').strip()
            targets_json = req.form.get('targets_json', '[]')
            try:
                targets = json.loads(targets_json)
            except Exception:
                targets = []

            interval_val = int(req.form.get('interval', 1))
            enabled = req.form.get('enabled') in ['True', 'on', 'true', True]
            use_proxy = req.form.get('use_proxy') in ['True', 'on', 'true', True]
            proxy_url_val = req.form.get('proxy_url', '').strip()
            use_flaresolverr = req.form.get('use_flaresolverr') in ['True', 'on', 'true', True]
            use_selenium = req.form.get('use_selenium') in ['True', 'on', 'true', True]
            use_torrent_info = req.form.get('use_torrent_info') in ['True', 'on', 'true', True]
            use_rss_file = req.form.get('use_rss_file') in ['True', 'on', 'true', True]
            rss_file = req.form.get('rss_file', '').strip()
            rss_file_path = req.form.get('rss_file_path', '').strip()
            rss_file_days = req.form.get('rss_file_days', '').strip()
            rss_file_items = req.form.get('rss_file_items', '').strip()
            quality = req.form.get('quality', '').strip()

            # 정규식 필터 파싱
            use_task_filter = req.form.get('use_task_filter') in ['True', 'on', 'true', True]
            accept_all = req.form.get('accept_all') in ['True', 'on', 'true', True]
            filter_reject = req.form.get('filter_reject', '').strip()
            filter_accept = req.form.get('filter_accept', '').strip()
            filter_reject_excluding = req.form.get('filter_reject_excluding', '').strip()

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
                'id': int(task_id) if (task_id and task_id != '-1') else -1,
                'name': task_name or f"task_{task_id}",
                'targets': targets,
                'interval': interval_val,
                'enabled': enabled,
                'use_proxy': use_proxy,
                'proxy_url': proxy_url_val,
                'use_flaresolverr': use_flaresolverr,
                'use_selenium': use_selenium,
                'use_torrent_info': use_torrent_info,
                'use_rss_file': use_rss_file,
                'rss_file': rss_file,
                'rss_file_path': rss_file_path,
                'rss_file_days': rss_file_days,
                'rss_file_items': rss_file_items,
            }
            if quality:
                item_data['quality'] = quality

            if use_task_filter:
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

            ret = FeedConfigUtil.save_task(item_data)
            return jsonify({'ret': ret, 'tasks': self.get_task_list()})

        elif command == 'remove_task':
            target_id = req.form.get('target_id')
            task = FeedConfigUtil.get_task(target_id)
            if task:
                self.delete_task_db(task)
                FeedConfigUtil.delete_task(target_id)
                ret = 'success'
            else:
                ret = 'fail'
            return jsonify({'ret': ret, 'tasks': self.get_task_list()})

        elif command == 'remove_task_db':
            target_id = req.form.get('target_id')
            task = FeedConfigUtil.get_task(target_id)
            if task:
                ret = self.delete_task_db(task)
            else:
                ret = 'fail'
            return jsonify({'ret': ret, 'tasks': self.get_task_list()})

        # DB 정리 명령
        elif command in ['db_delete', 'reset_db']:
            try:
                db.session.query(ModelFeedBbs).delete()
                db.session.commit()
                self.db_vacuum()
                return jsonify(True)
            except Exception as e:
                logger.error(f"[{self.name}] db_delete 에러: {e}")
                db.session.rollback()
                return jsonify(False)

        elif command == 'db_delete_day':
            try:
                day_val = arg1 or P.ModelSetting.get(f"{self.name}_db_delete_day")
                day = int(day_val)
                target_date = datetime.now() - timedelta(days=day)
                db.session.query(ModelFeedBbs).filter(ModelFeedBbs.created_time < target_date).delete()
                db.session.commit()
                self.db_vacuum()
                return jsonify(True)
            except Exception as e:
                logger.error(f"[{self.name}] db_delete_day 에러: {e}")
                db.session.rollback()
                return jsonify(False)

        # 토렌트 정보 및 다운로더 연동
        elif command == 'torrent_info':
            magnet_hash = req.form.get('hash')
            info_list = FeedTorrentInfo.get_torrent_info([magnet_hash])
            result_data = info_list[0] if (info_list and len(info_list) > 0) else None
            return jsonify(result_data)

        return super(ModuleFeed, self).process_command(command, arg1, arg2, arg3, req)

    def process_api(self, sub, req):
        try:
            # 태스크 통합 RSS 피드
            if sub in ['task', 'board']:
                task_id = req.args.get('id')
                task_name = req.args.get('name')
                sitename = req.args.get('site')
                boardname = req.args.get('board')
                subcat = req.args.get('subcat')

                if task_id or task_name:
                    return self._handle_task_rss(task_id=task_id, task_name=task_name)

                if sitename and boardname:
                    _, _, full_board_key = Task.parse_board_info(boardname, subcat)
                    return self._handle_board_rss(sitename, full_board_key)

                return jsonify({'ret': 'fail', 'msg': '태스크 또는 게시판 식별자 누락'}), 400

            elif sub == 'download':
                bbs_file_id = req.args.get('id')
                if bbs_file_id:
                    return self._handle_download_stream(bbs_file_id)
                return jsonify({'ret': 'fail', 'msg': '식별자 누락'}), 400

            elif sub == 'site_update':
                return self._handle_site_update()

            return jsonify({'ret': 'fail', 'msg': f'알 수 없는 API 명령: {sub}'}), 404
        except Exception as e:
            logger.error(f"[Feeder] process_api 에러 ({sub}): {e}")
            return jsonify({'ret': 'error', 'msg': str(e)}), 500

    def _handle_board_rss(self, sitename, boardname):
        try:
            feed_count = P.ModelSetting.get_int(f"{self.name}_feed_count") if P.ModelSetting else 100
            query_limit = max(feed_count * 3, 300)
            items = db.session.query(ModelFeedBbs).filter_by(site=sitename, board=boardname).order_by(ModelFeedBbs.id.desc()).limit(query_limit).all()

            task = FeedConfigUtil.get_task_by_board_key(sitename, boardname)
            global_cfg = FeedConfigUtil.get_global()
            filtered_items = []
            for bbs in items:
                is_pass, _ = FeedFilter.evaluate(bbs.as_dict(), task, global_cfg)
                if is_pass:
                    filtered_items.append(bbs)
                    if len(filtered_items) >= feed_count:
                        break

            xml_content = self.generate_rss_feed(f"SITE: {sitename} / BOARD: {boardname}", filtered_items)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[Feeder] _handle_board_rss 에러: {e}")
            return Response('Internal Error', status=500)

    def _handle_task_rss(self, task_id=None, task_name=None):
        try:
            task = FeedConfigUtil.get_task(task_id) if task_id else FeedConfigUtil.get_task_by_name(task_name)
            if not task:
                return jsonify({'ret': 'not_exist', 'msg': '태스크를 찾을 수 없습니다.'}), 404

            targets = task.get('targets', [])
            if not targets:
                xml_empty = self.generate_rss_feed(task.get('name', 'Task'), [])
                return Response(xml_empty, mimetype='application/xml; charset=utf-8')

            target_conditions = []
            for t in targets:
                s_name = t.get('site') or t.get('site_name')
                b_name = t.get('full_board_key') or t.get('board') or t.get('board_id')
                if s_name and b_name:
                    target_conditions.append(and_(ModelFeedBbs.site == s_name, ModelFeedBbs.board == b_name))

            if not target_conditions:
                xml_empty = self.generate_rss_feed(task.get('name', 'Task'), [])
                return Response(xml_empty, mimetype='application/xml; charset=utf-8')

            feed_count = P.ModelSetting.get_int(f"{self.name}_feed_count") if P.ModelSetting else 100
            query_limit = max(feed_count * 4, 400)
            candidates = db.session.query(ModelFeedBbs).filter(or_(*target_conditions)).order_by(ModelFeedBbs.id.desc()).limit(query_limit).all()

            global_cfg = FeedConfigUtil.get_global()
            from .util_feed import extract_info_hash
            seen_magnets = set()
            filtered_items = []
            for bbs in candidates:
                mag = bbs.magnet
                if mag:
                    info_hash = extract_info_hash(mag) or mag
                    if info_hash in seen_magnets:
                        continue
                    seen_magnets.add(info_hash)

                is_pass, _ = FeedFilter.evaluate(bbs.as_dict(), task, global_cfg)
                if is_pass:
                    filtered_items.append(bbs)
                    if len(filtered_items) >= feed_count:
                        break

            feed_title = f"TASK: {task.get('name', 'Feeder')}"
            xml_content = self.generate_rss_feed(feed_title, filtered_items)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[Feeder] _handle_task_rss 에러: {e}")
            return Response('Internal Error', status=500)

    handle_task_rss = _handle_task_rss

    def _handle_download_stream(self, bbs_file_id):
        try:
            tokens = bbs_file_id.split('_')
            post_id = int(tokens[0])
            file_index = int(tokens[1])

            post = ModelFeedBbs.get(id=post_id)
            if not post:
                abort(404)

            post_dict = post.as_dict()
            if not post_dict['files'] or len(post_dict['files']) <= file_index:
                abort(404)

            target_file = post_dict['files'][file_index]
            download_url = target_file[0]
            filename = target_file[1]

            target_task = None
            for t in FeedConfigUtil.get_tasks():
                for tgt in t.get('targets', []):
                    t_board = tgt.get('full_board_key') or tgt.get('board')
                    if tgt.get('site') == post.site and str(t_board) == str(post.board):
                        target_task = t
                        break
                if target_task:
                    break

            target_cfg = SimpleNamespace(
                use_proxy=target_task.get('use_proxy', False) if target_task else P.ModelSetting.get_bool(f"{self.name}_use_proxy"),
                proxy_url=target_task.get('proxy_url', '') if target_task else P.ModelSetting.get(f"{self.name}_proxy_url"),
                use_flaresolverr=target_task.get('use_flaresolverr', False) if target_task else False
            )

            byte_io = FeedScraper.download_file_stream(download_url, referer=post.url, scheduler_instance=target_cfg)

            try:
                return send_file(byte_io, mimetype='application/x-bittorrent' if filename.lower().endswith('.torrent') else 'application/octet-stream', as_attachment=True, download_name=filename)
            except TypeError:
                return send_file(byte_io, mimetype='application/x-bittorrent' if filename.lower().endswith('.torrent') else 'application/octet-stream', as_attachment=True, attachment_filename=filename)
        except Exception as e:
            logger.error(f"[Feeder] _handle_download_stream 에러: {e}")
            abort(500)

    def _handle_site_update(self):
        try:
            content_b64 = request.form.get('content') or request.args.get('content', '')
            decoded_text = base64.b64decode(content_b64).decode('utf-8')
            info = json.loads(decoded_text)
            site_name = info.get('NAME')

            if not site_name:
                return jsonify({'ret': 'error', 'log': 'NAME 누락'}), 400

            existing_site = ModelFeedSite.get(name=site_name)
            if existing_site:
                existing_site.info = info
                existing_site.content = decoded_text
                db.session.commit()
                return jsonify({'ret': 'update', 'log': f'{site_name} 업데이트 완료'})
            else:
                new_site = ModelFeedSite('web', info, decoded_text)
                db.session.add(new_site)
                db.session.commit()
                return jsonify({'ret': 'add', 'log': f'{site_name} 신규 추가 완료'})
        except Exception as e:
            logger.error(f"[Feeder] _handle_site_update 에러: {e}")
            return jsonify({'ret': 'error', 'log': str(e)}), 500

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
                    download_proxy_url = f"{ddns}/{P.package_name}/api/download?id={bbs.id}_{f_idx}{apikey_param}"

                    xml += '    <item>\n'
                    xml += f'      <title>{clean_xml_string(filename)}</title>\n'
                    xml += f'      <link>{clean_xml_string(download_proxy_url)}</link>\n'
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
                    deleted_count = db.session.query(ModelFeedBbs).filter(ModelFeedBbs.created_time < target_date).delete()
                    db.session.commit()
                    if deleted_count > 0:
                        self.db_vacuum()
                    logger.debug(f"[Feeder] {day}일 경과된 이전 수집 DB 자동 정리 완료 (삭제: {deleted_count}건)")
            except Exception as e:
                logger.error(f"[Feeder] db auto delete 에러: {e}")
                db.session.rollback()

        self.start_celery(TaskBase.start, None, "default")

    def delete_task_db(self, task: dict) -> str:
        try:
            for tgt in task.get('targets', []):
                site_val = tgt.get('site')
                board_val = tgt.get('full_board_key', tgt.get('board'))
                db.session.query(ModelFeedBbs).filter_by(site=site_val, board=board_val).delete()
            db.session.commit()
            self.db_vacuum()
            return 'success'
        except Exception as e:
            logger.error(f"[Feeder] delete_task_db 에러: {e}")
            db.session.rollback()
        return 'fail'

    def db_vacuum(self):
        try:
            bind_key = f'{P.package_name}_{self.name}'
            try:
                engine = db.get_engine(bind=bind_key)
            except Exception:
                engine = db.engine

            if engine.dialect.name == 'sqlite':
                raw_conn = engine.raw_connection()
                try:
                    raw_conn.isolation_level = None
                    cursor = raw_conn.cursor()
                    cursor.execute("VACUUM")
                    cursor.close()
                    logger.info(f"[{self.name}] SQLite DB VACUUM 정리 완료 ({bind_key}.db)")
                finally:
                    raw_conn.close()
        except Exception as e:
            logger.error(f"[{self.name}] db_vacuum 실행 오류: {e}")

    def get_task_list(self) -> list[dict]:
        ret = []
        raw_tasks = FeedConfigUtil.get_tasks()
        ddns = get_ddns().rstrip('/')
        apikey = get_system_apikey()

        for task in raw_tasks:
            info = dict(task)
            task_id = task.get('id')
            targets = task.get('targets', [])

            last_bbs = None
            if targets:
                target_conds = [and_(ModelFeedBbs.site == t.get('site'), ModelFeedBbs.board == t.get('full_board_key', t.get('board'))) for t in targets]
                last_bbs = db.session.query(ModelFeedBbs).filter(or_(*target_conds)).order_by(ModelFeedBbs.id.desc()).first()

            info['last'] = last_bbs.as_dict() if last_bbs else None
            info['targets'] = targets
            info['enabled'] = str(task.get('enabled', True)).lower() in ['true', 'on', '1']
            info['use_proxy'] = str(task.get('use_proxy', False)).lower() in ['true', 'on', '1']
            info['proxy_url'] = task.get('proxy_url', '')
            info['use_flaresolverr'] = str(task.get('use_flaresolverr', False)).lower() in ['true', 'on', '1']
            info['use_selenium'] = str(task.get('use_selenium', False)).lower() in ['true', 'on', '1']
            info['use_torrent_info'] = str(task.get('use_torrent_info', False)).lower() in ['true', 'on', '1']
            info['use_rss_file'] = str(task.get('use_rss_file', False)).lower() in ['true', 'on', '1']

            info['api'] = f"{ddns}/{P.package_name}/api/task?id={task_id}&apikey={apikey}"
            ret.append(info)
        return ret

    def get_search_form_info(self) -> dict:
        ret = {'site': [], 'board': {}, 'tasks': []}

        tasks = FeedConfigUtil.get_tasks()
        for t in tasks:
            ret['tasks'].append({
                'id': t.get('id'),
                'name': t.get('name', f"Task {t.get('id')}"),
                'targets': t.get('targets', [])
            })
            for tgt in t.get('targets', []):
                s = tgt.get('site')
                b = tgt.get('full_board_key', tgt.get('board'))
                if s and s not in ret['site']:
                    ret['site'].append(s)
                if s and b:
                    if s not in ret['board']:
                        ret['board'][s] = []
                    if b not in ret['board'][s]:
                        ret['board'][s].append(b)

        all_sites = ModelFeedSite.get_list()
        for s in all_sites:
            s_name = s.name if hasattr(s, 'name') else s.get('name')
            if s_name and s_name not in ret['site']:
                ret['site'].append(s_name)
            if s_name and s_name not in ret['board']:
                ret['board'][s_name] = []

        return ret
