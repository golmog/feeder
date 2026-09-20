# -*- coding: utf-8 -*-
import json
import base64
from datetime import datetime, timedelta
from types import SimpleNamespace
from flask import Response, send_file, abort, request, jsonify
from sqlalchemy import and_, or_, func, desc

from .setup import *
from .model_feed import ModelFeedSite, ModelFeedBbs, ModelFeedGroup
from .util_feed import (
    get_ddns, get_system_apikey, clean_xml_string,
    FeedConfigUtil, FeedCustomManager, FeedScraper, FeedTorrentInfo
)

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
            return jsonify({'ret': 'error', 'msg': str(e)})

    def process_command(self, command, arg1, arg2, arg3, req):
        # 수동 1회 실행 명령
        if command == 'one_execute':
            logger.info(f"[{self.name}] 1회 실행 명령 수신 -> Celery 워커 전달")
            from .task_feed import TaskBase
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

                # EXTRA 배열 호환성 동기화
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

            from .task_feed import Task
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

        # YAML 수집 스케쥴 관리 명령
        elif command == 'load_scheduler':
            sites = ModelFeedSite.get_list(by_dict=True)
            schedulers = self.get_scheduler_list()
            return jsonify({'site': sites, 'scheduler': schedulers})

        elif command == 'add_scheduler':
            scheduler_id = req.form.get('modal_scheduler_id', '-1').strip()
            site_name = req.form.get('site_name', '').strip()
            board_id = req.form.get('board_id', '').strip()
            subcat_id = req.form.get('subcat_id', '').strip()
            enabled = req.form.get('enabled') in ['True', 'on', 'true', True]
            use_proxy = req.form.get('use_proxy') in ['True', 'on', 'true', True]
            use_flaresolverr = req.form.get('use_flaresolverr') in ['True', 'on', 'true', True]
            use_selenium = req.form.get('use_selenium') in ['True', 'on', 'true', True]
            use_torrent_info = req.form.get('use_torrent_info') in ['True', 'on', 'true', True]

            try:
                interval_val = int(req.form.get('interval', 1))
                if interval_val < 1:
                    interval_val = 1
            except Exception:
                interval_val = 1

            if scheduler_id and scheduler_id != '-1' and int(scheduler_id) > 0:
                old_target = FeedConfigUtil.get_schedule(scheduler_id)
                if old_target:
                    if not site_name:
                        site_name = old_target.get('site_name', '')
                    if not board_id:
                        board_id = old_target.get('board_id', '')
                    if subcat_id == '':
                        subcat_id = old_target.get('subcat_id', '')

            proxy_url_val = req.form.get('proxy_url', '').strip()

            from .task_feed import Task
            b_id, s_id, _ = Task.parse_board_info(board_id, subcat_id)

            item_data = {
                'id': int(scheduler_id) if (scheduler_id and scheduler_id != '-1') else -1,
                'site_name': site_name,
                'board_id': b_id,
                'subcat_id': s_id,
                'interval': interval_val,
                'enabled': enabled,
                'use_proxy': use_proxy,
                'proxy_url': proxy_url_val,
                'use_flaresolverr': use_flaresolverr,
                'use_selenium': use_selenium,
                'use_torrent_info': use_torrent_info,
            }

            ret = FeedConfigUtil.save_schedule(item_data)
            return jsonify({'ret': ret, 'site': ModelFeedSite.get_list(by_dict=True), 'scheduler': self.get_scheduler_list()})

        elif command == 'remove_scheduler':
            target_id = req.form.get('target_id')
            target = FeedConfigUtil.get_schedule(target_id)
            if target:
                from .task_feed import Task
                _, _, full_board_key = Task.parse_board_info(target.get('board_id', ''), target.get('subcat_id', ''))
                self.delete_scheduler_db(target.get('site_name'), full_board_key)
                FeedConfigUtil.delete_schedule(target_id)
                ret = 'success'
            else:
                ret = 'fail'
            return jsonify({'ret': ret, 'site': ModelFeedSite.get_list(by_dict=True), 'scheduler': self.get_scheduler_list()})

        elif command == 'remove_scheduler_db':
            target_id = req.form.get('target_id')
            target = FeedConfigUtil.get_schedule(target_id)
            if target:
                from .task_feed import Task
                _, _, full_board_key = Task.parse_board_info(target.get('board_id', ''), target.get('subcat_id', ''))
                ret = self.delete_scheduler_db(target.get('site_name'), full_board_key)
            else:
                ret = 'fail'
            return jsonify({'ret': ret, 'site': ModelFeedSite.get_list(by_dict=True), 'scheduler': self.get_scheduler_list()})

        # 그룹 관리 명령
        elif command == 'load_group':
            return jsonify({'group': self.get_group_list(), 'info': self.get_search_form_info()})

        elif command == 'add_group':
            groupname = req.form.get('groupname', '').strip()
            if not groupname:
                return jsonify({'ret': 'empty'})
            if db.session.query(ModelFeedGroup).filter_by(groupname=groupname).first():
                return jsonify({'ret': 'already_exist'})
            entity = ModelFeedGroup(groupname)
            db.session.add(entity)
            db.session.commit()
            return jsonify({'ret': 'success', 'group': self.get_group_list(), 'info': self.get_search_form_info()})

        elif command == 'remove_group':
            group_id = req.form.get('group_id')
            entity = db.session.query(ModelFeedGroup).filter_by(id=group_id).first()
            if entity:
                db.session.delete(entity)
                db.session.commit()
                ret = 'success'
            else:
                ret = 'not_exist'
            return jsonify({'ret': ret, 'group': self.get_group_list(), 'info': self.get_search_form_info()})

        elif command == 'add_group_child':
            group_id = req.form.get('group_id')
            sitename = req.form.get('sitename')
            boardname = req.form.get('boardname')
            group_entity = db.session.query(ModelFeedGroup).filter_by(id=group_id).with_for_update().first()
            if not group_entity or not sitename or not boardname:
                return jsonify({'ret': 'fail'})

            boards = group_entity.get_boards()
            for b in boards:
                if b.get('site_name') == sitename and str(b.get('board_id')) == str(boardname):
                    return jsonify({'ret': 'already_exist'})
            boards.append({'site_name': sitename, 'board_id': boardname})
            group_entity.set_boards(boards)
            db.session.commit()
            return jsonify({'ret': 'success', 'group': self.get_group_list(), 'info': self.get_search_form_info()})

        elif command == 'remove_group_child':
            group_id = req.form.get('group_id')
            child_id = int(req.form.get('child_id', -1))
            group_entity = db.session.query(ModelFeedGroup).filter_by(id=group_id).with_for_update().first()
            if not group_entity:
                return jsonify({'ret': 'not_exist'})

            boards = group_entity.get_boards()
            if 0 <= child_id < len(boards):
                boards.pop(child_id)
                group_entity.set_boards(boards)
                db.session.commit()
                ret = 'success'
            else:
                ret = 'not_exist'
            return jsonify({'ret': ret, 'group': self.get_group_list(), 'info': self.get_search_form_info()})

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

    # FlaskFarm 표준 API 처리 엔드포인트
    def process_api(self, sub, req):
        try:
            if sub == 'board':
                sitename = req.args.get('site')
                boardname = req.args.get('board')
                subcat = req.args.get('subcat')
                scheduler_id = req.args.get('id')
                if scheduler_id:
                    return self._handle_board_id_rss(scheduler_id)
                if sitename and boardname:
                    from .task_feed import Task
                    _, _, full_board_key = Task.parse_board_info(boardname, subcat)
                    return self._handle_board_rss(sitename, full_board_key)
                return jsonify({'ret': 'fail', 'msg': '파라미터 누락'}), 400

            elif sub == 'group':
                groupname = req.args.get('name')
                if groupname:
                    return self._handle_group_rss(groupname)
                return jsonify({'ret': 'fail', 'msg': '그룹명 누락'}), 400

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
            items = db.session.query(ModelFeedBbs).filter_by(site=sitename, board=boardname).order_by(ModelFeedBbs.id.desc()).limit(feed_count).all()
            xml_content = self.generate_rss_feed(f"SITE: {sitename} / BOARD: {boardname}", items)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[Feeder] _handle_board_rss 에러: {e}")
            return Response('Internal Error', status=500)

    def _handle_board_id_rss(self, scheduler_id):
        try:
            target = FeedConfigUtil.get_schedule(scheduler_id)
            if not target:
                return jsonify({'ret': 'not_exist'}), 404

            sitename = target.get('site_name')
            from .task_feed import Task
            _, _, full_board_key = Task.parse_board_info(target.get('board_id', ''), target.get('subcat_id', ''))

            feed_count = P.ModelSetting.get_int(f"{self.name}_feed_count") if P.ModelSetting else 100
            items = db.session.query(ModelFeedBbs).filter_by(site=sitename, board=full_board_key).order_by(ModelFeedBbs.id.desc()).limit(feed_count).all()
            xml_content = self.generate_rss_feed(f"SITE: {sitename} / BOARD: {full_board_key}", items)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[Feeder] _handle_board_id_rss 에러: {e}")
            return Response('Internal Error', status=500)

    def _handle_group_rss(self, groupname):
        try:
            group_entity = db.session.query(ModelFeedGroup).filter_by(groupname=groupname).first()
            items = []
            if group_entity:
                boards = group_entity.get_boards()
                if boards:
                    group_conditions = [and_(ModelFeedBbs.site == b.get('site_name'), ModelFeedBbs.board == b.get('board_id')) for b in boards]
                    group_key = func.coalesce(
                        func.nullif(ModelFeedBbs.magnet, ''),
                        func.cast(ModelFeedBbs.id, db.String)
                    )
                    subq = (
                        db.session.query(func.max(ModelFeedBbs.id).label("max_id"))
                        .filter(or_(*group_conditions))
                        .group_by(group_key)
                        .subquery()
                    )
                    feed_count = P.ModelSetting.get_int(f"{self.name}_feed_count") if P.ModelSetting else 100
                    items = db.session.query(ModelFeedBbs).join(subq, ModelFeedBbs.id == subq.c.max_id).order_by(ModelFeedBbs.id.desc()).limit(feed_count).all()
                    logger.debug(f"[Feeder] 그룹 RSS 피드 추출 완료: 그룹명='{groupname}', 대상 게시글={len(items)}개")

            xml_content = self.generate_rss_feed(f"GROUP: {groupname}", items)
            return Response(xml_content, mimetype='application/xml; charset=utf-8')
        except Exception as e:
            logger.error(f"[Feeder] _handle_group_rss 에러: {e}")
            return Response('Internal Error', status=500)

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

            from .task_feed import Task
            target = None
            for s in FeedConfigUtil.get_schedules():
                _, _, s_full_key = Task.parse_board_info(s.get('board_id', ''), s.get('subcat_id', ''))
                if s.get('site_name') == post.site and str(s_full_key) == str(post.board):
                    target = s
                    break

            target_cfg = SimpleNamespace(
                use_proxy=target.get('use_proxy', False) if target else P.ModelSetting.get_bool(f"{self.name}_use_proxy"),
                proxy_url=target.get('proxy_url', '') if target else P.ModelSetting.get(f"{self.name}_proxy_url"),
                use_flaresolverr=target.get('use_flaresolverr', False) if target else False
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

    def generate_rss_feed(self, title: str, items: list) -> str:
        ddns = get_ddns()
        apikey = get_system_apikey()

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

                    download_proxy_url = f"{ddns}/{P.package_name}/api/download?id={bbs.id}_{f_idx}&apikey={apikey}"

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

        from .task_feed import TaskBase
        self.start_celery(TaskBase.start, None, "default")

    def delete_scheduler_db(self, site_name, board_id) -> str:
        try:
            db.session.query(ModelFeedBbs).filter_by(site=site_name, board=board_id).delete()
            db.session.commit()
            self.db_vacuum()
            return 'success'
        except Exception as e:
            logger.error(f"[Feeder] delete_scheduler_db 에러: {e}")
            db.session.rollback()
        return 'fail'

    def db_vacuum(self):
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
                    logger.info(f"[{self.name}] SQLite DB VACUUM 정리 완료")
                finally:
                    raw_conn.close()
        except Exception as e:
            logger.error(f"[{self.name}] db_vacuum 실행 오류: {e}")

    def get_scheduler_list(self) -> list[dict]:
        ret = []
        raw_schedules = FeedConfigUtil.get_schedules()
        ddns = get_ddns()
        apikey = get_system_apikey()
        from .task_feed import Task

        for item in raw_schedules:
            site_name = item.get('site_name', '')
            board_id = item.get('board_id', '')
            subcat_id = item.get('subcat_id', '')

            site_entity = ModelFeedSite.get(name=site_name)
            _, _, full_board_key = Task.parse_board_info(board_id, subcat_id)
            last_bbs = ModelFeedBbs.get_last_bbs(site_name, full_board_key)

            info = dict(item)
            info['last'] = last_bbs.as_dict() if last_bbs else None
            info['site_info'] = site_entity.as_dict() if site_entity else {}
            info['full_board_key'] = full_board_key

            info['enabled'] = str(item.get('enabled', True)).lower() in ['true', 'on', '1']
            info['use_proxy'] = str(item.get('use_proxy', False)).lower() in ['true', 'on', '1']
            info['proxy_url'] = item.get('proxy_url', '')
            info['use_flaresolverr'] = str(item.get('use_flaresolverr', False)).lower() in ['true', 'on', '1']
            info['use_selenium'] = str(item.get('use_selenium', False)).lower() in ['true', 'on', '1']
            info['use_torrent_info'] = str(item.get('use_torrent_info', False)).lower() in ['true', 'on', '1']

            info['api'] = f"{ddns}/{P.package_name}/api/board?site={site_name}&board={full_board_key}&apikey={apikey}"

            if last_bbs:
                info['one_day_more'] = (last_bbs.created_time < datetime.now() - timedelta(days=1))
            else:
                info['one_day_more'] = False

            ret.append(info)
        return ret

    def get_group_list(self) -> list[dict]:
        groups = db.session.query(ModelFeedGroup).order_by(ModelFeedGroup.id.asc()).all()
        ddns = get_ddns()
        apikey = get_system_apikey()
        ret = []

        for g in groups:
            item = g.as_dict()
            item['rss'] = f"{ddns}/{P.package_name}/api/group?name={item['groupname']}&apikey={apikey}"
            item['schedulers'] = [
                {'id': idx, 'sitename': b.get('site_name'), 'board_id': b.get('board_id')}
                for idx, b in enumerate(g.get_boards())
            ]
            ret.append(item)
        return ret

    def get_search_form_info(self) -> dict:
        ret = {'group': self.get_group_list(), 'site': [], 'board': {}}

        all_sites = ModelFeedSite.get_list()
        for s in all_sites:
            s_name = s.name if hasattr(s, 'name') else s.get('name')
            if s_name and s_name not in ret['site']:
                ret['site'].append(s_name)
                ret['board'][s_name] = []

        schedules = FeedConfigUtil.get_schedules()
        from .task_feed import Task
        for item in schedules:
            site = item.get('site_name')
            board = item.get('board_id')
            subcat = item.get('subcat_id')
            _, _, full_key = Task.parse_board_info(board, subcat)

            if site and site not in ret['site']:
                ret['site'].append(site)
            if site and full_key:
                if site not in ret['board']:
                    ret['board'][site] = []
                if full_key not in ret['board'][site]:
                    ret['board'][site].append(full_key)

        try:
            db_boards = db.session.query(ModelFeedBbs.site, ModelFeedBbs.board).distinct().all()
            for s, b in db_boards:
                if s and s not in ret['site']:
                    ret['site'].append(s)
                if s and b:
                    if s not in ret['board']:
                        ret['board'][s] = []
                    if b not in ret['board'][s]:
                        ret['board'][s].append(b)
        except Exception:
            pass

        return ret
