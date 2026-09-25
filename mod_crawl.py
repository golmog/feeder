# -*- coding: utf-8 -*-
import os
import json
import base64
import traceback
from datetime import datetime, timedelta
from types import SimpleNamespace
from flask import render_template, request, jsonify, abort, send_file
from sqlalchemy import and_, or_, desc

from .setup import *
from .model_crawl import ModelCrawlSite, ModelCrawlItem
from .util_crawl import (
    get_ddns, get_system_apikey, CrawlConfigUtil, CrawlCustomManager,
    CrawlScraper, CrawlTorrentInfo, split_magnets, extract_info_hash
)
from .task_crawl import TaskCrawlBase, TaskCrawl

name = 'crawl'


class ModuleCrawl(PluginModuleBase):

    def __init__(self, P):
        super(ModuleCrawl, self).__init__(P, 'setting', name=name, scheduler_desc="Feeder - 사이트 크롤러 및 수집 관리")
        self.web_list_model = ModelCrawlItem
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "10",
            f"{self.name}_db_delete_day": "30",
            f"{self.name}_db_auto_delete": "False",
            f"{self.name}_max_page": "5",
            f"{self.name}_always_max_page": "False",
            f"{self.name}_test_count": "3",
            f"{self.name}_crawler_delay": "2.0",
            f"{self.name}_crawler_max_retries": "3",
            f"{self.name}_allow_duplicate_magnet": "False",
            f"{self.name}_scheduler_count": "0",
            f"{self.name}_is_running": "False",
            f"{self.name}_test_running": "False",
            f"{self.name}_running_start_time": "0",
            f"{self.name}_use_proxy": "False",
            f"{self.name}_proxy_url": "",
            f"{self.name}_use_flaresolverr": "False",
            f"{self.name}_flaresolverr_url": "",
            f"{self.name}_use_selenium": "False",
            f"{self.name}_selenium_remote_url": "",
            f"{self.name}_selenium_timeout": "20",
            f"{self.name}_use_torrent_info": "False",
            f"{self.name}_torrent_info_method": "plugin",
            f"{self.name}_qb_url": "",
            f"{self.name}_qb_username": "",
            f"{self.name}_qb_password": "",
            f"{self.name}_qb_temp_category": "feeder_temp",
        }

    def plugin_load(self):
        try:
            P.ModelSetting.set(f"{self.name}_is_running", "False")
            P.ModelSetting.set(f"{self.name}_test_running", "False")
            P.ModelSetting.set(f"{self.name}_running_start_time", "0")
            # logger.info(f"[{self.name}] 플러그인 로드: 수집 런타임 락 플래그 초기화 완료")
        except Exception as e:
            logger.debug(f"[{self.name}] plugin_load 초기화 예외: {e}")
        try:
            super(ModuleCrawl, self).plugin_load()
        except Exception:
            pass

    def process_menu(self, page_name, req):
        try:
            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['sub'] = self.name
            arg['current_page'] = page_name
            arg['ddns'] = get_ddns()
            arg['apikey'] = get_system_apikey()

            if page_name == 'setting':
                arg['yaml_filepath'] = CrawlConfigUtil.get_filepath()
                arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
                arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
                return render_template(f'{P.package_name}_{self.name}_setting.html', arg=arg)

            arg['is_torrent_info_installed'] = True
            arg['site_info'] = self.get_search_form_info()
            return render_template(f'{P.package_name}_{self.name}_list.html', arg=arg)
        except Exception as e:
            logger.error(f"[{self.name}] process_menu 에러: {e}")
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def process_ajax(self, sub, req):
        try:
            if sub == 'setting_save':
                ret = super(ModuleCrawl, self).process_ajax(sub, req)
                try:
                    if F.scheduler.is_include(self.get_scheduler_name()):
                        F.scheduler.remove_job(self.get_scheduler_name())
                        F.scheduler.add_job_instance(self)
                        logger.info(f"[{self.name}] 설정 저장에 따른 스케줄러 주기 갱신 완료 ({P.ModelSetting.get(f'{self.name}_interval')}분)")
                except Exception as ex:
                    logger.debug(f"[{self.name}] 스케줄러 갱신 예외: {ex}")
                return ret

            if sub == 'web_list':
                return jsonify(self.web_list_model.web_list(req))

            command = req.form.get('command') or sub
            if command:
                res = self.process_command(command, req.form.get('arg1', ''), req.form.get('arg2', ''), req.form.get('arg3', ''), req)
                if res is not None:
                    return res

            return super(ModuleCrawl, self).process_ajax(sub, req)
        except Exception as e:
            logger.error(f"[{self.name}] process_ajax 에러: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'error', 'msg': str(e)})

    def process_command(self, command, arg1, arg2, arg3, req):
        if command == 'manual_crawl':
            is_running = P.ModelSetting.get_bool(f"{self.name}_is_running")
            if is_running:
                start_time = P.ModelSetting.get(f"{self.name}_running_start_time")
                if start_time and str(start_time).isdigit() and (time.time() - int(start_time) > 3600):
                    logger.warning(f"[{self.name}] 1시간 경과 락 감지 -> 락 강제 해제")
                    P.ModelSetting.set(f"{self.name}_is_running", "False")
                else:
                    return jsonify({'ret': 'running', 'msg': '현재 다른 수집 작업이 이미 실행 중입니다.'})

            crawler_id = req.form.get('crawler_id')
            target_c_id = int(crawler_id) if crawler_id and str(crawler_id).isdigit() else None
            target_desc = f"개별 수집기(ID: {target_c_id})" if target_c_id else "전체 수집기"

            import threading
            def dispatch_celery_task():
                try:
                    logger.info(f"[{self.name}] [즉시 실행] {target_desc} Celery 비동기 디스패치")
                    self.start_celery(TaskCrawlBase.start, None, "manual", target_c_id)
                except Exception as ex:
                    logger.error(f"[{self.name}] [즉시 실행] Celery 전달 실패: {ex}")

            threading.Thread(target=dispatch_celery_task, daemon=True).start()
            return jsonify({'ret': 'success', 'msg': f'{target_desc} 즉시 실행을 시작했습니다.'})

        elif command == 'load_site':
            CrawlCustomManager.sync_default_site_info()
            sites = ModelCrawlSite.get_list(by_dict=True)
            return jsonify({'site': sites})

        elif command == 'site_toggle_option':
            site_id = req.form.get('site_id')
            option_name = req.form.get('option')
            site = ModelCrawlSite.get(site_id=site_id)
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
                return jsonify({'ret': 'success', 'site': ModelCrawlSite.get_list(by_dict=True)})

            return jsonify({'ret': 'fail', 'log': '알 수 없는 옵션 항목입니다.'})

        elif command == 'test':
            site_id = req.form.get('site_id')
            board_input = req.form.get('board_id', '').strip()
            site = ModelCrawlSite.get(site_id=site_id)
            if not site or not board_input:
                return jsonify({'ret': 'fail', 'log': '사이트 또는 게시판 ID가 올바르지 않습니다.'})

            if P.ModelSetting.get_bool(f"{self.name}_is_running"):
                return jsonify({'ret': 'fail', 'log': '현재 백그라운드 크롤링이 실행 중입니다.'})

            board_id, subcat_id, full_board_key = TaskCrawl.parse_board_info(board_input)

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

            P.ModelSetting.set(f"{self.name}_test_running", "True")
            try:
                test_results = TaskCrawl.execute_board_crawl(site.info, board_id, is_test=True, max_count=test_count, target_cfg=test_target_cfg)
                if test_results is None:
                    test_results = []
            finally:
                P.ModelSetting.set(f"{self.name}_test_running", "False")

            return jsonify(test_results)

        elif command == 'site_delete':
            site_id = req.form.get('site_id')
            ret = ModelCrawlSite.delete(site_id)
            sites = ModelCrawlSite.get_list(by_dict=True)
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
            entity = ModelCrawlSite.get(site_id=int(site_id)) if is_edit_mode else None

            if entity:
                entity.name = site_name
                entity.info = info
                db.session.commit()
                return jsonify({'ret': 'edit_success', 'site': ModelCrawlSite.get_list(by_dict=True)})
            else:
                if ModelCrawlSite.get(name=site_name):
                    return jsonify({'ret': 'exist', 'log': f"동일 이름('{site_name}')의 사이트가 이미 존재합니다."})
                new_site = ModelCrawlSite('my', info, '직접 입력')
                db.session.add(new_site)
                db.session.commit()
                return jsonify({'ret': 'add_success', 'site': ModelCrawlSite.get_list(by_dict=True)})

        elif command == 'custom_script_list':
            custom_dir = CrawlCustomManager.get_custom_dir()
            files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
            files.sort()
            return jsonify({'ret': 'success', 'files': files})

        elif command == 'custom_script_read':
            filename = os.path.basename(req.form.get('filename', '').strip())
            custom_dir = CrawlCustomManager.get_custom_dir()
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

            custom_dir = CrawlCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            try:
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"[{self.name}] 커스텀 스크립트 저장 완료: {filename}")
                CrawlCustomManager.load_hooks()
                files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({
                    'ret': 'success',
                    'filename': filename,
                    'files': files,
                    'site': ModelCrawlSite.get_list(by_dict=True)
                })
            except Exception as e:
                logger.error(f"[{self.name}] custom_script_save 에러: {e}")
                return jsonify({'ret': 'error', 'log': str(e)})

        elif command == 'custom_script_delete':
            filename = os.path.basename(req.form.get('filename', '').strip())
            custom_dir = CrawlCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            if not filename or not os.path.exists(fpath):
                return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
            try:
                os.remove(fpath)
                logger.info(f"[{self.name}] 커스텀 스크립트 삭제 완료: {filename}")
                CrawlCustomManager.load_hooks()
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

            custom_dir = CrawlCustomManager.get_custom_dir()
            fpath = os.path.join(custom_dir, filename)
            try:
                file_obj.save(fpath)
                logger.info(f"[{self.name}] 커스텀 스크립트 업로드 완료: {filename}")
                CrawlCustomManager.load_hooks()
                files = [f for f in os.listdir(custom_dir) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({
                    'ret': 'success',
                    'filename': filename,
                    'files': files,
                    'site': ModelCrawlSite.get_list(by_dict=True)
                })
            except Exception as e:
                logger.error(f"[{self.name}] custom_script_upload 에러: {e}")
                return jsonify({'ret': 'error', 'log': str(e)})

        elif command == 'load_crawlers':
            sites = ModelCrawlSite.get_list(by_dict=True)
            crawlers = self.get_crawler_list()
            return jsonify({'site': sites, 'crawlers': crawlers})

        elif command == 'add_crawler':
            crawler_id = req.form.get('modal_crawler_id', '-1').strip()
            site = (req.form.get('site') or req.form.get('crawler_site', '')).strip()
            boards_json = req.form.get('boards_json', '[]')
            try:
                boards = json.loads(boards_json)
            except Exception:
                boards = []

            try:
                interval_val = int(req.form.get('crawler_interval') or req.form.get('interval', 1))
                if interval_val < 1:
                    interval_val = 1
            except Exception:
                interval_val = 1

            delay_raw = req.form.get('crawler_delay', '').strip()
            max_retries_raw = req.form.get('crawler_max_retries', '').strip()

            enabled = (req.form.get('crawler_enabled') or req.form.get('enabled')) in ['True', 'on', 'true', True]
            use_proxy = (req.form.get('crawler_use_proxy') or req.form.get('use_proxy')) in ['True', 'on', 'true', True]
            proxy_url_val = (req.form.get('crawler_proxy_url') or req.form.get('proxy_url', '')).strip()
            use_flaresolverr = (req.form.get('crawler_use_flaresolverr') or req.form.get('use_flaresolverr')) in ['True', 'on', 'true', True]
            use_selenium = (req.form.get('crawler_use_selenium') or req.form.get('use_selenium')) in ['True', 'on', 'true', True]
            use_torrent_info = (req.form.get('crawler_use_torrent_info') or req.form.get('use_torrent_info')) in ['True', 'on', 'true', True]

            item_data = {
                'id': int(crawler_id) if (crawler_id and crawler_id != '-1') else -1,
                'site': site,
                'boards': boards,
                'interval': interval_val,
                'delay': float(delay_raw) if delay_raw else '',
                'max_retries': int(max_retries_raw) if max_retries_raw.isdigit() else '',
                'enabled': enabled,
                'use_proxy': use_proxy,
                'proxy_url': proxy_url_val,
                'use_flaresolverr': use_flaresolverr,
                'use_selenium': use_selenium,
                'use_torrent_info': use_torrent_info,
            }

            ret = CrawlConfigUtil.save_crawler(item_data)
            return jsonify({'ret': ret, 'crawlers': self.get_crawler_list()})

        elif command == 'remove_crawler':
            target_id = req.form.get('target_id')
            crawler = CrawlConfigUtil.get_crawler(target_id)
            if crawler:
                self.delete_crawler_db(crawler)
                CrawlConfigUtil.delete_crawler(target_id)
                ret = 'success'
            else:
                ret = 'fail'
            return jsonify({'ret': ret, 'crawlers': self.get_crawler_list()})

        elif command == 'remove_crawler_db':
            target_id = req.form.get('target_id')
            crawler = CrawlConfigUtil.get_crawler(target_id)
            if crawler:
                ret = self.delete_crawler_db(crawler)
            else:
                ret = 'fail'
            return jsonify({'ret': ret, 'crawlers': self.get_crawler_list()})

        elif command == 'clear_board_db':
            site_val = req.form.get('site', '').strip()
            board_val = req.form.get('board', '').strip()
            subcat_val = req.form.get('subcat', '').strip()
            if not site_val or not board_val:
                return jsonify({'ret': 'fail', 'msg': '사이트 또는 게시판 정보 누락'})
            ret = self.delete_board_db(site_val, board_val, subcat_val)
            return jsonify({'ret': ret})

        elif command == 'torrent_info':
            magnet_hash = req.form.get('hash')
            info_list = CrawlTorrentInfo.get_torrent_info([magnet_hash])
            result_data = info_list[0] if (info_list and len(info_list) > 0) else None
            return jsonify(result_data)

        elif command == 'direct_download':
            title = req.form.get('title', '').strip()
            magnet = req.form.get('magnet', '').strip()
            profile_name = req.form.get('profile_name', '').strip()
            feed_name = req.form.get('feed_name', 'CRAWL_DIRECT')

            if not magnet:
                return jsonify({'ret': 'fail', 'msg': '마그넷/ed2k 링크가 누락되었습니다.'})

            from .model_download import ModelDownload

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
                yaml_data = CrawlConfigUtil.load_yaml()
                for p in yaml_data.get('DOWNLOAD_PROFILES', []):
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
                yaml_data = CrawlConfigUtil.load_yaml()
                enabled_downloaders = [d['name'] for d in yaml_data.get('DOWNLOADERS', []) if d.get('enabled', True)]
                dl_item.priority_chain = enabled_downloaders
                dl_item.destination_type = 'local'

            dl_item.status = 'pending'
            db.session.add(dl_item)
            db.session.commit()
            logger.info(f"[{self.name}] 다운로드 큐 직접 추가: {title} (체인: {' -> '.join(dl_item.priority_chain)})")
            return jsonify({'ret': 'success', 'msg': '다운로드 큐에 성공적으로 등록되었습니다.'})

        elif command == 'fix_ed2k_db':
            res = self.fix_database_magnets()
            return jsonify(res)

        elif command in ['db_delete', 'reset_db']:
            try:
                db.session.query(ModelCrawlItem).delete()
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
                db.session.query(ModelCrawlItem).filter(ModelCrawlItem.created_time < target_date).delete()
                db.session.commit()
                self.db_vacuum()
                return jsonify(True)
            except Exception as e:
                logger.error(f"[{self.name}] db_delete_day 에러: {e}")
                db.session.rollback()
                return jsonify(False)

        return super(ModuleCrawl, self).process_command(command, arg1, arg2, arg3, req)

    def process_api(self, sub, req):
        try:
            if sub == 'download':
                return self._handle_download_stream(req.args.get('id'))
            elif sub == 'site_update':
                return self._handle_site_update()
            return jsonify({'ret': 'fail', 'msg': f'알 수 없는 API 명령: {sub}'}), 404
        except Exception as e:
            logger.error(f"[CrawlAPI] process_api 에러 ({sub}): {e}")
            return jsonify({'ret': 'error', 'msg': str(e)}), 500

    def _handle_download_stream(self, bbs_file_id):
        try:
            tokens = bbs_file_id.split('_')
            post_id = int(tokens[0])
            file_index = int(tokens[1])

            post = ModelCrawlItem.get(id=post_id)
            if not post:
                abort(404)

            post_dict = post.as_dict()
            if not post_dict['files'] or len(post_dict['files']) <= file_index:
                abort(404)

            target_file = post_dict['files'][file_index]
            download_url = target_file[0]
            filename = target_file[1]

            target_crawler = CrawlConfigUtil.get_crawler_by_board(post.site, post.board)
            target_cfg = SimpleNamespace(
                use_proxy=target_crawler.get('use_proxy', False) if target_crawler else P.ModelSetting.get_bool(f"{self.name}_use_proxy"),
                proxy_url=target_crawler.get('proxy_url', '') if target_crawler else P.ModelSetting.get(f"{self.name}_proxy_url"),
                use_flaresolverr=target_crawler.get('use_flaresolverr', False) if target_crawler else False
            )

            byte_io = CrawlScraper.download_file_stream(download_url, referer=post.url, scheduler_instance=target_cfg)
            mimetype = 'application/x-bittorrent' if filename.lower().endswith('.torrent') else 'application/octet-stream'

            try:
                return send_file(byte_io, mimetype=mimetype, as_attachment=True, download_name=filename)
            except TypeError:
                return send_file(byte_io, mimetype=mimetype, as_attachment=True, attachment_filename=filename)
        except Exception as e:
            logger.error(f"[CrawlAPI] _handle_download_stream 에러: {e}")
            abort(500)

    def _handle_site_update(self):
        try:
            content_b64 = request.form.get('content') or request.args.get('content', '')
            decoded_text = base64.b64decode(content_b64).decode('utf-8')
            info = json.loads(decoded_text)
            site_name = info.get('NAME')

            if not site_name:
                return jsonify({'ret': 'error', 'log': 'NAME 누락'}), 400

            existing_site = ModelCrawlSite.get(name=site_name)
            if existing_site:
                existing_site.info = info
                existing_site.content = decoded_text
                db.session.commit()
                return jsonify({'ret': 'update', 'log': f'{site_name} 업데이트 완료'})
            else:
                new_site = ModelCrawlSite('web', info, decoded_text)
                db.session.add(new_site)
                db.session.commit()
                return jsonify({'ret': 'add', 'log': f'{site_name} 신규 추가 완료'})
        except Exception as e:
            logger.error(f"[CrawlAPI] _handle_site_update 에러: {e}")
            return jsonify({'ret': 'error', 'log': str(e)}), 500

    def scheduler_function(self):
        is_running = P.ModelSetting.get_bool(f"{self.name}_is_running")
        if is_running:
            start_time = P.ModelSetting.get(f"{self.name}_running_start_time")
            if start_time and str(start_time).isdigit() and (time.time() - int(start_time) > 3600):
                logger.warning(f"[{self.name}] 1시간 경과 고아 락 감지 -> 락 강제 해제")
                P.ModelSetting.set(f"{self.name}_is_running", "False")
            else:
                logger.info(f"[{self.name}] 이전 수집 작업이 진행 중이므로 스케줄 주기를 건너뜁니다.")
                return

        if P.ModelSetting.get_bool(f"{self.name}_db_auto_delete"):
            try:
                day = P.ModelSetting.get_int(f"{self.name}_db_delete_day")
                if day > 0:
                    target_date = datetime.now() - timedelta(days=day)
                    deleted_count = db.session.query(ModelCrawlItem).filter(ModelCrawlItem.created_time < target_date).delete()
                    db.session.commit()
                    if deleted_count > 0:
                        self.db_vacuum()
                    logger.debug(f"[{self.name}] {day}일 경과 수집 DB 자동 정리 완료 (삭제: {deleted_count}건)")
            except Exception as e:
                logger.error(f"[{self.name}] db auto delete 에러: {e}")
                db.session.rollback()

        logger.info(f"[{self.name}] [스케줄러 자동 실행] 정기 크롤링 워커 작업 전달")
        self.start_celery(TaskCrawlBase.start, None, "scheduler", None)

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
                    logger.info(f"[{self.name}] SQLite DB VACUUM 정리 완료 ({P.package_name}.db)")
                finally:
                    raw_conn.close()
        except Exception as e:
            logger.error(f"[{self.name}] db_vacuum 실행 오류: {e}")

    def delete_crawler_db(self, crawler: dict) -> str:
        try:
            site_val = crawler.get('site')
            for b in crawler.get('boards', []):
                board_val = b.get('full_board_key') or b.get('board')
                db.session.query(ModelCrawlItem).filter_by(site=site_val, board=board_val).delete()
            db.session.commit()
            self.db_vacuum()
            return 'success'
        except Exception as e:
            logger.error(f"[{self.name}] delete_crawler_db 에러: {e}")
            db.session.rollback()
        return 'fail'

    def delete_board_db(self, site: str, board: str, subcat: str = '') -> str:
        try:
            _, _, full_board_key = TaskCrawl.parse_board_info(board, subcat)
            deleted = db.session.query(ModelCrawlItem).filter_by(site=site, board=full_board_key).delete()
            db.session.commit()
            if deleted > 0:
                self.db_vacuum()
            logger.info(f"[{self.name}] 개별 게시판 DB 비우기 완료: [{site}] {full_board_key} (삭제: {deleted}건)")
            return 'success'
        except Exception as e:
            logger.error(f"[{self.name}] delete_board_db 에러: {e}")
            db.session.rollback()
        return 'fail'

    def fix_database_magnets(self):
        with F.app.app_context():
            try:
                from .model_download import ModelDownload

                fixed_bbs_count = 0
                bbs_rows = db.session.query(ModelCrawlItem).filter(ModelCrawlItem.magnet.isnot(None)).all()
                for row in bbs_rows:
                    if not row.magnet:
                        continue
                    links = split_magnets(row.magnet)
                    if not links:
                        continue
                    new_magnet_str = '\n'.join(links)
                    new_count = len(links)
                    if row.magnet != new_magnet_str or row.magnet_count != new_count:
                        row.magnet = new_magnet_str
                        row.magnet_count = new_count
                        fixed_bbs_count += 1

                fixed_dl_count = 0
                deleted_dl_count = 0
                dl_rows = db.session.query(ModelDownload).all()
                for dl in dl_rows:
                    mag = (dl.magnet or '').strip()
                    if mag == 'ed2k://' or mag == 'file' or mag == '/' or (mag.startswith('ed2k://') and not mag.endswith('|/')):
                        matched_bbs = None
                        if dl.title:
                            matched_bbs = db.session.query(ModelCrawlItem).filter(ModelCrawlItem.title == dl.title).first()

                        if matched_bbs and matched_bbs.magnet:
                            bbs_links = split_magnets(matched_bbs.magnet)
                            valid_ed2k = [l for l in bbs_links if l.startswith('ed2k://')]
                            if valid_ed2k:
                                dl.magnet = valid_ed2k[0]
                                dl.infohash = extract_info_hash(dl.magnet)
                                fixed_dl_count += 1
                                continue

                        if dl.status != 'completed':
                            db.session.delete(dl)
                            deleted_dl_count += 1

                db.session.commit()
                db.session.expire_all()
                logger.info(f"[{self.name}] DB 마그넷/ed2k 보정 완료: 수집게시판(BBS) {fixed_bbs_count}건 보정, 다운로드큐 {fixed_dl_count}건 복원, {deleted_dl_count}건 파편 레코드 삭제")
                return {'ret': 'success', 'bbs_fixed': fixed_bbs_count, 'dl_fixed': fixed_dl_count, 'dl_deleted': deleted_dl_count}
            except Exception as e:
                logger.error(f"[{self.name}] fix_database_magnets 에러: {e}")
                logger.error(traceback.format_exc())
                db.session.rollback()
                return {'ret': 'error', 'msg': str(e)}

    def get_crawler_list(self) -> list[dict]:
        ret = []
        crawlers = CrawlConfigUtil.get_crawlers()

        for c in crawlers:
            info = dict(c)
            site_name = c.get('site')
            boards = c.get('boards', [])

            last_bbs = None
            if boards:
                board_conds = [
                    and_(ModelCrawlItem.site == site_name, ModelCrawlItem.board == (b.get('full_board_key') or b.get('board')))
                    for b in boards
                ]
                last_bbs = db.session.query(ModelCrawlItem).filter(or_(*board_conds)).order_by(ModelCrawlItem.id.desc()).first()

            info['last'] = last_bbs.as_dict() if last_bbs else None
            info['boards'] = boards
            info['board_count'] = len(boards)
            info['delay'] = c.get('delay', '')
            info['max_retries'] = c.get('max_retries', '')
            info['enabled'] = str(c.get('enabled', True)).lower() in ['true', 'on', '1']
            info['use_proxy'] = str(c.get('use_proxy', False)).lower() in ['true', 'on', '1']
            info['proxy_url'] = c.get('proxy_url', '')
            info['use_flaresolverr'] = str(c.get('use_flaresolverr', False)).lower() in ['true', 'on', '1']
            info['use_selenium'] = str(c.get('use_selenium', False)).lower() in ['true', 'on', '1']
            info['use_torrent_info'] = str(c.get('use_torrent_info', False)).lower() in ['true', 'on', '1']

            ret.append(info)
        return ret

    def get_search_form_info(self) -> dict:
        ret = {'site': [], 'board': {}}

        for c in CrawlConfigUtil.get_crawlers():
            s = c.get('site')
            if not s:
                continue

            if s not in ret['site']:
                ret['site'].append(s)
                ret['board'][s] = []

            for b_item in c.get('boards', []):
                b_val = b_item.get('board', '')
                sub_val = str(b_item.get('subcat', '')).strip()
                _, _, f_key = TaskCrawl.parse_board_info(b_val, sub_val)
                if not f_key:
                    continue

                display_name = f_key if sub_val else str(b_val)
                if not any(x['key'] == f_key for x in ret['board'][s]):
                    ret['board'][s].append({'key': f_key, 'name': display_name})

        try:
            db_boards = db.session.query(ModelCrawlItem.site, ModelCrawlItem.board).distinct().all()
            for s, b in db_boards:
                if not s or not b:
                    continue
                if s not in ret['site']:
                    ret['site'].append(s)
                    ret['board'][s] = []

                if not any(x['key'] == b for x in ret['board'][s]):
                    ret['board'][s].append({'key': b, 'name': b})
        except Exception:
            pass

        all_sites = ModelCrawlSite.get_list()
        for s in all_sites:
            s_name = s.name if hasattr(s, 'name') else s.get('name')
            if s_name and s_name not in ret['site']:
                ret['site'].append(s_name)
            if s_name and s_name not in ret['board']:
                ret['board'][s_name] = []

        return ret
