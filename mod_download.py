# -*- coding: utf-8 -*-

import os
import json
import time
import traceback
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, Response, stream_with_context, has_request_context
from sqlalchemy import desc, or_, and_, func

from .setup import *
from .model_download import ModelDownload, ModelDownloadStat
from .util_feed import FeedConfigUtil, get_ddns, get_system_apikey
from .util_download import DownloaderManager
from .task_download import TaskDownloadBase

name = 'download'


class ModuleDownload(PluginModuleBase):

    def __init__(self, P):
        super(ModuleDownload, self).__init__(P, 'setting', name=name, scheduler_desc="Feeder - 오프라인/로컬 다운로드 및 클라우드 업로드")
        self.web_list_model = ModelDownload
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "5",
            f"{self.name}_db_delete_day": "30",
            f"{self.name}_db_auto_delete": "False",
            # 스토리지 및 업로드 수치 설정 (하드코딩 제거)
            f"{self.name}_local_staging_path": "",
            f"{self.name}_rclone_conf_path": "",
            f"{self.name}_rclone_remote_name": "net",
            f"{self.name}_rclone_shared_remote_name": "gf",
            f"{self.name}_shared_drive_id": "",
            f"{self.name}_rclone_bind_ip": "",
            f"{self.name}_rclone_chunk_size": "256M",
            f"{self.name}_mydrive_upload_threshold": "14GB",
            f"{self.name}_gdrive_upload_limit": "700GB",
            f"{self.name}_shared_drive_upload_limit": "3TB",
            f"{self.name}_shared_drive_quota_reset_time": "16:00",
            f"{self.name}_enable_sse": "True",
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

            elif page_name == 'queue':
                return render_template(f'{P.package_name}_{self.name}_queue.html', arg=arg)

            # 기본값: list (다운로드 리스트 이력 화면)
            return render_template(f'{P.package_name}_{self.name}_list.html', arg=arg)
        except Exception as e:
            logger.error(f"[{self.name}] process_menu 에러: {e}")
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def process_ajax(self, sub, req):
        try:
            command = req.form.get('command') or sub

            if command == 'one_execute':
                logger.info(f"[{self.name}] 1회 실행 명령 수신 -> Celery 워커 전달")
                self.start_celery(TaskDownloadBase.start, None, "manual")
                return jsonify({'ret': 'success', 'msg': '다운로드 파이프라인 작업을 Celery 워커에서 시작했습니다.'})

            # 큐 및 이력 웹 리스트
            elif sub == 'web_list' or command == 'web_list':
                return jsonify(self.web_list_model.web_list(req))

            # 설정 관리: 다운로더
            elif command == 'load_downloaders':
                DownloaderManager.load_engines()
                return jsonify({
                    'downloaders': FeedConfigUtil.get_downloaders(),
                    'available_types': list(DownloaderManager._engine_classes.keys())
                })

            elif command == 'save_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                ret = FeedConfigUtil.save_downloader(item_data)
                return jsonify({'ret': ret, 'downloaders': FeedConfigUtil.get_downloaders()})

            elif command == 'delete_downloader':
                target_name = req.form.get('name', '').strip()
                ok = FeedConfigUtil.delete_downloader(target_name)
                return jsonify({'ret': 'success' if ok else 'fail', 'downloaders': FeedConfigUtil.get_downloaders()})

            # 설정 관리: 프로필
            elif command == 'load_profiles':
                return jsonify({
                    'profiles': FeedConfigUtil.get_download_profiles(),
                    'downloaders': FeedConfigUtil.get_downloaders(),
                    'feeds': FeedConfigUtil.get_feeds()
                })

            elif command == 'save_profile':
                cfg_json = req.form.get('profile_json', '{}')
                item_data = json.loads(cfg_json)
                ret = FeedConfigUtil.save_download_profile(item_data)
                return jsonify({'ret': ret, 'profiles': FeedConfigUtil.get_download_profiles()})

            elif command == 'delete_profile':
                target_name = req.form.get('name', '').strip()
                yaml_data = FeedConfigUtil.load_yaml()
                profiles = [p for p in yaml_data.get('DOWNLOAD_PROFILES', []) if p.get('name') != target_name]
                yaml_data['DOWNLOAD_PROFILES'] = profiles
                FeedConfigUtil.save_yaml(yaml_data)
                return jsonify({'ret': 'success', 'profiles': FeedConfigUtil.get_download_profiles()})

            # 설정 관리: 구글 계정 풀
            elif command == 'load_accounts':
                return jsonify({
                    'accounts': FeedConfigUtil.get_gdrive_accounts(),
                    'stats': self.get_account_stats_summary()
                })

            elif command == 'save_accounts':
                acc_json = req.form.get('accounts_json', '[]')
                accounts = json.loads(acc_json)
                ok = FeedConfigUtil.save_gdrive_accounts(accounts)
                return jsonify({'ret': 'success' if ok else 'fail', 'accounts': FeedConfigUtil.get_gdrive_accounts()})

            elif command == 'batch_add_accounts':
                batch_text = req.form.get('batch_text', '').strip()
                accounts = FeedConfigUtil.get_gdrive_accounts()
                added_count = 0
                for line in batch_text.splitlines():
                    line = line.strip()
                    if not line or line.startswith('#') or ':' not in line:
                        continue
                    parts = line.split(':', 1)
                    uname = parts[0].strip()
                    mydrive_id = parts[1].strip()
                    if not any(a.get('username') == uname for a in accounts):
                        accounts.append({'username': uname, 'mydrive_rclone_id': mydrive_id})
                        added_count += 1
                FeedConfigUtil.save_gdrive_accounts(accounts)
                logger.info(f"[{self.name}] 구글 드라이브 SA 계정 일괄 등록: {added_count}개 추가됨")
                return jsonify({'ret': 'success', 'added_count': added_count, 'accounts': accounts})

            elif command == 'reset_blocked_accounts':
                db.session.query(ModelDownloadStat).filter_by(stat_type='account_block').delete()
                db.session.commit()
                logger.info(f"[{self.name}] 구글 드라이브 계정 차단 목록 수동 초기화 완료")
                return jsonify({'ret': 'success', 'stats': self.get_account_stats_summary()})

            # 큐 개별 항목 제어
            elif command == 'item_action':
                action = req.form.get('action')
                item_id = int(req.form.get('id', -1))
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item:
                    return jsonify({'ret': 'fail', 'msg': '항목을 찾을 수 없습니다.'})

                if action == 'retry':
                    item.status = 'pending'
                    item.current_engine_index = 0
                    item.error_message = None
                    db.session.commit()
                    logger.info(f"[{self.name}] 큐 항목 수동 재시도 설정: {item.title}")
                    return jsonify({'ret': 'success'})

                elif action == 'force_complete':
                    item.status = 'completed'
                    item.completed_time = datetime.now()
                    db.session.commit()
                    logger.info(f"[{self.name}] 큐 항목 강제 완료 처리: {item.title}")
                    return jsonify({'ret': 'success'})

                elif action == 'delete':
                    db.session.delete(item)
                    db.session.commit()
                    logger.info(f"[{self.name}] 큐 항목 삭제 완료 (ID: {item_id})")
                    return jsonify({'ret': 'success'})

            return super(ModuleDownload, self).process_ajax(sub, req)
        except Exception as e:
            logger.error(f"[{self.name}] process_ajax 에러: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'error', 'msg': str(e)})

    def process_api(self, sub, req):
        try:
            if sub == 'sse':
                return Response(
                    stream_with_context(self.generate_sse_stream()),
                    mimetype='text/event-stream'
                )
            return jsonify({'ret': 'fail', 'msg': f'알 수 없는 API 명령: {sub}'}), 404
        except Exception as e:
            logger.error(f"[{self.name}] process_api 에러 ({sub}): {e}")
            return jsonify({'ret': 'error', 'msg': str(e)}), 500

    def generate_sse_stream(self):
        while True:
            try:
                with F.app.app_context():
                    counts = {
                        'pending': db.session.query(ModelDownload).filter_by(status='pending').count(),
                        'downloading': db.session.query(ModelDownload).filter_by(status='downloading').count(),
                        'staging': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count(),
                        'uploading': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count(),
                        'completed': db.session.query(ModelDownload).filter_by(status='completed').count(),
                        'failed': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['failed', 'move_failed'])).count(),
                    }
                    data_str = json.dumps({'timestamp': int(time.time()), 'counts': counts})
                    yield f"data: {data_str}\n\n"
            except Exception as e:
                logger.debug(f"[{self.name}] SSE 스트림 예외: {e}")
            time.sleep(3)

    def get_account_stats_summary(self) -> dict:
        now = int(time.time())
        cutoff = now - 86400

        usage_rows = (
            db.session.query(
                ModelDownloadStat.stat_key,
                func.sum(ModelDownloadStat.stat_value).label('total')
            )
            .filter(
                ModelDownloadStat.stat_type == 'gdrive_usage',
                ModelDownloadStat.timestamp > cutoff
            )
            .group_by(ModelDownloadStat.stat_key)
            .all()
        )
        usage_map = {r[0]: int(r[1]) for r in usage_rows if r[0]}

        blocked_rows = (
            db.session.query(ModelDownloadStat.stat_key, ModelDownloadStat.stat_value)
            .filter(
                ModelDownloadStat.stat_type == 'account_block',
                ModelDownloadStat.stat_value > now
            )
            .all()
        )
        blocked_map = {r[0]: int(r[1] - now) for r in blocked_rows if r[0]}

        return {'usage_24h': usage_map, 'blocked_remains': blocked_map}

    def scheduler_function(self):
        if P.ModelSetting.get_bool(f"{self.name}_db_auto_delete"):
            try:
                day = P.ModelSetting.get_int(f"{self.name}_db_delete_day")
                if day > 0:
                    target_date = datetime.now() - timedelta(days=day)
                    deleted = (
                        db.session.query(ModelDownload)
                        .filter(
                            ModelDownload.status == 'completed',
                            ModelDownload.completed_time < target_date
                        )
                        .delete()
                    )
                    db.session.commit()
                    if deleted > 0:
                        logger.debug(f"[{self.name}] {day}일 경과 완료 다운로드 DB 자동 정리 (삭제: {deleted}건)")
            except Exception as e:
                logger.error(f"[{self.name}] DB 자동 삭제 에러: {e}")
                db.session.rollback()

        job_type = "manual" if has_request_context() else "default"
        logger.info(f"[{self.name}] 다운로드 워커 작업 전달 (모드: {job_type})")
        self.start_celery(TaskDownloadBase.start, None, job_type)
