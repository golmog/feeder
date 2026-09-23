# -*- coding: utf-8 -*-

import os
import re
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
            f"{self.name}_feed_sync_days": "3",
            f"{self.name}_batch_limit": "50",
            f"{self.name}_local_staging_path": "",
            f"{self.name}_exclude_pattern": "",
            f"{self.name}_enable_sse": "True",
            f"{self.name}_rclone_conf_path": "",
            f"{self.name}_rclone_remote_name": "net",
            f"{self.name}_gdrive_use_impersonate": "False",
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

            if sub in ['one_execute', 'scheduler_once'] or command in ['one_execute', 'scheduler_once']:
                return self.one_execute()

            # 큐 및 이력 웹 리스트
            if sub == 'web_list' or command == 'web_list':
                return jsonify(self.web_list_model.web_list(req))

            # 설정 관리: 다운로더 엔진
            elif command == 'load_downloaders':
                DownloaderManager.load_engines()
                return jsonify({
                    'downloaders': FeedConfigUtil.get_downloaders(),
                    'schemas': DownloaderManager.get_engine_schemas()
                })

            elif command == 'save_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                ret = FeedConfigUtil.save_downloader(item_data)
                return jsonify({'ret': ret, 'downloaders': FeedConfigUtil.get_downloaders()})

            elif command == 'test_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                engine = DownloaderManager.create_instance(item_data)
                if not engine:
                    return jsonify({'ret': 'fail', 'msg': f"엔진 인스턴스 생성 실패 ({item_data.get('engine_type')})"})
                success, msg = engine.test_connection()
                return jsonify({'ret': 'success' if success else 'fail', 'msg': msg})

            elif command == 'delete_downloader':
                target_name = req.form.get('name', '').strip()
                ok = FeedConfigUtil.delete_downloader(target_name)
                return jsonify({'ret': 'success' if ok else 'fail', 'downloaders': FeedConfigUtil.get_downloaders()})

            # 다운로드 전용 엔진 스크립트 관리
            elif command == 'download_script_list':
                from .util_download import ENGINES_DIR, ensure_custom_dirs
                ensure_custom_dirs()
                files = [f for f in os.listdir(ENGINES_DIR) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({'ret': 'success', 'files': files})

            elif command == 'download_script_read':
                from .util_download import ENGINES_DIR
                filename = os.path.basename(req.form.get('filename', '').strip())
                fpath = os.path.join(ENGINES_DIR, filename)
                if not filename or not os.path.exists(fpath):
                    return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return jsonify({'ret': 'success', 'filename': filename, 'content': content})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            elif command == 'download_script_save':
                from .util_download import ENGINES_DIR
                filename = os.path.basename(req.form.get('filename', '').strip())
                content = req.form.get('content', '')
                if not filename:
                    return jsonify({'ret': 'empty_filename', 'log': '파일명이 올바르지 않습니다.'})
                if not filename.endswith('.py'):
                    filename += '.py'
                fpath = os.path.join(ENGINES_DIR, filename)
                try:
                    with open(fpath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    DownloaderManager.load_engines()
                    files = [f for f in os.listdir(ENGINES_DIR) if f.endswith('.py') and not f.startswith('__')]
                    files.sort()
                    return jsonify({'ret': 'success', 'filename': filename, 'files': files, 'schemas': DownloaderManager.get_engine_schemas()})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            # 설정 관리: 프로필
            elif command == 'load_profiles':
                from .util_download import TransporterManager
                TransporterManager.load_transporters()
                return jsonify({
                    'profiles': FeedConfigUtil.get_download_profiles(),
                    'downloaders': FeedConfigUtil.get_downloaders(),
                    'feeds': FeedConfigUtil.get_feeds(),
                    'transporter_schemas': TransporterManager.get_transporter_schemas()
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

            # 이송 핸들러(Transporter) 전용 스크립트 관리
            elif command == 'transporter_script_list':
                from .util_download import TRANSPORTERS_DIR, ensure_custom_dirs
                ensure_custom_dirs()
                files = [f for f in os.listdir(TRANSPORTERS_DIR) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({'ret': 'success', 'files': files})

            elif command == 'transporter_script_read':
                from .util_download import TRANSPORTERS_DIR
                filename = os.path.basename(req.form.get('filename', '').strip())
                fpath = os.path.join(TRANSPORTERS_DIR, filename)
                if not filename or not os.path.exists(fpath):
                    return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return jsonify({'ret': 'success', 'filename': filename, 'content': content})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            elif command == 'transporter_script_save':
                from .util_download import TRANSPORTERS_DIR, TransporterManager
                filename = os.path.basename(req.form.get('filename', '').strip())
                content = req.form.get('content', '')
                if not filename:
                    return jsonify({'ret': 'empty_filename', 'log': '파일명이 올바르지 않습니다.'})
                if not filename.endswith('.py'):
                    filename += '.py'
                fpath = os.path.join(TRANSPORTERS_DIR, filename)
                try:
                    with open(fpath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    TransporterManager.load_transporters()
                    files = [f for f in os.listdir(TRANSPORTERS_DIR) if f.endswith('.py') and not f.startswith('__')]
                    files.sort()
                    return jsonify({'ret': 'success', 'filename': filename, 'files': files, 'transporter_schemas': TransporterManager.get_transporter_schemas()})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

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
                    parts = [p.strip() for p in line.split(':')]
                    if len(parts) >= 2:
                        uname = parts[0]
                        mydrive_id = parts[1]
                        remote_name = parts[2] if len(parts) >= 3 else ''

                        if not any(a.get('username') == uname for a in accounts):
                            acc_dict = {'username': uname, 'mydrive_rclone_id': mydrive_id}
                            if remote_name:
                                acc_dict['remote_name'] = remote_name
                            accounts.append(acc_dict)
                            added_count += 1

                FeedConfigUtil.save_gdrive_accounts(accounts)
                logger.info(f"[{self.name}] 구글 드라이브 계정 풀 일괄 등록: {added_count}개 추가됨")
                return jsonify({'ret': 'success', 'added_count': added_count, 'accounts': accounts})

            elif command == 'reset_blocked_accounts':
                db.session.query(ModelDownloadStat).filter_by(stat_type='account_block').delete()
                db.session.commit()
                logger.info(f"[{self.name}] 구글 드라이브 계정 차단 목록 수동 초기화 완료")
                return jsonify({'ret': 'success', 'stats': self.get_account_stats_summary()})

            # 과거 이력 가져오기: 마그넷 텍스트 목록 일괄 임포트
            elif command == 'import_history_text':
                raw_text = req.form.get('import_text', '').strip()
                lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
                now = datetime.now()
                added_count, skipped_count = 0, 0

                from .util_feed import extract_info_hash
                for line in lines:
                    infohash = extract_info_hash(line)
                    target_mag = f"magnet:?xt=urn:btih:{infohash}" if infohash else line

                    existing = ModelDownload.get_by_infohash(infohash) if infohash else ModelDownload.get_by_magnet(target_mag)
                    if existing:
                        skipped_count += 1
                        continue

                    item = ModelDownload(
                        feed_name='MIGRATED',
                        title=f"[기존완료이력] {infohash or target_mag[:30]}",
                        magnet=target_mag,
                        infohash=infohash
                    )
                    item.status = 'completed'
                    item.completed_time = now
                    db.session.add(item)
                    added_count += 1

                db.session.commit()
                logger.info(f"[{self.name}] 마그넷 텍스트 이력 임포트: {added_count}건 추가, {skipped_count}건 중복 스킵")
                return jsonify({'ret': 'success', 'added': added_count, 'skipped': skipped_count})

            # 과거 이력 가져오기: 외부 SQLite DB 파일 (.db) 범용 임포트
            elif command == 'import_history_db':
                db_path = req.form.get('db_path', '').strip()
                table_name = req.form.get('table_name', '').strip() or 'magnets'
                magnet_col = req.form.get('magnet_col', '').strip() or 'magnet'
                title_col = req.form.get('title_col', '').strip() or 'title'
                file_name_col = req.form.get('file_name_col', '').strip()
                where_clause = req.form.get('where_clause', '').strip()

                if not os.path.exists(db_path):
                    return jsonify({'ret': 'fail', 'msg': '지정한 DB 파일을 찾을 수 없습니다.'})

                import sqlite3
                from .util_feed import extract_info_hash

                added_count, skipped_count = 0, 0
                now = datetime.now()

                try:
                    conn = sqlite3.connect(db_path)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()

                    # 범용 쿼리 조립: 사용자가 WHERE 조건을 입력했을 때만 필터링, 비워두면 테이블 전체 임포트
                    query = f"SELECT * FROM {table_name}"
                    if where_clause:
                        clean_where = re.sub(r'^\s*where\s+', '', where_clause, flags=re.IGNORECASE).strip()
                        if clean_where:
                            query += f" WHERE {clean_where}"

                    cur.execute(query)
                    rows = cur.fetchall()

                    for r in rows:
                        r_dict = dict(r)
                        raw_mag = str(r_dict.get(magnet_col, '')).strip()
                        if not raw_mag:
                            continue

                        infohash = extract_info_hash(raw_mag)
                        target_mag = f"magnet:?xt=urn:btih:{infohash}" if infohash else raw_mag

                        existing = ModelDownload.get_by_infohash(infohash) if infohash else ModelDownload.get_by_magnet(target_mag)
                        if existing:
                            skipped_count += 1
                            continue

                        title_val = r_dict.get(title_col) or f"[기존완료이력] {infohash or target_mag[:30]}"
                        fname_val = r_dict.get(file_name_col) if file_name_col else None
                        fsize_val = r_dict.get('file_size') or 0

                        item = ModelDownload(
                            feed_name=r_dict.get('rss_feed_category') or 'MIGRATED',
                            title=title_val,
                            magnet=target_mag,
                            infohash=infohash
                        )
                        item.file_name = fname_val
                        item.file_size = fsize_val
                        item.status = 'completed'
                        item.completed_time = now
                        db.session.add(item)
                        added_count += 1

                    db.session.commit()
                    conn.close()
                    logger.info(f"[{self.name}] 외부 DB ({os.path.basename(db_path)}) 범용 이력 임포트: {added_count}건 완료, {skipped_count}건 중복 제외")
                    return jsonify({'ret': 'success', 'added': added_count, 'skipped': skipped_count})
                except Exception as ex:
                    logger.error(f"[{self.name}] 외부 DB 범용 임포트 중 오류: {ex}")
                    db.session.rollback()
                    return jsonify({'ret': 'fail', 'msg': str(ex)})

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
            elif sub == 'colab_claim':
                return self._handle_colab_claim(req)
            elif sub == 'colab_report':
                return self._handle_colab_report(req)
            return jsonify({'ret': 'fail', 'msg': f'알 수 없는 API 명령: {sub}'}), 404
        except Exception as e:
            logger.error(f"[{self.name}] process_api 에러 ({sub}): {e}")
            return jsonify({'ret': 'error', 'msg': str(e)}), 500

    def _verify_api_auth(self, req) -> bool:
        client_key = req.args.get('apikey') or req.form.get('apikey')
        if not client_key and req.is_json:
            client_key = (req.get_json(silent=True) or {}).get('apikey')
        return bool(client_key and client_key == get_system_apikey())

    def _handle_colab_claim(self, req):
        if not self._verify_api_auth(req):
            return jsonify({'ret': 'fail', 'msg': 'API 인증 실패'}), 401

        item = (
            db.session.query(ModelDownload)
            .filter_by(status='pending_colab')
            .order_by(ModelDownload.id.asc())
            .first()
        )
        if not item:
            return jsonify({'ret': 'success', 'has_task': False, 'msg': '대기 중인 Colab 전송 작업이 없습니다.'})

        # 선점 상태로 전환
        item.status = 'colab_transferring'
        db.session.commit()

        # 서버에 보관된 rclone.conf 내용 읽기
        rclone_conf_path = P.ModelSetting.get('download_rclone_conf_path') or FeedConfigUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        rclone_conf_text = ""
        if rclone_conf_path and os.path.exists(rclone_conf_path):
            try:
                with open(rclone_conf_path, 'r', encoding='utf-8') as f:
                    rclone_conf_text = f.read()
            except Exception as e:
                logger.error(f"[{self.name}] rclone.conf 파일 읽기 실패: {e}")

        # 목적지 구글 드라이브 경로 조립
        profile = FeedConfigUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest_cfg = profile.get('destination', {})
        remote_name = dest_cfg.get('remote_name') or P.ModelSetting.get('download_rclone_shared_remote_name') or 'gf'
        shared_drive_id = item.gdrive_remote_id or dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
        complete_path = (item.gdrive_complete_path or dest_cfg.get('complete_path') or 'uploads/default').strip('/')
        folder_name = item.file_name or f"item_{item.id}"

        if shared_drive_id:
            dest_full_path = f"{remote_name}:{{{shared_drive_id}}}/{complete_path}/{folder_name}"
        else:
            dest_full_path = f"{remote_name}:{complete_path}/{folder_name}"

        # AllDebrid 원격 소스 경로
        src_full_path = item.local_path or f"ad:magnets/{folder_name}"
        buffer_limit_gb = dest_cfg.get('buffer_limit_gb', 50)

        task_data = {
            'id': item.id,
            'title': item.title,
            'file_name': folder_name,
            'file_size': item.file_size or 0,
            'remote_source_path': src_full_path,
            'dest_path': dest_full_path,
            'buffer_limit_bytes': int(buffer_limit_gb) * 1024 * 1024 * 1024
        }

        logger.info(f"[{self.name}] [Colab API] 작업 선점 완료: {item.title} (ID: {item.id}) -> {dest_full_path}")
        return jsonify({
            'ret': 'success',
            'has_task': True,
            'item': task_data,
            'rclone_conf': rclone_conf_text
        })

    def _handle_colab_report(self, req):
        if not self._verify_api_auth(req):
            return jsonify({'ret': 'fail', 'msg': 'API 인증 실패'}), 401

        data = req.get_json(silent=True) or req.form or {}
        task_id = int(data.get('id', -1))
        is_success = bool(data.get('success', False))
        error_msg = data.get('error_msg', '')
        bytes_transferred = int(data.get('bytes_transferred', 0))

        item = db.session.query(ModelDownload).filter_by(id=task_id).first()
        if not item:
            return jsonify({'ret': 'fail', 'msg': '해당 작업 ID를 찾을 수 없습니다.'}), 404

        if is_success:
            item.status = 'completed'
            item.completed_time = datetime.now()
            item.error_message = None
            if bytes_transferred > 0:
                item.file_size = bytes_transferred

            # 다운로더(AllDebrid) 클라우드 원본 정리
            downloader_cfg = FeedConfigUtil.get_downloader_by_name(item.current_engine_name)
            if downloader_cfg and item.engine_task_id:
                try:
                    engine = DownloaderManager.create_instance(downloader_cfg)
                    if engine:
                        engine.delete_task(item.engine_task_id)
                except Exception as ex:
                    logger.debug(f"[{self.name}] AllDebrid 원본 마그넷 삭제 실패 (무시): {ex}")

            logger.info(f"[{self.name}] [Colab API] 전송 최종 완료 확정: {item.title} (ID: {item.id})")
        else:
            item.status = 'failed'
            item.error_message = f"Colab 전송 실패: {error_msg}"
            logger.warning(f"[{self.name}] [Colab API] 전송 실패 보고 수신: {item.title} ({error_msg})")

        db.session.commit()
        return jsonify({'ret': 'success'})

    def generate_sse_stream(self):
        while True:
            try:
                with F.app.app_context():
                    counts = {
                        'pending': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending', 'pending_colab'])).count(),
                        'downloading': db.session.query(ModelDownload).filter_by(status='downloading').count(),
                        'staging': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count(),
                        'uploading': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading', 'colab_transferring'])).count(),
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
        """다운로드 스케줄러 주기 타이머 및 프레임워크 1회 실행 표준 호출"""
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

        logger.info(f"[{self.name}] 스케줄러 주기 실행 -> 다운로드 워커 작업 전달 (모드: default)")
        self.start_celery(TaskDownloadBase.start, None, "default")
