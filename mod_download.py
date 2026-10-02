# -*- coding: utf-8 -*-
import os
import re
import json
import time
import traceback
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, Response, stream_with_context
from sqlalchemy import desc, func, case
import sqlite3

from .setup import *
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil
from .util_download import DownloadUtil
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
            f"{self.name}_is_running": "False",
            f"{self.name}_running_start_time": "0",
            f"{self.name}_feed_sync_hours": "72",
            f"{self.name}_use_local_staging": "True",
            f"{self.name}_local_staging_path": "",
            f"{self.name}_max_staging_items": "10",
            f"{self.name}_staging_workers": "2",
            f"{self.name}_upload_workers": "2",
            f"{self.name}_exclude_pattern": "",
            f"{self.name}_enable_sse": "True",
            f"{self.name}_rclone_conf_path": "",
            f"{self.name}_rclone_remote_name": "net",
            f"{self.name}_rclone_upload_remote_name": "",
            f"{self.name}_gdrive_use_impersonate": "False",
            f"{self.name}_gdrive_mydrive_remote_name": "gdrive_sa",
            f"{self.name}_rclone_shared_remote_name": "gf",
            f"{self.name}_shared_drive_id": "",
            f"{self.name}_rclone_chunk_size": "256M",
            f"{self.name}_mydrive_upload_threshold": "14GB",
            f"{self.name}_gdrive_upload_limit": "700GB",
            f"{self.name}_shared_drive_upload_limit": "3TB",
            f"{self.name}_shared_drive_quota_reset_time": "16:00",
            f"{self.name}_rclone_extra_options": "--timeout 30m",
        }

    def plugin_load(self):
        """플러그인 로드 시 이전 비정상 종료된 전송 중 작업을 직전 대기 상태로 일괄 초기화"""
        try:
            with F.app.app_context():
                # FF 재시작 후 남아 있을 수 있는 다운로드 파이프라인 런타임 락 초기화
                FeederUtil.init_runtime_locks()
                FeederUtil.reset_runtime_lock('download_pipeline')
                P.ModelSetting.set(f"{self.name}_is_running", "False")
                P.ModelSetting.set(f"{self.name}_running_start_time", "0")
                FeederUtil.init_sqlite_concurrency()

                stg_cnt = db.session.query(ModelDownload).filter_by(status='local_staging').update({'status': 'pending_local_staging'})
                up_cnt = db.session.query(ModelDownload).filter_by(status='uploading').update({'status': 'pending_upload'})
                rel_cnt = db.session.query(ModelDownload).filter_by(status='relay_transferring').update({'status': 'pending_relay'})
                db.session.commit()

                total_reset = stg_cnt + up_cnt + rel_cnt
                if total_reset > 0:
                    logger.info(f"[{self.name}] 플러그인 로드: 비정상 종료된 전송 작업 초기화 완료 (스테이징 {stg_cnt}건, 업로드 {up_cnt}건, 릴레이 {rel_cnt}건 ➔ 대기열 복원)")

                # 이전 세션의 잔여 IPC 실시간 전송률 파일 및 메모리 캐시 정리
                FeederUtil._rclone_live_stats.clear()
                try:
                    prog_dir = FeederUtil.get_tmp_dir('rclone_progress')
                    if os.path.exists(prog_dir):
                        for fname in os.listdir(prog_dir):
                            if fname.endswith('.json') or fname.endswith('.tmp'):
                                try:
                                    os.remove(os.path.join(prog_dir, fname))
                                except Exception:
                                    pass
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"[{self.name}] plugin_load 초기화 예외: {e}")
            logger.error(traceback.format_exc())

        try:
            super(ModuleDownload, self).plugin_load()
        except Exception:
            pass

    def process_menu(self, page_name, req):
        try:
            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['sub'] = self.name
            arg['current_page'] = page_name
            arg['ddns'] = FeederUtil.get_ddns()
            arg['apikey'] = FeederUtil.get_system_apikey()

            if page_name == 'setting':
                arg['yaml_filepath'] = FeederUtil.get_filepath()
                arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
                arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
                return render_template(f'{P.package_name}_{self.name}_setting.html', arg=arg)

            elif page_name == 'queue':
                return render_template(f'{P.package_name}_{self.name}_queue.html', arg=arg)

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

            if sub == 'web_list' or command == 'web_list':
                return jsonify(self.web_list_model.web_list(req))

            elif command == 'load_downloaders':
                DownloadUtil.load_engines()
                return jsonify({
                    'downloaders': FeederUtil.get_downloaders(),
                    'schemas': DownloadUtil.get_engine_schemas()
                })

            elif command == 'save_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                ret = FeederUtil.save_downloader(item_data)
                return jsonify({'ret': ret, 'downloaders': FeederUtil.get_downloaders()})

            elif command == 'test_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                engine = DownloadUtil.create_engine(item_data)
                if not engine:
                    return jsonify({'ret': 'fail', 'msg': f"엔진 인스턴스 생성 실패 ({item_data.get('engine_type')})"})
                success, msg = engine.test_connection()
                return jsonify({'ret': 'success' if success else 'fail', 'msg': msg})

            elif command == 'delete_downloader':
                target_name = req.form.get('name', '').strip()
                ok = FeederUtil.delete_downloader(target_name)
                return jsonify({'ret': 'success' if ok else 'fail', 'downloaders': FeederUtil.get_downloaders()})

            elif command == 'download_script_list':
                FeederUtil.ensure_custom_dirs()
                files = [f for f in os.listdir(FeederUtil.ENGINES_DIR) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({'ret': 'success', 'files': files})

            elif command == 'download_script_read':
                filename = os.path.basename(req.form.get('filename', '').strip())
                fpath = os.path.join(FeederUtil.ENGINES_DIR, filename)
                if not filename or not os.path.exists(fpath):
                    return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return jsonify({'ret': 'success', 'filename': filename, 'content': content})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            elif command == 'download_script_save':
                filename = os.path.basename(req.form.get('filename', '').strip())
                content = req.form.get('content', '')
                if not filename:
                    return jsonify({'ret': 'empty_filename', 'log': '파일명이 올바르지 않습니다.'})
                if not filename.endswith('.py'):
                    filename += '.py'
                fpath = os.path.join(FeederUtil.ENGINES_DIR, filename)
                try:
                    with open(fpath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    DownloadUtil.load_engines()
                    files = [f for f in os.listdir(FeederUtil.ENGINES_DIR) if f.endswith('.py') and not f.startswith('__')]
                    files.sort()
                    return jsonify({'ret': 'success', 'filename': filename, 'files': files, 'schemas': DownloadUtil.get_engine_schemas()})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            elif command == 'load_profiles':
                DownloadUtil.load_transporters()
                return jsonify({
                    'profiles': FeederUtil.get_download_profiles(),
                    'downloaders': FeederUtil.get_downloaders(),
                    'feeds': FeederUtil.get_feeds(),
                    'transporter_schemas': DownloadUtil.get_transporter_schemas()
                })

            elif command == 'save_profile':
                cfg_json = req.form.get('profile_json', '{}')
                item_data = json.loads(cfg_json)
                ret = FeederUtil.save_download_profile(item_data)
                return jsonify({'ret': ret, 'profiles': FeederUtil.get_download_profiles()})

            elif command == 'delete_profile':
                target_name = req.form.get('name', '').strip()
                ok = FeederUtil.delete_download_profile(target_name)
                return jsonify({'ret': 'success' if ok else 'fail', 'profiles': FeederUtil.get_download_profiles()})

            elif command == 'transporter_script_list':
                FeederUtil.ensure_custom_dirs()
                files = [f for f in os.listdir(FeederUtil.TRANSPORTERS_DIR) if f.endswith('.py') and not f.startswith('__')]
                files.sort()
                return jsonify({'ret': 'success', 'files': files})

            elif command == 'transporter_script_read':
                filename = os.path.basename(req.form.get('filename', '').strip())
                fpath = os.path.join(FeederUtil.TRANSPORTERS_DIR, filename)
                if not filename or not os.path.exists(fpath):
                    return jsonify({'ret': 'not_exist', 'log': '파일이 존재하지 않습니다.'})
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return jsonify({'ret': 'success', 'filename': filename, 'content': content})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            elif command == 'transporter_script_save':
                filename = os.path.basename(req.form.get('filename', '').strip())
                content = req.form.get('content', '')
                if not filename:
                    return jsonify({'ret': 'empty_filename', 'log': '파일명이 올바르지 않습니다.'})
                if not filename.endswith('.py'):
                    filename += '.py'
                fpath = os.path.join(FeederUtil.TRANSPORTERS_DIR, filename)
                try:
                    with open(fpath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    DownloadUtil.load_transporters()
                    files = [f for f in os.listdir(FeederUtil.TRANSPORTERS_DIR) if f.endswith('.py') and not f.startswith('__')]
                    files.sort()
                    return jsonify({'ret': 'success', 'filename': filename, 'files': files, 'transporter_schemas': DownloadUtil.get_transporter_schemas()})
                except Exception as e:
                    return jsonify({'ret': 'error', 'log': str(e)})

            elif command == 'load_accounts':
                return jsonify({
                    'accounts': FeederUtil.get_gdrive_accounts(),
                    'stats': self.get_account_stats_summary()
                })

            elif command == 'save_accounts':
                acc_json = req.form.get('accounts_json', '[]')
                accounts = json.loads(acc_json)
                ok = FeederUtil.save_gdrive_accounts(accounts)
                return jsonify({'ret': 'success' if ok else 'fail', 'accounts': FeederUtil.get_gdrive_accounts()})

            elif command == 'batch_add_accounts':
                batch_text = req.form.get('batch_text', '').strip()
                accounts = FeederUtil.get_gdrive_accounts()
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

                FeederUtil.save_gdrive_accounts(accounts)
                logger.info(f"[{self.name}] 구글 드라이브 계정 풀 일괄 등록: {added_count}개 추가됨")
                return jsonify({'ret': 'success', 'added_count': added_count, 'accounts': accounts})

            elif command == 'adopt_orphan':
                task_id = req.form.get('task_id', '').strip()
                thash = req.form.get('hash', '').strip().lower()
                engine_name = req.form.get('engine_name', '').strip()
                title = req.form.get('title', '').strip()
                file_name = req.form.get('file_name', '').strip()
                source_path = req.form.get('source_path', '').strip()
                raw_status = req.form.get('raw_status', '').strip()
                profile_name = req.form.get('profile_name', '').strip()
                try:
                    fsize = int(req.form.get('file_size', 0))
                except Exception:
                    fsize = 0

                # 중복 등록 검증
                existing = None
                if thash:
                    existing = ModelDownload.get_by_infohash(thash)
                if not existing and task_id:
                    existing = db.session.query(ModelDownload).filter_by(engine_task_id=task_id).first()
                if existing:
                    return jsonify({'ret': 'exist', 'msg': f'이미 큐에 등록되어 있는 작업입니다 (상태: {existing.status})'})

                # 지정 프로필 존재 여부 및 1순위 엔진 일치 엄격 검증
                selected_profile = None
                for p in FeederUtil.get_download_profiles():
                    if p.get('name') == profile_name:
                        selected_profile = p
                        break

                if not selected_profile:
                    return jsonify({'ret': 'fail', 'msg': '지정한 다운로드 프로필을 찾을 수 없습니다.'})

                chain = selected_profile.get('priority_chain', [])
                if not chain or chain[0].lower() != engine_name.lower():
                    first_engine = chain[0] if chain else '없음'
                    return jsonify({'ret': 'fail', 'msg': f'선택한 프로필의 1순위 엔진({first_engine})이 대상 엔진({engine_name})과 일치하지 않습니다.'})

                dest_cfg = FeederUtil.get_profile_destination(selected_profile, engine_name)
                dest_type = dest_cfg.get('type', 'local')
                use_local_staging = str(dest_cfg.get('use_local_staging', True)).lower() in ['true', 'on', '1']

                # 등록 일시를 현재 시각으로 초기화하여 타임아웃 조기 종료 방지
                now = datetime.now()
                target_mag = f"magnet:?xt=urn:btih:{thash}" if thash else f"engine:{engine_name}:{task_id}"

                item = ModelDownload(
                    feed_name=selected_profile.get('feeds', ['*'])[0] if selected_profile.get('feeds') else 'ADOPTED',
                    title=title or file_name or f"{engine_name}_{task_id}",
                    magnet=target_mag,
                    infohash=thash or None
                )
                item.file_name = file_name or title
                item.file_size = fsize
                item.priority_chain = chain
                item.current_engine_index = 0
                item.current_engine_name = engine_name
                item.engine_task_id = str(task_id)
                item.destination_type = dest_type
                item.gdrive_upload_path = dest_cfg.get('upload_path', '')
                item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
                item.local_path = source_path
                item.created_time = now
                item.updated_time = now
                item.engine_added_time = now
                item.last_status_time = now

                # 엔진 상태 및 프로필 스테이징 설정에 따라 알맞은 파이프라인 단계로 진입
                transporter = DownloadUtil.get_transporter(dest_type)
                is_relay = bool(transporter and getattr(transporter, 'IS_RELAY_HANDLER', False))

                if raw_status == 'completed':
                    if is_relay:
                        item.status = 'pending_relay'
                    elif not use_local_staging:
                        item.status = 'downloaded'
                    else:
                        item.status = 'pending_local_staging'
                else:
                    item.status = 'downloading'

                db.session.add(item)
                db.session.commit()
                logger.info(f"[{self.name}] 고아 작업 흡수 완료: {item.title} -> 상태: {item.status}, 프로필: {profile_name}")
                return jsonify({'ret': 'success', 'msg': f'[{item.title[:25]}] 작업을 큐에 성공적으로 등록했습니다.'})

            elif command == 'reset_blocked_accounts':
                db.session.query(ModelDownloadStat).filter_by(stat_type='account_block').delete()
                db.session.commit()
                logger.info(f"[{self.name}] 구글 드라이브 계정 차단 목록 수동 초기화 완료")
                return jsonify({'ret': 'success', 'stats': self.get_account_stats_summary()})

            elif command == 'import_history_text':
                raw_text = req.form.get('import_text', '').strip()
                lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
                now = datetime.now()
                added_count, skipped_count = 0, 0

                for line in lines:
                    infohash = FeederUtil.extract_info_hash(line)
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
                logger.info(f"[{self.name}] 마그넷 이력 임포트: {added_count}건 추가, {skipped_count}건 중복 스킵")
                return jsonify({'ret': 'success', 'added': added_count, 'skipped': skipped_count})

            elif command == 'import_history_db':
                db_path = req.form.get('db_path', '').strip()
                table_name = req.form.get('table_name', '').strip() or 'magnets'
                magnet_col = req.form.get('magnet_col', '').strip() or 'magnet'
                title_col = req.form.get('title_col', '').strip() or 'title'
                file_name_col = req.form.get('file_name_col', '').strip()
                where_clause = req.form.get('where_clause', '').strip()

                if not os.path.exists(db_path):
                    return jsonify({'ret': 'fail', 'msg': '지정한 DB 파일을 찾을 수 없습니다.'})

                added_count, skipped_count = 0, 0
                now = datetime.now()

                try:
                    conn = sqlite3.connect(db_path)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()

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

                        infohash = FeederUtil.extract_info_hash(raw_mag)
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
                    logger.info(f"[{self.name}] 외부 DB 이력 임포트: {added_count}건 완료, {skipped_count}건 중복 제외")
                    return jsonify({'ret': 'success', 'added': added_count, 'skipped': skipped_count})
                except Exception as ex:
                    logger.error(f"[{self.name}] 외부 DB 임포트 중 오류: {ex}")
                    db.session.rollback()
                    return jsonify({'ret': 'fail', 'msg': str(ex)})

            elif command in ['direct_add', 'direct_download']:
                title = req.form.get('title', '').strip()
                magnet = req.form.get('magnet', '').strip()
                profile_name = req.form.get('profile_name', '').strip()
                feed_name = req.form.get('feed_name', 'DIRECT')

                custom_chain_raw = req.form.get('priority_chain')
                custom_chain = json.loads(custom_chain_raw) if custom_chain_raw else None
                custom_dest_type = req.form.get('destination_type', '').strip()
                custom_dest_cfg_raw = req.form.get('destination_config')
                custom_dest_config = json.loads(custom_dest_cfg_raw) if custom_dest_cfg_raw else None

                res = FeederUtil.add_direct_download(
                    title=title,
                    magnet=magnet,
                    profile_name=profile_name,
                    feed_name=feed_name,
                    caller_name=self.name,
                    custom_chain=custom_chain,
                    custom_dest_type=custom_dest_type,
                    custom_dest_config=custom_dest_config
                )
                return jsonify(res)

            elif command == 'item_action':
                action = req.form.get('action')
                try:
                    item_id = int(req.form.get('id', -1))
                except Exception:
                    item_id = -1

                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item:
                    return jsonify({'ret': 'fail', 'msg': '항목을 찾을 수 없습니다.'})

                if action == 'retry':
                    profile_name = req.form.get('profile_name', '').strip()
                    selected_profile = None

                    if profile_name:
                        for p in FeederUtil.get_download_profiles():
                            if p.get('name') == profile_name:
                                selected_profile = p
                                break
                    if not selected_profile:
                        selected_profile = FeederUtil.get_download_profile_by_feed(item.feed_name)

                    if selected_profile:
                        item.priority_chain = list(selected_profile.get('priority_chain', []))
                        dest = FeederUtil.get_profile_destination(selected_profile, item.current_engine_name)
                        item.destination_type = dest.get('type', 'local')
                        item.gdrive_upload_path = dest.get('upload_path', '')
                        item.gdrive_complete_path = dest.get('complete_path', '')
                        item.gdrive_remote_id = dest.get('shared_drive_id', '')
                    else:
                        enabled_downloaders = [d['name'] for d in FeederUtil.get_downloaders() if d.get('enabled', True)]
                        if enabled_downloaders:
                            item.priority_chain = enabled_downloaders
                        item.destination_type = item.destination_type or 'local'

                    err_msg = str(item.error_message or '')
                    clean_title = item.title[:25]

                    # [소스 경로 없음] 실패 건 복구: CD2 마운트 루트에 실물이 있는지 확인 후 제자리로 이동 및 최종 완료 처리
                    if '소스 경로 없음' in err_msg or not item.local_path:
                        cd2_cfg = next((d for d in FeederUtil.get_downloaders() if d.get('engine_type') == 'cd2'), None)
                        if cd2_cfg:
                            mount_root = (cd2_cfg.get('cd2_mount_path') or '').strip()
                            target_fname = item.file_name or item.title
                            if mount_root and os.path.exists(mount_root) and target_fname:
                                found_path = None
                                candidate = os.path.join(mount_root, target_fname)
                                if os.path.exists(candidate):
                                    found_path = candidate
                                else:
                                    for entry in os.listdir(mount_root):
                                        if entry == target_fname:
                                            found_path = os.path.join(mount_root, entry)
                                            break

                                if found_path:
                                    upload_sub = (item.gdrive_upload_path or (selected_profile.get('destination', {}).get('upload_path') if selected_profile else '') or '').strip('/')
                                    target_dir = os.path.join(mount_root, upload_sub) if upload_sub else mount_root
                                    os.makedirs(target_dir, exist_ok=True)
                                    final_dest_path = os.path.join(target_dir, target_fname)

                                    if os.path.abspath(found_path) != os.path.abspath(final_dest_path):
                                        shutil.move(found_path, final_dest_path)
                                        logger.info(f"[{self.name}] [CD2 복구] 루트 잔여 파일을 수신 경로로 이동 완료: {found_path} -> {final_dest_path}")
                                    else:
                                        final_dest_path = found_path

                                    # 파일 실제 용량 반영
                                    if os.path.isfile(final_dest_path):
                                        item.file_size = os.path.getsize(final_dest_path)
                                    else:
                                        item.file_size = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(final_dest_path) for f in fs)

                                    item.local_path = final_dest_path
                                    item.status = 'completed'
                                    item.completed_time = datetime.now()
                                    item.error_message = None
                                    item.last_status_time = None
                                    db.session.commit()
                                    logger.info(f"[{self.name}] [CD2 복구] {item.title} 복구 및 최종 완료(completed) 처리 완료")
                                    return jsonify({'ret': 'success', 'msg': f'[{clean_title}] CD2 마운트 실물 파일을 제자리로 복구하여 최종 완료(completed) 처리했습니다.'})

                    # [업로드 실패] 건 복구: 로컬 스테이징 실물 파일이 보존되어 있는 경우 다운로드 생략 후 업로드 대기열 직행
                    if item.local_path and os.path.exists(item.local_path):
                        item.status = 'pending_upload'
                        item.error_message = None
                        item.last_status_time = None
                        db.session.commit()
                        logger.info(f"[{self.name}] 로컬 스테이징 파일 보존 확인 -> 업로드 대기열(pending_upload)로 즉시 재시도: {item.title}")
                        return jsonify({'ret': 'success', 'msg': f'[{clean_title}] 보존된 로컬 스테이징 파일을 감지하여 업로드 대기열에 등록했습니다.'})

                    # [체인 소진] 또는 일반 실패 건: 1순위 엔진부터 처음부터 재시작
                    item.status = 'pending'
                    item.current_engine_index = 0
                    item.current_engine_name = None
                    item.engine_task_id = None
                    item.error_message = None
                    item.last_status_time = None
                    db.session.commit()

                    chain_desc = ' -> '.join(item.priority_chain) if item.priority_chain else '기본'
                    logger.info(f"[{self.name}] 다운로드 재시도 대기열 등록 완료: {item.title} (체인: {chain_desc})")
                    return jsonify({'ret': 'success', 'msg': f'[{clean_title}] 1순위 엔진부터 다운로드를 처음부터 재시작합니다.'})

                elif action == 'force_complete':
                    item.status = 'completed'
                    item.completed_time = datetime.now()
                    item.error_message = None
                    db.session.commit()
                    logger.info(f"[{self.name}] 수동 완료 처리: {item.title} (ID: {item_id})")
                    return jsonify({'ret': 'success', 'msg': '최종 완료(completed) 처리되었습니다.'})

                elif action == 'delete':
                    db.session.delete(item)
                    db.session.commit()
                    logger.info(f"[{self.name}] 큐 항목 수동 삭제 완료: ID={item_id}")
                    return jsonify({'ret': 'success', 'msg': '항목이 삭제되었습니다.'})

            return super(ModuleDownload, self).process_ajax(sub, req)
        except Exception as e:
            logger.error(f"[{self.name}] process_ajax 에러: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'error', 'msg': str(e)})

    def process_api(self, sub, req):
        try:
            if sub == 'sse':
                response = Response(
                    stream_with_context(self.generate_sse_stream()),
                    mimetype='text/event-stream'
                )
                response.headers['Cache-Control'] = 'no-cache'
                response.headers['X-Accel-Buffering'] = 'no'
                return response
            elif sub == 'relay_claim':
                return self._handle_relay_claim(req)
            elif sub == 'relay_report':
                return self._handle_relay_report(req)
            elif sub == 'progress_update':
                return self._handle_progress_update(req)
            return jsonify({'ret': 'fail', 'msg': f'알 수 없는 API 명령: {sub}'}), 404
        except Exception as e:
            logger.error(f"[{self.name}] process_api 에러 ({sub}): {e}")
            return jsonify({'ret': 'error', 'msg': str(e)}), 500

    def _handle_progress_update(self, req):
        """Celery 워커로부터 실시간 Rclone 전송률 수신 및 웹 메모리 갱신"""
        data = req.get_json(silent=True) or req.form or {}
        item_id = data.get('item_id')
        action = data.get('action', 'update')
        if not item_id:
            return jsonify({'ret': 'fail', 'msg': 'item_id 누락'}), 400

        if action == 'clear':
            FeederUtil.clear_rclone_progress(item_id)
        else:
            prog_data = data.get('data') or {}
            FeederUtil.set_rclone_progress(item_id, prog_data)

        return jsonify({'ret': 'success'})

    def _verify_api_auth(self, req) -> bool:
        client_key = req.args.get('apikey') or req.form.get('apikey')
        if not client_key and req.is_json:
            client_key = (req.get_json(silent=True) or {}).get('apikey')
        return bool(client_key and client_key == FeederUtil.get_system_apikey())

    @staticmethod
    def _obscure_rclone_password(password: str) -> str:
        """rclone obscure 호출로 암호화된 Rclone 비밀번호 생성"""
        try:
            res = subprocess.run(["rclone", "obscure", password], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            if res.returncode == 0 and res.stdout.strip():
                return res.stdout.strip()
        except Exception:
            pass
        return password

    def _handle_relay_claim(self, req):
        """원격 릴레이 워커(VPS, 외부 서버, Colab 등)의 대기 작업 선점 API"""
        if not self._verify_api_auth(req):
            return jsonify({'ret': 'fail', 'msg': 'API 인증 실패'}), 401

        item = (
            db.session.query(ModelDownload)
            .filter_by(status='pending_relay')
            .order_by(ModelDownload.id.asc())
            .first()
        )
        if not item:
            return jsonify({'ret': 'success', 'has_task': False, 'msg': '대기 중인 원격 릴레이 전송 작업이 없습니다.'})

        item.status = 'relay_transferring'
        db.session.commit()

        rclone_conf_path = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        rclone_conf_text = ""
        sa_files = {}

        if rclone_conf_path and os.path.exists(rclone_conf_path):
            try:
                with open(rclone_conf_path, 'r', encoding='utf-8') as f:
                    rclone_conf_text = f.read()

                conf_dir = os.path.dirname(rclone_conf_path)
                sa_matches = re.findall(r'service_account_file\s*=\s*(.+)', rclone_conf_text)
                for raw_sa_path in set(sa_matches):
                    sa_path = raw_sa_path.strip().strip('"').strip("'")
                    sa_fname = os.path.basename(sa_path)

                    candidate_paths = [
                        sa_path,
                        os.path.join(conf_dir, sa_fname),
                        os.path.join(conf_dir, 'sa', sa_fname),
                        os.path.join(path_data, sa_fname)
                    ]
                    found_content = None
                    for cp in candidate_paths:
                        if os.path.exists(cp):
                            try:
                                with open(cp, 'r', encoding='utf-8') as sf:
                                    found_content = sf.read()
                                break
                            except Exception:
                                pass

                    if found_content:
                        sa_files[sa_fname] = found_content
                        remote_sa_path = f"/root/.config/rclone/sa/{sa_fname}"
                        rclone_conf_text = re.sub(
                            rf'service_account_file\s*=\s*{re.escape(raw_sa_path)}',
                            f'service_account_file = {remote_sa_path}',
                            rclone_conf_text
                        )
            except Exception as e:
                logger.error(f"[{self.name}] rclone.conf 분석 실패: {e}")

        # 다운로더 설정 조회 및 AllDebrid WebDAV 리모트 자동 주입
        downloader_cfg = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
        engine_type = downloader_cfg.get('engine_type', '').lower()
        engine_remote = downloader_cfg.get('remote_name', '').strip() or 'ad'
        engine_base = downloader_cfg.get('rclone_base_path', '').strip('/')

        if engine_type == 'alldebrid':
            apikey = downloader_cfg.get('apikey', '').strip()
            if apikey and f"[{engine_remote}]" not in rclone_conf_text:
                obscured_pass = self._obscure_rclone_password(apikey)
                ad_section = (
                    f"\n[{engine_remote}]\n"
                    f"type = webdav\n"
                    f"url = https://webdav.alldebrid.com\n"
                    f"vendor = other\n"
                    f"user = {apikey}\n"
                    f"pass = {obscured_pass}\n"
                )
                rclone_conf_text += ad_section
                logger.info(f"[{self.name}] [Relay API] AllDebrid WebDAV Rclone 설정([{engine_remote}]) 동적 주입 완료")

        profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name)
        remote_name = (
            dest_cfg.get('shared_remote_name')
            or dest_cfg.get('remote_name')
            or P.ModelSetting.get('download_rclone_shared_remote_name')
            or 'gdrive_shared'
        )
        shared_drive_id = item.gdrive_remote_id or dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
        complete_path = (item.gdrive_complete_path or dest_cfg.get('complete_path') or 'uploads/default').strip('/')
        folder_name = item.file_name or f"item_{item.id}"

        # 단일 파일 여부 판별 (확장자가 있고 끝이 슬래시가 아닌 경우)
        _, ext = os.path.splitext(folder_name)
        is_single_file = bool(ext and not folder_name.endswith(('/', '\\')))

        if shared_drive_id:
            dest_base_path = f"{remote_name}:{{{shared_drive_id}}}/{complete_path}"
        else:
            dest_base_path = f"{remote_name}:{complete_path}"

        dest_full_path = f"{dest_base_path}/{folder_name}"
        dest_dir_path = dest_base_path if is_single_file else dest_full_path

        if engine_remote and engine_base:
            src_full_path = item.local_path or f"{engine_remote}:{engine_base}/{folder_name}"
        elif engine_remote:
            src_full_path = item.local_path or f"{engine_remote}:{folder_name}"
        else:
            src_full_path = item.local_path or folder_name

        buffer_limit_gb = dest_cfg.get('buffer_limit_gb', 50)

        task_data = {
            'id': item.id,
            'title': item.title,
            'file_name': folder_name,
            'is_single_file': is_single_file,
            'file_size': item.file_size or 0,
            'remote_source_path': src_full_path,
            'dest_path': dest_full_path,
            'dest_dir_path': dest_dir_path,
            'buffer_limit_bytes': int(buffer_limit_gb) * 1024 * 1024 * 1024
        }

        logger.info(f"[{self.name}] [Relay API] 원격 릴레이 작업 선점 완료: {item.title} (ID: {item.id}, 단일파일={is_single_file})")
        return jsonify({
            'ret': 'success',
            'has_task': True,
            'item': task_data,
            'rclone_conf': rclone_conf_text,
            'sa_files': sa_files
        })

    def _handle_relay_report(self, req):
        """원격 릴레이 워커의 작업 완료/실패 결과 수신 API"""
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

            downloader_cfg = FeederUtil.get_downloader_by_name(item.current_engine_name)
            if downloader_cfg and item.engine_task_id:
                try:
                    engine = DownloadUtil.create_engine(downloader_cfg)
                    if engine:
                        engine.delete_task(item.engine_task_id)
                except Exception as ex:
                    logger.debug(f"[{self.name}] 원본 마그넷 삭제 실패 (무시): {ex}")

            logger.info(f"[{self.name}] [Relay API] 원격 릴레이 전송 완료 확정: {item.title} (ID: {item.id})")
        else:
            item.status = 'failed'
            item.error_message = f"릴레이 전송 실패: {error_msg}"
            logger.warning(f"[{self.name}] [Relay API] 원격 릴레이 전송 실패 보고: {item.title} ({error_msg})")

        db.session.commit()
        return jsonify({'ret': 'success'})

    def generate_sse_stream(self):
        """실시간 대시보드용 SSE 스트림 (통계 카운트 + 활성 작업 목록 동시 전송)"""
        sse_cycle = 0
        engine_tasks_cache = []
        last_stat_calc_time = 0
        cached_24h_upload_bytes = 0
        while True:
            try:
                with F.app.app_context():
                    sse_cycle += 1
                    now_ts = time.time()
                    if now_ts - last_stat_calc_time > 30:
                        last_stat_calc_time = now_ts
                        cutoff_ts = int(now_ts) - 86400
                        total_res = db.session.query(func.sum(ModelDownloadStat.stat_value)).filter(
                            ModelDownloadStat.stat_type == 'gdrive_usage',
                            ModelDownloadStat.timestamp > cutoff_ts
                        ).scalar()
                        cached_24h_upload_bytes = int(total_res) if total_res else 0

                    counts = {
                        'pending': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending', 'pending_relay'])).count(),
                        'downloading': db.session.query(ModelDownload).filter_by(status='downloading').count(),
                        'staging': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count(),
                        'uploading': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['downloaded', 'pending_upload', 'uploading', 'relay_transferring'])).count(),
                        'completed': db.session.query(ModelDownload).filter_by(status='completed').count(),
                        'failed': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['failed', 'move_failed'])).count(),
                    }

                    active_statuses = [
                        'pending', 'downloading', 'pending_local_staging', 'local_staging',
                        'downloaded', 'pending_upload', 'uploading', 'pending_relay', 'relay_transferring'
                    ]

                    # 상태별 우선순위 가중치: 실제 전송/진행 중인 작업을 신규 대기 항목보다 항상 최우선 정렬
                    status_priority = case(
                        (ModelDownload.status.in_(['local_staging', 'uploading', 'relay_transferring']), 1),
                        (ModelDownload.status == 'downloading', 2),
                        (ModelDownload.status.in_(['pending_local_staging', 'downloaded', 'pending_upload', 'pending_relay']), 3),
                        (ModelDownload.status == 'pending', 4),
                        else_=5
                    )

                    active_rows = (
                        db.session.query(ModelDownload)
                        .filter(ModelDownload.status.in_(active_statuses))
                        .order_by(status_priority, desc(ModelDownload.id))
                        .all()
                    )

                    # Rclone 진행률은 매 주기 갱신하고, 원격 엔진 목록은 2주기(6초)마다 갱신
                    if sse_cycle == 1 or sse_cycle % 2 == 0:
                        refreshed_tasks = []
                        refresh_success = True
                        for dl in FeederUtil.get_downloaders():
                            if dl.get('enabled', True):
                                engine = DownloadUtil.create_engine(dl)
                                if engine and hasattr(engine, 'get_status'):
                                    try:
                                        s_list, status_error = engine.get_status()
                                        if status_error:
                                            refresh_success = False
                                            logger.debug(f"[{self.name}] SSE 엔진 상태 갱신 오류({dl.get('name')}): {status_error}")
                                            continue
                                        if s_list:
                                            for c_item in s_list:
                                                c_item['engine_name'] = dl.get('name', 'alldebrid')
                                            refreshed_tasks.extend(s_list)
                                    except Exception as engine_ex:
                                        refresh_success = False
                                        logger.debug(f"[{self.name}] SSE 엔진 상태 갱신 실패({dl.get('name')}): {engine_ex}")
                        if refresh_success:
                            engine_tasks_cache = refreshed_tasks
                    cloud_tasks = engine_tasks_cache

                    engine_live_data = {str(s.get('task_id')): s for s in cloud_tasks if s.get('task_id')}
                    engine_live_data.update({str(s.get('hash')).lower(): s for s in cloud_tasks if s.get('hash')})

                    # DB의 활성 작업 ID 및 해시 세트
                    db_tids = {str(it.engine_task_id) for it in active_rows if it.engine_task_id}
                    db_hashes = {str(it.infohash).lower() for it in active_rows if it.infohash}

                    # 각 아이템 딕셔너리에 실시간 속도, 진행률 병합
                    active_list = []
                    for it in active_rows:
                        d = it.as_dict()

                        # 엔진 라이브 진행 정보는 DB 상태가 실제 다운로드 중(downloading)일 때만 반영
                        if it.status == 'downloading':
                            live_info = engine_live_data.get(str(it.engine_task_id)) or engine_live_data.get(str(it.infohash or '').lower())
                            if live_info:
                                dl_bytes = live_info.get('downloaded_bytes', 0)
                                dl_speed = live_info.get('download_speed', 0)
                                total_sz = live_info.get('file_size') or it.file_size or 0

                                if 'progress' in live_info:
                                    prog = float(live_info['progress'])
                                elif total_sz > 0 and dl_bytes > 0:
                                    prog = round((dl_bytes / total_sz) * 100, 1)
                                else:
                                    prog = 0.0

                                d['downloaded_bytes'] = dl_bytes
                                d['download_speed'] = dl_speed
                                d['progress'] = min(prog, 100.0)

                        # 로컬 스테이징 및 Google Drive 업로드 중일 때는 Rclone 실시간 IPC 데이터 병합
                        elif it.status in ['local_staging', 'uploading']:
                            rclone_stat = FeederUtil.get_rclone_progress(it.id)
                            if rclone_stat:
                                d['progress'] = rclone_stat.get('progress', 0.0)
                                d['download_speed'] = rclone_stat.get('download_speed', 0)
                                d['speed_str'] = rclone_stat.get('speed_str', '')
                                d['downloaded_bytes'] = rclone_stat.get('downloaded_bytes', 0)
                                d['file_size'] = rclone_stat.get('total_bytes') or d.get('file_size') or 0
                                d['eta'] = rclone_stat.get('eta', '')

                        # 단계 상태는 항상 DB에 저장된 실제 상태(it.status)를 최우선 유지
                        d['status'] = it.status
                        active_list.append(d)

                    # DB에 등록되지 않은 다운로더 엔진(AllDebrid, PikPak 등) 잔여 항목들을 동적 엔진명으로 큐에 노출
                    for c_task in cloud_tasks:
                        tid = str(c_task.get('task_id', ''))
                        thash = str(c_task.get('hash', '')).lower()
                        if tid and tid in db_tids:
                            continue
                        if thash and thash in db_hashes:
                            continue

                        eng_name = c_task.get('engine_name') or 'Engine'
                        raw_id = tid or thash or '0'
                        display_id = raw_id[:10] if len(raw_id) > 10 else raw_id

                        active_list.append({
                            'id': display_id,
                            'task_id': tid,
                            'hash': thash,
                            'feed_name': eng_name,
                            'title': c_task.get('filename') or f"{eng_name} Task {display_id}",
                            'file_name': c_task.get('filename') or '',
                            'file_size': c_task.get('file_size', 0),
                            'status': 'engine_unmanaged',
                            'current_engine_name': eng_name,
                            'local_path': c_task.get('source_path', ''),
                            'raw_status': c_task.get('status', ''),
                            'created_time': f"{eng_name} 보관중"
                        })

                    data_str = json.dumps({
                        'timestamp': int(time.time()),
                        'counts': counts,
                        'total_upload_24h': cached_24h_upload_bytes,
                        'active_list': active_list
                    })
                    yield f"data: {data_str}\n\n"

            except Exception as e:
                logger.debug(f"[{self.name}] SSE 스트림 전송 예외: {e}")
            finally:
                try:
                    db.session.remove()
                except Exception:
                    pass
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
        account_total = sum(v for k, v in usage_map.items() if k != 'SHARED_DRIVE_UPLOAD')
        total_24h = sum(usage_map.values())

        blocked_rows = (
            db.session.query(ModelDownloadStat.stat_key, ModelDownloadStat.stat_value)
            .filter(
                ModelDownloadStat.stat_type == 'account_block',
                ModelDownloadStat.stat_value > now
            )
            .all()
        )
        blocked_map = {r[0]: int(r[1] - now) for r in blocked_rows if r[0]}

        return {
            'usage_24h': usage_map,
            'blocked_remains': blocked_map,
            'account_total_usage': account_total,
            'total_usage_24h': total_24h
        }

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

        logger.info(f"[{self.name}] 스케줄러 주기 실행 -> 다운로드 워커 작업 전달")
        self.start_celery(TaskDownloadBase.start, None, "default")
