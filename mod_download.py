# -*- coding: utf-8 -*-
import os
import re
import json
import math
import time
import uuid
import traceback
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, Response, stream_with_context
from sqlalchemy import desc, func, case
import sqlite3
import shutil
import subprocess
import threading
import hashlib

from .setup import *
from .model_download import ModelDownload, ModelDownloadStat, ModelDownloadBlock
from .util_block import DownloadBlock
from .util_base import FeederUtil
from .util_download import DownloadUtil, PikPakEngine
from .task_download import TaskDownload, TaskDownloadBase
from .util_upload import UploadUtil
from .util_remote_worker import RemoteWorker
from .remote_worker_client import WorkerError
from .util_download_activity import DownloadActivity
from .util_dashboard import Dashboard

name = 'download'


class ModuleDownload(PluginModuleBase):

    @staticmethod
    def _ad_hash(value):
        value = str(value or '').strip()
        normalized = (FeederUtil.extract_info_hash('magnet:?xt=urn:btih:' + value) or '').lower()
        return normalized if re.fullmatch(r'[0-9a-f]{40}', normalized) else ''

    @staticmethod
    def _ad_revision(item):
        return hashlib.sha256(json.dumps({column.name: getattr(item, column.name)
            for column in ModelDownload.__table__.columns}, sort_keys=True, default=str).encode()).hexdigest()

    @staticmethod
    def _ad_candidates(engine_name=None):
        """선택 엔진 목록을 infohash로 비교한다. 작업 ID는 원격 명령의 대상으로만 사용한다."""
        remote, engines = [], {}
        for config in FeederUtil.get_downloaders():
            if (not config.get('enabled', True) or
                    config.get('engine_type') not in ('alldebrid', 'qbittorrent', 'cd2', 'pikpak') or
                    (engine_name is None and config.get('engine_type') != 'alldebrid') or
                    (engine_name and config.get('name') != engine_name)):
                continue
            engine = DownloadUtil.create_engine(config)
            if engine is None:
                raise ValueError(f'{config["name"]} 엔진을 준비하지 못했습니다.')
            tasks, error = engine.get_status()
            if (error or not isinstance(tasks, list) or
                    (not getattr(engine, 'status_list_complete', True) and config['engine_type'] != 'cd2')):
                raise ValueError(f'{config["name"]} 원격 목록 오류/불완전: 실행을 보류합니다.')
            engines[config['name']] = engine
            for task in tasks:
                if not isinstance(task, dict) or not task.get('task_id'):
                    raise ValueError(f'{config["name"]} 원격 목록 형식 오류: 실행을 보류합니다.')
                if engine_name is not None or task.get('status') == 'completed':
                    remote.append(dict(task, engine_name=config['name'], engine_type=config['engine_type']))
        if not engines:
            raise ValueError('선택한 활성 엔진이 없습니다.')
        rows = db.session.query(ModelDownload).populate_existing().all()
        by_hash = {}
        for item in rows:
            value = ModuleDownload._ad_hash(item.infohash)
            if value:
                by_hash.setdefault(value, []).append(item)
        remote_hashes = {}
        for task in remote:
            value = ModuleDownload._ad_hash(task.get('hash'))
            key = (task['engine_name'], value)
            remote_hashes[key] = remote_hashes.get(key, 0) + 1
        candidates = []
        for task in remote:
            value = ModuleDownload._ad_hash(task.get('hash'))
            candidate = {k: task.get(k) for k in ('engine_name', 'engine_type', 'task_id', 'source_path', 'filename')}
            candidate.update(hash=value, action='skip', reason='미등록: 대응 DB 기록 없음', item_id=None)
            matches = by_hash.get(value, []) if value else []
            candidate['registered'] = bool(matches)
            if not value:
                candidate['reason'] = '원격 infohash 미확인 · ID만으로 병합/삭제하지 않으며 기존 후처리는 유지합니다.'
            elif len(matches) > 1 or remote_hashes[(task['engine_name'], value)] > 1:
                candidate['reason'] = '동일 해시의 복수 DB 또는 같은 엔진 후보'
            elif len(matches) == 1:
                item = matches[0]
                state = item.upload_state or {}
                source = state.get('download_source') or {}
                name, tid = task['engine_name'], str(task['task_id'])
                path = str(task.get('source_path') or '').rstrip('/')
                candidate.update(item_id=item.id, title=item.title, status=item.status,
                    revision=ModuleDownload._ad_revision(item))
                worker = state.get('worker') or {}
                busy = bool(state.get('phase') or (worker and not worker.get('settled') and
                    worker.get('submission') not in ('new', 'rejected')))
                shared = False
                for other in rows:
                    other_state = other.upload_state or {}
                    if other.id == item.id or (other.status in ('completed', 'cancelled', 'blocked', 'failed') and not other_state.get('worker')):
                        continue
                    other_path = str(other.local_path or other_state.get('local_path') or '').rstrip('/')
                    # Pending legacy AD paths may be stale; use the same full, hash-based snapshot.
                    if (task['engine_type'] == 'alldebrid' and other.current_engine_name == name and
                            not other_state.get('worker') and not other_state.get('phase')):
                        other_sources = [r for r in remote if r['engine_name'] == name and
                                         ModuleDownload._ad_hash(r.get('hash')) == ModuleDownload._ad_hash(other.infohash)]
                        if len(other_sources) == 1 and other_sources[0].get('source_path'):
                            other_path = str(other_sources[0]['source_path']).rstrip('/')
                    if path and other_path and (path == other_path or path.startswith(other_path + '/') or other_path.startswith(path + '/')):
                        shared = True
                        break
                source_names = [str(task.get('filename') or '')]
                if task['engine_type'] == 'alldebrid':
                    source_names.append(f"{task.get('filename')} ({tid})")
                source_ok = bool(path and task.get('filename') and ':' in path and
                                 path.rsplit('/', 1)[-1] in source_names)
                if shared or busy:
                    candidate['reason'] = '다른 활성 작업과 소스 공유/워커·전송 진행 충돌'
                elif item.status == 'completed':
                    if item.destination_type == 'local':
                        candidate['reason'] = '로컬 최종 완료: 보존'
                    elif (item.destination_type and item.completed_time and item.gdrive_complete_path and
                          (item.video_manifest or {}).get('stored') is True):
                        candidate.update(action='delete', reason='비로컬 최종 보관 완료: 원격 잔여·원본 삭제 후보')
                    else:
                        candidate['reason'] = '최종 보관 완료 증거 미확정: 보존'
                elif (item.current_engine_name == name and str(item.engine_task_id) == tid and
                      item.status != 'pending'):
                    candidate['reason'] = '이미 같은 엔진 작업에 연결됨'
                elif task.get('status') != 'completed':
                    candidate['reason'] = '원격 다운로드 진행/대기: 보존'
                elif task['engine_type'] == 'alldebrid' and not source_ok:
                    candidate['reason'] = '원본 경로 미확정'
                elif item.status not in ('pending', 'upload_held', 'downloaded', 'waiting_transporter',
                                          'pending_local_staging', 'pending_upload', 'pending_relay'):
                    candidate['reason'] = '다운로드/워커 진행 또는 부적합한 상태'
                elif name not in (item.priority_chain or []):
                    candidate['reason'] = '기존 프로필 체인에 해당 엔진 없음'
                elif (state.get('registration_wait') or {}).get('uncertain'):
                    candidate['reason'] = '원격 접수 여부 미확정'
                else:
                    foreign = ((source.get('task_id') and (source.get('engine_name') != name or str(source.get('task_id')) != tid)) or
                               (item.engine_task_id and (item.current_engine_name != name or str(item.engine_task_id) != tid)))
                    if foreign:
                        candidate.update(action='manual_merge' if item.status == 'pending' else 'skip',
                            reason='다른 원격 체크포인트 충돌: ' + str(source.get('engine_name') or item.current_engine_name) +
                                   ' / ' + str(source.get('task_id') or item.engine_task_id) + ' · 항목별 선택 확인 필요')
                    else:
                        candidate.update(action='merge', reason='유일 해시: 기존 행을 업로드 경로로 연결')
            candidates.append(candidate)
        if engine_name is not None:
            observed = {(task['engine_name'], str(task['task_id'])) for task in remote}
            for item in rows:
                if (item.current_engine_name in engines and item.engine_task_id and
                        getattr(engines[item.current_engine_name], 'status_list_complete', True) and
                        item.status not in ('completed', 'cancelled', 'blocked', 'failed') and
                        (item.current_engine_name, str(item.engine_task_id)) not in observed):
                    candidates.append(dict(engine_name=item.current_engine_name, task_id=str(item.engine_task_id),
                        hash=ModuleDownload._ad_hash(item.infohash), source_path=item.local_path,
                        filename=item.file_name, item_id=item.id, title=item.title, status=item.status,
                        revision=ModuleDownload._ad_revision(item), action='skip',
                        reason='DB 활성 작업만 존재 · 원격 전체 목록에서 미발견: 기존 작업 보존'))
            for name, engine in engines.items():
                if not getattr(engine, 'status_list_complete', True):
                    candidates.append(dict(engine_name=name, task_id='', item_id=None, action='skip',
                        reason='부분 목록 · 수신한 작업만 확인하며, 미수신 DB 작업의 부재는 판단하지 않습니다.'))
        return candidates, engines

    @staticmethod
    def _ad_reabsorb(selected=None, confirm_delete=False, engine_name=None):
        owner = 'ad-reabsorb:' + uuid.uuid4().hex
        locked = False
        try:
            if selected is not None:
                control = FeederUtil.get_download_control_state()
                if not control['pause_requested'] or control['running']:
                    raise ValueError('전체 작업이 완전히 중단된 뒤 연결할 수 있습니다. 중단 잠금은 유지됩니다.')
                db.session.rollback()
                locked = FeederUtil.acquire_runtime_lock('download_pipeline', owner, stale_after=None)
                if not locked:
                    raise ValueError('다른 파이프라인 작업이 실행 중입니다.')
            candidates, engines = ModuleDownload._ad_candidates(engine_name)
            if selected is None:
                return {'ret': 'success', 'candidates': candidates, 'engine_name': engine_name or ''}
            results = []
            for chosen in selected:
                candidate = next((c for c in candidates if all(c.get(k) == chosen.get(k) for k in
                    ('engine_name', 'task_id', 'hash', 'source_path', 'item_id', 'revision', 'action'))), None)
                result = dict(engine_name=chosen.get('engine_name'), task_id=chosen.get('task_id'), result='skip')
                current = db.session.get(ModelDownload, candidate['item_id'], populate_existing=True) if candidate and candidate['item_id'] else None
                if candidate is None or candidate['action'] not in ('merge', 'manual_merge', 'delete'):
                    result['reason'] = '미리보기 이후 변경/이미 처리/안전 후보 아님'
                elif current is None or ModuleDownload._ad_revision(current) != candidate['revision']:
                    result['reason'] = '미리보기 이후 DB 변경/이미 처리'
                elif candidate['action'] == 'delete':
                    if not confirm_delete:
                        result['reason'] = '원격 잔여·원본 삭제 명시적 확인 없음'
                    else:
                        # 삭제 직전 같은 ID/hash/완료를 다시 확인한다. 다른 ID로 대체하지 않는다.
                        refreshed, engines = ModuleDownload._ad_candidates(engine_name)
                        if not any(c == candidate for c in refreshed):
                            result['reason'] = '삭제 직전 DB/소스 귀속 변경'
                            results.append(result)
                            continue
                        engine = engines[candidate['engine_name']]
                        tasks, error = engine.get_status()
                        exact = [t for t in tasks if str(t.get('task_id')) == str(candidate['task_id']) and
                                 ModuleDownload._ad_hash(t.get('hash')) == candidate['hash']]
                        if (error or len(exact) != 1 or (not getattr(engine, 'status_list_complete', True) and
                                                       candidate['engine_type'] != 'cd2')):
                            result['reason'] = '삭제 직전 원격 상태 불일치/불완전'
                        else:
                            options = {'delete_files': True} if candidate['engine_type'] in ('qbittorrent', 'cd2', 'pikpak') else {}
                            ok = engine.delete_task(str(candidate['task_id']), **options)
                            result.update(result='success' if ok else 'fail', reason='원격 잔여·원본 삭제' if ok else '원격 삭제 실패')
                elif candidate['action'] == 'manual_merge' and not chosen.get('manual_confirm'):
                    result['reason'] = '다른 원격 체크포인트의 항목별 확인 없음'
                else:
                    item = db.session.get(ModelDownload, candidate['item_id'])
                    state = dict(item.upload_state or {})
                    previous = state.get('download_source') or {}
                    if previous and (previous.get('engine_name') != candidate['engine_name'] or
                                     str(previous.get('task_id')) != str(candidate['task_id'])):
                        state['source_history'] = list(state.get('source_history') or []) + [dict(previous,
                            local_path=item.local_path, current_engine_name=item.current_engine_name, engine_task_id=item.engine_task_id)]
                    elif item.engine_task_id and (item.current_engine_name != candidate['engine_name'] or
                                                 str(item.engine_task_id) != str(candidate['task_id'])):
                        state['source_history'] = list(state.get('source_history') or []) + [{
                            'engine_name': item.current_engine_name, 'task_id': item.engine_task_id, 'local_path': item.local_path}]
                    state['download_source'] = {'engine_name': candidate['engine_name'], 'task_id': str(candidate['task_id']),
                                                'source_path': candidate['source_path'], 'upload_path': item.gdrive_upload_path}
                    state.pop('registration_wait', None)
                    state.pop('hold', None)
                    item.upload_state = state
                    item.current_engine_name = candidate['engine_name']
                    item.current_engine_index = item.priority_chain.index(candidate['engine_name'])
                    item.engine_task_id = str(candidate['task_id'])
                    item.local_path = candidate['source_path']
                    # 경로 해석·스테이징·릴레이 판정은 기존 완료 폴링에 맡긴다.
                    item.status = 'downloading'
                    item.error_message = None
                    item.last_status_time = datetime.now()
                    db.session.commit()
                    result.update(result='success', reason='기존 행 연결 → 기존 다운로드 완료 후처리 대기')
                results.append(result)
                logger.info('[엔진·DB 동기화] %s/%s %s · %s', result['engine_name'], result['task_id'], result['result'], result.get('reason', ''))
            return {'ret': 'success', 'results': results, 'success_count': sum(r['result'] == 'success' for r in results),
                    'failed_count': sum(r['result'] == 'fail' for r in results),
                    'skip_count': sum(r['result'] == 'skip' for r in results),
                    'conflict_count': sum(c['action'] == 'manual_merge' for c in candidates)}
        except Exception as exc:
            db.session.rollback()
            logger.exception('[엔진·DB 동기화] 처리 실패')
            return {'ret': 'fail', 'msg': str(exc) if isinstance(exc, ValueError) else '엔진·DB 동기화 실패: 플랫폼 로그 확인'}
        finally:
            if locked:
                FeederUtil.release_runtime_lock('download_pipeline', owner)

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
            f"{self.name}_upload_workers": "2",
            f"{self.name}_duplicate_min_video_size_mb": "100",
            f"{self.name}_upload_root": "incoming",
            f"{self.name}_complete_root": "uploads",
            f"{self.name}_exclude_pattern": "",
            f"{self.name}_enable_sse": "True",
            f"{self.name}_skip_disabled_engines": "True",
            f"{self.name}_rclone_conf_path": "",
            f"{self.name}_rclone_remote_name": "",
            f"{self.name}_rclone_upload_remote_name": "",
            f"{self.name}_use_gdrive_pool": "False",
            f"{self.name}_gdrive_use_impersonate": "False",
            f"{self.name}_gdrive_mydrive_remote_name": "",
            f"{self.name}_rclone_shared_remote_name": "",
            f"{self.name}_shared_drive_id": "",
            f"{self.name}_rclone_options_migrated": "False",
            f"{self.name}_rclone_transfers": "2",
            f"{self.name}_mydrive_upload_threshold": "14GB",
            f"{self.name}_gdrive_upload_limit": "700GB",
            f"{self.name}_shared_drive_upload_limit": "3TB",
            f"{self.name}_shared_drive_quota_reset_time": "16:00",
            f"{self.name}_rclone_extra_options": "--drive-chunk-size 256M --timeout 60m",
            f"{self.name}_worker_url": "",
            f"{self.name}_worker_network_mode": "tailscale",
            f"{self.name}_worker_token": "",
            f"{self.name}_worker_token_path": "",
            f"{self.name}_worker_session_id": "",
        }

    def plugin_load(self):
        """플러그인 로드 시 이전 비정상 종료된 전송 중 작업을 직전 대기 상태로 일괄 초기화"""
        try:
            with F.app.app_context():
                if not RemoteWorker.begin_session():
                    return
                FeederUtil.migrate_rclone_options()
                RemoteWorker.migrate_token()
                # FF 재시작 후 남아 있을 수 있는 다운로드 파이프라인 런타임 락 초기화
                FeederUtil.init_runtime_locks()
                FeederUtil.reset_runtime_lock('download_pipeline')
                P.ModelSetting.set(f"{self.name}_is_running", "False")
                P.ModelSetting.set(f"{self.name}_running_start_time", "0")
                FeederUtil.init_sqlite_concurrency()
                FeederUtil.migrate_db()
                DownloadBlock.initialize()
                for block in db.session.query(ModelDownloadBlock).all():
                    if any(s.get('pending') for s in (block.cleanup or [])):
                        DownloadBlock.start_cleanup(block.content_key)

                stg_cnt = db.session.query(ModelDownload).filter_by(status='local_staging').update({'status': 'pending_local_staging'})
                # 이전 세션에 접수된 원격 작업도 새 설정으로 재등록한다.
                # 이미 복사를 마친 단계는 건드리지 않고 최종 이동 체크포인트를 보존한다.
                for item in db.session.query(ModelDownload).filter_by(status='pending_upload').all():
                    state = dict(item.upload_state or {})
                    if state.get('phase') == 'copying' and state.get('worker'):
                        state['restart_recopy'] = True
                        item.upload_state = state

                # 미완료 복사만 새 전송 대상으로 복원한다. 복사 완료 체크포인트는 최종 이동을 재개한다.
                active_uploads = db.session.query(ModelDownload).filter_by(status='uploading').all()
                up_cnt = len(active_uploads)
                for item in active_uploads:
                    state = dict(item.upload_state or {})
                    if state.get('phase') == 'copying' and state.get('incoming'):
                        if state.get('worker'):
                            # 원격 실행 종료 확인 전에는 새 job/정리 명령을 제출하지 않는다.
                            state['restart_recopy'] = True
                        else:
                            state['cleanup_pending'] = True
                        item.upload_state = state
                    item.status = 'pending_upload'
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
            arg['download_worker_token_saved'] = bool(arg.pop('download_worker_token', ''))
            arg.pop('download_worker_token_path', None)
            arg.setdefault('download_skip_disabled_engines', 'True')
            arg['auto_disabled_engines'] = FeederUtil.auto_disabled_engines()
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
                arg['sync_engines'] = [c['name'] for c in FeederUtil.get_downloaders() if c.get('enabled', True) and
                                       c.get('engine_type') in ('alldebrid', 'qbittorrent', 'cd2', 'pikpak')]
                return render_template(f'{P.package_name}_{self.name}_queue.html', arg=arg)

            return render_template(f'{P.package_name}_{self.name}_list.html', arg=arg)
        except Exception as e:
            logger.error(f"[{self.name}] process_menu 에러: {e}")
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def process_ajax(self, sub, req):
        try:
            command = req.form.get('command') or sub

            if command == 'setting_save':
                roots = FeederUtil.get_upload_roots()
                for key in roots:
                    roots[key] = req.form.get('download_' + key, roots[key])
                error = (FeederUtil.relative_path_error(roots['upload_root']) or
                         FeederUtil.relative_path_error(roots['complete_root']))
                if not error:
                    destination = FeederUtil.resolve_destination_paths({'type': 'remote_worker'}, roots, {})
                    error = destination.get('_path_error', '')
                if not error:
                    for engine in FeederUtil.get_downloaders():
                        candidate = dict(roots, **engine)
                        for profile in FeederUtil.get_download_profiles() + [{}]:
                            if profile and str(engine['name']).strip().lower() not in [str(name).strip().lower() for name in profile.get('priority_chain') or []]:
                                continue
                            error = FeederUtil.get_engine_destination(candidate, profile).get('_path_error', '')
                            if error:
                                break
                        if error:
                            break
                if error:
                    return jsonify({'ret': 'warning', 'msg': error})
                return super(ModuleDownload, self).process_ajax(sub, req)

            if command in ('queue_bootstrap', 'queue_page', 'queue_engines', 'queue_progress', 'queue_upload_counts'):
                try:
                    if command in ('queue_bootstrap', 'queue_page'):
                        result = Dashboard.queue(cursor=req.form.get('cursor') if command == 'queue_page' else None,
                            known=json.loads(req.form.get('known') or '{}'))
                    elif command == 'queue_engines':
                        result = Dashboard.engine_view(visible=json.loads(req.form.get('visible') or '[]'))
                    elif command == 'queue_progress':
                        ids = [int(i) for i in json.loads(req.form.get('ids') or '[]') if str(i).isdigit()]
                        result = Dashboard.progress(ids)
                    else:
                        result = Dashboard.upload_counts(self.get_upload_queue_counts)
                    return jsonify(result)
                except Exception as exc:
                    return jsonify({'success': False, 'error': 'snapshot_expired' if str(exc) == 'snapshot_expired'
                                    else 'dashboard_query_failed', 'observed_at': time.time()})
                finally:
                    db.session.remove()

            if command in ['pipeline_control_state', 'pause_pipeline', 'resume_pipeline']:
                if command == 'pause_pipeline':
                    FeederUtil.set_download_paused(True)
                control = FeederUtil.get_download_control_state()
                action = 'paused' if command == 'pause_pipeline' else 'state'
                submitted = False
                message = '진행 중인 작업의 최종 이동·정리를 마친 뒤 중단합니다.' if command == 'pause_pipeline' else ''
                start_requested = False
                if command == 'resume_pipeline':
                    recovery = any(key in req.form for key in ('recover_lock', 'confirm_finished', 'expected_owner'))
                    if recovery:
                        expected_owner = req.form.get('expected_owner')
                        if (req.form.get('recover_lock') != 'true' or req.form.get('confirm_finished') != 'true'
                                or not expected_owner or not control['pause_requested']):
                            return jsonify({'ret': 'warning', 'action': 'conflict', 'task_submitted': False,
                                'msg': '중단 예약 상태에서 이전 Celery 작업과 실제 전송의 종료를 확인한 경우에만 복구할 수 있습니다.',
                                'control': control})
                        if (not control['pipeline_locked'] or control['pipeline_owner'] != expected_owner or
                                not FeederUtil.release_runtime_lock('download_pipeline', expected_owner)):
                            return jsonify({'ret': 'warning', 'action': 'conflict', 'task_submitted': False,
                                'msg': '잠금 소유자가 바뀌었거나 이미 해제되어 복구를 보류합니다. 현재 상태를 다시 확인해주세요.',
                                'control': FeederUtil.get_download_control_state()})
                        P.ModelSetting.set('download_is_running', 'False')
                        P.ModelSetting.set('download_running_start_time', '0')
                        with FeederUtil._rclone_stats_lock:
                            FeederUtil._rclone_live_stats.clear()
                        action, start_requested = 'recovered', True
                        message = '잔여 잠금을 복구하고 새 실행을 요청했습니다. 실제 실행 상태는 화면에서 확인해주세요.'
                    elif control['pipeline_locked']:
                        action = 'continued' if control['pause_requested'] else 'already_running'
                        message = ('중단 예약을 취소하고 기존 작업을 계속합니다. 새 작업은 등록하지 않습니다.'
                                   if control['pause_requested'] else '이미 실행 잠금이 있어 새 작업을 등록하지 않습니다.')
                    else:
                        action, start_requested = 'started', True
                        message = '작업 재개를 요청했습니다. 실제 실행 상태는 화면에서 확인해주세요.'
                    FeederUtil.set_download_paused(False)
                    control = FeederUtil.get_download_control_state()
                if start_requested:
                    # start_celery는 작업 완료까지 기다리므로 HTTP 요청에서 호출하지 않는다.
                    if F.config.get('use_celery', False):
                        TaskDownloadBase.start.apply_async(args=('default',))
                    else:
                        threading.Thread(
                            target=TaskDownload.run_pipeline,
                            name='feeder-download-resume', daemon=True
                        ).start()
                    submitted = True
                response = {'ret': 'success', 'control': control, 'action': action,
                            'task_submitted': submitted, 'msg': message}
                if command == 'pipeline_control_state' and req.form.get('dashboard_version') != '2':
                    response['counts'] = self.get_upload_queue_counts()
                return jsonify(response)

            if sub in ['one_execute', 'scheduler_once'] or command in ['one_execute', 'scheduler_once']:
                if FeederUtil.is_download_paused():
                    return jsonify({'ret': 'warning', 'msg': '중단 상태입니다. 다운로드 큐에서 작업을 재개하세요.'})
                return self.one_execute()

            if command == 'list_filter_options':
                return jsonify(self.web_list_model.list_filter_options())

            if sub == 'web_list' or command == 'web_list':
                return jsonify(self.web_list_model.web_list(req))

            elif command == 'load_downloaders':
                DownloadUtil.load_engines()
                return jsonify({
                    'downloaders': FeederUtil.get_downloaders(),
                    'schemas': DownloadUtil.get_engine_schemas(),
                    'upload_roots': FeederUtil.get_upload_roots(),
                    'transporter_schemas': DownloadUtil.get_transporter_schemas()
                })

            elif command == 'save_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                ret = FeederUtil.save_downloader(item_data)
                return jsonify({'ret': ret, 'downloaders': FeederUtil.get_downloaders()})

            elif command == 'pikpak_rclone_auth':
                return jsonify(PikPakEngine.rclone_auth_action(req.form.get('remote_name', ''),
                    req.form.get('username', ''), req.form.get('action', '')))

            elif command == 'test_downloader':
                cfg_json = req.form.get('downloader_json', '{}')
                item_data = json.loads(cfg_json)
                engine = DownloadUtil.create_engine(item_data)
                if not engine:
                    return jsonify({'ret': 'fail', 'msg': f"엔진 인스턴스 생성 실패 ({item_data.get('engine_type')})"})
                success, msg = engine.test_connection()
                return jsonify({'ret': 'success' if success else 'fail', 'msg': msg})

            elif command in ('test_remote_worker', 'save_remote_worker'):
                try:
                    endpoint = req.form.get('worker_url')
                    token = req.form.get('worker_token', '').strip() or None
                    network_mode = req.form.get('worker_network_mode', P.ModelSetting.get('download_worker_network_mode') or 'tailscale')
                    client = RemoteWorker.client(endpoint=endpoint.strip() if endpoint is not None else None, token=token,
                                                 network_mode=network_mode)
                    if command == 'save_remote_worker':
                        RemoteWorker._write_settings({
                            'download_worker_token': token or P.ModelSetting.get('download_worker_token'),
                            'download_worker_url': (endpoint if endpoint is not None else P.ModelSetting.get('download_worker_url')).strip(),
                            'download_worker_token_path': '',
                            'download_worker_network_mode': network_mode,
                        })
                        return jsonify({'ret': 'success', 'msg': '워커 연결 설정을 저장했습니다.'})
                    health = client.ensure_version(force_update=True)
                    configuration = client.configuration()
                    version = tuple(int(part) for part in health.get('version', '').split('.'))
                    if version < (0, 7, 3):
                        raise WorkerError('Rclone 파일 동시 전송 수를 적용하려면 워커 0.7.3 이상으로 업데이트해주세요.')
                    limit = max(1, P.ModelSetting.get_int('download_upload_workers'))
                    return jsonify({'ret': 'success', 'msg': f"워커 {health['version']} 연결됨 · 실행 {health['active_jobs']} / 대기 {health['queued_jobs']} · Feeder 동시 전송 설정 {limit}개 · 작업당 파일 {FeederUtil.get_rclone_transfers()}개"})
                except WorkerError as exc:
                    if exc.status == 202:
                        return jsonify({'ret': 'success', 'msg': str(exc), 'updating': True})
                    message = '인증에 실패했습니다. 워커 설치 시 안내된 토큰을 확인해주세요.' if exc.status == 401 else str(exc)
                    return jsonify({'ret': 'fail', 'msg': message})
                except (ValueError, KeyError, TypeError):
                    return jsonify({'ret': 'fail', 'msg': '워커 응답 형식 또는 버전을 확인해주세요.'})

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
                    'local_staging_root': FeederUtil.get_local_staging_root(),
                    'downloaders': FeederUtil.get_downloaders(),
                    'feeds': FeederUtil.get_feeds(),
                    'upload_roots': FeederUtil.get_upload_roots(),
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
                    FeederUtil.cooperate()
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

            elif command == 'adopt_orphan_preview':
                config = FeederUtil.get_downloader_by_name(req.form.get('engine_name', '')) or {}
                if config.get('engine_type') not in ('alldebrid', 'qbittorrent', 'cd2', 'pikpak'):
                    return jsonify({'ret': 'new'})
                preview = ModuleDownload._ad_reabsorb(engine_name=config['name'])
                if preview['ret'] == 'success':
                    preview['candidates'] = [c for c in preview['candidates'] if c['engine_name'] == config['name'] and
                        str(c['task_id']) == req.form.get('task_id')]
                    if not any(c.get('registered') or c.get('item_id') for c in preview['candidates']):
                        return jsonify({'ret': 'new'})
                return jsonify(preview)

            elif command in ('ad_reabsorb_preview', 'ad_reabsorb_apply', 'engine_sync_preview', 'engine_sync_apply'):
                selected = None
                if command.endswith('_apply'):
                    selected = json.loads(req.form.get('selected', '[]'))
                    if not isinstance(selected, list):
                        return jsonify({'ret': 'fail', 'msg': '선택 목록 형식 오류'})
                engine_name = req.form.get('engine_name', '') if command.startswith('engine_sync_') else None
                return jsonify(ModuleDownload._ad_reabsorb(selected, req.form.get('confirm_delete') == 'true', engine_name))

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

                config = FeederUtil.get_downloader_by_name(engine_name) or {}
                if config.get('engine_type') in ('alldebrid', 'qbittorrent', 'cd2', 'pikpak'):
                    preview = ModuleDownload._ad_reabsorb(engine_name=engine_name)
                    if preview['ret'] != 'success':
                        return jsonify(preview)
                    preview['candidates'] = [c for c in preview['candidates'] if
                        c['engine_name'] == engine_name and str(c['task_id']) == task_id]
                    if any(c.get('registered') or c.get('item_id') for c in preview['candidates']):
                        preview['ret'] = 'preview'
                        return jsonify(preview)

                if config.get('engine_type') == 'alldebrid' and raw_status == 'completed':
                    candidates, _ = ModuleDownload._ad_candidates()
                    remote = next((c for c in candidates if c['engine_name'] == engine_name and
                                   str(c['task_id']) == task_id and c['hash'] == ModuleDownload._ad_hash(thash)), None)
                    if not remote or not remote.get('hash') or not remote.get('source_path') or not remote.get('filename'):
                        return jsonify({'ret': 'fail', 'msg': 'AD 완료 원본 경로/해시를 확인하지 못했습니다.'})
                    source_path = remote['source_path']

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
                    FeederUtil.cooperate()
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
                dest_type = dest_cfg.get('type') or ''
                error = FeederUtil.validate_destination(dest_cfg)
                if error:
                    return jsonify({'ret': 'fail', 'msg': error})
                use_local_staging = FeederUtil.get_upload_options(selected_profile, engine_name)['use_local_staging']

                # 등록 일시를 현재 시각으로 초기화하여 타임아웃 조기 종료 방지
                now = datetime.now()
                target_mag = f"magnet:?xt=urn:btih:{thash}" if thash else f"engine:{engine_name}:{task_id}"
                if ModelDownload.is_blocked(thash, target_mag):
                    return jsonify({'ret': 'fail', 'msg': '차단된 콘텐츠입니다. 차단 목록에서 먼저 해제해주세요.'})

                item = ModelDownload(
                    feed_name=selected_profile.get('feeds', ['*'])[0] if selected_profile.get('feeds') else 'ADOPTED',
                    title=title or file_name or f"{engine_name}_{task_id}",
                    magnet=target_mag,
                    infohash=thash or None
                )
                item.file_name = file_name or title
                item.file_size = fsize
                item.priority_chain = chain
                item.upload_state = {'routing': {'profile_name': profile_name}}
                if config.get('engine_type') == 'alldebrid':
                    item.upload_state['download_source'] = {
                        'engine_name': engine_name, 'task_id': str(task_id), 'upload_path': dest_cfg.get('upload_path', '')}
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
                if transporter and getattr(transporter, 'FORCE_REMOTE_SOURCE', False):
                    use_local_staging = False

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
                    FeederUtil.cooperate()
                    infohash = FeederUtil.extract_info_hash(line)
                    target_mag = f"magnet:?xt=urn:btih:{infohash}" if infohash else line
                    if ModelDownload.is_blocked(infohash, target_mag):
                        skipped_count += 1
                        continue

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
                        FeederUtil.cooperate()
                        r_dict = dict(r)
                        raw_mag = str(r_dict.get(magnet_col, '')).strip()
                        if not raw_mag:
                            continue

                        infohash = FeederUtil.extract_info_hash(raw_mag)
                        target_mag = f"magnet:?xt=urn:btih:{infohash}" if infohash else raw_mag
                        if ModelDownload.is_blocked(infohash, target_mag):
                            skipped_count += 1
                            continue

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

            elif command == 'direct_add_batch':
                return jsonify(self._direct_add_batch(req))

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

            elif command == 'block_orphan':
                engine_name = req.form.get('engine_name', '').strip()
                task_id = req.form.get('task_id', '').strip()
                infohash = req.form.get('hash', '').strip().lower() or None
                if not task_id or not FeederUtil.get_downloader_by_name(engine_name):
                    return jsonify({'ret': 'fail', 'msg': '원격 작업과 다운로더 설정을 확인해주세요.'})
                magnet = f'magnet:?xt=urn:btih:{infohash}' if infohash else f'engine:{engine_name}:{task_id}'
                item = db.session.query(ModelDownload).filter_by(
                    current_engine_name=engine_name, engine_task_id=task_id).first()
                if item is None:
                    item = ModelDownload('REMOTE', req.form.get('title') or f'{engine_name}_{task_id}', magnet, infohash)
                    item.status = 'blocked'
                    item.current_engine_name = engine_name
                    item.engine_task_id = task_id
                    item.priority_chain = [engine_name]
                    db.session.add(item)
                    db.session.commit()
                DownloadBlock.block(item)
                return jsonify({'ret': 'success', 'msg': '원격 작업을 차단하고 중단·정리를 요청했습니다.'})

            elif command == 'blacklist_list':
                page = max(1, int(req.form.get('page', 1)))
                query = db.session.query(ModelDownloadBlock)
                total = query.count()
                rows = query.order_by(ModelDownloadBlock.created_time.desc()).offset((page - 1) * 50).limit(50).all()
                return jsonify({'ret': 'success', 'page': page, 'total': total, 'list': [{
                    'key': row.content_key, 'title': row.title,
                    'pending': any(s.get('pending') for s in (row.cleanup or [])),
                    'error': row.error_message,
                } for row in rows]})

            elif command == 'blacklist_action':
                key = req.form.get('key', '')
                if req.form.get('action') == 'unblock':
                    return jsonify(DownloadBlock.unblock(key))
                if db.session.get(ModelDownloadBlock, key) is None:
                    return jsonify({'ret': 'fail', 'msg': '차단 기록을 찾을 수 없습니다.'})
                DownloadBlock.start_cleanup(key)
                return jsonify({'ret': 'success', 'msg': '임시 파일 정리를 다시 요청했습니다.'})

            elif command == 'item_action':
                action = req.form.get('action')
                try:
                    item_id = int(req.form.get('id', -1))
                except Exception:
                    item_id = -1

                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item:
                    return jsonify({'ret': 'fail', 'msg': '항목을 찾을 수 없습니다.'})

                if action == 'execute_single':
                    try:
                        owner = TaskDownload.reserve_single_item(item_id)
                    except ValueError as exc:
                        return jsonify({'ret': 'fail', 'msg': str(exc)})
                    try:
                        if F.config.get('use_celery', False):
                            TaskDownloadBase.start.apply_async(args=('single', item_id, owner))
                        else:
                            threading.Thread(target=TaskDownload.run_single_item, args=(item_id, owner),
                                name=f'feeder-download-single-{item_id}', daemon=True).start()
                    except Exception:
                        FeederUtil.release_runtime_lock('download_pipeline', owner)
                        raise
                    return jsonify({'ret': 'success', 'msg': f'작업 #{item_id}만 실행합니다. 전체 작업 중단은 유지됩니다.',
                                    'control': FeederUtil.get_download_control_state()})

                if action == 'block':
                    DownloadBlock.block(item)
                    return jsonify({'ret': 'success', 'msg': '차단했습니다. 전송을 중단하고 임시 파일을 정리합니다.'})
                if ModelDownload.is_blocked(item.infohash, item.magnet):
                    if action == 'delete':
                        key = ModelDownloadBlock.key(item.infohash, item.magnet)
                        block = db.session.get(ModelDownloadBlock, key)
                        if not any(s.get('pending') for s in (block.cleanup or [])):
                            db.session.delete(item)
                            db.session.commit()
                            return jsonify({'ret': 'success', 'msg': '항목을 삭제했습니다. 콘텐츠 차단은 유지됩니다.'})
                    return jsonify({'ret': 'fail', 'msg': '차단된 콘텐츠입니다. 차단 목록에서 해제하거나 정리 상태를 확인해주세요.'})

                if action == 'redownload':
                    return jsonify(TaskDownload.redownload(item_id))

                if action in ('retry', 'delete', 'force_fail', 'force_complete') and (item.upload_state or {}).get('worker'):
                    try:
                        resumed = RemoteWorker.prepare_action(item, action)
                    except WorkerError as exc:
                        return jsonify({'ret': 'fail', 'msg': str(exc)})
                    if resumed:
                        if action == 'retry':
                            state = dict(item.upload_state or {})
                            state.pop('engine_retry', None)
                            item.upload_state = state
                            db.session.commit()
                        return jsonify({'ret': 'success', 'msg': '복사를 반복하지 않고 최종 이동을 다시 시도합니다.'})

                if action == 'retry':
                    state = dict(item.upload_state or {})
                    state.pop('engine_retry', None)
                    item.upload_state = state
                    source_subpath = FeederUtil.get_download_source_subpath(item)
                    item.source_unavailable_since = None
                    item.upload_retry_after = None
                    item.transfer_error_type = None
                    item.completed_time = None
                    profile_name = req.form.get('profile_name', '').strip()
                    selected_profile = None

                    if profile_name:
                        for p in FeederUtil.get_download_profiles():
                            FeederUtil.cooperate()
                            if p.get('name') == profile_name:
                                selected_profile = p
                                break
                    if not selected_profile:
                        selected_profile = FeederUtil.get_item_profile(item)
                    elif profile_name:
                        routing = dict((item.upload_state or {}).get('routing') or {}, profile_name=profile_name)
                        item.upload_state = dict(item.upload_state or {}, routing=routing)

                    if selected_profile:
                        item.priority_chain = list(selected_profile.get('priority_chain', []))
                        dest = FeederUtil.get_profile_destination(selected_profile, item.current_engine_name, item)
                        item.destination_type = dest.get('type') or ''
                        item.gdrive_upload_path = dest.get('upload_path', '')
                        item.gdrive_complete_path = dest.get('complete_path', '')
                        item.gdrive_remote_id = dest.get('shared_drive_id', '')
                    else:
                        enabled_downloaders = [d['name'] for d in FeederUtil.get_downloaders() if d.get('enabled', True)]
                        if enabled_downloaders:
                            item.priority_chain = enabled_downloaders
                        item.destination_type = FeederUtil.get_profile_destination(None, item=item).get('type') or ''

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
                                        FeederUtil.cooperate()
                                        if entry == target_fname:
                                            found_path = os.path.join(mount_root, entry)
                                            break

                                if found_path:
                                    upload_sub = source_subpath.strip('/')
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
                                        item.file_size = FeederUtil.directory_size(final_dest_path)

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

                elif action == 'force_fail':
                    UploadUtil.kill_item_rclone(item_id)
                    item.status = 'failed'
                    item.completed_time = None
                    item.error_message = '사용자 수동 실패 처리'
                    db.session.commit()
                    return jsonify({'ret': 'success', 'msg': '실패 처리되었습니다. 원본 파일은 보존합니다.'})

                elif action == 'force_complete':
                    item.status = 'completed'
                    item.completed_time = datetime.now()
                    item.error_message = None
                    db.session.commit()
                    logger.info(f"[{self.name}] 수동 완료 처리: {item.title} (ID: {item_id})")
                    return jsonify({'ret': 'success', 'msg': '최종 완료(completed) 처리되었습니다.'})

                elif action == 'delete':
                    # 실행 중인 Rclone 프로세스 즉시 강제 종료
                    UploadUtil.kill_item_rclone(item_id)

                    # 레코드 유실 전 원격 소스(AllDebrid, CD2 등)에서 작업 선제 삭제
                    DownloadUtil.delete_remote_tasks(item)

                    # 로컬 스테이징 임시 파일 정리
                    if item.local_path and os.path.exists(item.local_path) and ':' not in item.local_path:
                        try:
                            if os.path.isdir(item.local_path):
                                shutil.rmtree(item.local_path, ignore_errors=True)
                            elif os.path.isfile(item.local_path):
                                os.remove(item.local_path)
                        except Exception:
                            pass

                    # 재등록 방지를 위한 영구 격리(cancelled) 처리
                    # 이미 취소된 항목을 다시 삭제할 때만 DB에서 완전 제거
                    if item.status == 'cancelled':
                        db.session.delete(item)
                        db.session.commit()
                        logger.info(f"[{self.name}] 취소된 작업 영구 DB 삭제 완료: ID={item_id}")
                        return jsonify({'ret': 'success', 'msg': '작업이 DB에서 완전히 삭제되었습니다.'})

                    item.status = 'cancelled'
                    item.error_message = '사용자 수동 삭제/취소 (영구 격리)'
                    item.last_status_time = datetime.now()
                    db.session.commit()
                    logger.info(f"[{self.name}] 큐 항목 소스 삭제 및 영구 격리(cancelled) 완료: ID={item_id}")
                    return jsonify({'ret': 'success', 'msg': '원격 소스에서 삭제되었으며, 재등록되지 않도록 영구 격리되었습니다.'})

            return super(ModuleDownload, self).process_ajax(sub, req)
        except Exception as e:
            logger.error(f"[{self.name}] process_ajax 에러: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'error', 'msg': str(e)})

    def _direct_add_batch(self, req):
        """편집 후 체인·목적지를 검증하고 모든 링크에 같은 등록 설정을 전달한다."""
        text = req.form.get('links_text', '')
        if len(text.encode('utf-8')) > 1024 * 1024:
            return {'ret': 'fail', 'msg': '전체 입력은 1MiB 이하로 나누어 등록해주세요.'}
        lines = [(number, line.strip()) for number, line in enumerate(text.splitlines(), 1) if line.strip()]
        if not lines or len(lines) > 200:
            return {'ret': 'fail', 'msg': '빈 줄을 제외하고 1~200개의 링크를 입력해주세요.'}
        profile_name = req.form.get('profile_name', '').strip()
        try:
            profile = next((p for p in FeederUtil.get_download_profiles() if p.get('name') == profile_name), None) if profile_name else {}
            if profile is None:
                return {'ret': 'fail', 'msg': '선택한 설정 프리셋을 찾을 수 없습니다. 목록을 다시 불러와주세요.'}
            raw_chain = req.form.get('priority_chain')
            custom_chain = json.loads(raw_chain) if raw_chain else None
            chain = custom_chain if custom_chain is not None else profile.get('priority_chain')
            destination_type = req.form.get('destination_type', '').strip()
            raw_destination = req.form.get('destination_config')
            destination_config = json.loads(raw_destination) if raw_destination else {}
            if not isinstance(destination_config, dict):
                return {'ret': 'fail', 'msg': '목적지 세부 설정 형식을 확인해주세요.'}
            if not isinstance(chain, list) or not chain or any(not isinstance(name, str) or not name.strip() for name in chain):
                return {'ret': 'fail', 'msg': '우선순위 체인에 다운로더 엔진을 1개 이상 추가해주세요.'}
            configs = [FeederUtil.get_downloader_by_name(name) for name in chain]
            if any(not config or FeederUtil.validate_destination(dict(destination_config, type=destination_type) if destination_type
                   else FeederUtil.get_profile_destination(profile, name))
                   for name, config in zip(chain, configs)):
                return {'ret': 'fail', 'msg': '선택한 프로필의 엔진과 목적지 설정을 확인해주세요.'}
            if not DownloadUtil._engine_classes:
                DownloadUtil.load_engines()
            supported = set()
            for config in configs:
                engine = DownloadUtil._engine_classes.get(config.get('engine_type', '').lower())
                supported.update(getattr(engine, 'SUPPORTED_PROTOCOLS', []))
        except Exception:
            db.session.rollback()
            return {'ret': 'fail', 'msg': '프로필과 엔진 설정을 확인하지 못했습니다. 설정을 확인한 뒤 다시 시도해주세요.'}

        counts = dict.fromkeys(('added', 'duplicate', 'blocked', 'invalid', 'unsupported', 'failed'), 0)
        results, seen = [], set()
        messages = {'added': '다운로드 대기열에 추가했습니다.', 'duplicate': '이미 등록됐거나 입력에서 중복된 링크입니다.',
                    'blocked': '차단된 콘텐츠입니다.', 'invalid': '유효한 Magnet 또는 ED2K 파일 링크를 입력해주세요.',
                    'unsupported': '선택한 엔진 체인에 이 링크를 지원하는 엔진이 없습니다.', 'failed': '등록하지 못했습니다. 설정을 확인한 뒤 다시 시도해주세요.'}
        for number, link in lines:
            try:
                FeederUtil.cooperate()
                protocol = 'magnet' if link.startswith('magnet:?') else 'ed2k' if link.startswith('ed2k://|file|') else ''
                valid = (protocol == 'magnet' and re.fullmatch(r'magnet:\?[^\s\x00-\x1f\x7f]+', link) and
                         re.search(r'(?:[?&])xt=urn:btih:(?:[a-f0-9]{40}|[a-z2-7]{32})(?=&|$)', link, re.IGNORECASE)) or (
                         protocol == 'ed2k' and re.fullmatch(r'ed2k://\|file\|[^|\x00-\x1f\x7f]+\|[0-9]+\|[a-f0-9]{32}\|(?:[^|\x00-\x1f\x7f]*\|)*/', link, re.IGNORECASE))
                infohash = FeederUtil.extract_info_hash(link) if valid else None
                if (len(link.encode('utf-8')) > 16384 or
                        not re.fullmatch(r'[a-f0-9]{40}' if protocol == 'magnet' else r'[a-f0-9]{32}', infohash or '')):
                    status = 'invalid'
                elif infohash in seen:
                    status = 'duplicate'
                else:
                    seen.add(infohash)
                    if ModelDownload.is_blocked(infohash, link):
                        status = 'blocked'
                    elif ModelDownload.get_by_infohash(infohash) or ModelDownload.get_by_magnet(link):
                        status = 'duplicate'
                    elif protocol not in supported:
                        status = 'unsupported'
                    else:
                        result = FeederUtil.add_direct_download(title='', magnet=link, profile_name=profile_name,
                                                               feed_name='DIRECT', caller_name=self.name, custom_chain=custom_chain,
                                                               custom_dest_type=destination_type, custom_dest_config=destination_config)
                        status = {'success': 'added', 'exist': 'duplicate'}.get(result.get('ret'), 'failed')
                        if status == 'failed' and ModelDownload.is_blocked(infohash, link):
                            status = 'blocked'
                        if status == 'failed':
                            db.session.rollback()
            except Exception as exc:
                db.session.rollback()
                logger.warning('[DownloadBatch] %s번째 줄 등록 실패 (%s)', number, type(exc).__name__)
                status = 'failed'
            counts[status] += 1
            results.append({'line': number, 'status': status, 'message': messages[status]})
        return {'ret': 'success', 'total': len(lines), 'counts': counts, 'results': results}

    def process_api(self, sub, req):
        try:
            if sub == 'activity':
                try:
                    cursor = max(0, int(req.headers.get('Last-Event-ID') or req.args.get('after', 0)))
                except (TypeError, ValueError):
                    cursor = 0
                if req.args.get('snapshot') == '1':
                    return jsonify({'entries': DownloadActivity.read(cursor), 'timestamp': time.time()})
                response = Response(stream_with_context(self.generate_activity_stream(cursor)), mimetype='text/event-stream')
                response.headers['Cache-Control'] = 'no-cache'
                response.headers['X-Accel-Buffering'] = 'no'
                return response
            elif sub == 'sse':
                response = Response(
                    stream_with_context(self.generate_sse_stream(version=req.args.get('version', '1'))),
                    mimetype='text/event-stream'
                )
                response.headers['Cache-Control'] = 'no-cache'
                response.headers['X-Accel-Buffering'] = 'no'
                return response
            elif sub == 'relay_claim':
                return self._handle_relay_claim(req)
            elif sub == 'relay_report':
                return self._handle_relay_report(req)
            elif sub == 'relay_progress':
                return self._handle_relay_progress(req)
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

        destination = FeederUtil.get_profile_destination(FeederUtil.get_item_profile(item), item.current_engine_name, item)
        error = FeederUtil.validate_destination(destination)
        if error:
            return jsonify({'ret': 'fail', 'msg': error}), 400
        FeederUtil.pin_transfer_destination(item, destination=destination)
        item.status = 'relay_transferring'
        item.upload_state = dict(item.upload_state or {}, relay_claim_id=uuid.uuid4().hex)
        db.session.commit()
        FeederUtil.clear_rclone_progress(item.id)

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
                    f"url = https://webdav.debrid.it/\n"
                    f"vendor = other\n"
                    f"user = {apikey}\n"
                    f"pass = {obscured_pass}\n"
                )
                rclone_conf_text += ad_section
                logger.info(f"[{self.name}] [Relay API] AllDebrid WebDAV Rclone 설정([{engine_remote}]) 동적 주입 완료")

        profile = FeederUtil.get_item_profile(item) or {}
        dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name, item)
        remote_name = (
            dest_cfg.get('shared_remote_name')
            or dest_cfg.get('remote_name')
            or P.ModelSetting.get('download_rclone_shared_remote_name')
            or ''
        )
        shared_drive_id = item.gdrive_remote_id or dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
        complete_path = (dest_cfg.get('complete_path') or item.gdrive_complete_path or '').strip('/')
        source_name = item.source_file_name or item.file_name or f"item_{item.id}"
        folder_name = DownloadUtil.reserve_output_folder(item, source_name)
        is_single_file = DownloadUtil.is_single_file(source_name, item.local_path or '')

        if shared_drive_id:
            dest_base_path = f"{remote_name}:{{{shared_drive_id}}}/{complete_path}"
        else:
            dest_base_path = f"{remote_name}:{complete_path}"

        dest_dir_path = f"{dest_base_path}/{folder_name}"
        dest_full_path = f"{dest_dir_path}/{source_name}" if is_single_file else dest_dir_path

        if engine_remote:
            source_sub = FeederUtil.get_download_source_subpath(item).strip('/')
            source_parts = '/'.join(part for part in (engine_base, source_sub, source_name) if part)
            src_full_path = item.local_path or f"{engine_remote}:{source_parts}"
        else:
            src_full_path = item.local_path or source_name

        buffer_limit_gb = dest_cfg.get('buffer_limit_gb', 50)

        task_data = {
            'id': item.id,
            'claim_id': item.upload_state['relay_claim_id'],
            'title': item.title,
            'file_name': source_name,
            'output_folder_name': folder_name,
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

    def _handle_relay_progress(self, req):
        """현재 선점한 릴레이 작업의 유효한 전송 통계만 반영한다."""
        if not self._verify_api_auth(req):
            return jsonify({'ret': 'fail', 'msg': 'API 인증 실패'}), 401
        data = req.get_json(silent=True) or req.form or {}
        try:
            task_id = int(data.get('id', -1))
            values = {}
            for key, maximum in (('bytes_transferred', 2**63 - 1), ('total_bytes', 2**63 - 1),
                                 ('speed_bytes_per_sec', 2**63 - 1), ('eta_seconds', 2**31 - 1)):
                value = data.get(key)
                if key == 'eta_seconds' and value is None:
                    values[key] = None
                    continue
                if isinstance(value, bool):
                    raise ValueError()
                value = float(value)
                if not math.isfinite(value) or not 0 <= value <= maximum:
                    raise ValueError()
                if key in ('bytes_transferred', 'total_bytes') and not value.is_integer():
                    raise ValueError()
                values[key] = value
            transferred, total = values['bytes_transferred'], values['total_bytes']
            if total and transferred > total:
                raise ValueError()
        except (ValueError, TypeError, OverflowError):
            return jsonify({'ret': 'fail', 'msg': '진행 통계가 올바르지 않습니다.'}), 400
        item = db.session.get(ModelDownload, task_id)
        if not item:
            return jsonify({'ret': 'fail', 'msg': '해당 작업 ID를 찾을 수 없습니다.'}), 404
        claim_id = (item.upload_state or {}).get('relay_claim_id')
        if item.status != 'relay_transferring' or not claim_id or data.get('claim_id') != claim_id:
            return jsonify({'ret': 'fail', 'msg': '현재 릴레이 선점과 일치하지 않습니다.'}), 409
        speed, eta = values['speed_bytes_per_sec'], values['eta_seconds']
        FeederUtil.set_rclone_progress(item.id, {
            'progress': min(100, 100 * transferred / total) if total else 0,
            'downloaded_bytes': int(transferred), 'total_bytes': int(total),
            'download_speed': speed, 'speed_str': FeederUtil.format_bytes(speed) + '/s',
            'eta': '' if eta is None else str(eta)})
        return jsonify({'ret': 'success'})

    def _handle_relay_report(self, req):
        """원격 릴레이 워커의 작업 완료/실패 결과 수신 API"""
        if not self._verify_api_auth(req):
            return jsonify({'ret': 'fail', 'msg': 'API 인증 실패'}), 401

        data = req.get_json(silent=True) or req.form or {}
        task_id = int(data.get('id', -1))
        raw_success = data.get('success', False)
        if isinstance(raw_success, bool):
            is_success = raw_success
        elif str(raw_success).strip().lower() in ['true', '1', 'on', 'yes']:
            is_success = True
        elif str(raw_success).strip().lower() in ['false', '0', 'off', 'no', '']:
            is_success = False
        else:
            return jsonify({'ret': 'fail', 'msg': 'success 값이 올바르지 않습니다.'}), 400
        error_msg = data.get('error_msg', '')
        bytes_transferred = int(data.get('bytes_transferred', 0))

        item = db.session.query(ModelDownload).filter_by(id=task_id).first()
        if not item:
            return jsonify({'ret': 'fail', 'msg': '해당 작업 ID를 찾을 수 없습니다.'}), 404
        claim_id = (item.upload_state or {}).get('relay_claim_id')
        if item.status != 'relay_transferring' or (claim_id and data.get('claim_id') != claim_id):
            return jsonify({'ret': 'fail', 'msg': '현재 릴레이 선점과 일치하지 않습니다.'}), 409

        if is_success:
            item.status = 'completed'
            item.mark_video_stored()
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

        state = dict(item.upload_state or {})
        state.pop('relay_claim_id', None)
        item.upload_state = state
        db.session.commit()
        FeederUtil.clear_rclone_progress(item.id)
        return jsonify({'ret': 'success'})

    def get_upload_queue_counts(self) -> dict:
        """제한과 같은 현재 큐 집계에 큐 진입 전 대기 작업을 합산한다."""
        _, occupied = TaskDownload._get_upload_queue_state()
        waiting = {item_id for (item_id,) in db.session.query(ModelDownload.id).filter(
            ModelDownload.status.in_(['pending_local_staging', 'downloaded', 'waiting_transporter', 'relay_transferring'])
        ).all()}
        total = len(occupied | waiting)
        return {'uploading': total, 'upload_current': len(occupied), 'upload_total': total}

    def generate_activity_stream(self, cursor=0):
        """엔진 상태 조회와 독립적으로 작업 활동과 수신 확인 신호를 전송한다."""
        yield 'retry: 3000\n\n'
        while True:
            try:
                entries = DownloadActivity.read(cursor)
                data = json.dumps({'entries': entries, 'timestamp': time.time()}, ensure_ascii=False)
                if entries:
                    cursor = entries[-1]['id']
                    yield f'id: {cursor}\ndata: {data}\n\n'
                else:
                    yield f'data: {data}\n\n'
            except (OSError, sqlite3.Error):
                yield 'event: activity_error\ndata: {}\n\n'
            time.sleep(3)

    def generate_sse_stream(self, version='1'):
        """v2는 갱신 신호만 전송한다. 외부 엔진/디스크 조회를 구독자에게 종속시키지 않는다."""
        yield 'retry: 5000\n\n'
        while True:
            if version == '2':
                yield 'data: ' + json.dumps({'version': 2, 'kind': 'refresh', 'observed_at': time.time()}) + '\n\n'
            else:
                # 이전 브라우저의 이벤트 형식은 유지하되 DB 전체 스냅샷은 30초 간격으로 제한한다.
                try:
                    metric = Dashboard.upload_counts(self.get_upload_queue_counts)
                    if metric['counts'] is None:
                        # 구형 onmessage가 누락된 숫자를 0으로 덮지 않도록 준비 전에는 상태 이벤트만 보낸다.
                        yield 'event: upload_counts_state\ndata: ' + json.dumps({
                            'state': metric['state'], 'observed_at': metric['observed_at']}) + '\n\n'
                        time.sleep(30)
                        continue
                    with F.app.app_context():
                        try:
                            rows = db.session.query(ModelDownload).filter(ModelDownload.status.in_(list(Dashboard.ranks))).all()
                            data = {'timestamp': time.time(), 'counts': Dashboard.counts(),
                                    'active_list': [Dashboard.serialize(item) for item in rows]}
                            data['counts'].update(metric['counts'])
                            data['upload_counts_state'] = metric['state']
                            data.update(control=FeederUtil.get_download_control_state(),
                                        total_upload_24h=data['counts']['total_upload_24h'],
                                        total_download_24h=data['counts']['total_download_24h'])
                        finally:
                            db.session.remove()
                    yield 'data: ' + json.dumps(data) + '\n\n'
                except Exception:
                    yield 'event: dashboard_error\ndata: {}\n\n'
            time.sleep(5 if version == '2' else 30)

    def get_account_stats_summary(self) -> dict:
        now = int(time.time())
        cutoff = now - 86400

        usage_rows = (
            db.session.query(
                ModelDownloadStat.stat_key,
                func.sum(ModelDownloadStat.stat_value).label('total')
            )
            .filter(
                ModelDownloadStat.stat_type.in_(['gdrive_usage', 'gdrive_reservation']),
                ModelDownloadStat.timestamp > cutoff
            )
            .group_by(ModelDownloadStat.stat_key)
            .all()
        )
        usage_map = {r[0]: int(r[1]) for r in usage_rows if r[0]}
        account_total = sum(v for k, v in usage_map.items() if k not in ('SHARED_DRIVE_UPLOAD', 'SHARED_DRIVE_DIRECT_UPLOAD'))
        total_24h = usage_map.get('SHARED_DRIVE_UPLOAD', account_total)

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
        # FF의 정기 실행과 1회 실행 콜백은 요청 컨텍스트 없이 호출된다.
        with F.app.app_context():
            return self._scheduler_function()

    def _scheduler_function(self):
        if FeederUtil.is_download_paused():
            logger.debug('[DownloadPipeline] 사용자 중단 상태: 정기 실행 보류')
            return
        if P.ModelSetting.get_bool(f"{self.name}_db_auto_delete"):
            try:
                day = P.ModelSetting.get_int(f"{self.name}_db_delete_day")
                if day > 0:
                    target_date = datetime.now() - timedelta(days=day)
                    deleted = (
                        db.session.query(ModelDownload)
                        .filter(
                            ModelDownload.success_filter(),
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
