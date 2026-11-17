# -*- coding: utf-8 -*-
import os
import re
import time
import shutil
import subprocess
import threading
import uuid
import traceback
import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from sqlalchemy import or_, func, and_, event

from .setup import *
from .model_crawl import ModelCrawlItem
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil
from .util_feed import FeedUtil
from .util_download import DownloadUtil
from .util_file_dedup import FileDedup
from .util_upload import UploadUtil
from .util_remote_worker import RemoteWorker
from .remote_worker_client import WorkerError
from .worker.rclone_logging import redact_log
from .util_download_activity import DownloadActivity


class TaskDownloadBase:

    @F.celery.task(bind=True, acks_late=False)
    def start(self, *args):
        logger.info(f"[DownloadTask] Celery Task 수신 인자: {args}")
        job_type = "default"
        for arg in args:
            if isinstance(arg, str) and arg == "default":
                job_type = arg
                break

        delivery_info = getattr(self.request, 'delivery_info', {}) or {}
        is_redelivered = delivery_info.get('redelivered') or getattr(self.request, 'redelivered', False)

        if is_redelivered:
            logger.warning(f"[DownloadTask] [{job_type}] 이전 세션 비정상 종료로 재전송된 고아 태스크 실행 취소")
            return

        if len(args) == 3 and args[0] == 'single':
            TaskDownload.run_single_item(int(args[1]), args[2])
            return
        TaskDownload.run_pipeline()


class TaskDownload:

    # 현재 실행 세션 내 실패/중단된 작업 ID 집합 (현재 주기 내 즉시 재시도 차단용)
    failed_ids_in_session = set()
    _run_mutex = threading.Lock()
    _registration_context = threading.local()
    _CD2_REGISTRATION_LIMIT = 100
    SINGLE_STATUSES = ('pending', 'downloading', 'pending_local_staging', 'downloaded',
                       'waiting_transporter', 'pending_upload', 'move_failed')

    @staticmethod
    @contextmanager
    def _registration_scope():
        """외부 실행에서만 예산을 만들고 중첩 순회·오류 복구는 같은 값을 사용한다."""
        context = TaskDownload._registration_context
        if getattr(context, 'remaining', None) is not None:
            yield
            return
        context.remaining = TaskDownload._CD2_REGISTRATION_LIMIT
        context.vip_snapshots = {}
        context.vip_initialized = False
        try:
            yield
        finally:
            del context.remaining
            del context.vip_snapshots
            del context.vip_initialized

    @staticmethod
    def _init_pikpak_vip_snapshots():
        snapshots = TaskDownload._registration_context.vip_snapshots
        if TaskDownload._registration_context.vip_initialized:
            return
        TaskDownload._registration_context.vip_initialized = True
        accounts, shared = {}, {}
        for config in FeederUtil.get_downloaders():
            if config.get('engine_type') != 'pikpak' or not config.get('enabled', True):
                continue
            account = str(config.get('username') or '').strip().lower() or str(config.get('remote_name') or 'pikpak').strip()
            if account not in accounts:
                engine = DownloadUtil.create_engine(config)
                accounts[account] = engine.registration_eligibility(force=True) if engine else {'state': 'unknown'}
                auth = getattr(engine, '_auth_error', None) if engine else None
                if accounts[account]['state'] == 'unknown' and auth:
                    FeederUtil.engine_auth_error(engine, auth[0], 'vip_status')
            vip = dict(accounts[account])
            vip['expired'] = vip['state'] == 'not_vip' or bool(vip.get('expires_at_utc') and
                datetime.fromisoformat(vip['expires_at_utc']).timestamp() <= time.time())
            snapshots[config['name']] = vip
            shared[config['name']] = {key: vip.get(key) for key in ('state', 'checked_at_utc', 'expires_at_utc')}
            shared[config['name']].update(engine_name=config['name'], config_revision=DownloadUtil.pikpak_config_revision(config))
        P.ModelSetting.set('download_pikpak_vip_snapshot', json.dumps(shared))

    @staticmethod
    def _admit_registration(config, consume=True):
        if str(config.get('engine_type', '')).lower() != 'cd2':
            return True
        context = TaskDownload._registration_context
        if context.remaining <= 0:
            return False
        if consume:
            context.remaining -= 1
        return True

    @staticmethod
    def _selected_query(query):
        item_id = FeederUtil.single_download_id()
        return query.filter(ModelDownload.id == item_id) if item_id is not None else query

    @staticmethod
    def _enabled_engine_query(query, disabled_names):
        """LIMIT·수량 집계 전에 공용 판정과 같은 소스 엔진 우선순위로 제외한다."""
        if not disabled_names:
            return query
        source = func.lower(func.coalesce(
            func.nullif(func.trim(ModelDownload.current_engine_name), ''),
            func.nullif(func.trim(ModelDownload.upload_state['download_source']['engine_name'].as_string()), ''),
            func.nullif(func.trim(ModelDownload.upload_state['engine_name'].as_string()), ''), ''))
        return query.filter(~source.in_(disabled_names))

    @staticmethod
    def redownload(item_id):
        """전송 재개를 건너뛰고 첫 엔진에 새 다운로드를 등록할 상태로 되돌린다."""
        from .util_block import DownloadBlock
        owner = 'redownload:' + uuid.uuid4().hex
        lock_name = f'redownload:{item_id}'
        db.session.rollback()
        FeederUtil.init_runtime_locks()
        if not FeederUtil.acquire_runtime_lock(lock_name, owner, stale_after=21600):
            return {'ret': 'fail', 'msg': '이 항목은 이미 처음부터 다시 받기를 준비 중입니다.'}
        prepared = False
        try:
            item = db.session.get(ModelDownload, item_id)
            if not item or ModelDownload.is_blocked(item.infohash, item.magnet):
                return {'ret': 'fail', 'msg': '항목이 없거나 차단된 콘텐츠입니다.'}
            if item.status not in ('pending', 'pending_local_staging', 'downloaded', 'waiting_transporter',
                                   'pending_upload', 'move_failed', 'failed', 'completed', 'cancelled'):
                return {'ret': 'fail', 'msg': '진행 중인 전송이 종료된 뒤 처음부터 다시 받을 수 있습니다.'}
            profile = FeederUtil.get_item_profile(item) or {}
            routing = dict((item.upload_state or {}).get('routing') or {})
            raw_chain = profile.get('priority_chain') if profile or routing.get('profile_name') else item.priority_chain
            chain = []
            seen = set()
            if isinstance(raw_chain, (list, tuple)):
                for engine_name in raw_chain:
                    name = str(engine_name).strip()
                    config = FeederUtil.get_downloader_by_name(name) if name else None
                    if config:
                        name = str(config.get('name') or name).strip()
                    if name and name.lower() not in seen:
                        seen.add(name.lower())
                        chain.append(name)
            old_state = dict(item.upload_state or {})
            snapshot = DownloadBlock.snapshot(item)
            item.status = 'cancelled'
            item.error_message = '처음부터 다시 받기 준비 중: 기존 작업과 엔진 큐를 확인합니다.'
            db.session.commit()
            prepared = True
            if not chain:
                raise ValueError('다운로드 프로필의 엔진 체인이 비어 있거나 유효하지 않습니다. 프로필 설정을 확인해주세요.')
            if not FeederUtil.get_downloader_by_name(chain[0]):
                raise ValueError(f'{chain[0]}: 프로필의 첫 다운로드 엔진 설정을 찾을 수 없습니다.')
            destination = routing.get('destination') if isinstance(routing.get('destination'), dict) else FeederUtil.get_profile_destination(profile, chain[0])
            error = FeederUtil.validate_destination(destination)
            if error:
                raise ValueError(error)
            if snapshot['state'].get('cleanup_error'):
                raise ValueError(snapshot['state']['cleanup_error'])
            cleanup_item = SimpleNamespace(infohash=item.infohash, magnet=item.magnet, title=item.title,
                current_engine_name=item.current_engine_name, engine_task_id=item.engine_task_id,
                priority_chain=chain)
            DownloadActivity.record('처음부터 다시 받기', f'작업 #{item_id} · 기존 작업 종료와 엔진 큐 삭제 확인 중')
            UploadUtil.kill_item_rclone(item_id)
            if old_state.get('worker'):
                RemoteWorker.cancel_snapshot(old_state)
                worker = old_state['worker']
                job = RemoteWorker.client(worker['endpoint']).job(worker['job_id'])
                item = db.session.get(ModelDownload, item_id)
                settled = RemoteWorker.settled_worker(worker, job) if job else dict(worker, settled=True)
                item.upload_state = dict(item.upload_state or {}, worker=settled)
                UploadUtil.settle_usage(item, None, job.get('chargeable_bytes') if job else None)
                db.session.commit()
            else:
                UploadUtil.settle_usage(item, None)
                db.session.commit()
            deferred = list(old_state.get('deferred_remote_cleanup') or [])
            pending_cleanup = DownloadUtil.delete_remote_tasks(cleanup_item, strict=True, verify_absent=True, defer_errors=True) or []
            for entry in pending_cleanup:
                already_known = not entry.get('task_id') and any(
                    str(old.get('engine_name') or '').strip().lower() == str(entry.get('engine_name') or '').strip().lower() and
                    old.get('infohash') == entry.get('infohash') for old in deferred)
                if entry not in deferred and not already_known:
                    deferred.append(entry)
            item = db.session.get(ModelDownload, item_id)
            if deferred:
                item.upload_state = dict(item.upload_state or {}, deferred_remote_cleanup=deferred)
                db.session.commit()
            DownloadBlock.cleanup_temporary(snapshot)
            item = db.session.get(ModelDownload, item_id)
            if not item or ModelDownload.is_blocked(item.infohash, item.magnet):
                return {'ret': 'fail', 'msg': '항목이 삭제되었거나 차단되었습니다. 다시 등록하지 않습니다.'}
            if (item.upload_state or {}).get('worker'):
                item.upload_state = dict(item.upload_state, cleanup_pending=False)
                db.session.commit()
                RemoteWorker.acknowledge(dict(item.upload_state))
            item.priority_chain = chain
            item.current_engine_index = 0
            for field in ('current_engine_name', 'engine_task_id', 'engine_added_time', 'local_path',
                          'gdrive_account', 'completed_time', 'last_status_time', 'remote_missing_since',
                          'last_move_attempt_time', 'error_message', 'transfer_error_type',
                          'source_unavailable_since', 'upload_retry_after'):
                setattr(item, field, None)
            FeederUtil.clear_transfer_state(item)
            item.file_name = item.source_file_name or item.title
            item.file_size = 0
            item.video_manifest = None
            item.destination_type = destination.get('type') or ''
            item.gdrive_upload_path = destination.get('upload_path', '')
            item.gdrive_complete_path = destination.get('complete_path', '')
            item.gdrive_remote_id = destination.get('shared_drive_id') or ''
            item.status = 'pending'
            db.session.commit()
            TaskDownload.failed_ids_in_session.discard(item_id)
            cleanup_message = '원격 정리 보류(계정 복구 후 정리)' if deferred else '원격 정리 완료'
            DownloadActivity.record('처음부터 다시 받기', f'작업 #{item_id} · {cleanup_message} · 첫 엔진 다운로드 대기열 등록')
            return {'ret': 'success', 'msg': f'{cleanup_message}. 임시 파일 정리 후 1순위 엔진 다운로드 대기열에 등록했습니다. 중단 상태라면 개별 실행 또는 작업 재개를 눌러주세요.'}
        except Exception as exc:
            db.session.rollback()
            message = redact_log(str(exc)) if isinstance(exc, (ValueError, WorkerError, RuntimeError)) else '기존 작업 또는 임시 파일 정리가 끝나지 않았습니다. 플랫폼 로그를 확인해주세요.'
            logger.warning('[DownloadRestart] 작업 %s 준비 실패: %s', item_id, message)
            if prepared:
                item = db.session.get(ModelDownload, item_id)
                if item and item.status == 'cancelled':
                    item.error_message = '처음부터 다시 받기 보류: ' + message
                    db.session.commit()
            return {'ret': 'fail', 'msg': message}
        finally:
            FeederUtil.release_runtime_lock(lock_name, owner)

    @staticmethod
    def reserve_single_item(item_id):
        """요청 시 실행 잠금을 예약하여 중복 클릭과 전체 실행을 함께 막는다."""
        control = FeederUtil.get_download_control_state()
        if not control['pause_requested'] or control['running']:
            raise ValueError('전체 작업이 완전히 중단된 상태에서 개별 실행할 수 있습니다.')
        item = db.session.get(ModelDownload, item_id)
        if not item or item.status not in TaskDownload.SINGLE_STATUSES:
            raise ValueError('실행 가능한 대기 항목이 아닙니다. 실패 항목은 먼저 재시도해주세요.')
        if ModelDownload.is_blocked(item.infohash, item.magnet):
            raise ValueError('차단된 항목은 실행할 수 없습니다.')
        if item.status != 'pending' and FeederUtil.item_engine_disabled(item):
            raise ValueError('소스 엔진이 중단된 항목입니다. 엔진을 활성화한 뒤 실행해주세요.')
        db.session.rollback()
        owner = f'single:{item_id}:{uuid.uuid4().hex}'
        FeederUtil.init_runtime_locks()
        if not FeederUtil.acquire_runtime_lock('download_pipeline', owner, stale_after=None):
            raise ValueError('다른 작업이 실행 중입니다. 종료 후 다시 시도해주세요.')
        return owner

    @staticmethod
    def run_single_item(item_id, owner):
        """전체 중단을 유지하고 선택 항목의 현재 단계와 후속 단계만 실행한다."""
        with F.app.app_context():
            acquired = False
            try:
                # 지연 전달된 요청이 새 실행의 잠금을 사용하지 못하게 소유권을 확인한다.
                engine = db.engines.get(P.package_name)
                if engine is None:
                    engine = db.engine
                with engine.connect() as connection:
                    actual_owner = connection.execute(db.text(f"SELECT owner FROM {P.package_name}_runtime_lock WHERE lock_name='download_pipeline'")).scalar()
                if actual_owner != owner:
                    return
                acquired = TaskDownload._run_mutex.acquire(blocking=False)
                if not acquired or not FeederUtil.is_download_paused():
                    return
                item = db.session.get(ModelDownload, item_id)
                if not item or item.status not in TaskDownload.SINGLE_STATUSES or ModelDownload.is_blocked(item.infohash, item.magnet):
                    return
                if item.status != 'pending' and FeederUtil.item_engine_disabled(item):
                    DownloadActivity.record('개별 실행', f'작업 #{item_id} 소스 엔진 중단 · 실행 보류', 'warning')
                    return
                db.session.remove()
                with FeederUtil.single_download_scope(item_id), TaskDownload._registration_scope():
                    TaskDownload._init_pikpak_vip_snapshots()
                    TaskDownload.failed_ids_in_session.clear()
                    DownloadActivity.record('개별 실행', f'작업 #{item_id} 실행 시작 · 전체 중단 유지')
                    TaskDownload.sync_active_items_with_profiles()
                    if not UploadUtil.drain_all_sa_mydrives():
                        DownloadActivity.record('개별 실행', f'작업 #{item_id} 사전 정리 미완료 · 실행 보류', 'warning')
                        return
                    for stage in (TaskDownload.dispatch_pending_downloads, TaskDownload.poll_active_downloads,
                                  TaskDownload.process_local_staging, TaskDownload.route_completed_downloads,
                                  TaskDownload.process_uploads, TaskDownload.retry_move_failed):
                        from .util_block import DownloadBlock
                        if DownloadBlock.stopped(item_id):
                            break
                        current = db.session.get(ModelDownload, item_id)
                        if current and current.status != 'pending' and FeederUtil.item_engine_disabled(current):
                            break
                        stage()
                        db.session.remove()
                    item = db.session.get(ModelDownload, item_id)
                    status = item.status if item else '삭제됨'
                    DownloadActivity.record('개별 실행', f'작업 #{item_id} 실행 종료 · 상태: {status} · 전체 중단 유지')
            except Exception:
                db.session.rollback()
                logger.exception('[DownloadPipeline] 작업 #%s 개별 실행 오류', item_id)
                DownloadActivity.record('개별 실행', f'작업 #{item_id} 실행 오류 · 플랫폼 로그를 확인하세요.', 'error')
            finally:
                FeederUtil.release_runtime_lock('download_pipeline', owner)
                if acquired:
                    TaskDownload._run_mutex.release()
                db.session.remove()

    @staticmethod
    def run_pipeline(manual: bool = False):
        with F.app.app_context():
            if FeederUtil.is_download_paused():
                logger.info('[DownloadPipeline] 사용자 중단 상태이므로 실행을 보류합니다.')
                return
        if not TaskDownload._run_mutex.acquire(blocking=False):
            logger.info("[DownloadPipeline] 이미 실행 중인 다운로드 파이프라인이 있어 중복 실행을 건너뜁니다.")
            return

        owner = uuid.uuid4().hex
        db_lock_acquired = False
        with F.app.app_context():
            try:
                FeederUtil.init_runtime_locks()
                db_lock_acquired = FeederUtil.acquire_runtime_lock('download_pipeline', owner, stale_after=None)
                if not db_lock_acquired:
                    logger.info("[DownloadPipeline] 다른 Celery 워커가 실행 중이어서 중복 실행을 건너뜁니다.")
                    return
                if FeederUtil.is_download_paused():
                    return
                P.ModelSetting.set('download_is_running', 'True')
                P.ModelSetting.set('download_running_start_time', str(int(time.time())))

                with TaskDownload._registration_scope():
                    TaskDownload._init_pikpak_vip_snapshots()
                    TaskDownload._run_pipeline_locked(manual=manual)
            finally:
                if db_lock_acquired:
                    FeederUtil.release_runtime_lock('download_pipeline', owner)
                    P.ModelSetting.set('download_is_running', 'False')
                    P.ModelSetting.set('download_running_start_time', '0')
                TaskDownload._run_mutex.release()
                FeederUtil.db_checkpoint()

    @staticmethod
    def _run_pipeline_locked(manual: bool = False):
        with F.app.app_context():
            progress = [False]
            session_type = None

            def note_progress(session, context):
                from sqlalchemy import inspect as inspect_state
                try:
                    for row in session.new.union(session.deleted).union(session.dirty):
                        if not isinstance(row, ModelDownload):
                            continue
                        if row in session.new or row in session.deleted or any(
                                attribute.history.has_changes() for attribute in inspect_state(row).attrs
                                if attribute.key not in ('updated_time', 'last_status_time', 'error_message',
                                                         'remote_missing_since', 'upload_retry_after')):
                            progress[0] = True
                            break
                except Exception:
                    # 진전 여부가 불명확하면 기존 연쇄 순회를 유지한다.
                    progress[0] = True

            try:
                if FeederUtil.is_download_paused():
                    return
                mode_str = "수동 실행" if manual else "스케쥴러 자동 실행"
                logger.info(f"[DownloadPipeline] 다운로드 파이프라인 가동 ({mode_str})")
                DownloadActivity.record('실행', f'다운로드 작업 시작 · {mode_str}')

                # 현재 큐 현황 집계 및 요약 최우선 출력
                q_counts = {
                    'pending': db.session.query(ModelDownload).filter(ModelDownload.status == 'pending').count(),
                    'downloading': db.session.query(ModelDownload).filter(ModelDownload.status == 'downloading').count(),
                    'staging': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count(),
                    'downloaded': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['downloaded', 'waiting_transporter'])).count(),
                    'relay': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_relay', 'relay_transferring'])).count(),
                    'uploading': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count(),
                    'failed': db.session.query(ModelDownload).filter(ModelDownload.failure_filter()).count(),
                }
                total_active = q_counts['pending'] + q_counts['downloading'] + q_counts['staging'] + q_counts['downloaded'] + q_counts['relay'] + q_counts['uploading']
                logger.info(f"[DownloadPipeline] 현재 큐 요약 (활성 {total_active}건) | 대기:{q_counts['pending']} | 진행:{q_counts['downloading']} | 스테이징:{q_counts['staging']} | 다운완료:{q_counts['downloaded']} | 릴레이:{q_counts['relay']} | 업로드:{q_counts['uploading']} | 실패:{q_counts['failed']}")
                db.session.rollback()

                profiles = FeederUtil.get_download_profiles()
                if not profiles:
                    logger.debug("[DownloadPipeline] 등록된 다운로드 프로필(DOWNLOAD_PROFILES)이 없습니다.")
                    DownloadActivity.record('실행', '등록된 다운로드 프로필이 없어 작업을 종료합니다.')
                    return

                # SA 내 드라이브 잔여 파일을 먼저 복구한 뒤 새 업로드를 시작한다.
                DownloadActivity.record('사전 정리', '새 업로드에 앞서 원격 잔여 파일 복구를 시작합니다.')
                if not UploadUtil.drain_all_sa_mydrives():
                    if not FeederUtil.is_download_paused():
                        logger.warning("[DownloadPipeline] 원격 잔여 파일 복구 미완료: 새 업로드를 보류하고 다음 주기에 재확인합니다.")
                        DownloadActivity.record('사전 정리', '원격 복구가 완료되지 않아 새 업로드를 보류합니다. 상세 원인은 플랫폼 로그를 확인하세요.', 'warning')
                    return

                if FeederUtil.is_download_paused():
                    return

                # 피드 최신 데이터 동기화 및 큐 등록
                TaskDownload.sync_feed_items(profiles)

                if FeederUtil.is_download_paused():
                    return

                # 활성 큐 작업들에 대해 현재 설정된 최신 프로필 동적 동기화
                DownloadActivity.record('프로필 반영', '활성 큐에 최신 다운로드 프로필을 반영합니다.')
                TaskDownload.sync_active_items_with_profiles()
                DownloadActivity.record('프로필 반영', '활성 큐 프로필 반영 완료')

                # 현재 실행 세션 실패 목록 초기화
                TaskDownload.failed_ids_in_session.clear()
                try:
                    session_type = db.session.session_factory.class_
                    event.listen(session_type, 'after_flush', note_progress)
                except Exception:
                    session_type = None
                wait_summaries = set()

                # 단일 스케줄 내 연쇄 관통 루프 (최대 3회 패스)
                max_cascade_passes = 3
                for cascade_pass in range(1, max_cascade_passes + 1):
                    progress[0] = False
                    FeederUtil.cooperate()
                    if FeederUtil.is_download_paused():
                        return
                    pass_start_time = time.time()
                    pending_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending').count()
                    downloading_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloading').count()
                    staging_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count()
                    downloaded_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['downloaded', 'waiting_transporter'])).count()
                    uploading_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count()

                    total_active_before = pending_cnt + downloading_cnt + staging_cnt + downloaded_cnt + uploading_cnt
                    db.session.rollback()
                    if total_active_before == 0 and cascade_pass > 1:
                        break

                    logger.info(f"[DownloadPipeline] === 연쇄 패스 {cascade_pass}/{max_cascade_passes} 시작 (활성 작업: {total_active_before}개) ===")

                    for stage_label, stage in (
                        ('다운로드 작업 등록', TaskDownload.dispatch_pending_downloads if pending_cnt > 0 else None),
                        ('다운로더 상태 확인', TaskDownload.poll_active_downloads),
                        ('로컬 스테이징', TaskDownload.process_local_staging),
                        ('완료 항목 이송', TaskDownload.route_completed_downloads),
                        ('업로드', TaskDownload.process_uploads),
                    ):
                        if FeederUtil.is_download_paused():
                            return
                        if stage is not None:
                            DownloadActivity.record(stage_label, f'처리 시작 · {cascade_pass}/{max_cascade_passes}차 순회')
                            stage_started = time.monotonic()
                            if stage_label == '완료 항목 이송':
                                stage(wait_summaries=wait_summaries)
                            else:
                                stage()
                            DownloadActivity.record(stage_label, f'단계 처리 종료 · {time.monotonic() - stage_started:.1f}초 소요')

                    # 해당 패스에서 실제 상태 전이가 발생했는지 판별
                    staging_after = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count()
                    downloaded_after = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['downloaded', 'waiting_transporter'])).count()
                    uploading_after = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count()
                    db.session.rollback()

                    pass_elapsed = time.time() - pass_start_time
                    logger.info(f"[DownloadPipeline] === 연쇄 패스 {cascade_pass}/{max_cascade_passes} 완료 (소요시간: {pass_elapsed:.1f}초) ===")

                    # 스테이징, 이송, 업로드 등 활성 파이프라인에서 처리할 수량이 더 이상 없으면 즉시 패스 종료
                    if (staging_after + downloaded_after + uploading_after) == 0:
                        break
                    if session_type is not None and not progress[0]:
                        logger.debug('[DownloadPipeline] 새 작업·상태·체크포인트 진전이 없어 연쇄 순회를 종료합니다.')
                        break

                if FeederUtil.is_download_paused():
                    return
                # 이전 실패 항목 재시도
                DownloadActivity.record('이동 복구', '이동 실패 항목을 확인합니다.')
                TaskDownload.retry_move_failed()

                if FeederUtil.is_download_paused():
                    return
                # 대기 중인 원격 릴레이(Colab 등) 작업 점검 및 워커 트리거
                DownloadActivity.record('릴레이', '릴레이 대기 작업을 확인합니다.')
                TaskDownload.check_pending_relays()

                logger.info(f"[DownloadPipeline] 다운로드 파이프라인 주기 완료 ({mode_str})")
                DownloadActivity.record('완료', '다운로드 작업 주기 완료 · 다음 실행을 기다립니다.')

            except Exception as e:
                if isinstance(e, WorkerError) and e.code == 'wait_timeout':
                    cause = getattr(e, 'wait_code', None)
                    reason = {'missing_job': '미정산 원격 작업 기록 없음 · 연결 장애 아님',
                              'unavailable': '워커 연결·서비스 장애', 'invalid_response': '워커 응답 형식 확인 불가',
                              'updating': '워커 업데이트 대기', 'checking': '워커 준비 확인 대기'}.get(cause, '원인 확인 필요')
                    detail = f'code={cause}' if cause in ('missing_job', 'unavailable', 'invalid_response', 'updating', 'checking') else 'code=unknown'
                    if type(e.status) is int and 100 <= e.status <= 599:
                        detail += f', HTTP {e.status}'
                    item_id = getattr(e, 'wait_item_id', None)
                    if type(item_id) is int:
                        detail += f', 작업 #{item_id}'
                    message = f'원격 워커 대기 120초 초과 · 마지막 원인: {reason} ({detail}) · 현재 실행 종료, 다음 스케줄러 주기 재시도'
                    DownloadActivity.record('원격 워커 대기', message, 'warning')
                    logger.warning('[DownloadPipeline] %s', message)
                    return
                DownloadActivity.record('오류', '다운로드 작업 중 오류가 발생했습니다. 상세 원인은 플랫폼 로그를 확인하세요.', 'error')
                logger.error(f"[DownloadPipeline] 파이프라인 처리 중 오류: {e}")
                logger.error(traceback.format_exc())
            finally:
                if session_type is not None:
                    event.remove(session_type, 'after_flush', note_progress)
                try:
                    if FeederUtil.is_download_paused():
                        DownloadActivity.record('중단', '작업 중단 상태입니다. 작업 재개 전까지 새 작업을 시작하지 않습니다.')
                except Exception:
                    pass

    @staticmethod
    def _worker_results(worker, item_ids, max_workers, activity_stage=None, can_assign=None, on_wait=None):
        """실행 슬롯만 배정하고 중단 요청 이후에는 다음 작업을 시작하지 않는다."""
        from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
        selected_id = FeederUtil.single_download_id()

        def guarded_worker(item_id):
            with F.app.app_context(), FeederUtil.single_download_scope(selected_id):
                try:
                    if selected_id is not None and selected_id != item_id:
                        return
                    if not FeederUtil.is_download_paused():
                        return worker(item_id)
                finally:
                    db.session.remove()

        remaining = iter(item_ids)
        finished = 0
        last_report = time.monotonic()
        last_activity_counts = None
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            active = {}
            def active_ids():
                return {identity for future, identity in active.items() if not future.done()}
            exhausted = False
            while active or not exhausted:
                if on_wait is not None:
                    on_wait(active_ids)
                while not exhausted and len(active) < max_workers:
                    if FeederUtil.is_download_paused():
                        exhausted = True
                        break
                    try:
                        item_id = next(remaining)
                    except StopIteration:
                        exhausted = True
                        break
                    if can_assign is not None and not can_assign(item_id):
                        continue
                    active[executor.submit(guarded_worker, item_id)] = item_id
                if not active:
                    break
                done, _ = wait(active, timeout=3, return_when=FIRST_COMPLETED)
                for future in done:
                    finished += 1
                    yield active.pop(future), future
                if activity_stage and (finished == len(item_ids) or time.monotonic() - last_report >= 3):
                    activity_counts = (finished, len(active), len(item_ids) - finished - len(active))
                    if activity_counts != last_activity_counts:
                        DownloadActivity.record(activity_stage, f'처리 종료 {finished}/{len(item_ids)}건 · 진행 중 {activity_counts[1]}건 · 미배정 {activity_counts[2]}건')
                        last_activity_counts = activity_counts
                    last_report = time.monotonic()
        if activity_stage and finished < len(item_ids):
            DownloadActivity.record(activity_stage, f'처리 종료 {finished}/{len(item_ids)}건 · 남은 {len(item_ids) - finished}건은 대기 유지')

    @staticmethod
    def sync_feed_items(profiles: list[dict]):
        """피드(FEEDS) 테이블에 적재된 최신 아이템을 다운로드 큐(ModelDownload)에 동기화"""
        from .model_feed import ModelFeedItem

        start_time = time.time()
        all_feeds = FeederUtil.get_feeds()
        new_items_count = 0
        logger.info(f"[DownloadPipeline] 피드 동기화 및 신규 다운로드 작업 검토 시작 (대상 피드: {len(all_feeds)}개, 프로필: {len(profiles)}개)")
        DownloadActivity.record('피드 검토', f'피드 동기화 시작 · 피드 {len(all_feeds)}개, 프로필 {len(profiles)}개')
        last_activity = time.monotonic()

        for profile_index, profile in enumerate(profiles, 1):
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return
            target_feed_names = profile.get('feeds', [])
            priority_chain = profile.get('priority_chain', [])
            if not priority_chain:
                continue

            destination_cfg = FeederUtil.get_profile_destination(profile, priority_chain[0])
            dest_type = destination_cfg.get('type') or ''

            for feed_index, feed in enumerate(all_feeds, 1):
                FeederUtil.cooperate()
                if FeederUtil.is_download_paused():
                    return
                if time.monotonic() - last_activity >= 3:
                    DownloadActivity.record('피드 검토', f'프로필 {profile_index}/{len(profiles)} · 피드 {feed_index}/{len(all_feeds)} 검토 중 · 신규 {new_items_count}건')
                    last_activity = time.monotonic()
                f_name = feed.get('name')
                if '*' not in target_feed_names and f_name not in target_feed_names:
                    continue

                sync_hours = profile.get('sync_hours')
                if isinstance(sync_hours, str):
                    sync_hours = sync_hours.strip() or None
                if sync_hours is None and profile.get('sync_days') is not None:
                    legacy_sync_days = profile.get('sync_days')
                    if isinstance(legacy_sync_days, str):
                        legacy_sync_days = legacy_sync_days.strip() or None
                    try:
                        sync_hours = int(legacy_sync_days) * 24
                    except (TypeError, ValueError):
                        sync_hours = None

                if sync_hours is None:
                    try:
                        sync_hours = P.ModelSetting.get_int('download_feed_sync_hours')
                    except Exception:
                        try:
                            sync_hours = P.ModelSetting.get_int('download_feed_sync_days') * 24
                        except Exception:
                            sync_hours = 72
                else:
                    try:
                        sync_hours = int(sync_hours)
                    except (TypeError, ValueError):
                        logger.warning(
                            f"[DownloadPipeline] 프로필 '{profile.get('name', '')}'의 "
                            f"sync_hours 값이 유효하지 않아 전역 설정을 사용합니다: {sync_hours!r}"
                        )
                        try:
                            sync_hours = P.ModelSetting.get_int('download_feed_sync_hours')
                        except Exception:
                            sync_hours = 72

                query = db.session.query(ModelFeedItem).filter_by(feed_name=f_name)
                if sync_hours > 0:
                    limit_date = datetime.now() - timedelta(hours=sync_hours)
                    query = query.filter(ModelFeedItem.created_time >= limit_date)

                candidates = query.order_by(ModelFeedItem.id.desc()).all()

                for candidate_index, feed_item in enumerate(candidates, 1):
                    FeederUtil.cooperate()
                    if FeederUtil.is_download_paused():
                        return
                    if time.monotonic() - last_activity >= 3:
                        DownloadActivity.record('피드 검토', f'프로필 {profile_index}/{len(profiles)} · 피드 {feed_index}/{len(all_feeds)} · 항목 {candidate_index}/{len(candidates)} · 신규 {new_items_count}건')
                        last_activity = time.monotonic()
                    feed_dict = feed_item.as_dict()
                    magnets = feed_dict.get('magnet', [])
                    if not magnets:
                        continue

                    # 마그넷(BTIH) 우선 선별, 부재 시 ed2k 채택
                    target_mag = None
                    for m in magnets:
                        FeederUtil.cooperate()
                        if str(m).lower().startswith('magnet:'):
                            target_mag = m
                            break
                    if not target_mag:
                        target_mag = magnets[0]

                    infohash = FeederUtil.extract_info_hash(target_mag)
                    if ModelDownload.is_blocked(infohash, target_mag):
                        continue

                    existing = None
                    if infohash:
                        existing = ModelDownload.get_by_infohash(infohash)
                    if not existing:
                        existing = ModelDownload.get_by_magnet(target_mag)
                    if existing:
                        continue

                    dl_item = ModelDownload(
                        feed_name=f_name,
                        title=feed_item.title,
                        magnet=target_mag,
                        infohash=infohash
                    )
                    dl_item.priority_chain = priority_chain
                    dl_item.current_engine_index = 0
                    dl_item.destination_type = dest_type
                    if not dest_type:
                        dl_item.error_message = '엔진 기본 목적지 또는 엔진별 오버라이드를 설정해주세요.'
                    dl_item.gdrive_upload_path = destination_cfg.get('upload_path', '')
                    dl_item.gdrive_complete_path = destination_cfg.get('complete_path', '')
                    dl_item.gdrive_remote_id = destination_cfg.get('shared_drive_id', '')

                    db.session.add(dl_item)
                    db.session.commit()
                    new_items_count += 1

        elapsed = time.time() - start_time
        DownloadActivity.record('피드 검토', f'피드 동기화 완료 · 신규 {new_items_count}건 · {elapsed:.1f}초 소요')
        if new_items_count > 0:
            logger.info(f"[DownloadPipeline] 피드 동기화 완료: 신규 {new_items_count}건 큐 등록 (소요시간: {elapsed:.1f}초)")

    @staticmethod
    def sync_active_items_with_profiles():
        """활성 큐 작업들에 대해 현재 설정된 최신 프로필(목적지, 경로, 우선순위체인)을 동적으로 반영하고 상태를 리라우팅"""
        from .util_block import DownloadBlock
        active_statuses = [
            'pending', 'downloading', 'pending_local_staging', 'local_staging',
            'downloaded', 'waiting_transporter', 'pending_upload', 'uploading', 'pending_relay', 'relay_transferring'
        ]
        items = TaskDownload._selected_query(db.session.query(ModelDownload)).filter(ModelDownload.status.in_(active_statuses)).all()
        if not items:
            return

        start_time = time.time()
        logger.info(f"[DownloadPipeline] 활성 큐 {len(items)}건 최신 프로필 동적 동기화 시작")

        # 이 함수 호출에서만 설정을 재사용하며 이름이 중복되면 첫 엔진을 유지한다.
        all_profiles = FeederUtil.get_download_profiles()
        all_downloaders = FeederUtil.get_downloaders()
        disabled_engines = FeederUtil.disabled_engine_names(all_downloaders)
        downloaders_by_name = {}
        for downloader in all_downloaders:
            downloaders_by_name.setdefault(str(downloader.get('name', '')).strip().lower(), downloader)
        global_staging_opt = P.ModelSetting.get_bool('download_use_local_staging') if P.ModelSetting else True
        shared_drive_default_id = P.ModelSetting.get('download_shared_drive_id') or ''

        updated_count = 0
        for item in DownloadActivity.track('프로필 반영', items):
            # 명시된 프로필명이 없을 때만 기존 순서대로 피드를 매칭한다.
            FeederUtil.cooperate()
            if FeederUtil.item_engine_disabled(item, disabled_engines):
                continue
            if FeederUtil.transfer_destination_locked(item) or item.status == 'local_staging':
                # 저장한 원격 작업이 끝나기 전에는 프로필 변경으로 소유권을 옮기지 않는다.
                continue
            routing = (item.upload_state or {}).get('routing') or {}
            profile = None
            if 'profile_name' in routing:
                profile = (next((p for p in all_profiles if p.get('name') == routing['profile_name']), {})
                           if routing['profile_name'] else {'priority_chain': item.priority_chain or []})
            target_feed = str(item.feed_name if routing else (item.feed_name or '')).strip().lower()
            for p in all_profiles if profile is None else []:
                FeederUtil.cooperate()
                p_feeds = [str(f).strip().lower() for f in p.get('feeds', [])]
                if target_feed in p_feeds or '*' in p_feeds:
                    profile = p
                    break

            if not profile:
                continue

            # 현재 실행 중인 엔진 또는 현재 우선순위 체인 순번의 엔진 결정
            effective_engine = item.current_engine_name
            if not effective_engine and item.priority_chain:
                chain_idx = item.current_engine_index or 0
                if chain_idx < len(item.priority_chain):
                    effective_engine = item.priority_chain[chain_idx]

            engine_cfg = downloaders_by_name.get(str(effective_engine).strip().lower(), {})
            destination_engine = effective_engine
            if not destination_engine:
                # 목적지의 프로필 체인 fallback과 기존 스테이징 엔진 해석을 각각 보존한다.
                chain = item.priority_chain or profile.get('priority_chain') or []
                index = item.current_engine_index or 0
                destination_engine = item.current_engine_name or (chain[index] if 0 <= index < len(chain) else '')
            destination_engine_cfg = downloaders_by_name.get(str(destination_engine).strip().lower(), {})
            # 기본 목적지보다 엔진별 지정 목적지(destination_by_engine)를 최우선 적용
            dest_cfg = FeederUtil.get_profile_destination(profile, effective_engine, item, engine_config=destination_engine_cfg)
            current_dest_type = dest_cfg.get('type') or ''
            current_chain = routing.get('priority_chain', profile.get('priority_chain', []))

            # 우선순위 체인 동적 동기화
            if item.status == 'pending' and current_chain and item.priority_chain != current_chain:
                item.priority_chain = current_chain

            # 목적지 세부 설정 동적 동기화
            dest_changed = (item.destination_type != current_dest_type)
            old_route = (item.destination_type, item.gdrive_upload_path, item.gdrive_complete_path, item.gdrive_remote_id)
            item.destination_type = current_dest_type
            if item.status != 'downloading':
                item.gdrive_upload_path = dest_cfg.get('upload_path', '')
            item.gdrive_complete_path = dest_cfg.get('complete_path', '')
            item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or shared_drive_default_id
            if old_route != (item.destination_type, item.gdrive_upload_path, item.gdrive_complete_path, item.gdrive_remote_id):
                updated_count += 1

            if not current_dest_type:
                message = '엔진 기본 목적지 또는 엔진별 오버라이드를 설정해주세요.'
                if item.error_message != message:
                    item.error_message = message
                    updated_count += 1
                continue
            transporter = DownloadUtil.get_transporter(current_dest_type)
            is_relay_dest = bool(transporter and getattr(transporter, 'IS_RELAY_HANDLER', False))

            # 원격 경로 판별 시 불필요한 os.path.exists 호출을 생략하고 문자열로 고속 판별
            src_path = item.local_path or ''
            is_remote_src = bool(src_path and (':' in src_path or src_path.startswith(('http://', 'https://'))))

            # 로컬 스테이징 사용 여부 계층 판별
            profile_settings = dict(profile)
            profile_settings.update(dest_cfg)
            use_local_staging = FeederUtil.resolve_setting(
                'use_local_staging',
                profile_settings,
                engine_cfg,
                global_staging_opt,
                True
            )
            use_local_staging = str(use_local_staging).lower() in ['true', 'on', '1']
            if transporter and getattr(transporter, 'FORCE_REMOTE_SOURCE', False):
                use_local_staging = False

            # 목적지 변경 또는 스테이징 옵션(On/Off) 변경에 따른 동적 상태 리라우팅
            if not is_relay_dest and item.status in ['pending_relay', 'relay_transferring']:
                if is_remote_src and use_local_staging:
                    item.status = 'pending_local_staging'
                    logger.info(f"[DownloadPipeline] 프로필 변경 감지: 최신 목적지({current_dest_type}, 로컬스테이징)에 맞춰 로컬 스테이징(pending_local_staging)으로 리라우팅 -> {item.title}")
                else:
                    item.status = 'downloaded'
                    logger.info(f"[DownloadPipeline] 프로필 변경 감지: 최신 목적지({current_dest_type}, 다이렉트)에 맞춰 전송 대기(downloaded)로 리라우팅 -> {item.title}")
                updated_count += 1

            elif not is_relay_dest and is_remote_src and not use_local_staging and item.status in ['pending_local_staging', 'local_staging']:
                item.status = 'downloaded'
                logger.info(f"[DownloadPipeline] 설정 변경 감지: 로컬 스테이징 해제 -> 다이렉트 전송 대기(downloaded)로 리라우팅 -> {item.title}")
                updated_count += 1

            elif not is_relay_dest and is_remote_src and use_local_staging and item.status == 'downloaded' and not os.path.exists(src_path):
                item.status = 'pending_local_staging'
                logger.info(f"[DownloadPipeline] 설정 변경 감지: 로컬 스테이징 적용 -> 로컬 스테이징(pending_local_staging)으로 리라우팅 -> {item.title}")
                updated_count += 1

            elif is_relay_dest and item.status in ['pending_local_staging', 'downloaded', 'waiting_transporter', 'pending_upload']:
                item.status = 'pending_relay'
                logger.info(f"[DownloadPipeline] 프로필 변경 감지: 최신 목적지({current_dest_type})에 맞춰 원격 릴레이 대기(pending_relay)로 리라우팅 -> {item.title}")
                updated_count += 1

            elif dest_changed:
                updated_count += 1

        if updated_count > 0:
            for item in list(db.session.dirty):
                if isinstance(item, ModelDownload) and DownloadBlock.stopped(item.id):
                    db.session.expire(item)
            db.session.commit()

        elapsed = time.time() - start_time
        if updated_count > 0:
            logger.info(f"[DownloadPipeline] 활성 큐 {len(items)}건 프로필 동적 동기화 완료 (갱신: {updated_count}건, 소요시간: {elapsed:.1f}초)")

    @staticmethod
    def _engine_retry_text(message):
        message = re.sub(r'(?i)(?:magnet:\?|ed2k://)[^\s]+', '[링크 숨김]', str(message))
        message = re.sub(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?::[0-9]+)?\b', '[주소 숨김]', message)
        message = re.sub(r'\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b', '[JWT 숨김]', message)
        return redact_log(message, limit=700)

    @staticmethod
    def _engine_retry_record(item, retry, message):
        # Assign a new JSON value so SQLAlchemy persists nested checkpoint changes.
        retry = dict(retry, last_message=TaskDownload._engine_retry_text(message))
        item.upload_state = dict(item.upload_state or {}, engine_retry=retry)
        item.error_message = retry['last_message']
        db.session.commit()
        logger.warning('[EngineRetry] %s', item.error_message)

    @staticmethod
    def _engine_chain_failure(item):
        retry = (item.upload_state or {}).get('engine_retry') or {}
        reasons = list(retry.get('failures') or [])
        last = item.error_message or retry.get('last_message')
        if last and last not in reasons:
            reasons.append(last)
        return '모든 다운로더 우선순위 체인 소진' + (' · 마지막 실패: ' + TaskDownload._engine_retry_text(' | '.join(reversed(reasons[-8:]))) if reasons else '')

    @staticmethod
    def _retry_engine_error(item_id, engine, config, remote, complete=True):
        with TaskDownload._registration_scope():
            return TaskDownload._retry_engine_error_scoped(item_id, engine, config, remote, complete)

    @staticmethod
    def _retry_engine_error_scoped(item_id, engine, config, remote, complete=True):
        """Persisted, bounded error recovery. Unknown remote outcomes always reconcile first."""
        from .util_block import DownloadBlock

        expected_tid = None
        expected_status = None

        def current():
            if FeederUtil.is_download_paused() or DownloadBlock.stopped(item_id):
                db.session.rollback()
                return None
            row = db.session.query(ModelDownload).populate_existing().filter(
                ModelDownload.id == item_id, ModelDownload.status.in_(['pending', 'downloading'])).first()
            if not row or (row.current_engine_index or 0) != index:
                return None
            chain = row.priority_chain or []
            if index >= len(chain) or chain[index] != name or (row.infohash, row.magnet) != expected_source:
                return None
            if expected_status is not None and (row.status != expected_status or row.engine_task_id != expected_tid):
                return None
            if any((row.upload_state or {}).get(key) for key in ('worker', 'destination_snapshot', 'phase')):
                return None
            if row.current_engine_name and row.current_engine_name != name:
                return None
            return row

        item = db.session.get(ModelDownload, item_id)
        if not item:
            return
        name = config['name']
        if remote and FeederUtil.engine_auth_error(engine, remote.get('error') or remote.get('error_message'), 'task_status'):
            return
        if not (FeederUtil.get_downloader_by_name(name) or {}).get('enabled', True):
            return
        index = item.current_engine_index or 0
        expected_tid, expected_status = item.engine_task_id, item.status
        expected_source = (item.infohash, item.magnet)
        # Transfer/worker checkpoints own their source; never clean them on engine error.
        if any((item.upload_state or {}).get(key) for key in ('worker', 'destination_snapshot', 'phase')):
            return
        item = current()
        if not item or not complete:
            return
        state = item.upload_state or {}
        old = state.get('engine_retry') or {}
        retry = dict(old) if old.get('engine') == name and old.get('index') == index else {
            'engine': name, 'index': index, 'used': 0, 'failures': list(old.get('failures') or [])[-7:]}
        now = datetime.now().timestamp()
        if now < retry.get('next_at', 0):
            return
        # Release poll metadata writes before a potentially slow remote RPC.
        db.session.commit()
        # Cached dispatch/poll rows are insufficient for destructive recovery.
        # A fresh complete list also detects duplicate hashes and accepted timeout RPCs.
        try:
            rows, status_error = engine.get_status()
            if FeederUtil.engine_auth_error(engine, status_error, 'get_status'):
                return
        except Exception:
            rows, status_error = [], '조회 실패'
        item = current()
        if not item:
            return
        matches = [r for r in rows if
                   (item.engine_task_id and str(r.get('task_id') or r.get('id') or '') == str(item.engine_task_id)) or
                   (item.infohash and str(r.get('hash') or '').lower() == item.infohash.lower())]
        if status_error or not getattr(engine, 'status_list_complete', True) or len(matches) > 1:
            retry['next_at'] = now + 60
            TaskDownload._engine_retry_record(item, retry,
                f'[{name}] 원격 목록 미확인/작업 중복 · 정리 확인 필요, 재등록 보류')
            return
        remote = matches[0] if matches else None
        stage = retry.get('stage')
        used = retry.get('used', 0)
        tid = str((remote or {}).get('task_id') or (remote or {}).get('id') or item.engine_task_id or '')
        if (remote and item.engine_task_id and tid != str(item.engine_task_id) and stage != 'uncertain'):
            retry['next_at'] = now + 60
            TaskDownload._engine_retry_record(item, retry,
                f'[{name}] 원격 작업 ID 불일치 · 소유권 확인 필요, 정리/재등록 보류')
            return
        if remote and remote.get('status') != 'error':
            item.status = 'downloading'
            item.current_engine_name = name
            item.engine_task_id = tid
            if retry.get('upload_path') is not None:
                item.gdrive_upload_path = retry['upload_path']
                item.upload_state = dict(item.upload_state or {}, download_source={
                    'engine_name': name, 'task_id': tid, 'upload_path': retry['upload_path']})
            retry.update(stage='monitoring', next_at=0)
            TaskDownload._engine_retry_record(item, retry, f'[{name}] 기존 작업 재연동 · 자동 재시도 {used}/2')
            return
        if remote:
            item.current_engine_name = name
            item.engine_task_id = tid
            expected_tid = tid
            code = remote.get('statusCode', remote.get('state', 'error'))
            if config.get('engine_type') == 'cd2':
                cause = 'CD2 ERROR(3), 상세 실패 사유 미제공'
            else:
                detail = remote.get('error_message') or remote.get('error') or remote.get('message') or ''
                cause = f'ERROR({code})' + (f', {redact_log(str(detail))[:300]}' if detail else ', 상세 실패 사유 미제공')
            cause = TaskDownload._engine_retry_text(cause)
            retry['cause'] = cause
            retry['remote_code'] = code if isinstance(code, (int, float)) else TaskDownload._engine_retry_text(code)
        cause = retry.get('cause', '원격 오류, 상세 실패 사유 미제공')

        def record(message):
            TaskDownload._engine_retry_record(item, retry, f'[{name}] {cause} · {message}')

        if current() is None:
            return
        other_owner = db.session.query(ModelDownload.id).filter(
            ModelDownload.id != item_id,
            ModelDownload.status.notin_(['failed', 'cancelled', 'blocked']),
            or_(and_(ModelDownload.current_engine_name == name, ModelDownload.engine_task_id == tid),
                and_(ModelDownload.infohash == item.infohash, ModelDownload.status.in_([
                    'completed', 'downloaded', 'pending_local_staging', 'local_staging',
                    'waiting_transporter', 'pending_upload', 'uploading', 'move_failed'])))).first()
        if other_owner:
            retry['next_at'] = now + 60
            record('다른 작업의 원본 소유권 확인 필요 · 정리/재등록 보류')
            return
        if stage == 'uncertain' and not remote and used >= 2 and retry.get('transient'):
            retry['next_at'] = now + 60
            record(f'자동 재시도 {used}/2 · 재등록 접수 확인 필요 · 연결 오류 대기, 중복 등록/폴백 보류')
            return
        if stage not in ('scheduled', 'cleanup'):
            if used < 2:
                delay = (60, 180)[used]
                retry.update(stage='scheduled', next_at=now + delay)
                record(f'동일 엔진 재시도 {used + 1}/2 예약 ({delay}초 후)')
                return
            retry.update(stage='cleanup', exhausting=True)
            stage = 'cleanup'
        if retry['stage'] == 'scheduled':
            limit = DownloadUtil.get_queue_limit(config)
            if limit > 0 and sum(r.get('status') == 'downloading' for r in rows) >= limit:
                retry['next_at'] = now + 60
                record(f'자동 재시도 {used}/2 · 큐 포화 대기')
                return
            used += 1
            retry.update(used=used, stage='cleanup', next_at=now + 60)
            item.current_engine_name = name
            item.engine_task_id = tid or item.engine_task_id
            expected_tid = item.engine_task_id
            record(f'자동 재시도 {used}/2 시작')
            # Save before RPC; an interrupted process reconciles rather than repeating restart.
            if remote and tid and config.get('engine_type') != 'cd2' and hasattr(engine, 'restart_task'):
                try:
                    restarted = engine.restart_task(tid)
                except Exception:
                    restarted = False
                item = current()
                if not item:
                    return
                if restarted:
                    item.status = 'downloading'
                    retry.update(stage='monitoring', next_at=0)
                    record(f'자동 재시도 {used}/2 · 원격 재시작 요청 성공')
                    return
                record(f'자동 재시도 {used}/2 · 원격 재시작 불가/응답 미확인, 상태 재확인')
                # Re-enter next cycle with a fresh complete list before deleting anything.
                return
        # Only the matched error history may be removed. Verify a complete fresh list.
        if remote and tid:
            retry['next_at'] = now + 60
            record(f'자동 재시도 {used}/2 · 오류 이력 정리 확인 중')
            if current() is None:
                return
            try:
                deleted = engine.delete_task(tid)
            except Exception:
                deleted = False
            item = current()
            if not item:
                return
            if deleted is not True:
                retry.update(stage='cleanup', next_at=now + 60)
                record(f'자동 재시도 {used}/2 · 오류 이력 정리 실패 · 정리 확인 필요, 재등록 보류')
                return
            try:
                rows, err = engine.get_status()
                if FeederUtil.engine_auth_error(engine, err, 'get_status'):
                    return
            except Exception:
                rows, err = [], '조회 실패'
            item = current()
            if not item:
                return
            matches = [r for r in rows if str(r.get('task_id') or r.get('id') or '') == tid or
                       (item.infohash and str(r.get('hash') or '').lower() == item.infohash.lower())]
            if err or not getattr(engine, 'status_list_complete', True) or matches:
                retry.update(stage='cleanup', next_at=now + 60)
                record(f'자동 재시도 {used}/2 · 오류 이력 삭제 결과 미확인 · 정리 확인 필요')
                return
        if retry.get('exhausting'):
            message = f'[{name}] {cause}, 자동 재시도 {used}/2 소진'
            retry.update(stage='exhausted', next_at=0,
                         failures=(list(retry.get('failures') or []) + [message])[-8:])
            item.current_engine_index = index + 1
            item.status = 'pending'
            item.engine_task_id = None
            item.current_engine_name = None
            record(f'자동 재시도 {used}/2 소진 → ' + ('다음 엔진' if index + 1 < len(item.priority_chain or []) else '다음 엔진 없음'))
            return
        profile = FeederUtil.get_item_profile(item) or {}
        dest = FeederUtil.get_profile_destination(profile, name, item)
        error = FeederUtil.validate_destination(dest)
        if error:
            retry.update(stage='cleanup', next_at=now + 60)
            record('목적지 설정 확인 필요 · 재등록 보류')
            return
        try:
            upload_path = FeederUtil.get_download_subpath(profile, name, item)
        except ValueError:
            retry.update(stage='cleanup', next_at=now + 60)
            record('목적지 경로 확인 필요 · 재등록 보류')
            return
        if not TaskDownload._admit_registration(config, consume=False):
            return
        retry.update(stage='uncertain', next_at=now + 60, upload_path=upload_path)
        item.status = 'pending'
        expected_status = 'pending'
        record(f'자동 재시도 {used}/2 · 재등록 접수 확인 중')
        if current() is None:
            return
        try:
            if not TaskDownload._admit_registration(config):
                return
            success, new_tid, err = DownloadUtil.add_magnet(engine, item.magnet, title=item.title, upload_path=upload_path)
            if not success and FeederUtil.engine_auth_error(engine, err, 'add_magnet'):
                return
        except Exception:
            success, new_tid, err = False, None, '재등록 응답 미확인'
        item = current()
        if not item:
            return
        if not success or not new_tid:
            retry['transient'] = any(word in str(err).lower() for word in ('timeout', 'timed out', 'connection', 'connect', 'unavailable', '통신', '연결', '미확인'))
            if any(word in str(err).lower() for word in ('queue full', 'maximum allowed', 'too many', 'active magnets', 'limit', 'trial')):
                retry.update(used=max(0, used - 1), stage='scheduled', next_at=now + 60)
            # No repeated add until a complete subsequent list has reconciled acceptance.
            record(f'자동 재시도 {retry["used"]}/2 · 재등록 거부/접수 미확인: {redact_log(str(err))[:300]} · 원격 목록 재확인 대기')
            return
        item.status = 'downloading'
        item.engine_task_id = str(new_tid)
        item.current_engine_name = name
        item.engine_added_time = datetime.now()
        item.remote_missing_since = None
        item.destination_type = dest.get('type') or ''
        item.gdrive_upload_path = upload_path
        item.gdrive_complete_path = dest.get('complete_path', '')
        item.gdrive_remote_id = dest.get('shared_drive_id') or ''
        item.upload_state = dict(item.upload_state or {}, download_source={
            'engine_name': name, 'task_id': str(new_tid), 'upload_path': upload_path})
        retry.update(stage='monitoring', next_at=0)
        record(f'자동 재시도 {used}/2 · 동일 엔진 재등록 성공')

    @staticmethod
    def dispatch_pending_downloads():
        with TaskDownload._registration_scope():
            if FeederUtil.is_download_paused():
                return
            TaskDownload._init_pikpak_vip_snapshots()
            return TaskDownload._dispatch_pending_downloads_scoped()

    @staticmethod
    def _clear_registration_wait(item):
        state = dict(item.upload_state or {})
        state.pop('registration_wait', None)
        item.upload_state = state

    @staticmethod
    def _registration_wait(item, config, error, uncertain=False):
        """등록 확인 대기를 기록하고 PikPak 다운로드 타이머와 분리한다."""
        now = datetime.now()
        engine_name = config.get('name')
        index = item.current_engine_index or 0
        previous = (item.upload_state or {}).get('registration_wait') or {}
        if previous.get('engine') != engine_name or previous.get('index') != index:
            previous = {}
        first = datetime.fromisoformat(previous['first_failed_at']) if previous else now
        uncertain = bool(uncertain or previous.get('uncertain'))
        wait = dict(previous, engine=engine_name, index=index,
                    first_failed_at=first.isoformat(), next_check_at=(now + timedelta(seconds=60)).isoformat(),
                    uncertain=uncertain, last_error=DownloadUtil.engine_error_detail(None, error, limit=700))
        item.upload_state = dict(item.upload_state or {}, registration_wait=wait)
        hours = max(0, int(config.get('stalled_timeout_hours', 24)))
        elapsed = max(0, (now - first).total_seconds())
        limit_text = f'{hours}시간' if hours else '무제한'
        reason = '접수 여부 불명 · 원격 ID/hash 수동 확인 필요 · 재등록/폴백 보류' if uncertain else '현재 엔진 재연결 대기'
        if config.get('engine_type') == 'pikpak':
            item.error_message = f'[{engine_name}] {reason} · 다음 주기 재확인: {wait["last_error"]}'
        else:
            item.error_message = f'[{engine_name}] {reason} · 경과 {int(elapsed // 60)}분 / 제한 {limit_text} · 다음 주기 재확인: {wait["last_error"]}'
        db.session.commit()
        return bool(config.get('engine_type') != 'pikpak' and hours and elapsed >= hours * 3600 and not uncertain)

    @staticmethod
    def _registration_fallback(item, engine_name, error):
        profile = FeederUtil.get_item_profile(item)
        item.current_engine_index = (item.current_engine_index or 0) + 1
        item.status = 'pending'
        item.engine_task_id = None
        chain = item.priority_chain or []
        if item.current_engine_index < len(chain):
            next_dest = FeederUtil.get_profile_destination(profile, chain[item.current_engine_index], item)
            item.destination_type = next_dest.get('type') or ''
        TaskDownload._clear_registration_wait(item)
        item.error_message = f'[{engine_name}] 추가 실패 ({TaskDownload._engine_retry_text(error)}) -> 다음 엔진 폴백'
        db.session.commit()
        logger.warning('[DownloadDispatch] 작업 #%s %s', item.id, item.error_message)

    @staticmethod
    def _dispatch_pending_downloads_scoped():
        from .util_block import DownloadBlock

        def pending_item(item_id):
            if DownloadBlock.stopped(item_id):
                db.session.rollback()
                return None
            return db.session.query(ModelDownload).populate_existing().filter_by(id=item_id, status='pending').first()

        items = TaskDownload._selected_query(db.session.query(ModelDownload)).filter_by(status='pending').with_entities(
            ModelDownload.id, ModelDownload.priority_chain, ModelDownload.current_engine_index, ModelDownload.magnet,
            ModelDownload.title, ModelDownload.feed_name, ModelDownload.gdrive_upload_path).order_by(ModelDownload.id.asc()).all()
        if not items:
            return
        total_items = len(items)
        registered_count = linked_count = 0
        DownloadActivity.record('다운로드 작업 등록', f'등록 검토 시작 · 대상 {total_items}건')
        pending_snapshots = []
        for item in items:
            FeederUtil.cooperate()
            pending_snapshots.append({
                'id': item.id,
                'priority_chain': list(item.priority_chain or []),
                'current_engine_index': item.current_engine_index or 0,
                'magnet': item.magnet,
                'title': item.title,
                'feed_name': item.feed_name,
                'gdrive_upload_path': item.gdrive_upload_path,
                'target_hash': (FeederUtil.extract_info_hash(item.magnet) or '').lower(),
            })
        db.session.rollback()

        # 다운로더 엔진별 실제 원격 진행 중 큐(In Progress: 다운로딩 + 대기열 전체) 사전 집계
        engine_in_progress_counts = {}
        full_engines = set()
        engine_instances = {}
        engine_status_maps = {}
        engine_errors = {}

        # Invocation-local lookup: stat is cheap even for large pending queues.
        # Capture the signature before reading so a concurrent edit during the read
        # is detected on the next lookup. No global/TTL YAML cache is involved.
        downloader_map = {}
        config_signature = None

        def refresh_downloaders(force=False):
            nonlocal downloader_map, config_signature
            try:
                stat = os.stat(FeederUtil.get_filepath())
                signature = (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size)
            except OSError:
                signature = None
            if force or signature != config_signature:
                config_signature = signature
                downloaders = FeederUtil.get_downloaders()
                downloader_map = {}
                for config in downloaders:
                    downloader_map.setdefault(str(config.get('name', '')).strip().lower(), config)
                return downloaders

        def downloader_by_name(name):
            refresh_downloaders()
            return downloader_map.get(str(name).strip().lower())

        def next_available_engine(chain, index, link):
            protocol = 'ed2k' if str(link).lower().startswith('ed2k://') else 'magnet'
            for next_index in range(index + 1, len(chain)):
                candidate = downloader_by_name(chain[next_index])
                if candidate and candidate.get('enabled', True):
                    candidate_engine = engine_instances.get(chain[next_index]) or DownloadUtil.create_engine(candidate)
                    if candidate_engine and protocol in getattr(candidate_engine, 'SUPPORTED_PROTOCOLS', ['magnet']):
                        engine_instances[chain[next_index]] = candidate_engine
                        return next_index, chain[next_index], candidate
            return None

        downloaders = refresh_downloaders(force=True)
        for engine_index, dl in enumerate(downloaders, 1):
            FeederUtil.cooperate()
            e_name = dl.get('name')
            if not e_name or not dl.get('enabled', True):
                continue

            e_limit = DownloadUtil.get_queue_limit(dl)

            engine_inst = DownloadUtil.create_engine(dl)
            if not engine_inst:
                continue
            engine_instances[e_name] = engine_inst

            # 모든 엔진 공통: 원격 작업 목록을 1회 사전 조회하여 infohash 맵 구성
            if hasattr(engine_inst, 'get_status'):
                try:
                    DownloadActivity.record('다운로드 작업 등록', f'엔진 목록 사전 조회 {engine_index}/{len(downloaders)} · 항목 검토 0/{total_items}건')
                    remote_tasks, status_error = engine_inst.get_status()
                    if FeederUtil.engine_auth_error(engine_inst, status_error, 'get_status'):
                        refresh_downloaders(force=True)
                        continue
                    if status_error:
                        engine_errors[e_name] = DownloadUtil.log_engine_error(engine_inst, 'get_status · 신규 등록 대기', status_error)
                    if not status_error:
                        task_map = {}
                        for t in remote_tasks:
                            FeederUtil.cooperate()
                            h = (t.get('hash') or '').lower()
                            if h:
                                task_map[h] = t
                        engine_status_maps[e_name] = task_map

                        in_prog_cnt = sum(1 for t in remote_tasks if t.get('status') == 'downloading')
                        engine_in_progress_counts[e_name] = in_prog_cnt

                        if e_limit > 0 and in_prog_cnt >= e_limit:
                            full_engines.add(e_name)
                            logger.info(f"[DownloadDispatch] [{e_name}] 원격 진행중 큐 한도({e_limit}건) 도달 (현재 In Progress: {in_prog_cnt}건) -> 신규 추가 일시 대기")
                except Exception as chk_err:
                    engine_errors[e_name] = DownloadUtil.log_engine_error(engine_inst, 'get_status · 신규 등록 대기',
                        f'{type(chk_err).__name__}: {chk_err}')

        # 원격 조회가 불가했던 엔진용 로컬 DB 백업 집계
        if any(name not in engine_in_progress_counts for name in engine_instances):
            active_counts_query = (
                db.session.query(ModelDownload.current_engine_name, func.count(ModelDownload.id))
                .filter_by(status='downloading')
                .group_by(ModelDownload.current_engine_name)
                .all()
            )
            for r in active_counts_query:
                FeederUtil.cooperate()
                if r[0] not in engine_in_progress_counts:
                    engine_in_progress_counts[r[0]] = int(r[1])
            db.session.rollback()

        last_activity = time.monotonic()
        capacity_waiting = 0
        cd2_waiting = 0
        for item_index, snapshot in enumerate(pending_snapshots, 1):
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return
            chain = snapshot['priority_chain']
            curr_idx = snapshot['current_engine_index']
            if curr_idx < len(chain):
                name = chain[curr_idx]
                config = downloader_by_name(name) or {}
                cached_engine = engine_instances.get(name)
                limit = DownloadUtil.get_queue_limit(config)
                protocol = 'ed2k' if str(snapshot['magnet']).lower().startswith('ed2k://') else 'magnet'
                if (config.get('enabled', True) and cached_engine and
                        getattr(TaskDownload._registration_context, 'vip_snapshots', {}).get(name, {}).get('state') not in ('not_vip', 'cutoff') and
                        (name in full_engines or (limit > 0 and engine_in_progress_counts.get(name, 0) >= limit)) and
                        protocol in getattr(cached_engine, 'SUPPORTED_PROTOCOLS', ['magnet']) and
                        not engine_status_maps.get(name, {}).get(snapshot['target_hash'])):
                    capacity_waiting += 1
                    continue
            if item_index == 1 or time.monotonic() - last_activity >= 3:
                DownloadActivity.record('다운로드 작업 등록', f'검토 {item_index - 1}/{total_items}건 · 신규 등록 {registered_count}건 · 기존 연동 {linked_count}건 · 현재 작업 #{snapshot["id"]}')
                last_activity = time.monotonic()
            item_id = snapshot['id']
            protected = pending_item(item_id)
            if protected is None:
                continue
            protected_state = protected.upload_state or {}
            source_owned = bool(protected.engine_task_id or protected.local_path or
                    (protected_state.get('download_source') or {}).get('task_id') or
                    any(protected_state.get(key) for key in ('worker', 'destination_snapshot', 'phase')))
            chain = snapshot['priority_chain']
            curr_idx = snapshot['current_engine_index']

            if curr_idx >= len(chain):
                if source_owned:
                    continue
                item = pending_item(item_id)
                if not item:
                    continue
                item.status = 'failed'
                item.error_message = TaskDownload._engine_chain_failure(item)
                db.session.commit()
                logger.warning('[DownloadDispatch] %s', item.error_message)
                continue

            engine_name = chain[curr_idx]

            downloader_cfg = downloader_by_name(engine_name)
            if not downloader_cfg or not downloader_cfg.get('enabled', True):
                setting = P.ModelSetting.get('download_skip_disabled_engines')
                skip = str('True' if setting is None else setting).lower() in ('true', '1', 'on')
                registration = protected_state.get('registration_wait') or {}
                unconfirmed = (registration.get('engine') == engine_name and
                               registration.get('index') == curr_idx and registration.get('uncertain'))
                if not skip or source_owned or unconfirmed:
                    continue
                next_engine = next_available_engine(chain, curr_idx, snapshot['magnet'])
                if next_engine is None:
                    continue
                curr_idx, engine_name, downloader_cfg = next_engine
                item = pending_item(item_id)
                if not item:
                    continue
                item.current_engine_index = curr_idx
                db.session.commit()

            engine_limit = DownloadUtil.get_queue_limit(downloader_cfg)
            current_count = engine_in_progress_counts.get(engine_name, 0)

            engine = engine_instances.get(engine_name)
            if engine is None:
                engine = DownloadUtil.create_engine(downloader_cfg)
                if engine is not None:
                    engine_instances[engine_name] = engine
            if not engine:
                continue

            # 성공한 목록의 hash 대조만 기존 작업 연동과 접수 여부 확인에 사용한다.
            target_hash_lower = snapshot['target_hash']
            existing_task = engine_status_maps.get(engine_name, {}).get(target_hash_lower) if target_hash_lower else None

            item = pending_item(item_id)
            if not item:
                continue
            retry = (item.upload_state or {}).get('engine_retry') or {}
            if retry.get('engine') == engine_name and retry.get('index') == curr_idx and retry.get('stage') != 'exhausted':
                TaskDownload._retry_engine_error(item_id, engine, downloader_cfg, existing_task,
                    engine_name in engine_status_maps and getattr(engine, 'status_list_complete', True))
                refresh_downloaders(force=config_signature is None)
                continue

            # 모든 엔진 공통: 원격 엔진에 이미 존재하는 작업인 경우 표준 규격에 따라 연동/재시작
            if existing_task:
                if pending_item(item_id) is None:
                    continue
                existing_tid = str(existing_task.get('task_id') or existing_task.get('id') or '')
                std_status = existing_task.get('status')

                if std_status == 'error':
                    TaskDownload._clear_registration_wait(item)
                    db.session.commit()
                    TaskDownload._retry_engine_error(item_id, engine, downloader_cfg, existing_task,
                        engine_name in engine_status_maps and getattr(engine, 'status_list_complete', True))
                    refresh_downloaders(force=config_signature is None)
                    continue
                if std_status == 'completed':
                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진에 이미 완료 상태로 존재하는 작업 감지 -> 작업 연동: {snapshot['title']} (TaskID: {existing_tid})")
                else:
                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진에 이미 진행 중인 작업 감지 -> 작업 연동: {snapshot['title']} (TaskID: {existing_tid})")

                item = pending_item(item_id)
                if item:
                    registered_source = FeederUtil.get_download_source_subpath(item) if (
                        item.current_engine_name == engine_name and str(item.engine_task_id) == str(existing_tid)) else None
                    item.status = 'downloading'
                    item.current_engine_name = engine_name
                    item.engine_task_id = existing_tid
                    item.engine_added_time = datetime.now()
                    item.source_unavailable_since = None
                    item.upload_retry_after = None
                    item.transfer_error_type = None
                    item.last_status_time = datetime.now()
                    item.remote_missing_since = None
                    item.error_message = None
                    TaskDownload._clear_registration_wait(item)

                    # 할당된 엔진의 개별 목적지 설정 즉시 반영
                    profile = FeederUtil.get_item_profile(item) or {}
                    dest_cfg = FeederUtil.get_profile_destination(profile, engine_name, item)
                    item.destination_type = dest_cfg.get('type') or ''
                    item.gdrive_upload_path = registered_source if registered_source is not None else dest_cfg.get('upload_path', '')
                    item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                    item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or ''

                    # 원격 조회에서 이미 집계된 작업의 연동은 신규 큐 슬롯을 소비하지 않는다.
                    db.session.commit()
                    linked_count += 1
                continue

            if source_owned:
                continue

            wait = (item.upload_state or {}).get('registration_wait') or {}
            if wait.get('engine') != engine_name or wait.get('index') != curr_idx:
                wait = {}
            if wait.get('uncertain'):
                if (target_hash_lower and engine_name in engine_status_maps and
                        getattr(engine, 'status_list_complete', True) and
                        getattr(engine, 'REGISTRATION_HASH_LOOKUP', False)):
                    wait = dict(wait, uncertain=False)
                    item.upload_state = dict(item.upload_state or {}, registration_wait=wait)
                    db.session.commit()
                else:
                    TaskDownload._registration_wait(item, downloader_cfg,
                        engine_errors.get(engine_name) or wait.get('last_error', '원격 접수 여부 확인 불가'), uncertain=True)
                    continue
            if getattr(engine, 'ENGINE_ID', '') == 'pikpak':
                vip = TaskDownload._registration_context.vip_snapshots.get(engine_name, {'state': 'unknown', 'reason': 'VIP 미확인'})
                message = engine.vip_summary(vip)
                if vip['state'] in ('not_vip', 'cutoff'):
                    TaskDownload._clear_registration_wait(item)
                    setting = P.ModelSetting.get('download_skip_disabled_engines')
                    skip = str('True' if setting is None else setting).lower() in ('true', '1', 'on')
                    next_engine = next_available_engine(chain, curr_idx, snapshot['magnet']) if skip else None
                    if next_engine is not None:
                        item.current_engine_index = next_engine[0]
                        profile = FeederUtil.get_item_profile(item)
                        next_dest = FeederUtil.get_profile_destination(profile, next_engine[1], item)
                        item.destination_type = next_dest.get('type') or ''
                    item.error_message = message
                    db.session.commit()
                    continue
                if vip['state'] == 'unknown':
                    TaskDownload._registration_wait(item, downloader_cfg, message)
                    continue
            if engine_name in engine_errors:
                if TaskDownload._registration_wait(item, downloader_cfg, engine_errors[engine_name]):
                    TaskDownload._registration_fallback(item, engine_name, engine_errors[engine_name])
                continue
            if wait.get('next_check_at') and datetime.fromisoformat(wait['next_check_at']) > datetime.now():
                continue

            # 링크 프로토콜(ed2k vs magnet) 지원 여부 판별
            is_ed2k = str(snapshot['magnet']).lower().startswith('ed2k://')
            target_proto = "ed2k" if is_ed2k else "magnet"
            supported_protos = getattr(engine, 'SUPPORTED_PROTOCOLS', ['magnet'])
            if target_proto not in supported_protos:
                logger.debug(f"[DownloadDispatch] [{engine_name}] {target_proto} 프로토콜 미지원 -> 다음 순위 엔진으로 즉시 폴백: {snapshot['title']}")
                item = pending_item(item_id)
                if item:
                    item.current_engine_index = curr_idx + 1
                    db.session.commit()
                continue

            if engine_name in full_engines or (engine_limit > 0 and current_count >= engine_limit):
                full_engines.add(engine_name)
                capacity_waiting += 1
                continue

            item = pending_item(item_id)
            if not item:
                continue
            profile = FeederUtil.get_item_profile(item)
            dest_cfg = FeederUtil.get_profile_destination(profile, engine_name, item)
            destination_error = FeederUtil.validate_destination(dest_cfg)
            if destination_error:
                item.error_message = destination_error
                db.session.commit()
                continue
            try:
                current_upload_path = FeederUtil.get_download_subpath(profile, engine_name, item)
            except ValueError as exc:
                item.error_message = str(exc)
                db.session.commit()
                continue

            if pending_item(item_id) is None:
                continue
            if getattr(engine, 'ENGINE_ID', '') == 'pikpak':
                cached, cache_reason = engine.cached_magnet(snapshot['magnet'])
                db.session.expire_all()
                item = pending_item(item_id)
                if item is None or (item.current_engine_index or 0) != curr_idx:
                    continue
                current_state = item.upload_state or {}
                if (item.engine_task_id or item.local_path or
                        (current_state.get('download_source') or {}).get('task_id') or
                        any(current_state.get(key) for key in ('worker', 'destination_snapshot', 'phase')) or
                        (current_state.get('registration_wait') or {}).get('uncertain')):
                    continue
                if cached == 'miss':
                    TaskDownload._registration_fallback(item, engine_name, cache_reason)
                    continue
                if cached == 'unknown':
                    TaskDownload._registration_wait(item, downloader_cfg, cache_reason)
                    continue
            if not TaskDownload._admit_registration(downloader_cfg):
                cd2_waiting += 1
                continue
            success, task_id, err = DownloadUtil.add_magnet(
                engine,
                snapshot['magnet'],
                title=snapshot['title'],
                upload_path=current_upload_path,
                new_registration=True,
                vip_snapshot=TaskDownload._registration_context.vip_snapshots.get(engine_name)
            )
            db.session.expire_all()
            item = pending_item(item_id)
            if item is None:
                if success and task_id:
                    engine.delete_task(task_id)
                continue
            if success:
                item.status = 'downloading'
                item.current_engine_name = engine_name
                item.engine_task_id = str(task_id)
                item.engine_added_time = datetime.now()
                item.source_unavailable_since = None
                item.upload_retry_after = None
                item.transfer_error_type = None
                item.last_status_time = datetime.now()
                item.remote_missing_since = None
                item.error_message = None
                
                # 할당된 엔진의 개별 목적지 설정 즉시 반영
                dest_cfg = FeederUtil.get_profile_destination(profile, engine_name, item)
                item.destination_type = dest_cfg.get('type') or ''
                item.gdrive_upload_path = current_upload_path
                item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or ''

                state = dict(item.upload_state or {})
                state.pop('registration_wait', None)
                state['download_source'] = {'engine_name': engine_name, 'task_id': str(task_id),
                                            'upload_path': current_upload_path}
                item.upload_state = state

                engine_in_progress_counts[engine_name] = current_count + 1
                if target_hash_lower:
                    engine_status_maps.setdefault(engine_name, {})[target_hash_lower] = {
                        'task_id': str(task_id), 'hash': target_hash_lower, 'status': 'downloading'}

                db.session.commit()
                registered_count += 1
                logger.info(f"[DownloadDispatch] [{engine_name}] 작업 추가 성공: {snapshot['title']} (TaskID: {task_id})")
            else:
                if FeederUtil.engine_auth_error(engine, err, 'add_magnet'):
                    refresh_downloaders(force=True)
                    item.error_message = '엔진 인증 오류 · 비활성 엔진 처리 대기'
                    db.session.commit()
                    continue
                failure = DownloadUtil.registration_failure(engine, err)
                safe_err = DownloadUtil.engine_error_detail(engine, err)
                err_str = TaskDownload._engine_retry_text(safe_err)

                if failure['kind'] == 'queue':
                    full_engines.add(engine_name)
                    item.error_message = f"[{engine_name}] 큐 한도 도달 대기: {err_str}"
                    db.session.commit()
                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진 활성 한도 도달 ({err_str}) -> 신규 추가 일시 중단 및 대기열(pending) 유지: {snapshot['title']}")
                elif failure['kind'] == 'permanent':
                    TaskDownload._registration_fallback(item, engine_name, safe_err)
                else:
                    engine_errors[engine_name] = safe_err
                    expired = TaskDownload._registration_wait(item, downloader_cfg, safe_err,
                        uncertain=not failure.get('not_submitted', False))
                    if expired:
                        TaskDownload._registration_fallback(item, engine_name, safe_err)

        db.session.commit()
        if capacity_waiting:
            logger.info('[DownloadDispatch] 포화 엔진의 신규 등록 %s건 일괄 대기 · 기존 작업 연동과 다른 엔진은 계속 처리합니다.', capacity_waiting)
        if cd2_waiting:
            message = f'CD2 회차당 신규 등록 시도 {TaskDownload._CD2_REGISTRATION_LIMIT}건 한도 · {cd2_waiting}건 다음 실행 대기'
            logger.info('[DownloadDispatch] %s', message)
            DownloadActivity.record('다운로드 작업 등록', message)
        DownloadActivity.record('다운로드 작업 등록', f'검토 완료 · {total_items}/{total_items}건 · 신규 등록 {registered_count}건 · 기존 연동 {linked_count}건')

    @staticmethod
    def _poll_deferred_remote_cleanup(engine_name, engine, rows):
        """새 보관 완료 뒤 이전 작업 ID만 정리하고 다음 전체 목록으로 부재를 확인한다."""
        if FeederUtil.single_download_id() is not None:
            return set(), set()
        protected, targets, unresolved = set(), set(), set()
        key = str(engine_name).strip().lower()
        complete = getattr(engine, 'status_list_complete', True) is True
        active_ids = {str(tid) for (tid,) in db.session.query(ModelDownload.engine_task_id).filter(
            func.lower(func.trim(ModelDownload.current_engine_name)) == key,
            ModelDownload.status != 'completed', ModelDownload.engine_task_id.isnot(None)).all()}

        def stored(item):
            if item.status != 'completed':
                return False
            proof = item.video_manifest or {}
            if not isinstance(proof, dict):
                return False
            if not proof:
                return True
            if proof.get('duplicate_of'):
                if type(proof['duplicate_of']) is not int:
                    return False
                minimum = FileDedup.minimum_bytes()
                sizes = FileDedup.selected_sizes(proof, minimum)
                if not proof.get('source_key') or not sizes:
                    return False
                original = db.session.get(ModelDownload, proof['duplicate_of'])
                source = (original.video_manifest or {}) if original else {}
                return bool(original and original.status == 'completed' and isinstance(source, dict) and
                            source.get('stored') is True and not source.get('duplicate_of') and
                            source.get('source_key') == proof['source_key'] and FileDedup.selected_sizes(source, minimum) == sizes)
            return proof.get('stored') is True

        items = db.session.query(ModelDownload).filter(
            ModelDownload.upload_state['deferred_remote_cleanup'].as_string().isnot(None)).all()
        changed = False
        for item in items:
            kept = []
            for entry in (item.upload_state or {}).get('deferred_remote_cleanup') or []:
                if not isinstance(entry, dict):
                    kept.append(entry)
                    continue
                if str(entry.get('engine_name') or '').strip().lower() != key:
                    kept.append(entry)
                    continue
                tid = str(entry.get('task_id') or '')
                content = DownloadUtil._queue_hash(entry.get('infohash'))
                if tid:
                    protected.add(tid)
                if str(item.current_engine_name or '').strip().lower() == key and item.engine_task_id:
                    protected.add(str(item.engine_task_id))
                for remote in rows:
                    if content and DownloadUtil._queue_match(remote, content, '', False) is True:
                        protected.add(str(remote.get('task_id') or remote.get('id') or ''))
                matches = [row for row in rows if str(row.get('task_id') or row.get('id') or '') == tid] if tid else []
                if not (complete and tid and stored(item)):
                    kept.append(entry)
                    unresolved.add(tid)
                elif not matches:
                    changed = True
                else:
                    kept.append(entry)
                    if len(matches) == 1 and tid not in active_ids and DownloadUtil._queue_match(matches[0], content, tid, True) is True:
                        targets.add(tid)
                    else:
                        unresolved.add(tid)
            if kept != (item.upload_state or {}).get('deferred_remote_cleanup'):
                state = dict(item.upload_state or {})
                if kept:
                    state['deferred_remote_cleanup'] = kept
                else:
                    state.pop('deferred_remote_cleanup', None)
                item.upload_state = state or None
        if changed:
            db.session.commit()
        return protected, targets - unresolved

    @staticmethod
    def poll_active_downloads():
        with TaskDownload._registration_scope():
            return TaskDownload._poll_active_downloads_scoped()

    @staticmethod
    def _poll_active_downloads_scoped():
        from .util_block import DownloadBlock
        now = datetime.now()
        enabled_downloaders = [d for d in FeederUtil.get_downloaders() if d.get('enabled', True)]
        if FeederUtil.single_download_id() is not None:
            selected = db.session.get(ModelDownload, FeederUtil.single_download_id())
            enabled_downloaders = [d for d in enabled_downloaders if selected and d.get('name') == selected.current_engine_name]
        if not enabled_downloaders:
            return

        for downloader_cfg in DownloadActivity.track('다운로더 상태 확인', enabled_downloaders, '엔진'):
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                db.session.commit()
                return
            engine_name = downloader_cfg.get('name')
            if not engine_name:
                continue

            engine = DownloadUtil.create_engine(downloader_cfg)
            if not engine:
                continue

            # 해당 엔진에 할당된 downloading 작업 조회 (엔진별 독립 조회)
            engine_items = (
                TaskDownload._selected_query(db.session.query(ModelDownload))
                .filter_by(status='downloading', current_engine_name=engine_name)
                .order_by(ModelDownload.id.asc())
                .all()
            )

            # 원격 상태 조회가 DB의 활성 읽기 트랜잭션을 기다리지 않도록 조회 종료
            db.session.rollback()
            if FeederUtil.single_download_id() is not None and not engine_items:
                continue
            DownloadActivity.record('다운로더 상태 확인', f'{engine_name} 원격 목록 조회 중 · 할당된 작업 {len(engine_items)}건')
            try:
                status_list, err = engine.get_status()
            except Exception as exc:
                status_list, err = [], DownloadUtil.log_engine_error(engine, 'get_status',
                    f'{type(exc).__name__}: {exc}')
            DownloadActivity.record('다운로더 상태 확인', f'{engine_name} 원격 목록 조회 ' + ('실패' if err else f'완료 · 수신 {len(status_list)}건'), 'warning' if err else 'info')
            if err:
                FeederUtil.engine_auth_error(engine, err, 'get_status')
                DownloadUtil.log_engine_error(engine, 'get_status · 상태 재확인 대기', err)
                err = TaskDownload._engine_retry_text(err)
                logger.warning(f"[DownloadPoll] [{engine_name}] 상태 조회 실패: {err}")
                for it in engine_items:
                    FeederUtil.cooperate()
                    it.remote_missing_since = None
                db.session.commit()
                continue

            if any(FeederUtil.engine_auth_error(engine, row.get('error') or row.get('error_message'), 'task_status')
                   for row in status_list if row.get('status') == 'error'):
                continue

            # 클라우드 마운트 엔진(CD2): 완료 작업이 위치한 임시 업로드 서브폴더만 특정하여 캐시 선제 갱신
            if (engine_name == 'cd2' or getattr(engine, 'OUTPUT_TYPE', '') == 'cloud_storage') and hasattr(engine, 'refresh_cache'):
                if any(s.get('status') == 'completed' for s in status_list):
                    refreshed_vpaths = set()
                    for it_chk in DownloadActivity.track('다운로더 상태 확인', engine_items, '캐시 검토', engine_name):
                        FeederUtil.cooperate()
                        p_chk = FeederUtil.get_item_profile(it_chk) or {}
                        d_chk = FeederUtil.get_profile_destination(p_chk, engine_name, it_chk)
                        u_sub = FeederUtil.get_download_source_subpath(it_chk, d_chk.get('upload_path') or '').strip('/\\')
                        target_vpath = f"{engine.vpath}/{u_sub}".rstrip('/') if u_sub else engine.vpath
                        if target_vpath not in refreshed_vpaths:
                            refreshed_vpaths.add(target_vpath)
                            DownloadActivity.record('다운로더 상태 확인', f'{engine_name} 완료 작업 폴더 캐시 갱신 중 · {len(refreshed_vpaths)}번째 경로')
                            engine.refresh_cache(path=target_vpath)

            status_map_by_tid = {str(s.get('task_id')): s for s in status_list if s.get('task_id')}
            status_map_by_hash = {str(s.get('hash')).lower(): s for s in status_list if s.get('hash')}

            # 모든 엔진 공통:
            # - Feeder DB에서 이미 완료(completed)된 작업은 원격 상태(완료/에러)와 무관하게 원격 엔진에서 삭제 정리
            # - Feeder DB에서 이미 다른 엔진으로 이관된 작업의 에러 잔여물도 원격 엔진에서 삭제 정리
            all_remote_hashes = [s.get('hash').lower() for s in status_list if s.get('hash')]
            all_remote_tids = [str(s.get('task_id')) for s in status_list if s.get('task_id')]
            deferred_protected, deferred_targets = TaskDownload._poll_deferred_remote_cleanup(engine_name, engine, status_list)

            # Confirmed history entries retain ownership-checked cleanup on partial lists.
            # Error retry/re-registration separately requires complete lists in _retry_engine_error().
            if FeederUtil.single_download_id() is None and (all_remote_hashes or all_remote_tids):
                done_or_migrated_rows = db.session.query(
                    ModelDownload.infohash,
                    ModelDownload.engine_task_id,
                    ModelDownload.status,
                    ModelDownload.current_engine_name
                ).filter(
                    or_(
                        and_(ModelDownload.status == 'completed',
                             ModelDownload.video_manifest['duplicate_of'].as_integer().is_(None)),
                        and_(
                            ModelDownload.status.in_(['downloading', 'pending']),
                            ModelDownload.current_engine_name.isnot(None),
                            ModelDownload.current_engine_name != engine_name
                        )
                    ),
                    or_(
                        ModelDownload.infohash.in_(all_remote_hashes),
                        ModelDownload.engine_task_id.in_(all_remote_tids)
                    )
                ).all()

                cleanup_targets = set(deferred_targets)
                for r_hash, r_tid, r_status, r_engine in done_or_migrated_rows:
                    FeederUtil.cooperate()
                    target_key = (r_hash or '').lower()
                    s_item = status_map_by_hash.get(target_key) or status_map_by_tid.get(str(r_tid))
                    if not s_item:
                        continue
                    s_tid = str(s_item.get('task_id') or s_item.get('id') or '')
                    s_status = s_item.get('status')

                    # DB 완료 건은 상태 무관 삭제, 타 엔진 이관 건은 에러 상태인 경우 즉시 삭제
                    if (r_status == 'completed' or s_status == 'error') and s_tid not in deferred_protected:
                        cleanup_targets.add(s_tid or target_key)

                manual_targets = set()
                if (downloader_cfg.get('engine_type') == 'cd2' and
                        downloader_cfg.get('cd2_cleanup_manual_completed') is True and
                        not FeederUtil.is_download_paused()):
                    candidates = {s['cd2_infohash'] for s in status_list
                                  if s.get('status') == 'completed' and s.get('statusCode') == 2 and
                                  isinstance(s.get('cd2_infohash'), str) and
                                  re.fullmatch(r'(?:[0-9a-f]{32}|[0-9a-f]{40})', s['cd2_infohash'])}
                    if candidates:
                        # 모든 상태·엔진의 대응 기록을 한 번에 확인해 후처리 중인 항목도 보호한다.
                        owned_rows = db.session.query(ModelDownload.infohash, ModelDownload.engine_task_id).filter(
                            or_(func.lower(ModelDownload.infohash).in_(candidates),
                                func.lower(ModelDownload.engine_task_id).in_(candidates))
                        ).all()
                        owned_ids = {str(value).lower() for row in owned_rows for value in row if value}
                        manual_targets = candidates - owned_ids - cleanup_targets - deferred_protected
                        cleanup_targets.update(manual_targets)

                if manual_targets and FeederUtil.is_download_paused():
                    cleanup_targets.difference_update(manual_targets)
                    manual_targets.clear()
                if cleanup_targets:
                    db.session.rollback()
                    DownloadActivity.record('다운로더 상태 확인', f'{engine_name} 원격 히스토리 정리 중 · 대상 {len(cleanup_targets)}건')
                    cleanup_ok = True
                    if hasattr(engine, 'delete_tasks'):
                        try:
                            cleanup_ok = bool(engine.delete_tasks(list(cleanup_targets)))
                        except Exception:
                            cleanup_ok = False
                    elif hasattr(engine, 'delete_task'):
                        for tid in DownloadActivity.track('다운로더 상태 확인', list(cleanup_targets), '원격 정리 항목', engine_name):
                            FeederUtil.cooperate()
                            try:
                                cleanup_ok = bool(engine.delete_task(tid)) and cleanup_ok
                            except Exception:
                                cleanup_ok = False
                    else:
                        cleanup_ok = False
                    if cleanup_ok:
                        logger.info(f"[DownloadPoll] [{engine_name}] 원격 히스토리 {len(cleanup_targets)}건 정리 완료 (외부 완료: {len(manual_targets)}건)")
                    else:
                        logger.warning(f"[DownloadPoll] [{engine_name}] 원격 히스토리 {len(cleanup_targets)}건 정리 실패 · 다음 상태 확인 시 재시도 (외부 완료: {len(manual_targets)}건)")

            matched_count = sum(1 for it in engine_items if (str(it.engine_task_id) in status_map_by_tid or (it.infohash and str(it.infohash).lower() in status_map_by_hash)))
            logger.debug(f"[DownloadPoll] [{engine_name}] 전체 토렌트 {len(status_list)}건 수신 (큐 매칭: {matched_count}건, 미등록 잔여: {len(status_list) - matched_count}건)")

            try:
                stalled_hours = int(downloader_cfg.get('stalled_timeout_hours', 24))
            except Exception:
                stalled_hours = 24

            for it in DownloadActivity.track('다운로더 상태 확인', engine_items, '항목', engine_name):
                FeederUtil.cooperate()
                if FeederUtil.is_download_paused():
                    db.session.commit()
                    return
                if DownloadBlock.stopped(it.id):
                    db.session.rollback()
                    continue
                db.session.refresh(it)
                if it.status != 'downloading':
                    continue
                matched_status = status_map_by_tid.get(str(it.engine_task_id))
                if not matched_status and it.infohash:
                    matched_status = status_map_by_hash.get(str(it.infohash).lower())

                if not matched_status:
                    retry = (it.upload_state or {}).get('engine_retry') or {}
                    if retry.get('engine') == engine_name and retry.get('stage') in ('cleanup', 'uncertain', 'scheduled'):
                        TaskDownload._retry_engine_error(it.id, engine, downloader_cfg, None,
                            getattr(engine, 'status_list_complete', True))
                        continue
                    if not getattr(engine, 'status_list_complete', True):
                        it.remote_missing_since = None
                        continue
                    try:
                        missing_hours = max(0, float(downloader_cfg.get('missing_timeout_hours', 24)))
                    except (TypeError, ValueError):
                        missing_hours = 24
                    if missing_hours == 0:
                        it.remote_missing_since = None
                        continue
                    if it.remote_missing_since is None:
                        it.remote_missing_since = now
                    elif (now - it.remote_missing_since).total_seconds() >= missing_hours * 3600:
                        it.status = 'failed'
                        it.error_message = f'[{engine_name}] 원격 전체 목록에서 {missing_hours:g}시간 이상 미발견 (엔진 내 실패)'
                        logger.warning(f'[DownloadPoll] {it.error_message}: {it.title}')
                    continue

                it.remote_missing_since = None
                # 완료 히스토리는 현재 downloading이 아닌 항목도 역동기화한다.
                # 그 외 상태는 기존 다운로드 polling에서만 처리한다.
                if matched_status.get('status') != 'completed' and it.status != 'downloading':
                    continue

                it.last_status_time = now
                std_status = matched_status.get('status')
                fname = matched_status.get('filename') or it.file_name
                fsize = matched_status.get('file_size') or it.file_size
                src_path = matched_status.get('source_path')

                if fname:
                    it.file_name = fname
                if fsize:
                    it.file_size = fsize
                if src_path and not it.local_path:
                    it.local_path = src_path

                if std_status == 'completed':
                    source_path = matched_status.get('source_path', '')
                    if downloader_cfg.get('engine_type') in ('pikpak', 'alldebrid') and not source_path:
                        it.error_message = ('PikPak' if downloader_cfg.get('engine_type') == 'pikpak' else 'AllDebrid') + ' 다운로드 완료 · 파일 경로 확인 대기: ' + (
                            matched_status.get('error_message') or '다음 상태 확인에서 다시 조회합니다.')
                        db.session.commit()
                        continue
                    if (it.error_message or '').startswith(('PikPak 다운로드 완료 · 파일 경로 확인 대기:', 'AllDebrid 다운로드 완료 · 파일 경로 확인 대기:')):
                        it.error_message = None
                    it.local_path = source_path

                    # 원격 엔진이 보고한 실제 원본 파일명 보존
                    raw_engine_name = matched_status.get('filename') or fname or it.file_name or ''

                    DownloadUtil.reserve_output_folder(it, raw_engine_name)
                    if FileDedup.check(it):
                        continue

                    profile = FeederUtil.get_item_profile(it) or {}
                    dest_cfg = FeederUtil.get_profile_destination(profile, engine_name, it)
                    direct_cd2 = bool(hasattr(engine, 'direct_worker_enabled') and
                                      engine.direct_worker_enabled(dest_cfg.get('type')))
                    if direct_cd2:
                        try:
                            source_path = engine.direct_source(raw_engine_name, FeederUtil.get_download_source_subpath(it, dest_cfg.get('upload_path') or ''))
                            it.local_path = source_path
                        except Exception:
                            it.error_message = 'CD2 직접 전송 경로 조회 실패: 다음 상태 확인에서 다시 조회합니다.'
                            continue

                    # 직접 URL 워커 이외의 CD2 경로는 기존 FUSE 실물 확인을 유지한다.
                    if not direct_cd2 and (engine_name == 'cd2' or getattr(engine, 'OUTPUT_TYPE', '') == 'cloud_storage'):
                        base_mount_root = engine.get_local_mount_path() if hasattr(engine, 'get_local_mount_path') else DownloadUtil.get_local_root(downloader_cfg)

                        profile = FeederUtil.get_item_profile(it) or {}
                        dest_cfg = FeederUtil.get_profile_destination(profile, engine_name, it)
                        upload_sub = FeederUtil.get_download_source_subpath(it, dest_cfg.get('upload_path') or '').strip('/\\')
                        search_roots = []
                        if base_mount_root:
                            if upload_sub:
                                search_roots.append(os.path.join(base_mount_root, upload_sub))
                            search_roots.append(base_mount_root)

                        target_search_name = raw_engine_name or it.file_name or it.title or ''
                        matched_real_path = ""

                        for search_dir in search_roots:
                            FeederUtil.cooperate()
                            if not os.path.exists(search_dir):
                                continue

                            if it.local_path and os.path.exists(it.local_path):
                                matched_real_path = it.local_path
                                break

                            search_candidates = []
                            for candidate in [raw_engine_name, it.file_name, it.title]:
                                FeederUtil.cooperate()
                                if candidate and candidate not in search_candidates:
                                    search_candidates.append(candidate)

                            for candidate in search_candidates:
                                FeederUtil.cooperate()
                                direct_path = os.path.join(search_dir, candidate)
                                if os.path.exists(direct_path):
                                    matched_real_path = direct_path
                                    break

                            if not matched_real_path:
                                try:
                                    entries = os.listdir(search_dir)
                                    for candidate in search_candidates:
                                        FeederUtil.cooperate()
                                        if candidate in entries:
                                            matched_real_path = os.path.join(search_dir, candidate)
                                            break
                                except Exception:
                                    pass

                            if matched_real_path:
                                break

                        if not matched_real_path:
                            logger.debug(f"[DownloadPoll] [{engine_name}] 115 완료 감지 -> 마운트 경로 파일 출현 대기 중: {target_search_name}")
                            continue

                        # 실물 파일이 확인된 경우 실제 경로 및 용량 매핑
                        it.local_path = matched_real_path
                        source_path = matched_real_path
                        if os.path.isfile(matched_real_path):
                            it.file_size = os.path.getsize(matched_real_path)
                        else:
                            it.file_size = FeederUtil.directory_size(matched_real_path)
                        logger.info(f"[DownloadPoll] [{engine_name}] 마운트 실물 파일 확인 완료 ({target_search_name}, {FeederUtil.format_bytes(it.file_size)}) -> 인계 준비")

                    profile = FeederUtil.get_item_profile(it) or {}
                    dest_cfg = FeederUtil.get_profile_destination(profile, engine_name, it)
                    dest_type = dest_cfg.get('type') or ''
                    it.destination_type = dest_type
                    it.gdrive_upload_path = dest_cfg.get('upload_path', '')
                    it.gdrive_complete_path = dest_cfg.get('complete_path', '')
                    it.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''

                    transporter = DownloadUtil.get_transporter(dest_type)
                    is_relay_dest = bool(transporter and getattr(transporter, 'IS_RELAY_HANDLER', False))
                    is_remote_src = not os.path.exists(source_path) and (':' in source_path or source_path.startswith(('http://', 'https://')))

                    # 로컬 스테이징 사용 여부 계층 판별: 개별 프로필 오버라이드 -> 전역 DB 설정 -> 기본값 True
                    profile_settings = dict(profile)
                    profile_settings.update(dest_cfg)
                    use_local_staging = FeederUtil.resolve_setting(
                        'use_local_staging',
                        profile_settings,
                        downloader_cfg,
                        P.ModelSetting.get_bool('download_use_local_staging') if P.ModelSetting else True,
                        True
                    )
                    use_local_staging = str(use_local_staging).lower() in ['true', 'on', '1']
                    if transporter and getattr(transporter, 'FORCE_REMOTE_SOURCE', False):
                        use_local_staging = False

                    if is_relay_dest:
                        it.status = 'pending_relay'
                        transporter.transport(it, source_path, dest_cfg)
                        logger.info(f"[DownloadPoll] [{engine_name}] 완료 확인 -> 원격 릴레이 대기열({it.status}) 인계: {it.title}")
                    elif is_remote_src and not use_local_staging:
                        # 로컬 스테이징 미사용(다이렉트 스트리밍 모드)
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 완료 확인 -> 다이렉트(무스테이징) 전송 대기열 인계: {it.title}")
                    elif is_remote_src and use_local_staging:
                        it.status = 'pending_local_staging'
                        logger.info(f"[DownloadPoll] [{engine_name}] 원격 완료 확인 -> 로컬 스테이징 대기열 인계: {it.title}")
                    else:
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 다운로드 완료 확인 (이송 대기): {it.title}")

                    if DownloadBlock.stopped(it.id):
                        db.session.rollback()
                        continue
                    state = dict(it.upload_state or {})
                    state.pop('engine_retry', None)
                    it.upload_state = state
                    db.session.commit()
                else:
                    # 타임아웃 판정 시점 계산 (누락 시 현재 시각 기준 방어)
                    added_time = it.engine_added_time or it.created_time or now
                    is_timed_out = bool(stalled_hours > 0 and (now - added_time).total_seconds() > (stalled_hours * 3600))

                    if std_status == 'error':
                        TaskDownload._retry_engine_error(it.id, engine, downloader_cfg, matched_status,
                            getattr(engine, 'status_list_complete', True))
                    elif is_timed_out and getattr(engine, 'status_list_complete', True):
                        # 엔진 설정 타임아웃이 초과된 경우에만 유일하게 차순위 엔진으로 폴백
                        logger.warning(f"[DownloadPoll] [{engine_name}] 지연 제한시간({stalled_hours}시간) 초과 감지 -> 다음 엔진 폴백: {it.title}")
                        try:
                            engine.delete_task(it.engine_task_id)
                        except Exception:
                            pass

                        if DownloadBlock.stopped(it.id):
                            db.session.rollback()
                            continue
                        it.current_engine_index = (it.current_engine_index or 0) + 1
                        it.status = 'pending'
                        it.engine_task_id = None
                        it.error_message = f"[{engine_name}] 지연 제한시간({stalled_hours}시간) 초과"
                        db.session.commit()

        for changed in list(db.session.dirty):
            if isinstance(changed, ModelDownload) and DownloadBlock.stopped(changed.id):
                db.session.expire(changed)
        db.session.commit()

    @staticmethod
    def _get_upload_queue_state(staging_root=None, disabled_names=None):
        """스테이징·업로드 상태와 잔여 로컬 버퍼를 작업 ID 기준으로 합산한다."""
        try:
            limit = max(0, P.ModelSetting.get_int('download_max_staging_items'))
        except Exception:
            limit = 10
        disabled_names = FeederUtil.disabled_engine_names() if disabled_names is None else disabled_names
        occupied = {item_id for (item_id,) in TaskDownload._enabled_engine_query(db.session.query(ModelDownload.id), disabled_names).filter(
            ModelDownload.status.in_(['local_staging', 'pending_upload', 'uploading'])
        ).all()}
        if staging_root is None:
            staging_root = FeederUtil.get_local_staging_root()
        root_prefix = os.path.abspath(staging_root).rstrip(os.sep) + os.sep
        staging_paths = TaskDownload._enabled_engine_query(db.session.query(ModelDownload.id, ModelDownload.local_path), disabled_names).filter(
            ModelDownload.status != 'upload_held',
            ModelDownload.local_path.startswith(root_prefix, autoescape=True)
        ).all()
        for item_id, path in DownloadActivity.track('큐 점검', staging_paths, '스테이징 경로', report_start=False):
            FeederUtil.cooperate()
            if os.path.exists(path):
                occupied.add(item_id)
        return limit, occupied

    @staticmethod
    def _expire_source_failures(query, downloaders=None):
        """원본 실패 유예가 있는 후보만 확인하고 미확정 AD 만료 검토는 생략한다."""
        candidates = query.filter(ModelDownload.transfer_error_type == 'SOURCE_UNAVAILABLE',
                                  ModelDownload.source_unavailable_since.isnot(None))
        ad_engines = [config['name'] for config in (FeederUtil.get_downloaders() if downloaders is None else downloaders)
                      if config.get('name') and config.get('engine_type') == 'alldebrid']
        if ad_engines:
            candidates = candidates.filter(or_(ModelDownload.current_engine_name.is_(None),
                                               ~ModelDownload.current_engine_name.in_(ad_engines)))
        for item in candidates.all():
            FeederUtil.cooperate()
            UploadUtil.expire_source_failure(item)

    @staticmethod
    def _apply_pikpak_vip_hold(statuses, phase, downloaders):
        """실행 시작의 단일 판정으로 시작 전 작업만 보류하고 원래 상태를 복원한다."""
        snapshots = getattr(TaskDownload._registration_context, 'vip_snapshots', {})
        for config in downloaders:
            if config.get('engine_type') != 'pikpak' or not config.get('enabled', True):
                continue
            rows = TaskDownload._selected_query(db.session.query(ModelDownload)).filter(
                ModelDownload.current_engine_name == config['name'],
                ModelDownload.status.in_(list(statuses) + ['upload_held'])).all()
            candidates = []
            for item in rows:
                state = item.upload_state or {}
                hold = state.get('hold') or {}
                if item.status == 'upload_held' and (
                        hold.get('reason') != 'pikpak_vip' or hold.get('resume_status') not in statuses):
                    continue
                remote = state.get('worker') or {}
                if (remote and remote.get('submission') not in ('new', 'rejected')) or state.get('phase') in ('uploaded', 'shared_incoming', 'moved'):
                    continue
                candidates.append(item)
            if not candidates:
                continue
            vip = snapshots.get(config['name'], {'state': 'unknown'})
            known = vip['state'] in ('eligible', 'cutoff', 'not_vip')
            expired = vip.get('expired', vip['state'] == 'not_vip')
            for item in candidates:
                state = dict(item.upload_state or {})
                if expired and item.status != 'upload_held':
                    state['hold'] = {'reason': 'pikpak_vip', 'resume_status': item.status}
                    item.status = 'upload_held'
                elif known and not expired and item.status == 'upload_held':
                    item.status = state.pop('hold')['resume_status']
                else:
                    continue
                item.upload_state = state
            db.session.commit()

    @staticmethod
    def process_local_staging():
        if FeederUtil.is_download_paused():
            return
        downloaders = FeederUtil.get_downloaders()
        disabled_engines = FeederUtil.disabled_engine_names(downloaders)
        # 이전 비정상 종료로 멈춘 고아 local_staging 작업을 pending_local_staging으로 자동 복구
        stuck_staging = TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines).filter_by(status='local_staging').all()
        if stuck_staging:
            for s_it in stuck_staging:
                FeederUtil.cooperate()
                s_it.status = 'pending_local_staging'
            db.session.commit()
            logger.info(f"[LocalStaging] 멈춰있던 스테이징 작업 {len(stuck_staging)}건을 대기열(pending_local_staging)로 자동 복구했습니다.")
        TaskDownload._apply_pikpak_vip_hold(('pending_local_staging',), 'staging', downloaders)

        # 로컬 스테이징 기본 루트 디렉터리 준비
        staging_root = FeederUtil.get_local_staging_root()

        query = TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines).filter(ModelDownload.status == 'pending_local_staging')
        TaskDownload._expire_source_failures(query, downloaders)
        queue_limit, occupied = TaskDownload._get_upload_queue_state(staging_root, disabled_engines)
        if queue_limit > 0 and len(occupied) >= queue_limit:
            logger.info(f"[LocalStaging] Google Drive 업로드 최대 큐({queue_limit}건) 도달 (현재: {len(occupied)}건). 신규 스테이징을 대기합니다.")
            DownloadActivity.record('로컬 스테이징', f'업로드 최대 큐 {len(occupied)}/{queue_limit}건 · 새 스테이징 대기')
            return

        # 현재 세션 실패 항목을 제외하고, 로컬 디스크 허용 잔여 슬롯만큼만 정밀 조회
        if FeederUtil.single_download_id() is None:
            query = query.filter(or_(ModelDownload.upload_retry_after.is_(None),
                                     ModelDownload.upload_retry_after <= datetime.now()))
        if TaskDownload.failed_ids_in_session:
            query = query.filter(~ModelDownload.id.in_(list(TaskDownload.failed_ids_in_session)))

        if queue_limit > 0:
            slots_available = queue_limit - len(occupied)
            items = query.order_by(ModelDownload.id.asc()).limit(slots_available).all()
        else:
            items = query.order_by(ModelDownload.id.asc()).all()

        if RemoteWorker.reserves_ad():
            # 같은 AD 계정의 파일 데이터를 홈 서버와 원격 서버에서 동시에 받지 않는다.
            items = [item for item in items if not RemoteWorker.is_ad(item)]

        if not items:
            return
        os.makedirs(staging_root, exist_ok=True)
        item_ids = [item.id for item in items]
        db.session.rollback()
        db.session.remove()

        try:
            staging_workers = max(1, P.ModelSetting.get_int('download_upload_workers'))
        except Exception:
            staging_workers = 2

        logger.info(f"[LocalStaging] 로컬 스테이징 Rclone 다운로드 병렬 실행 (대상: {len(items)}건, 동시 워커: {staging_workers}개)")
        DownloadActivity.record('로컬 스테이징', f'전송 준비 · 대상 {len(item_ids)}건, 동시 {staging_workers}개')

        def _staging_worker(item_id):
            with F.app.app_context():
                target_item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not target_item or target_item.status != 'pending_local_staging':
                    return
                if FeederUtil.item_engine_disabled(target_item):
                    return
                if FileDedup.check(target_item):
                    return
                destination = FeederUtil.get_profile_destination(FeederUtil.get_item_profile(target_item), item=target_item)
                destination_error = FeederUtil.validate_destination(destination)
                if destination_error:
                    target_item.error_message = destination_error
                    db.session.commit()
                    return
                if not UploadUtil.resolve_ad_source(target_item):
                    target_item.error_message = 'AllDebrid 원본 경로 확인 대기'
                    db.session.commit()
                    return
                target_item.status = 'local_staging'
                db.session.commit()

                src_path = target_item.local_path
                raw_name = target_item.file_name or f"item_{target_item.id}"
                is_single_file = DownloadUtil.is_single_file(raw_name, src_path)
                profile = FeederUtil.get_item_profile(target_item)
                target_folder_name = DownloadUtil.reserve_output_folder(target_item, raw_name)

                target_item_id = target_item.id
                target_file_size = target_item.file_size
                current_engine_name = target_item.current_engine_name
                engine_task_id = target_item.engine_task_id
                try:
                    if is_single_file and FeederUtil.relative_path_error(raw_name, single=True):
                        raise ValueError('로컬 스테이징 파일명에 경로를 포함할 수 없습니다.')
                    item_staging_dir = FeederUtil.get_local_staging_path(target_item, profile, staging_root)
                except ValueError as exc:
                    target_item.status = 'pending_local_staging'
                    target_item.error_message = str(exc)
                    db.session.commit()
                    return
                staging_path = os.path.relpath(os.path.dirname(item_staging_dir), staging_root)
                target_item.upload_state = dict(target_item.upload_state or {}, staging_local_path=item_staging_dir,
                                                       engine_name=current_engine_name, engine_task_id=engine_task_id)
                db.session.commit()
                if FeederUtil.item_engine_disabled(target_item):
                    db.session.remove()
                    return
                if os.path.exists(item_staging_dir):
                    shutil.rmtree(item_staging_dir, ignore_errors=True)
                os.makedirs(item_staging_dir, exist_ok=True)

                if is_single_file:
                    rclone_cmd_type = "copyto"
                    dest_download_path = os.path.join(item_staging_dir, raw_name)
                else:
                    rclone_cmd_type = "copy"
                    dest_download_path = item_staging_dir

                logger.info(f"[LocalStaging] 다운로드 시작: {staging_path}/{target_folder_name} (명령: {rclone_cmd_type})")

                rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
                cmd = ["rclone", rclone_cmd_type, src_path, dest_download_path]
                if rclone_conf:
                    cmd.extend(["--config", rclone_conf])

                cmd.extend(FeederUtil.get_rclone_exclude_options())
                cmd.extend(FeederUtil.get_rclone_transfer_options())
                cmd.extend(UploadUtil.rclone_stats_options())

                db.session.remove()

                success = False
                out = ""
                try:
                    current = db.session.get(ModelDownload, target_item_id)
                    if FeederUtil.item_engine_disabled(current):
                        db.session.remove()
                        return
                    success, out = UploadUtil.run_rclone(
                        cmd,
                        f"로컬 스테이징 {target_folder_name}",
                        item_id=target_item_id,
                        file_size=target_file_size,
                        watchdog_timeout=300
                    )
                    if not success:
                        logger.error(f"[LocalStaging] Rclone 다운로드 실패: {out.strip()[:200]}")
                except Exception as ex:
                    logger.error(f"[LocalStaging] Rclone 실행 예외 ({target_folder_name}): {ex}")
                    success = False

                # 다운로드 종료 후 다시 세션을 열어 최종 상태만 원자적으로 갱신하고 즉시 닫기
                with F.app.app_context():
                    it_update = db.session.query(ModelDownload).filter_by(id=target_item_id).first()
                    if FeederUtil.item_engine_disabled(it_update):
                        db.session.remove()
                        return
                    if it_update and it_update.status == 'blocked':
                        from .util_block import DownloadBlock
                        DownloadBlock.remove_staging(item_staging_dir)
                    elif it_update and it_update.status == 'local_staging':
                        if success and os.path.exists(item_staging_dir):
                            downloader_cfg = FeederUtil.get_downloader_by_name(current_engine_name)
                            if downloader_cfg:
                                engine = DownloadUtil.create_engine(downloader_cfg)
                                if engine and engine_task_id and getattr(engine, 'ENGINE_ID', '') != 'pikpak':
                                    try:
                                        if engine.delete_task(engine_task_id) is not True:
                                            logger.warning('[LocalStaging] 작업 %s: 엔진 히스토리 삭제 미확인 · ID를 보존하고 업로드 후 다시 정리합니다.', target_item_id)
                                    except Exception:
                                        logger.warning('[LocalStaging] 작업 %s: 엔진 히스토리 삭제 실패 · 검증된 스테이징 결과와 ID를 보존합니다.', target_item_id)

                            it_update.local_path = item_staging_dir
                            it_update.file_name = target_folder_name
                            it_update.status = 'downloaded'
                            it_update.error_message = None
                            it_update.transfer_error_type = None
                            it_update.source_unavailable_since = None
                            it_update.upload_retry_after = None
                            logger.info(f"[LocalStaging] 폴더 스테이징 완료: {staging_path}/{target_folder_name} -> downloaded 전환")
                        else:
                            if os.path.exists(item_staging_dir):
                                shutil.rmtree(item_staging_dir, ignore_errors=True)
                            TaskDownload.failed_ids_in_session.add(it_update.id)
                            UploadUtil.transfer_failed(it_update.id, out or '로컬 스테이징 Rclone 전송 실패',
                                                       retry_status='pending_local_staging', source_operation=True)
                            it_update = db.session.get(ModelDownload, target_item_id)
                            logger.warning('[LocalStaging] %s 스테이징 실패 처리 · 상태: %s', target_folder_name, it_update.status)
                        db.session.commit()
                    db.session.remove()

        for item_id, future in TaskDownload._worker_results(_staging_worker, item_ids, staging_workers, activity_stage='로컬 스테이징'):
            FeederUtil.cooperate()
            try:
                future.result()
            except Exception as th_err:
                logger.error(f"[LocalStaging] 워커 스레드 예외 (ID: {item_id}): {th_err}")
                with F.app.app_context():
                    failed_item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                    if failed_item and failed_item.status == 'local_staging' and not FeederUtil.item_engine_disabled(failed_item):
                        failed_item.status = 'pending_local_staging'
                        failed_item.error_message = f"로컬 스테이징 워커 예외: {str(th_err)[:150]}"
                        TaskDownload.failed_ids_in_session.add(item_id)
                        db.session.commit()
                    db.session.remove()

    @staticmethod
    def _route_candidate_ids(query, queue_limit, occupied):
        """목적지 문맥별로 분류하고 새 슬롯이 필요한 후보만 잔여 수만큼 고른다."""
        if queue_limit <= 0:
            return [row[0] for row in query.with_entities(ModelDownload.id).order_by(ModelDownload.id.asc()).all()], 0
        import json
        from sqlalchemy import cast, String
        columns = (ModelDownload.feed_name, ModelDownload.current_engine_name, ModelDownload.current_engine_index,
                   cast(ModelDownload.priority_chain, String), cast(ModelDownload.upload_state['routing'], String),
                   cast(ModelDownload.upload_state['destination_snapshot'], String), ModelDownload.destination_type)
        queue_groups = []
        for context in query.with_entities(*columns).distinct().all():
            try:
                feed, engine, index, chain, routing, snapshot, destination_type = context
                item = SimpleNamespace(id=None, feed_name=feed, current_engine_name=engine, current_engine_index=index or 0,
                    priority_chain=json.loads(chain) if chain else [], destination_type=destination_type,
                    upload_state={'routing': json.loads(routing) if routing else None,
                                  'destination_snapshot': json.loads(snapshot) if snapshot else None})
                profile = FeederUtil.get_item_profile(item) or {}
                destination = FeederUtil.get_profile_destination(profile, engine, item)
                if destination.get('type') in ('gdrive_rotation', 'remote_worker'):
                    queue_groups.append(and_(*(column.is_(None) if value is None else column == value
                                               for column, value in zip(columns, context))))
            except Exception:
                # 문맥을 확정하지 못하면 실제 항목의 기존 목적지 해석 경로를 보존한다.
                continue
        if not queue_groups:
            return [row[0] for row in query.with_entities(ModelDownload.id).order_by(ModelDownload.id.asc()).all()], 0
        needs_slot = and_(func.coalesce(or_(*queue_groups), False), ~ModelDownload.id.in_(occupied))
        new_count = query.filter(needs_slot).count()
        slots = max(0, queue_limit - len(occupied))
        admitted = [row[0] for row in query.with_entities(ModelDownload.id).filter(needs_slot).order_by(
            ModelDownload.id.asc()).limit(slots).all()] if slots else []
        eligible = query.filter(or_(~needs_slot, ModelDownload.id.in_(admitted)))
        return [row[0] for row in eligible.with_entities(ModelDownload.id).order_by(ModelDownload.id.asc()).all()], new_count - len(admitted)

    @staticmethod
    def route_completed_downloads(wait_summaries=None):
        if FeederUtil.is_download_paused():
            return
        disabled_engines = FeederUtil.disabled_engine_names()
        TaskDownload._apply_pikpak_vip_hold(('downloaded', 'waiting_transporter'), 'route', FeederUtil.get_downloaders())
        queue_limit, occupied = TaskDownload._get_upload_queue_state(disabled_names=disabled_engines)
        query = TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines).filter(ModelDownload.status.in_(['downloaded', 'waiting_transporter']))
        item_ids, deferred = TaskDownload._route_candidate_ids(query, queue_limit, occupied)
        db.session.rollback()
        db.session.remove()
        other_count = 0

        def report_wait():
            if not deferred:
                return
            key = (queue_limit, len(occupied), deferred, other_count)
            if wait_summaries is not None:
                if key in wait_summaries:
                    return
                wait_summaries.add(key)
            message = f'업로드 최대 큐 {len(occupied)}/{queue_limit} · 신규 업로드 진입 보류 {deferred}건 · 기타 목적지 처리 {other_count}건'
            logger.info('[RouteComplete] %s', message)
            DownloadActivity.record('완료 항목 이송', message)

        if not item_ids:
            report_wait()
            return

        now = datetime.now()

        for item_index, item_id in enumerate(DownloadActivity.track('완료 항목 이송', item_ids), 1):
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item or item.status not in ['downloaded', 'waiting_transporter']:
                continue
            if FeederUtil.item_engine_disabled(item, disabled_engines):
                continue
            profile = FeederUtil.get_item_profile(item) or {}
            dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name, item)
            dest_type = dest_cfg.get('type') or ''
            source_path = item.local_path
            if dest_type in ('gdrive_rotation', 'remote_worker') and queue_limit > 0:
                queue_limit, occupied = TaskDownload._get_upload_queue_state(disabled_names=disabled_engines)
                if item_id not in occupied and queue_limit > 0 and len(occupied) >= queue_limit:
                    db.session.rollback()
                    deferred += 1
                    continue
            else:
                other_count += 1
            if FileDedup.check(item):
                continue
            db.session.refresh(item)
            if item.status not in ('downloaded', 'waiting_transporter'):
                continue
            if FeederUtil.item_engine_disabled(item):
                continue
            profile = FeederUtil.get_item_profile(item) or {}
            dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name, item)
            dest_type = dest_cfg.get('type') or ''
            DownloadUtil.reserve_output_folder(item)
            title = item.title

            # 로컬 단순 경로 이동: '/'로 시작하면 시스템 절대경로로 직접 처리, 상대경로인 경우에만 엔진 로컬 루트와 결합
            if dest_type == 'local' and item.current_engine_name:
                downloader_cfg = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
                local_root = DownloadUtil.get_local_root(downloader_cfg)
                complete_path = (dest_cfg.get('complete_path') or dest_cfg.get('target_folder') or '').strip()
                if complete_path:
                    dest_cfg = dict(dest_cfg)
                    if complete_path.startswith('/'):
                        # '/'로 시작하는 절대경로: mergerfs 등 지정된 시스템 절대경로 그대로 이동
                        dest_cfg['complete_path'] = os.path.abspath(complete_path)
                        logger.debug(f"[RouteComplete] 시스템 절대경로 적용: {dest_cfg['complete_path']}")
                    elif local_root:
                        # 상대경로: 엔진 설정 루트 하위 경로로 결합
                        dest_cfg['complete_path'] = os.path.join(local_root, complete_path)
                        logger.debug(f"[RouteComplete] 엔진 로컬 루트 기준 결합 경로: {complete_path} -> {dest_cfg['complete_path']}")

            transporter = DownloadUtil.get_transporter(dest_type)
            if not transporter:
                item.destination_type = dest_type
                item.status = 'waiting_transporter'
                item.completed_time = None
                item.error_message = f'이송 핸들러({dest_type})를 찾거나 초기화할 수 없습니다. 설정 복구 대기 중입니다.'
                db.session.commit()
                logger.warning(f"[RouteComplete] 이송 핸들러({dest_type}) 복구 대기: {title}")
                continue
            if item.status == 'waiting_transporter':
                item.status = 'downloaded'
                item.error_message = None
                db.session.commit()

            # 구글 드라이브 계정 풀의 경우 멀티스레드 업로드 큐(pending_upload)로 넘겨 process_uploads에서 전담 처리
            if dest_type in ('gdrive_rotation', 'remote_worker'):
                if queue_limit > 0:
                    queue_limit, occupied = TaskDownload._get_upload_queue_state(disabled_names=disabled_engines)
                    if queue_limit > 0 and item_id not in occupied and len(occupied) >= queue_limit:
                        db.session.rollback()
                        deferred += 1
                        continue
                db.session.refresh(item)
                if item.status not in ('downloaded', 'waiting_transporter'):
                    continue
                item.destination_type = dest_type
                item.gdrive_upload_path = dest_cfg.get('upload_path', '')
                item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
                item.status = 'pending_upload'
                db.session.commit()
                occupied.add(item_id)
                continue

            db.session.refresh(item)
            db.session.remove()
            logger.info(f"[RouteComplete] 이송 핸들러 [{dest_type}] 호출 시작: {title}")
            DownloadActivity.record('완료 항목 이송', f'작업 #{item_id} 이송 처리 중 · {item_index}/{len(item_ids)}번째 항목')
            try:
                if FeederUtil.item_engine_disabled(item):
                    continue
                success, next_status, msg = transporter.transport(item, source_path, dest_cfg)
                moved_path = getattr(item, 'local_path', source_path)
                db.session.remove()
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item or item.status not in ['downloaded', 'waiting_transporter']:
                    continue
                item.destination_type = dest_type
                item.local_path = moved_path
                item.status = next_status

                if next_status == 'completed':
                    if success:
                        item.mark_video_stored()
                    item.completed_time = now
                    item.error_message = None
                    FeederUtil.clear_transfer_state(item)
                    logger.info(f"[RouteComplete] 최종 완료 확정: {title} ({msg})")
                elif next_status == 'failed':
                    item.error_message = msg
                    logger.warning(f"[RouteComplete] 이송 실패: {title} ({msg})")
                else:
                    logger.info(f"[RouteComplete] 상태 전이 ({next_status}): {title} ({msg})")

            except Exception as ex:
                db.session.remove()
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item or item.status not in ['downloaded', 'waiting_transporter']:
                    continue
                item.destination_type = dest_type
                logger.error(f"[RouteComplete] 이송 핸들러 예외 ({dest_type}): {ex}")
                item.status = 'failed'
                item.error_message = f"이송 핸들러 예외: {str(ex)}"

            db.session.commit()

        report_wait()

    @staticmethod
    def process_uploads():
        if FeederUtil.is_download_paused():
            return
        downloaders = FeederUtil.get_downloaders()
        disabled_engines = FeederUtil.disabled_engine_names(downloaders)
        # 이전 비정상 종료로 멈춘 uploading 작업을 pending_upload로 자동 복구
        stuck_uploads = TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines).filter_by(status='uploading').all()
        if stuck_uploads:
            for u_it in stuck_uploads:
                FeederUtil.cooperate()
                u_it.status = 'pending_upload'
            db.session.commit()
            logger.info(f"[UploadUtil] 멈춰있던 업로드 작업 {len(stuck_uploads)}건을 대기열(pending_upload)로 자동 복구했습니다.")
        TaskDownload._apply_pikpak_vip_hold(('pending_upload',), 'upload', downloaders)

        # 현재 세션에서 실패한 항목은 쿼리에서 안전하게 제외하고, 미시도 작업을 우선 정렬
        query = TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines).filter(ModelDownload.status == 'pending_upload')
        TaskDownload._expire_source_failures(query, downloaders)
        if FeederUtil.single_download_id() is None:
            query = query.filter(or_(ModelDownload.upload_retry_after.is_(None),
                                     ModelDownload.upload_retry_after <= datetime.now()))
        if TaskDownload.failed_ids_in_session:
            query = query.filter(~ModelDownload.id.in_(list(TaskDownload.failed_ids_in_session)))

        # 최근 실패한 작업은 뒤로 미루고, 한 번도 실패하지 않은 대기 작업을 1순위로 조회
        items = query.order_by(ModelDownload.last_status_time.asc().nullsfirst(), ModelDownload.id.asc()).all()
        def destination_type(item):
            if (item.upload_state or {}).get('worker'):
                return 'remote_worker'
            if FeederUtil.transfer_destination_locked(item):
                return item.destination_type
            return FeederUtil.get_profile_destination(FeederUtil.get_item_profile(item), item=item).get('type') or ''
        if RemoteWorker.reserves_ad():
            items = [item for item in items if destination_type(item) == 'remote_worker' or not RemoteWorker.is_ad(item)]
        item_ids = [item.id for item in items]
        # 기존 접수 작업은 신규 제출 게이트와 별도로 먼저 조회한다.
        remote_items = {}
        for item in items:
            state = item.upload_state or {}
            remote = state.get('worker') or {}
            if destination_type(item) == 'remote_worker' or remote:
                remote_items[item.id] = dict(remote)
        # 신규 제출이 기다리더라도 기존 접수 작업의 조회·정산을 먼저 진행한다.
        item_ids.sort(key=lambda item_id: not bool(remote_items.get(item_id) and
            remote_items[item_id].get('submission') not in ('new', 'rejected')))
        existing_jobs = []
        for item in TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines).filter(
                ModelDownload.status.in_(['pending_upload', 'uploading']), ModelDownload.upload_state.isnot(None)).all():
            remote = (item.upload_state or {}).get('worker') or {}
            if remote and not remote.get('settled') and remote.get('submission') not in ('new', 'rejected'):
                existing_jobs.append((item.id, dict(remote)))
        existing_by_id = dict(existing_jobs)
        checked_jobs = set()
        waiting_endpoints = set()
        reconcile_pending = {identity: remote for identity, remote in existing_jobs if identity not in item_ids}
        reconcile_after = {}
        active_upload_ids = lambda: set()
        db.session.rollback()
        db.session.remove()
        if not item_ids and not reconcile_pending:
            return

        try:
            upload_workers = max(1, P.ModelSetting.get_int('download_upload_workers'))
        except Exception:
            upload_workers = 2

        logger.info(f"[UploadUtil] Google Drive 업로드 병렬 실행 (대기: {len(items)}건, 동시 워커: {upload_workers}개)")
        DownloadActivity.record('업로드', f'업로드 처리 시작 · 대기 {len(items)}건, 동시 전송 {upload_workers}개')
        DownloadActivity.record('업로드', '업로드 계정 및 사용량 확인 중')
        manager = UploadUtil.get_account_manager()
        db.session.remove()

        def reconcile(active_ids=None):
            nonlocal active_upload_ids
            if active_ids is not None:
                active_upload_ids = active_ids
            if FeederUtil.is_download_paused():
                return
            batches = {}
            for identity, remote in list(reconcile_pending.items()):
                if identity not in active_upload_ids():
                    batches.setdefault(remote.get('endpoint'), []).append((identity, remote.get('job_id')))
            for endpoint, entries in batches.items():
                cached = RemoteWorker._readiness.get(endpoint) or {}
                if cached.get('code') in ('auth', 'configuration', 'update_failed'):
                    continue
                now = time.monotonic()
                if now < max(reconcile_after.get(endpoint, 0), cached.get('after', 0)):
                    continue
                reconcile_after[endpoint] = now + 10
                for identity, job_id in entries:
                    if FeederUtil.is_download_paused():
                        return
                    current = db.session.get(ModelDownload, identity)
                    if FeederUtil.item_engine_disabled(current):
                        reconcile_pending.pop(identity, None)
                        db.session.remove()
                        continue
                    current = RemoteWorker.pending_job(identity, reconcile_pending[identity])
                    if current is None:
                        checked_jobs.add(identity)
                        reconcile_pending.pop(identity, None)
                        db.session.remove()
                        continue
                    # 새 제출·취소 없이 단건 GET 결과만 기존 검증/정산/이동 흐름에 전달한다.
                    RemoteWorker.execute(current, manager, reconcile=True)
                    current = db.session.get(ModelDownload, identity)
                    if not current or not current.remote_submission() or (current.status == 'uploading' and not current.error_message):
                        checked_jobs.add(identity)
                    if not current or not current.remote_submission() or current.status not in ('pending_upload', 'uploading'):
                        reconcile_pending.pop(identity, None)
                        if current:
                            DownloadActivity.record('원격 작업 재확인', f'작업 #{identity} 종료 결과 반영 · {current.status}')
                    db.session.remove()
                    latest = RemoteWorker._readiness.get(endpoint) or {}
                    if latest.get('after', 0) > time.monotonic() and latest.get('code') != 'full':
                        break

        def record_defer(current):
            reason = redact_log(current.error_message or '사유 미기록',
                                (P.ModelSetting.get('download_worker_token'),), limit=4000)
            reason = re.sub(r'\b[A-Za-z][A-Za-z0-9_-]*:[^\s,;]+', '[원격 경로 숨김]', reason)
            reason = re.sub(r'(?<![\w])(?:/[^\s,;]+|[A-Za-z]:[\\/][^\s,;]+)', '[경로 숨김]', reason)
            reason = ' '.join(reason.split())[:220]
            receipt = current.remote_submission()
            suffix = '원격 작업 상태 재확인 중' if receipt else '다음 실행에서 재확인'
            if suffix not in reason:
                reason += ' · ' + suffix
            DownloadActivity.record('업로드', f'작업 #{current.id} 업로드 보류 · {reason}', 'warning')

        def check_assignment(item_id):
            db.session.remove()
            current = db.session.get(ModelDownload, item_id)
            if not current or current.status != 'pending_upload' or FeederUtil.item_engine_disabled(current):
                db.session.remove()
                return False
            current_type = destination_type(current)
            state = current.upload_state or {}
            latest_remote = state.get('worker') or {}
            if current_type != 'remote_worker' and not latest_remote:
                db.session.remove()
                return True
            if not FeederUtil.transfer_destination_locked(current) and not re.fullmatch(r'[A-Za-z0-9_-]+:.+', current.local_path or ''):
                current.error_message = '원격 워커에는 원격 소스가 필요합니다. 로컬 스테이징 파일은 보존했습니다. 목적지를 복구하거나 처음부터 다시 받기를 실행해주세요.'
                db.session.commit()
                db.session.remove()
                return False
            db.session.remove()
            remote = remote_items.get(item_id, {})
            if (not RemoteWorker.same_job(latest_remote, remote) or
                    (not latest_remote and current_type != 'remote_worker')):
                return False
            remote = latest_remote
            if remote.get('settled') and state.get('phase') in ('uploaded', 'shared_incoming', 'moved'):
                return True
            client = None
            try:
                client = RemoteWorker.client(remote.get('endpoint'))
                # 이미 접수됐거나 응답이 유실된 작업은 업데이트 중에도 조회·정산한다.
                session_id = P.ModelSetting.get('download_worker_session_id')
                if (remote and remote.get('submission') not in ('new', 'rejected') and
                        (RemoteWorker._sessions.get(client.endpoint) == session_id or
                         (remote.get('payload') or {}).get('session_id') == session_id)):
                    return True
                if client.endpoint in waiting_endpoints:
                    return False
                cached = RemoteWorker._readiness.get(client.endpoint)
                if cached and time.monotonic() < cached['after']:
                    raise WorkerError(cached['message'], cached['status'], cached['code'])
                for existing_id, existing in existing_jobs:
                    if existing_id in checked_jobs or existing_id in active_upload_ids() or existing.get('endpoint') != client.endpoint:
                        continue
                    if (RemoteWorker._sessions.get(client.endpoint) != session_id and
                            (existing.get('payload') or {}).get('session_id') != session_id):
                        # 이전 실행의 404·취소·정산은 세션 전환 절차에서 확인한다.
                        continue
                    if RemoteWorker.pending_job(existing_id, existing) is None:
                        checked_jobs.add(existing_id)
                        reconcile_pending.pop(existing_id, None)
                        continue
                    db.session.remove()
                    job = client.job(existing['job_id'])
                    if job is None:
                        if RemoteWorker.pending_job(existing_id, existing) is None:
                            checked_jobs.add(existing_id)
                            reconcile_pending.pop(existing_id, None)
                            continue
                        raise RemoteWorker.wait_for_worker(client,
                            WorkerError('기존 원격 작업의 접수 여부 확인 대기', 404, 'missing_job'), existing_id)
                    RemoteWorker._validate_job(existing, job)
                    checked_jobs.add(existing_id)
                with RemoteWorker._submit_lock:
                    RemoteWorker.submission_ready(client, manager)
                return True
            except WorkerError as exc:
                if client is None:
                    # 잘못된 접속 설정도 항목마다 실패 기록을 남기지 않는다.
                    client = SimpleNamespace(endpoint='(워커 연결 설정 확인 필요)')
                    exc = WorkerError(str(exc), code='configuration')
                cached = RemoteWorker._readiness.get(client.endpoint)
                if not cached or time.monotonic() >= cached['after']:
                    RemoteWorker.wait_for_worker(client, exc)
                if exc.code in ('auth', 'configuration', 'update_failed'):
                    waiting_endpoints.add(client.endpoint)
                    return False
                return RemoteWorker._readiness[client.endpoint]
            finally:
                db.session.remove()

        def can_assign(item_id):
            # 실행 슬롯·DB 세션·제출 잠금 밖에서 같은 첫 미접수 항목을 기다린다.
            deadline = None
            while not FeederUtil.is_download_paused():
                reconcile()
                result = check_assignment(item_id)
                if isinstance(result, bool):
                    return result
                if result['code'] == 'full':
                    deadline = None
                elif deadline is None:
                    # 이전 실행의 readiness.since와 무관한 현재 항목의 연결 대기 한도다.
                    deadline = time.monotonic() + 120
                while True:
                    if FeederUtil.is_download_paused():
                        return False
                    now = time.monotonic()
                    if deadline is not None and now >= deadline:
                        timeout = WorkerError('원격 워커 대기 시간 초과', result.get('status'), 'wait_timeout')
                        timeout.wait_code = result['code']
                        timeout.wait_item_id = result.get('item_id')
                        raise timeout
                    if now >= result['after']:
                        break
                    delay = min(1.0, result['after'] - now)
                    time.sleep(min(delay, deadline - now) if deadline is not None else delay)
            return False

        def _upload_worker(item_id):
            with F.app.app_context():
                target_item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not target_item or target_item.status != 'pending_upload':
                    return
                if FeederUtil.item_engine_disabled(target_item):
                    return
                if FileDedup.check(target_item):
                    return True
                state = target_item.upload_state or {}
                destination = FeederUtil.refresh_transfer_destination(target_item)
                if not (state.get('worker') or state.get('phase')):
                    destination_error = FeederUtil.validate_destination(destination)
                    if destination_error:
                        target_item.error_message = destination_error
                        db.session.commit()
                        return
                    options = FeederUtil.get_upload_options(FeederUtil.get_item_profile(target_item), item=target_item)
                    if target_item.destination_type not in ('gdrive_rotation', 'remote_worker'):
                        target_item.status = 'downloaded'
                        db.session.commit()
                        return None
                    if (target_item.destination_type != 'remote_worker' and options['use_local_staging'] and
                            target_item.local_path and not os.path.exists(target_item.local_path) and ':' in target_item.local_path):
                        target_item.status = 'pending_local_staging'
                        db.session.commit()
                        return None
                file_name = target_item.file_name
                DownloadActivity.record('업로드', f'작업 #{item_id} 업로드 처리 시작')
                if target_item.destination_type == 'remote_worker' or (target_item.upload_state or {}).get('worker'):
                    success = RemoteWorker.execute(target_item, manager)
                else:
                    success = UploadUtil.execute_upload(target_item, manager)
                if FeederUtil.item_engine_disabled(db.session.get(ModelDownload, item_id)):
                    return
                expected = existing_by_id.get(item_id)
                if expected and RemoteWorker.pending_job(item_id, expected) is None:
                    checked_jobs.add(item_id)
                    reconcile_pending.pop(item_id, None)
                if success is None:
                    TaskDownload.failed_ids_in_session.add(item_id)
                    current = db.session.get(ModelDownload, item_id)
                    if current and current.status == 'pending_upload':
                        if current.remote_submission():
                            reconcile_pending[item_id] = dict((current.upload_state or {})['worker'])
                        record_defer(current)
                    return
                current = db.session.get(ModelDownload, item_id)
                outcome = ('완료' if success else '최종 실패' if current and current.status == 'failed'
                           else '원격 작업 실패' if current and current.status == 'move_failed'
                           else '보류 · 다음 실행에서 재확인')
                if not success and current and current.status == 'pending_upload':
                    if current.remote_submission():
                        reconcile_pending[item_id] = dict((current.upload_state or {})['worker'])
                    record_defer(current)
                elif success or not (current and current.upload_retry_after and current.destination_type == 'remote_worker'):
                    DownloadActivity.record('업로드', f'작업 #{item_id} 업로드 처리 {outcome}', 'info' if success else 'warning')
                if not success:
                    TaskDownload.failed_ids_in_session.add(item_id)
                    db.session.remove()
                    current = db.session.get(ModelDownload, item_id)
                    state = current.status if current else 'deleted'
                    if state == 'pending_upload':
                        if current.destination_type == 'remote_worker' or (current.upload_state or {}).get('worker'):
                            logger.debug('[UploadUtil] 원격 작업 %s 일시 대기: %s', item_id, current.error_message or '다음 실행에서 재확인')
                        else:
                            logger.warning('[UploadUtil] %s 업로드 보류 -> 다음 실행에서 재확인 (%s)', file_name,
                                           current.transfer_error_type or '설정·계정 대기')
                    else:
                        logger.warning('[UploadUtil] %s 업로드 미완료 -> 현재 상태: %s', file_name, state)
                    db.session.remove()

        for item_id, future in TaskDownload._worker_results(_upload_worker, item_ids, upload_workers,
                                                          activity_stage='업로드', can_assign=can_assign, on_wait=reconcile):
            FeederUtil.cooperate()
            try:
                future.result()
            except Exception as th_err:
                DownloadActivity.record('업로드', f'작업 #{item_id} 처리 중 오류가 발생했습니다. 플랫폼 로그를 확인하세요.', 'error')
                logger.error(f"[UploadUtil] 업로드 워커 스레드 예외: {th_err}")

    @staticmethod
    def retry_move_failed():
        if FeederUtil.is_download_paused():
            return
        one_hour_ago = datetime.now() - timedelta(hours=1)
        disabled_engines = FeederUtil.disabled_engine_names()
        queue_limit, occupied = TaskDownload._get_upload_queue_state(disabled_names=disabled_engines)
        item_ids = [row[0] for row in (
            TaskDownload._enabled_engine_query(TaskDownload._selected_query(db.session.query(ModelDownload)), disabled_engines)
            .filter(
                ModelDownload.status == 'move_failed',
                ModelDownload.completed_time.is_(None),
                or_(ModelDownload.last_move_attempt_time.is_(None), ModelDownload.last_move_attempt_time < one_hour_ago,
                    FeederUtil.single_download_id() is not None)
            )
            .with_entities(ModelDownload.id).order_by(ModelDownload.id.asc()).all()
        )]
        if not item_ids:
            return
        if queue_limit > 0 and len(occupied) >= queue_limit and not occupied.intersection(item_ids):
            db.session.remove()
            message = f'업로드 최대 큐 {len(occupied)}/{queue_limit} · 이동 복구 신규 슬롯 {len(item_ids)}건 일괄 대기'
            logger.info('[UploadUtil] %s', message)
            DownloadActivity.record('이동 복구', message)
            return
        db.session.remove()
        manager = UploadUtil.get_account_manager()
        db.session.remove()
        deferred = 0
        for item_id in DownloadActivity.track('이동 복구', item_ids):
            FeederUtil.cooperate()
            if FeederUtil.is_download_paused():
                return
            if queue_limit > 0 and item_id not in occupied and len(occupied) >= queue_limit:
                deferred += 1
                continue
            if queue_limit > 0:
                queue_limit, occupied = TaskDownload._get_upload_queue_state(disabled_names=disabled_engines)
                if queue_limit > 0 and item_id not in occupied and len(occupied) >= queue_limit:
                    db.session.remove()
                    deferred += 1
                    continue
            target = db.session.query(ModelDownload).filter_by(id=item_id, status='move_failed').first()
            if not target:
                db.session.remove()
                continue
            if FeederUtil.item_engine_disabled(target):
                db.session.remove()
                continue
            target.last_move_attempt_time = datetime.now()
            db.session.commit()
            db.session.remove()
            target = db.session.query(ModelDownload).filter_by(id=item_id, status='move_failed').first()
            if target:
                DownloadActivity.record('이동 복구', f'작업 #{item_id} 이동 복구 또는 재업로드 처리 중')
                UploadUtil.retry_failed_upload(target, manager)
                if queue_limit > 0:
                    db.session.remove()
                    queue_limit, occupied = TaskDownload._get_upload_queue_state(disabled_names=disabled_engines)
        if deferred:
            message = f'업로드 최대 큐 {len(occupied)}/{queue_limit} · 이동 복구 신규 슬롯 {deferred}건 일괄 대기'
            logger.info('[UploadUtil] %s', message)
            DownloadActivity.record('이동 복구', message)

    @staticmethod
    def check_pending_relays():
        """큐에 방치된 pending_relay 항목이 있고 현재 가동 중인 워커가 없다면 자동 재기동"""
        if FeederUtil.is_download_paused():
            return
        try:
            disabled_engines = FeederUtil.disabled_engine_names()
            query = TaskDownload._enabled_engine_query(db.session.query(ModelDownload), disabled_engines)
            busy_count = query.filter_by(status='relay_transferring').count()
            pending_items = query.filter_by(status='pending_relay').all()

            if not pending_items and busy_count == 0:
                return

            if busy_count > 0:
                logger.info(f"[DownloadPipeline] 현재 릴레이 전송 진행 중인 작업({busy_count}건)이 있어 대기합니다. (대기열: {len(pending_items)}건)")
                DownloadActivity.record('릴레이 점검', f'진행 중 {busy_count}건 · 대기 {len(pending_items)}건 · 기존 전송 완료 대기')
                return

            logger.info(f"[DownloadPipeline] 미완료 릴레이 대기 작업 {len(pending_items)}건 감지 ➔ 워커 자동 기동 호출")

            triggered_dest_types = set()
            for it in DownloadActivity.track('릴레이 점검', pending_items):
                FeederUtil.cooperate()
                if FeederUtil.is_download_paused():
                    return
                dest_type = it.destination_type
                if not dest_type or dest_type in triggered_dest_types:
                    continue

                profile = FeederUtil.get_item_profile(it) or {}
                dest_cfg = FeederUtil.get_profile_destination(profile, it.current_engine_name, it)
                transporter = DownloadUtil.get_transporter(dest_type)

                if transporter and hasattr(transporter, 'resume_relay'):
                    if FeederUtil.item_engine_disabled(it):
                        continue
                    DownloadActivity.record('릴레이 점검', f'작업 #{it.id}의 릴레이 워커 기동 요청 중')
                    transporter.resume_relay(dest_cfg)
                    triggered_dest_types.add(dest_type)

        except Exception as ex:
            logger.error(f"[DownloadPipeline] 잔여 릴레이 점검 중 예외: {ex}")
