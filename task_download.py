# -*- coding: utf-8 -*-
import os
import re
import time
import shutil
import subprocess
import threading
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from sqlalchemy import or_

from .setup import *
from .model_crawl import ModelCrawlItem
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil
from .util_feed import FeedUtil
from .util_download import DownloadUtil
from .util_upload import UploadUtil


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

        TaskDownload.run_pipeline()


class TaskDownload:

    # 현재 실행 세션 내 실패/중단된 작업 ID 집합 (현재 주기 내 즉시 재시도 차단용)
    failed_ids_in_session = set()
    _run_mutex = threading.Lock()

    @staticmethod
    def run_pipeline(manual: bool = False):
        if not TaskDownload._run_mutex.acquire(blocking=False):
            logger.info("[DownloadPipeline] 이미 실행 중인 다운로드 파이프라인이 있어 중복 실행을 건너뜁니다.")
            return

        owner = uuid.uuid4().hex
        db_lock_acquired = False
        try:
            with F.app.app_context():
                FeederUtil.init_runtime_locks()
                db_lock_acquired = FeederUtil.acquire_runtime_lock('download_pipeline', owner)
                if not db_lock_acquired:
                    logger.info("[DownloadPipeline] 다른 Celery 워커가 실행 중이어서 중복 실행을 건너뜁니다.")
                    return
                P.ModelSetting.set('download_is_running', 'True')
                P.ModelSetting.set('download_running_start_time', str(int(time.time())))

            TaskDownload._run_pipeline_locked(manual=manual)
        finally:
            if db_lock_acquired:
                with F.app.app_context():
                    FeederUtil.release_runtime_lock('download_pipeline', owner)
                    P.ModelSetting.set('download_is_running', 'False')
                    P.ModelSetting.set('download_running_start_time', '0')
            TaskDownload._run_mutex.release()
            FeederUtil.db_checkpoint()

    @staticmethod
    def _run_pipeline_locked(manual: bool = False):
        with F.app.app_context():
            try:
                mode_str = "수동 실행" if manual else "스케쥴러 자동 실행"
                logger.info(f"[DownloadPipeline] 다운로드 파이프라인 가동 ({mode_str})")

                # 현재 큐 현황 집계 및 요약 최우선 출력
                q_counts = {
                    'pending': db.session.query(ModelDownload).filter(ModelDownload.status == 'pending').count(),
                    'downloading': db.session.query(ModelDownload).filter(ModelDownload.status == 'downloading').count(),
                    'staging': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count(),
                    'downloaded': db.session.query(ModelDownload).filter(ModelDownload.status == 'downloaded').count(),
                    'relay': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_relay', 'relay_transferring'])).count(),
                    'uploading': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count(),
                    'failed': db.session.query(ModelDownload).filter(ModelDownload.status.in_(['failed', 'move_failed'])).count(),
                }
                total_active = q_counts['pending'] + q_counts['downloading'] + q_counts['staging'] + q_counts['downloaded'] + q_counts['relay'] + q_counts['uploading']
                logger.info(f"[DownloadPipeline] 현재 큐 요약 (활성 {total_active}건) | 대기:{q_counts['pending']} | 진행:{q_counts['downloading']} | 스테이징:{q_counts['staging']} | 다운완료:{q_counts['downloaded']} | 릴레이:{q_counts['relay']} | 업로드:{q_counts['uploading']} | 실패:{q_counts['failed']}")
                db.session.rollback()

                profiles = FeederUtil.get_download_profiles()
                if not profiles:
                    logger.debug("[DownloadPipeline] 등록된 다운로드 프로필(DOWNLOAD_PROFILES)이 없습니다.")
                    return

                # SA 내 드라이브 고아 파일 사전 정리 및 용량 확보
                UploadUtil.drain_all_sa_mydrives()

                # 피드 최신 데이터 동기화 및 큐 등록
                TaskDownload.sync_feed_items(profiles)

                # 활성 큐 작업들에 대해 현재 설정된 최신 프로필 동적 동기화
                TaskDownload.sync_active_items_with_profiles()

                # 현재 실행 세션 실패 목록 초기화
                TaskDownload.failed_ids_in_session.clear()

                # 단일 스케줄 내 연쇄 관통 루프 (최대 3회 패스)
                max_cascade_passes = 3
                for cascade_pass in range(1, max_cascade_passes + 1):
                    pass_start_time = time.time()
                    pending_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending').count()
                    downloading_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloading').count()
                    staging_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count()
                    downloaded_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloaded').count()
                    uploading_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count()

                    total_active_before = pending_cnt + downloading_cnt + staging_cnt + downloaded_cnt + uploading_cnt
                    db.session.rollback()
                    if total_active_before == 0 and cascade_pass > 1:
                        break

                    logger.info(f"[DownloadPipeline] === 연쇄 패스 {cascade_pass}/{max_cascade_passes} 시작 (활성 작업: {total_active_before}개) ===")

                    # 다운로더 큐 디스패치
                    if pending_cnt > 0:
                        logger.info(f"[DownloadPipeline] 다운로더 큐 디스패치 시작 (대기: {pending_cnt}건)")
                        TaskDownload.dispatch_pending_downloads()

                    # 활성 다운로드 엔진 상태 점검
                    if downloading_cnt > 0:
                        logger.info(f"[DownloadPipeline] 활성 다운로드 엔진 상태 점검 (진행: {downloading_cnt}건)")
                    TaskDownload.poll_active_downloads()

                    # 로컬 스테이징 (대기 항목 존재 시 내부에서 실시간 쿼리하여 처리)
                    TaskDownload.process_local_staging()

                    # 다운로드 완료 항목 이송 라우팅 (로컬 스테이징 완료 또는 다이렉트 전송 대기 항목)
                    TaskDownload.route_completed_downloads()

                    # Google Drive 업로드 파이프라인 (pending_upload 실시간 쿼리하여 즉시 전송)
                    TaskDownload.process_uploads()

                    # 해당 패스에서 실제 상태 전이가 발생했는지 판별
                    staging_after = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count()
                    downloaded_after = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloaded').count()
                    uploading_after = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count()
                    db.session.rollback()

                    pass_elapsed = time.time() - pass_start_time
                    logger.info(f"[DownloadPipeline] === 연쇄 패스 {cascade_pass}/{max_cascade_passes} 완료 (소요시간: {pass_elapsed:.1f}초) ===")

                    # 스테이징, 이송, 업로드 등 활성 파이프라인에서 처리할 수량이 더 이상 없으면 즉시 패스 종료
                    if (staging_after + downloaded_after + uploading_after) == 0:
                        break

                # 이전 실패 항목 재시도
                TaskDownload.retry_move_failed()

                # 대기 중인 원격 릴레이(Colab 등) 작업 점검 및 워커 트리거
                TaskDownload.check_pending_relays()

                logger.info(f"[DownloadPipeline] 다운로드 파이프라인 주기 완료 ({mode_str})")

            except Exception as e:
                logger.error(f"[DownloadPipeline] 파이프라인 처리 중 오류: {e}")
                logger.error(traceback.format_exc())

    @staticmethod
    def sync_feed_items(profiles: list[dict]):
        """피드(FEEDS) 테이블에 적재된 최신 아이템을 다운로드 큐(ModelDownload)에 동기화"""
        from .model_feed import ModelFeedItem

        start_time = time.time()
        all_feeds = FeederUtil.get_feeds()
        new_items_count = 0
        logger.info(f"[DownloadPipeline] 피드 동기화 및 신규 다운로드 작업 검토 시작 (대상 피드: {len(all_feeds)}개, 프로필: {len(profiles)}개)")

        for profile in profiles:
            target_feed_names = profile.get('feeds', [])
            priority_chain = profile.get('priority_chain', [])
            if not priority_chain:
                continue

            destination_cfg = profile.get('destination', {})
            dest_type = destination_cfg.get('type', 'local')

            for feed in all_feeds:
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

                for feed_item in candidates:
                    feed_dict = feed_item.as_dict()
                    magnets = feed_dict.get('magnet', [])
                    if not magnets:
                        continue

                    # 마그넷(BTIH) 우선 선별, 부재 시 ed2k 채택
                    target_mag = None
                    for m in magnets:
                        if str(m).lower().startswith('magnet:'):
                            target_mag = m
                            break
                    if not target_mag:
                        target_mag = magnets[0]

                    infohash = FeederUtil.extract_info_hash(target_mag)

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
                    dl_item.gdrive_upload_path = destination_cfg.get('upload_path', '')
                    dl_item.gdrive_complete_path = destination_cfg.get('complete_path', '')
                    dl_item.gdrive_remote_id = destination_cfg.get('shared_drive_id', '')

                    db.session.add(dl_item)
                    db.session.commit()
                    new_items_count += 1

        elapsed = time.time() - start_time
        if new_items_count > 0:
            logger.info(f"[DownloadPipeline] 피드 동기화 완료: 신규 {new_items_count}건 큐 등록 (소요시간: {elapsed:.1f}초)")

    @staticmethod
    def sync_active_items_with_profiles():
        """활성 큐 작업들에 대해 현재 설정된 최신 프로필(목적지, 경로, 우선순위체인)을 동적으로 반영하고 상태를 리라우팅"""
        active_statuses = [
            'pending', 'downloading', 'pending_local_staging', 'local_staging',
            'downloaded', 'pending_upload', 'uploading', 'pending_relay', 'relay_transferring'
        ]
        items = db.session.query(ModelDownload).filter(ModelDownload.status.in_(active_statuses)).all()
        if not items:
            return

        start_time = time.time()
        logger.info(f"[DownloadPipeline] 활성 큐 {len(items)}건 최신 프로필 동적 동기화 시작")

        # 루프 전 프로필 목록 1회 메모리 캐싱 (1,600번의 반복적인 YAML 디스크 I/O 제거)
        all_profiles = FeederUtil.get_download_profiles()
        global_staging_opt = P.ModelSetting.get_bool('download_use_local_staging') if P.ModelSetting else True
        shared_drive_default_id = P.ModelSetting.get('download_shared_drive_id') or ''

        updated_count = 0
        for item in items:
            # 메모리 캐시에서 프로필 매핑 O(1) 고속 검색
            profile = None
            target_feed = str(item.feed_name or '').strip().lower()
            for p in all_profiles:
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

            # 기본 목적지보다 엔진별 지정 목적지(destination_by_engine)를 최우선 적용
            dest_cfg = FeederUtil.get_profile_destination(profile, effective_engine)
            current_dest_type = dest_cfg.get('type', 'local')
            current_chain = profile.get('priority_chain', [])
            engine_cfg = FeederUtil.get_downloader_by_name(effective_engine) or {}

            # 우선순위 체인 동적 동기화
            if item.status == 'pending' and current_chain and item.priority_chain != current_chain:
                item.priority_chain = current_chain

            # 목적지 세부 설정 동적 동기화
            dest_changed = (item.destination_type != current_dest_type)
            item.destination_type = current_dest_type
            item.gdrive_upload_path = dest_cfg.get('upload_path', '')
            item.gdrive_complete_path = dest_cfg.get('complete_path', '')
            item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or shared_drive_default_id

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
                logger.info(f"[DownloadPipeline] 설정 변경 감지: 로컬 스테이징 해제 -> 다이렉트(on-the-fly) 전송 대기(downloaded)로 리라우팅 -> {item.title}")
                updated_count += 1

            elif not is_relay_dest and is_remote_src and use_local_staging and item.status == 'downloaded' and not os.path.exists(src_path):
                item.status = 'pending_local_staging'
                logger.info(f"[DownloadPipeline] 설정 변경 감지: 로컬 스테이징 적용 -> 로컬 스테이징(pending_local_staging)으로 리라우팅 -> {item.title}")
                updated_count += 1

            elif is_relay_dest and item.status in ['pending_local_staging', 'downloaded', 'pending_upload']:
                item.status = 'pending_relay'
                logger.info(f"[DownloadPipeline] 프로필 변경 감지: 최신 목적지({current_dest_type})에 맞춰 원격 릴레이 대기(pending_relay)로 리라우팅 -> {item.title}")
                updated_count += 1

            elif dest_changed:
                updated_count += 1

        if updated_count > 0:
            db.session.commit()

        elapsed = time.time() - start_time
        if updated_count > 0:
            logger.info(f"[DownloadPipeline] 활성 큐 {len(items)}건 프로필 동적 동기화 완료 (갱신: {updated_count}건, 소요시간: {elapsed:.1f}초)")

    @staticmethod
    def dispatch_pending_downloads():
        items = ModelDownload.get_list_by_status(['pending'])
        if not items:
            return
        pending_snapshots = []
        for item in items:
            pending_snapshots.append({
                'id': item.id,
                'priority_chain': list(item.priority_chain or []),
                'current_engine_index': item.current_engine_index or 0,
                'magnet': item.magnet,
                'title': item.title,
                'feed_name': item.feed_name,
                'gdrive_upload_path': item.gdrive_upload_path,
            })
        db.session.rollback()

        # 다운로더 엔진별 실제 원격 진행 중 큐(In Progress: 다운로딩 + 대기열 전체) 사전 집계
        engine_in_progress_counts = {}
        full_engines = set()
        engine_instances = {}
        engine_status_maps = {}

        for dl in FeederUtil.get_downloaders():
            e_name = dl.get('name')
            if not e_name or not dl.get('enabled', True):
                continue

            try:
                e_limit = int(dl.get('max_active_tasks') or 0)
            except Exception:
                e_limit = 0

            engine_inst = DownloadUtil.create_engine(dl)
            if not engine_inst:
                continue
            engine_instances[e_name] = engine_inst

            # 모든 엔진 공통: 원격 작업 목록을 1회 사전 조회하여 infohash 맵 구성
            if hasattr(engine_inst, 'get_status'):
                try:
                    remote_tasks, _ = engine_inst.get_status()
                    if remote_tasks:
                        task_map = {}
                        for t in remote_tasks:
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
                    logger.debug(f"[DownloadDispatch] [{e_name}] 원격 큐 사전 점검 예외: {chk_err}")

        # 원격 조회가 불가했던 엔진용 로컬 DB 백업 집계
        if not engine_in_progress_counts:
            active_counts_query = (
                db.session.query(ModelDownload.current_engine_name, func.count(ModelDownload.id))
                .filter_by(status='downloading')
                .group_by(ModelDownload.current_engine_name)
                .all()
            )
            for r in active_counts_query:
                if r[0] not in engine_in_progress_counts:
                    engine_in_progress_counts[r[0]] = int(r[1])
            db.session.rollback()

        for snapshot in pending_snapshots:
            item_id = snapshot['id']
            chain = snapshot['priority_chain']
            curr_idx = snapshot['current_engine_index']

            if curr_idx >= len(chain):
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item:
                    continue
                item.status = 'failed'
                item.error_message = '모든 다운로더 우선순위 체인 소진'
                db.session.commit()
                logger.warning(f"[DownloadDispatch] 다운로더 체인 소진으로 실패 처리: {snapshot['title']}")
                continue

            engine_name = chain[curr_idx]

            # 이미 원격 큐(In Progress) 한도에 도달한 엔진이면 신규 요청 없이 즉시 통과
            if engine_name in full_engines:
                continue

            downloader_cfg = FeederUtil.get_downloader_by_name(engine_name)
            if not downloader_cfg or not downloader_cfg.get('enabled', True):
                # 엔진 일시 비활성 시 다음 엔진으로 폴백하지 않고 대기(pending) 유지
                logger.debug(f"[DownloadDispatch] 다운로더 [{engine_name}] 비활성 상태 -> 대기열(pending) 유지: {snapshot['title']}")
                continue

            try:
                engine_limit = int(downloader_cfg.get('max_active_tasks') or 0)
            except Exception:
                engine_limit = 0

            current_count = engine_in_progress_counts.get(engine_name, 0)
            if engine_limit > 0 and current_count >= engine_limit:
                full_engines.add(engine_name)
                logger.info(f"[DownloadDispatch] [{engine_name}] 진행중 큐 한도({engine_limit}건) 도달 (현재 In Progress: {current_count}건) -> 신규 추가 일시 대기")
                continue

            engine = DownloadUtil.create_engine(downloader_cfg)
            if not engine:
                continue

            target_hash = FeederUtil.extract_info_hash(snapshot['magnet'])
            target_hash_lower = target_hash.lower() if target_hash else ""
            existing_task = engine_status_maps.get(engine_name, {}).get(target_hash_lower) if target_hash_lower else None

            # 모든 엔진 공통: 원격 엔진에 이미 존재하는 작업인 경우 표준 규격에 따라 연동/재시작
            if existing_task:
                existing_tid = str(existing_task.get('task_id') or existing_task.get('id') or '')
                std_status = existing_task.get('status')

                if std_status == 'error':
                    restarted = False
                    if hasattr(engine, 'restart_task'):
                        try:
                            restarted = engine.restart_task(existing_tid)
                        except Exception as ex:
                            logger.debug(f"[DownloadDispatch] [{engine_name}] 기존 작업 재시작 예외: {ex}")

                    if not restarted:
                        # 원격 엔진이 재시작을 거부함 (AllDebrid no peer 등) -> 원격 작업 정리 후 다음 엔진으로 즉시 폴백
                        logger.warning(f"[DownloadDispatch] [{engine_name}] 원격 에러 작업 재시작 불가 -> 다음 엔진 폴백: {snapshot['title']} (TaskID: {existing_tid})")
                        try:
                            engine.delete_task(existing_tid)
                        except Exception:
                            pass
                        item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                        if item:
                            item.current_engine_index = curr_idx + 1
                            item.status = 'pending'
                            item.engine_task_id = None
                            item.error_message = f"[{engine_name}] 원격 작업 재시작 거부 (다음 엔진 폴백)"
                            db.session.commit()
                        continue

                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진에 에러 상태로 존재하는 작업 감지 -> 재시작(restart) 성공 및 연동: {snapshot['title']} (TaskID: {existing_tid})")
                elif std_status == 'completed':
                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진에 이미 완료 상태로 존재하는 작업 감지 -> 작업 연동: {snapshot['title']} (TaskID: {existing_tid})")
                else:
                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진에 이미 진행 중인 작업 감지 -> 작업 연동: {snapshot['title']} (TaskID: {existing_tid})")

                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if item:
                    item.status = 'downloading'
                    item.current_engine_name = engine_name
                    item.engine_task_id = existing_tid
                    item.engine_added_time = datetime.now()
                    item.last_status_time = datetime.now()
                    item.error_message = None

                    # 할당된 엔진의 개별 목적지 설정 즉시 반영
                    profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
                    dest_cfg = FeederUtil.get_profile_destination(profile, engine_name)
                    item.destination_type = dest_cfg.get('type', 'local')
                    item.gdrive_upload_path = dest_cfg.get('upload_path', '')
                    item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                    item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or ''

                    engine_in_progress_counts[engine_name] = current_count + 1
                    db.session.commit()
                continue

            # 링크 프로토콜(ed2k vs magnet) 지원 여부 판별
            is_ed2k = str(snapshot['magnet']).lower().startswith('ed2k://')
            target_proto = "ed2k" if is_ed2k else "magnet"
            supported_protos = getattr(engine, 'SUPPORTED_PROTOCOLS', ['magnet'])
            if target_proto not in supported_protos:
                logger.debug(f"[DownloadDispatch] [{engine_name}] {target_proto} 프로토콜 미지원 -> 다음 순위 엔진으로 즉시 폴백: {snapshot['title']}")
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if item:
                    item.current_engine_index = curr_idx + 1
                    db.session.commit()
                continue

            profile = FeederUtil.get_download_profile_by_feed(snapshot['feed_name']) or {}
            dest_cfg = FeederUtil.get_profile_destination(profile, engine_name)
            current_upload_path = (snapshot['gdrive_upload_path'] or dest_cfg.get('upload_path') or '').strip('/')

            success, task_id, err = DownloadUtil.add_magnet(
                engine,
                snapshot['magnet'],
                title=snapshot['title'],
                upload_path=current_upload_path
            )
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item:
                continue
            if success:
                item.status = 'downloading'
                item.current_engine_name = engine_name
                item.engine_task_id = str(task_id)
                item.engine_added_time = datetime.now()
                item.last_status_time = datetime.now()
                item.error_message = None
                
                # 할당된 엔진의 개별 목적지 설정 즉시 반영
                dest_cfg = FeederUtil.get_profile_destination(profile, engine_name)
                item.destination_type = dest_cfg.get('type', 'local')
                item.gdrive_upload_path = dest_cfg.get('upload_path', '')
                item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or ''

                engine_in_progress_counts[engine_name] = current_count + 1

                db.session.commit()
                logger.info(f"[DownloadDispatch] [{engine_name}] 작업 추가 성공: {snapshot['title']} (TaskID: {task_id})")
            else:
                err_str = str(err)
                err_lower = err_str.lower()
                is_limit_error = any(k in err_lower for k in ["maximum allowed", "limit", "too many", "active magnets", "trial", "queue full"])

                if is_limit_error:
                    full_engines.add(engine_name)
                    item.error_message = f"[{engine_name}] 큐 한도 도달 대기: {err_str}"
                    db.session.commit()
                    logger.info(f"[DownloadDispatch] [{engine_name}] 원격 엔진 활성 한도 도달 ({err_str}) -> 신규 추가 일시 중단 및 대기열(pending) 유지: {snapshot['title']}")
                else:
                    # no peer, 토렌트 오류 등으로 추가 거부된 경우 다음 우선순위 엔진으로 즉시 폴백
                    item.current_engine_index = curr_idx + 1
                    item.status = 'pending'
                    item.engine_task_id = None
                    
                    # 차순위 엔진이 존재하면 해당 엔진의 목적지 설정으로 선제 동기화
                    if curr_idx + 1 < len(chain):
                        next_engine = chain[curr_idx + 1]
                        next_dest_cfg = FeederUtil.get_profile_destination(profile, next_engine)
                        item.destination_type = next_dest_cfg.get('type', 'local')

                    item.error_message = f"[{engine_name}] 추가 실패 ({err_str}) -> 다음 엔진 폴백"
                    db.session.commit()
                    logger.warning(f"[DownloadDispatch] [{engine_name}] 마그넷 추가 불가 ({err_str}) -> 다음 엔진 폴백: {snapshot['title']}")

        db.session.commit()

    @staticmethod
    def poll_active_downloads():
        items = ModelDownload.get_list_by_status(['downloading'], limit=100)

        if not items:
            return

        items_by_engine = {}
        for it in items:
            e_name = it.current_engine_name
            if e_name not in items_by_engine:
                items_by_engine[e_name] = []
            items_by_engine[e_name].append(it)

        now = datetime.now()

        for engine_name, engine_items in items_by_engine.items():
            downloader_cfg = FeederUtil.get_downloader_by_name(engine_name)
            if not downloader_cfg:
                continue

            engine = DownloadUtil.create_engine(downloader_cfg)
            if not engine:
                continue

            # 원격 상태 조회가 DB의 활성 읽기 트랜잭션을 기다리지 않도록 worklist 조회를 종료
            db.session.rollback()
            status_list, err = engine.get_status()
            if err:
                logger.warning(f"[DownloadPoll] [{engine_name}] 상태 조회 실패: {err}")
                continue

            status_map_by_tid = {str(s.get('task_id')): s for s in status_list if s.get('task_id')}
            status_map_by_hash = {str(s.get('hash')).lower(): s for s in status_list if s.get('hash')}

            # 엔진 히스토리에서 완료 항목을 먼저 역추적한다. Feeder 쪽 상태가
            # downloading이 아니거나 current_engine_name이 유실된 경우에도
            # hash/task ID로 원래 큐 항목을 찾아 완료 전이를 적용한다.
            completed_statuses = [s for s in status_list if s.get('status') == 'completed']
            reverse_items = []
            if completed_statuses:
                completed_hashes = {
                    str(s.get('hash')).lower() for s in completed_statuses if s.get('hash')
                }
                completed_task_ids = {
                    str(s.get('task_id')) for s in completed_statuses if s.get('task_id')
                }
                match_filters = []
                if completed_hashes:
                    match_filters.append(ModelDownload.infohash.in_(completed_hashes))
                if completed_task_ids:
                    match_filters.append(ModelDownload.engine_task_id.in_(completed_task_ids))
                if match_filters:
                    reverse_items = db.session.query(ModelDownload).filter(
                        ModelDownload.status.in_(['downloading', 'pending']),
                        or_(
                            ModelDownload.current_engine_name == engine_name,
                            ModelDownload.current_engine_name.is_(None)
                        ),
                        or_(*match_filters)
                    ).all()
                    known_ids = {item.id for item in engine_items}
                    for reverse_item in reverse_items:
                        if reverse_item.id not in known_ids:
                            reverse_item.current_engine_name = engine_name
                            engine_items.append(reverse_item)
                    if reverse_items:
                        db.session.rollback()
                        logger.info(
                            f"[DownloadPoll] [{engine_name}] 완료 히스토리 역추적 매칭: "
                            f"{len(reverse_items)}건"
                        )

            # CD2 등 히스토리가 쌓이는 엔진의 경우: Feeder에서 이미 최종 완료(completed)된 작업들을 CD2 히스토리에서 일괄 삭제 정리
            if engine_name == 'cd2' and hasattr(engine, 'delete_tasks'):
                completed_hashes = [s.get('hash').lower() for s in status_list if s.get('status') == 'completed' and s.get('hash')]
                if completed_hashes:
                    already_done_rows = db.session.query(ModelDownload.infohash, ModelDownload.engine_task_id).filter(
                        ModelDownload.status == 'completed',
                        ModelDownload.infohash.in_(completed_hashes)
                    ).all()

                    cleanup_ids = set()
                    for r_hash, r_tid in already_done_rows:
                        if r_hash:
                            cleanup_ids.add(r_hash)
                        elif r_tid:
                            cleanup_ids.add(r_tid)

                    if cleanup_ids:
                        db.session.rollback()
                        logger.info(f"[DownloadPoll] [cd2] Feeder에서 이미 최종 완료(completed)된 작업 {len(cleanup_ids)}건 감지 -> CD2 오프라인 히스토리 자동 정리")
                        engine.delete_tasks(list(cleanup_ids))

            matched_count = sum(1 for it in engine_items if (str(it.engine_task_id) in status_map_by_tid or (it.infohash and str(it.infohash).lower() in status_map_by_hash)))
            logger.debug(f"[DownloadPoll] [{engine_name}] 전체 토렌트 {len(status_list)}건 수신 (큐 매칭: {matched_count}건, 미등록 잔여: {len(status_list) - matched_count}건)")

            try:
                stalled_hours = int(downloader_cfg.get('stalled_timeout_hours', 24))
            except Exception:
                stalled_hours = 24

            for it in engine_items:
                matched_status = status_map_by_tid.get(str(it.engine_task_id))
                if not matched_status and it.infohash:
                    matched_status = status_map_by_hash.get(str(it.infohash).lower())

                if not matched_status:
                    continue

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
                    it.local_path = source_path

                    if fname:
                        duplicate_item = (
                            db.session.query(ModelDownload.id)
                            .filter(ModelDownload.file_name == fname, ModelDownload.id != it.id)
                            .first()
                        )
                        if duplicate_item:
                            short_hash = (it.infohash[:6] if it.infohash else "dup")
                            n, e = os.path.splitext(fname)
                            fname = f"{n}_{short_hash}{e}" if e else f"{fname}_{short_hash}"
                            it.file_name = fname
                            logger.info(f"[DownloadPoll] 동일 파일명 감지 -> '{fname}' 분리: {it.title}")
                    # CD2(115 클라우드) 엔진인 경우 마운트 경로 실물 파일 검증 및 경로 매칭
                    if engine_name == 'cd2' or getattr(engine, 'OUTPUT_TYPE', '') == 'cloud_storage':
                        mount_root = (downloader_cfg.get('cd2_mount_path') or '').strip()
                        target_search_name = fname or it.file_name or it.title
                        matched_real_path = ""

                        if mount_root and os.path.exists(mount_root):
                            # 마운트 루트 직하 및 서브폴더에서 실물 파일/폴더 탐색
                            candidate_path = os.path.join(mount_root, target_search_name)
                            if os.path.exists(candidate_path):
                                matched_real_path = candidate_path
                            else:
                                for r, dirs, files in os.walk(mount_root):
                                    if target_search_name in dirs or target_search_name in files:
                                        matched_real_path = os.path.join(r, target_search_name)
                                        break

                        if not matched_real_path:
                            # 115 원격 완료되었으나 아직 로컬 마운트에 출현하지 않은 경우 성급히 넘기지 않고 대기 유지
                            logger.debug(f"[DownloadPoll] [{engine_name}] 115 완료 감지 -> 마운트 경로 파일 출현 대기 중: {target_search_name}")
                            continue

                        # 실물 파일이 확인된 경우 실제 경로 및 용량 매핑
                        it.local_path = matched_real_path
                        source_path = matched_real_path
                        if os.path.isfile(matched_real_path):
                            it.file_size = os.path.getsize(matched_real_path)
                        else:
                            it.file_size = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(matched_real_path) for f in fs)
                        logger.info(f"[DownloadPoll] [{engine_name}] 마운트 실물 파일 확인 완료 ({target_search_name}, {FeederUtil.format_bytes(it.file_size)}) -> 인계 준비")

                    profile = FeederUtil.get_download_profile_by_feed(it.feed_name) or {}
                    dest_cfg = FeederUtil.get_profile_destination(profile, engine_name)
                    dest_type = dest_cfg.get('type') or it.destination_type or 'local'
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

                    if is_relay_dest:
                        it.status = 'pending_relay'
                        transporter.transport(it, source_path, dest_cfg)
                        logger.info(f"[DownloadPoll] [{engine_name}] 완료 확인 -> 원격 릴레이 대기열({it.status}) 인계: {it.title}")
                    elif is_remote_src and not use_local_staging:
                        # 로컬 스테이징 미사용(다이렉트 on-the-fly 스트리밍 모드)
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 완료 확인 -> 다이렉트(on-the-fly 무스테이징) 전송 대기열 인계: {it.title}")
                    elif is_remote_src and use_local_staging:
                        it.status = 'pending_local_staging'
                        logger.info(f"[DownloadPoll] [{engine_name}] 원격 완료 확인 -> 로컬 스테이징 대기열 인계: {it.title}")
                    else:
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 다운로드 완료 확인 (이송 대기): {it.title}")

                    db.session.commit()
                else:
                    # 타임아웃 판정 시점 계산 (누락 시 현재 시각 기준 방어)
                    added_time = it.engine_added_time or it.created_time or now
                    is_timed_out = bool(stalled_hours > 0 and (now - added_time).total_seconds() > (stalled_hours * 3600))

                    if is_timed_out:
                        # 엔진 설정 타임아웃이 초과된 경우에만 유일하게 차순위 엔진으로 폴백
                        logger.warning(f"[DownloadPoll] [{engine_name}] 지연 제한시간({stalled_hours}시간) 초과 감지 -> 다음 엔진 폴백: {it.title}")
                        try:
                            engine.delete_task(it.engine_task_id)
                        except Exception:
                            pass

                        it.current_engine_index = (it.current_engine_index or 0) + 1
                        it.status = 'pending'
                        it.engine_task_id = None
                        it.error_message = f"[{engine_name}] 지연 제한시간({stalled_hours}시간) 초과"
                        db.session.commit()
                    elif std_status == 'error':
                        restarted = False
                        if hasattr(engine, 'restart_task'):
                            try:
                                restarted = engine.restart_task(it.engine_task_id)
                            except Exception:
                                pass

                        if not restarted:
                            # 원격 엔진이 재시작 거부 시 지체 없이 다음 엔진으로 폴백
                            logger.warning(f"[DownloadPoll] [{engine_name}] 에러 작업 재시작 실패 -> 다음 엔진 폴백: {it.title}")
                            try:
                                engine.delete_task(it.engine_task_id)
                            except Exception:
                                pass
                            it.current_engine_index = (it.current_engine_index or 0) + 1
                            it.status = 'pending'
                            it.engine_task_id = None
                            it.error_message = f"[{engine_name}] 원격 재시작 불가 에러"
                            db.session.commit()
                        else:
                            it.error_message = f"[{engine_name}] 엔진 에러 수신 (재시작 요청 성공)"
                            db.session.commit()
                            logger.debug(f"[DownloadPoll] [{engine_name}] 다운로드 에러 작업 재시작 요청 완료: {it.title}")

        db.session.commit()

    @staticmethod
    def process_local_staging():
        # 이전 비정상 종료로 멈춘 고아 local_staging 작업을 pending_local_staging으로 자동 복구
        stuck_staging = db.session.query(ModelDownload).filter_by(status='local_staging').all()
        if stuck_staging:
            for s_it in stuck_staging:
                s_it.status = 'pending_local_staging'
            db.session.commit()
            logger.info(f"[LocalStaging] 멈춰있던 스테이징 작업 {len(stuck_staging)}건을 대기열(pending_local_staging)로 자동 복구했습니다.")

        # 로컬 스토리지 보호: 현재 로컬 디스크를 점유 중인 작업 수 점검 (0은 무제한)
        try:
            max_staging_items = max(0, P.ModelSetting.get_int('download_max_staging_items'))
        except Exception:
            max_staging_items = 10

        if max_staging_items > 0:
            current_local_count = db.session.query(ModelDownload).filter(
                ModelDownload.status.in_(['local_staging', 'downloaded'])
            ).count()
            if current_local_count >= max_staging_items:
                logger.info(f"[LocalStaging] 로컬 스테이징 보관 한도({max_staging_items}건) 도달 (현재: {current_local_count}건). 업로드 완료 시까지 신규 스테이징을 대기합니다.")
                return

        # 현재 세션 실패 항목을 제외하고, 로컬 디스크 허용 잔여 슬롯만큼만 정밀 조회
        query = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending_local_staging')
        if TaskDownload.failed_ids_in_session:
            query = query.filter(~ModelDownload.id.in_(list(TaskDownload.failed_ids_in_session)))

        if max_staging_items > 0:
            slots_available = max_staging_items - current_local_count
            items = query.order_by(ModelDownload.id.asc()).limit(slots_available).all()
        else:
            items = query.order_by(ModelDownload.id.asc()).all()

        if not items:
            return
        item_ids = [item.id for item in items]
        db.session.rollback()
        db.session.remove()

        staging_root = P.ModelSetting.get('download_local_staging_path') or FeederUtil.get_global().get('local_staging_path') or os.path.join(FeederUtil.get_tmp_dir(), 'staging')
        os.makedirs(staging_root, exist_ok=True)

        try:
            staging_workers = max(1, P.ModelSetting.get_int('download_staging_workers'))
        except Exception:
            staging_workers = 2

        logger.info(f"[LocalStaging] 로컬 스테이징 Rclone 다운로드 병렬 실행 (대상: {len(items)}건, 동시 워커: {staging_workers}개)")

        def _staging_worker(item_id):
            with F.app.app_context():
                target_item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not target_item:
                    return
                target_item.status = 'local_staging'
                db.session.commit()

                src_path = target_item.local_path
                raw_name = target_item.file_name or f"item_{target_item.id}"
                short_hash = (target_item.infohash[:6] if target_item.infohash else f"id_{target_item.id}")

                base_name, ext = os.path.splitext(raw_name)
                is_single_file = bool(ext and not raw_name.endswith(('/', '\\')))
                folder_base_name = base_name if is_single_file else raw_name

                profile = FeederUtil.get_download_profile_by_feed(target_item.feed_name)
                append_hash_opt = True
                if profile and 'append_hash_on_conflict' in profile:
                    append_hash_opt = bool(profile['append_hash_on_conflict'])
                else:
                    append_hash_opt = bool(FeederUtil.get_global().get('append_hash_on_conflict', True))

                need_hash_suffix = False
                if append_hash_opt:
                    duplicate_item = (
                        db.session.query(ModelDownload.id)
                        .filter(
                            ModelDownload.id != target_item.id,
                            ModelDownload.file_name == folder_base_name
                        )
                        .first()
                    )
                    if duplicate_item:
                        need_hash_suffix = True

                target_folder_name = f"{folder_base_name}_[{short_hash}]" if need_hash_suffix else folder_base_name

                target_item_id = target_item.id
                target_file_size = target_item.file_size
                current_engine_name = target_item.current_engine_name
                engine_task_id = target_item.engine_task_id
                dest_cfg = FeederUtil.get_profile_destination(profile, current_engine_name)
                staging_path = (dest_cfg.get('staging_path') or '').strip('/\\')
                if not staging_path:
                    profile_name = (profile.get('name') if profile else None) or 'default'
                    staging_path = profile_name

                item_staging_dir = os.path.join(staging_root, staging_path, target_folder_name)
                if os.path.exists(item_staging_dir):
                    shutil.rmtree(item_staging_dir, ignore_errors=True)
                os.makedirs(item_staging_dir, exist_ok=True)

                is_remote_cloud = not os.path.exists(src_path) and (':' in src_path or src_path.startswith(('http://', 'https://')))
                if is_single_file:
                    rclone_cmd_type = "copyto"
                    dest_download_path = os.path.join(item_staging_dir, raw_name)
                else:
                    rclone_cmd_type = "copy"
                    dest_download_path = item_staging_dir

                logger.info(f"[LocalStaging] 다운로드 시작: {staging_path}/{target_folder_name} (명령: {rclone_cmd_type})")

                rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
                cmd = [
                    "rclone", rclone_cmd_type, src_path, dest_download_path,
                    "--stats", "1s", "--stats-one-line", "--log-level", "INFO",
                    "--multi-thread-streams", "0",
                    "--retries", "3"
                ]
                if rclone_conf:
                    cmd.extend(["--config", rclone_conf])

                cmd.extend(FeederUtil.get_rclone_exclude_options())
                cmd.extend(FeederUtil.get_rclone_extra_options())

                db.session.remove()

                success = False
                out = ""
                try:
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
                    if it_update:
                        if success and os.path.exists(item_staging_dir):
                            downloader_cfg = FeederUtil.get_downloader_by_name(current_engine_name)
                            if downloader_cfg:
                                engine = DownloadUtil.create_engine(downloader_cfg)
                                if engine and engine_task_id:
                                    engine.delete_task(engine_task_id)

                            it_update.local_path = item_staging_dir
                            it_update.file_name = target_folder_name
                            it_update.status = 'downloaded'
                            it_update.error_message = None
                            logger.info(f"[LocalStaging] 폴더 스테이징 완료: {upload_path}/{target_folder_name} -> downloaded 전환")
                        else:
                            if os.path.exists(item_staging_dir):
                                shutil.rmtree(item_staging_dir, ignore_errors=True)
                            it_update.status = 'pending_local_staging'
                            it_update.error_message = f"로컬 스테이징 실패: {out.strip()[:150]}" if out else "로컬 스테이징 Rclone 전송 실패"
                            TaskDownload.failed_ids_in_session.add(it_update.id)
                            logger.warning(f"[LocalStaging] {target_folder_name} 스테이징 중단 ({it_update.error_message}) -> 이번 세션 제외 후 다음 턴 재시도")
                        db.session.commit()
                    db.session.remove()

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=staging_workers) as executor:
            futures = {
                executor.submit(_staging_worker, item_id): item_id
                for item_id in item_ids
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as th_err:
                    item_id = futures[future]
                    logger.error(f"[LocalStaging] 워커 스레드 예외 (ID: {item_id}): {th_err}")
                    with F.app.app_context():
                        failed_item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                        if failed_item and failed_item.status == 'local_staging':
                            failed_item.status = 'pending_local_staging'
                            failed_item.error_message = f"로컬 스테이징 워커 예외: {str(th_err)[:150]}"
                            TaskDownload.failed_ids_in_session.add(item_id)
                            db.session.commit()
                        db.session.remove()

    @staticmethod
    def route_completed_downloads():
        items = ModelDownload.get_list_by_status(['downloaded'])
        if not items:
            return
        item_ids = [item.id for item in items]
        db.session.rollback()
        db.session.remove()

        now = datetime.now()

        for item_id in item_ids:
            item = db.session.query(ModelDownload).filter_by(id=item_id).first()
            if not item:
                continue
            profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
            dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name)
            dest_type = dest_cfg.get('type') or item.destination_type or 'local'
            source_path = item.local_path
            title = item.title

            # 모든 로컬 출력 엔진의 목적지는 엔진 설정 루트 하위 경로로 해석한다.
            if dest_type == 'local' and item.current_engine_name:
                downloader_cfg = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
                local_root = DownloadUtil.get_local_root(downloader_cfg)
                complete_path = (dest_cfg.get('complete_path') or dest_cfg.get('target_folder') or '').strip()
                if local_root and complete_path:
                    # 선행 슬래시는 절대 경로가 아니라 루트 하위 경로 구분자로 취급한다.
                    relative_complete_path = complete_path.lstrip('/\\')
                    dest_cfg = dict(dest_cfg)
                    dest_cfg['complete_path'] = os.path.join(local_root, relative_complete_path)
                    logger.debug(
                        f"[RouteComplete] 엔진 로컬 루트 기준 최종 경로 해석: "
                        f"{complete_path} -> {dest_cfg['complete_path']}"
                    )

            transporter = DownloadUtil.get_transporter(dest_type)
            if not transporter:
                item.destination_type = dest_type
                item.status = 'completed'
                item.completed_time = now
                db.session.commit()
                logger.warning(f"[RouteComplete] 등록되지 않은 이송 핸들러({dest_type}) -> 로컬 완료 대체: {title}")
                continue

            # 구글 드라이브 계정 풀의 경우 멀티스레드 업로드 큐(pending_upload)로 넘겨 process_uploads에서 전담 처리
            if dest_type == 'gdrive_rotation':
                item.destination_type = dest_type
                item.gdrive_upload_path = dest_cfg.get('upload_path', '')
                item.gdrive_complete_path = dest_cfg.get('complete_path', '')
                item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''
                item.status = 'pending_upload'
                db.session.commit()
                continue

            db.session.remove()
            logger.info(f"[RouteComplete] 이송 핸들러 [{dest_type}] 호출 시작: {title}")
            try:
                success, next_status, msg = transporter.transport(item, source_path, dest_cfg)
                moved_path = getattr(item, 'local_path', source_path)
                db.session.remove()
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item:
                    continue
                item.destination_type = dest_type
                item.local_path = moved_path
                item.status = next_status

                if next_status == 'completed':
                    item.completed_time = now
                    item.error_message = None
                    logger.info(f"[RouteComplete] 최종 완료 확정: {title} ({msg})")
                elif next_status == 'failed':
                    item.error_message = msg
                    logger.warning(f"[RouteComplete] 이송 실패: {title} ({msg})")
                else:
                    logger.info(f"[RouteComplete] 상태 전이 ({next_status}): {title} ({msg})")

            except Exception as ex:
                db.session.remove()
                item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not item:
                    continue
                item.destination_type = dest_type
                logger.error(f"[RouteComplete] 이송 핸들러 예외 ({dest_type}): {ex}")
                item.status = 'failed'
                item.error_message = f"이송 핸들러 예외: {str(ex)}"

            db.session.commit()

    @staticmethod
    def process_uploads():
        # 이전 비정상 종료로 멈춘 uploading 작업을 pending_upload로 자동 복구
        stuck_uploads = db.session.query(ModelDownload).filter_by(status='uploading').all()
        if stuck_uploads:
            for u_it in stuck_uploads:
                u_it.status = 'pending_upload'
            db.session.commit()
            logger.info(f"[UploadUtil] 멈춰있던 업로드 작업 {len(stuck_uploads)}건을 대기열(pending_upload)로 자동 복구했습니다.")

        # 현재 세션에서 실패한 항목은 쿼리에서 안전하게 제외하고, 미시도 작업을 우선 정렬
        query = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending_upload')
        if TaskDownload.failed_ids_in_session:
            query = query.filter(~ModelDownload.id.in_(list(TaskDownload.failed_ids_in_session)))

        # 최근 실패한 작업은 뒤로 미루고, 한 번도 실패하지 않은 대기 작업을 1순위로 조회
        items = query.order_by(ModelDownload.last_status_time.asc().nullsfirst(), ModelDownload.id.asc()).all()
        if not items:
            return
        item_ids = [item.id for item in items]
        db.session.rollback()
        db.session.remove()

        try:
            upload_workers = max(1, P.ModelSetting.get_int('download_upload_workers'))
        except Exception:
            upload_workers = 2

        logger.info(f"[UploadUtil] Google Drive 업로드 병렬 실행 (대기: {len(items)}건, 동시 워커: {upload_workers}개)")
        manager = UploadUtil.get_account_manager()

        def _upload_worker(item_id):
            with F.app.app_context():
                target_item = db.session.query(ModelDownload).filter_by(id=item_id).first()
                if not target_item:
                    return
                success = UploadUtil.execute_upload(target_item, manager)
                if not success:
                    TaskDownload.failed_ids_in_session.add(target_item.id)
                    logger.warning(f"[UploadUtil] {target_item.file_name} 업로드 실패 -> 이번 세션 제외 후 다음 턴 재시도")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=upload_workers) as executor:
            futures = [executor.submit(_upload_worker, item_id) for item_id in item_ids]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as th_err:
                    logger.error(f"[UploadUtil] 업로드 워커 스레드 예외: {th_err}")

    @staticmethod
    def retry_move_failed():
        one_hour_ago = datetime.now() - timedelta(hours=1)
        items = (
            db.session.query(ModelDownload)
            .filter(
                ModelDownload.status == 'move_failed',
                (ModelDownload.last_move_attempt_time.is_(None) | (ModelDownload.last_move_attempt_time < one_hour_ago))
            )
            .all()
        )
        if not items:
            return

        manager = UploadUtil.get_account_manager()
        for item in items:
            item.last_move_attempt_time = datetime.now()
            db.session.commit()
            UploadUtil.execute_upload(item, manager)

    @staticmethod
    def check_pending_relays():
        """큐에 방치된 pending_relay 항목이 있고 현재 가동 중인 워커가 없다면 자동 재기동"""
        try:
            busy_count = db.session.query(ModelDownload).filter_by(status='relay_transferring').count()
            pending_items = db.session.query(ModelDownload).filter_by(status='pending_relay').all()

            if not pending_items and busy_count == 0:
                return

            if busy_count > 0:
                logger.info(f"[DownloadPipeline] 현재 릴레이 전송 진행 중인 작업({busy_count}건)이 있어 대기합니다. (대기열: {len(pending_items)}건)")
                return

            logger.info(f"[DownloadPipeline] 미완료 릴레이 대기 작업 {len(pending_items)}건 감지 ➔ 워커 자동 기동 호출")

            triggered_dest_types = set()
            for it in pending_items:
                dest_type = it.destination_type
                if not dest_type or dest_type in triggered_dest_types:
                    continue

                profile = FeederUtil.get_download_profile_by_feed(it.feed_name) or {}
                dest_cfg = FeederUtil.get_profile_destination(profile, item.current_engine_name)
                transporter = DownloadUtil.get_transporter(dest_type)

                if transporter and hasattr(transporter, 'resume_relay'):
                    transporter.resume_relay(dest_cfg)
                    triggered_dest_types.add(dest_type)

        except Exception as ex:
            logger.error(f"[DownloadPipeline] 잔여 릴레이 점검 중 예외: {ex}")
