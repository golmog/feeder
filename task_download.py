# -*- coding: utf-8 -*-
import os
import re
import time
import shutil
import subprocess
from datetime import datetime, timedelta
from types import SimpleNamespace

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

    @staticmethod
    def run_pipeline(manual: bool = False):
        with F.app.app_context():
            try:
                mode_str = "수동 실행" if manual else "스케쥴러 자동 실행"
                logger.info(f"[DownloadPipeline] 다운로드 파이프라인 가동 ({mode_str})")

                # 현재 큐 현황 집계 및 요약 출력
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

                profiles = FeederUtil.get_download_profiles()
                if not profiles:
                    logger.debug("[DownloadPipeline] 등록된 다운로드 프로필(DOWNLOAD_PROFILES)이 없습니다.")
                    return

                # SA 내 드라이브 고아 파일 사전 정리
                UploadUtil.drain_all_sa_mydrives()

                # 피드 최신 데이터 동기화
                TaskDownload.sync_feed_items(profiles)

                # 활성 큐 작업들에 대해 현재 설정된 최신 프로필(목적지 및 체인) 동적 동기화 및 리라우팅
                TaskDownload.sync_active_items_with_profiles()

                # 단일 스케줄 내 논스톱 연쇄 관통 루프 (최대 3회 패스)
                # 이전 단계에서 상태가 바뀐 항목을 즉시 다음 단계로 밀어붙여 한 주기 내 최종 완료 유도
                max_cascade_passes = 3
                for cascade_pass in range(1, max_cascade_passes + 1):
                    # 활성 대기/진행 큐 집계
                    pending_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending').count()
                    downloading_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloading').count()
                    staging_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_local_staging', 'local_staging'])).count()
                    downloaded_cnt = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloaded').count()
                    uploading_cnt = db.session.query(ModelDownload).filter(ModelDownload.status.in_(['pending_upload', 'uploading'])).count()

                    total_active_before = pending_cnt + downloading_cnt + staging_cnt + downloaded_cnt + uploading_cnt
                    if total_active_before == 0 and cascade_pass > 1:
                        break

                    logger.debug(f"[DownloadPipeline] 연쇄 파이프라인 패스 {cascade_pass}/{max_cascade_passes} 실행 (활성 작업: {total_active_before}개)")

                    # 순차 실행: 디스패치 -> 상태조회 -> 스테이징 -> 이송 라우팅 -> 업로드
                    TaskDownload.dispatch_pending_downloads()
                    TaskDownload.poll_active_downloads()
                    TaskDownload.process_local_staging()
                    TaskDownload.route_completed_downloads()
                    TaskDownload.process_uploads()

                    # 방금 패스에서 추가 전이가 발생하지 않았으면 조기 종료
                    pending_after = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending').count()
                    staging_after = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending_local_staging').count()
                    downloaded_after = db.session.query(ModelDownload).filter(ModelDownload.status == 'downloaded').count()
                    uploading_after = db.session.query(ModelDownload).filter(ModelDownload.status == 'pending_upload').count()

                    if (pending_after + staging_after + downloaded_after + uploading_after) == 0:
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

        all_feeds = FeederUtil.get_feeds()
        new_items_count = 0

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

                sync_days = profile.get('sync_days')
                if sync_days is None:
                    try:
                        sync_days = P.ModelSetting.get_int('download_feed_sync_days')
                    except Exception:
                        sync_days = 3
                else:
                    sync_days = int(sync_days)

                query = db.session.query(ModelFeedItem).filter_by(feed_name=f_name)
                if sync_days > 0:
                    limit_date = datetime.now() - timedelta(days=sync_days)
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
                    new_items_count += 1

        if new_items_count > 0:
            db.session.commit()
            logger.info(f"[DownloadPipeline] 신규 다운로드 작업 {new_items_count}건 등록 완료")

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

        updated_count = 0
        for item in items:
            profile = FeederUtil.get_download_profile_by_feed(item.feed_name)
            if not profile:
                continue

            dest_cfg = profile.get('destination', {})
            current_dest_type = dest_cfg.get('type', 'local')
            current_chain = profile.get('priority_chain', [])

            # 우선순위 체인 동적 동기화 (아직 시작 전인 pending 상태일 때)
            if item.status == 'pending' and current_chain and item.priority_chain != current_chain:
                item.priority_chain = current_chain

            # 목적지 세부 설정 동적 동기화
            dest_changed = (item.destination_type != current_dest_type)
            item.destination_type = current_dest_type
            item.gdrive_upload_path = dest_cfg.get('upload_path', '')
            item.gdrive_complete_path = dest_cfg.get('complete_path', '')
            item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''

            transporter = DownloadUtil.get_transporter(current_dest_type)
            is_relay_dest = bool(transporter and getattr(transporter, 'IS_RELAY_HANDLER', False))

            src_path = item.local_path or ''
            is_remote_src = not os.path.exists(src_path) and (':' in src_path or src_path.startswith(('http://', 'https://')))

            # 로컬 스테이징 사용 여부 동적 판별
            use_local_staging = dest_cfg.get('use_local_staging')
            if use_local_staging is None:
                use_local_staging = profile.get('use_local_staging', True)
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
                # 로컬 스테이징 옵션이 꺼진 경우 즉시 다이렉트 전송 대기열로 인계
                item.status = 'downloaded'
                logger.info(f"[DownloadPipeline] 설정 변경 감지: 로컬 스테이징 해제 -> 다이렉트(on-the-fly) 전송 대기(downloaded)로 리라우팅 -> {item.title}")
                updated_count += 1

            elif not is_relay_dest and is_remote_src and use_local_staging and item.status == 'downloaded' and not os.path.exists(item.local_path or ''):
                # 로컬 스테이징 옵션이 켜졌는데 소스가 원격인 채로 대기 중인 경우 스테이징 대기열로 복귀
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
            logger.info(f"[DownloadPipeline] 활성 큐 {updated_count}건에 대해 현재 최신 프로필 설정 동적 동기화 완료")

    @staticmethod
    def dispatch_pending_downloads():
        try:
            batch_limit = P.ModelSetting.get_int('download_batch_limit')
        except Exception:
            batch_limit = 50
        items = ModelDownload.get_list_by_status(['pending'], limit=batch_limit)
        if not items:
            return

        for item in items:
            chain = item.priority_chain or []
            curr_idx = item.current_engine_index or 0

            if curr_idx >= len(chain):
                item.status = 'failed'
                item.error_message = '모든 다운로더 우선순위 체인 소진'
                logger.warning(f"[DownloadDispatch] 다운로더 체인 소진으로 실패 처리: {item.title}")
                continue

            engine_name = chain[curr_idx]
            downloader_cfg = FeederUtil.get_downloader_by_name(engine_name)
            if not downloader_cfg or not downloader_cfg.get('enabled', True):
                logger.warning(f"[DownloadDispatch] 다운로더 [{engine_name}] 비활성 또는 미등록. 다음 순위로 전환: {item.title}")
                item.current_engine_index = curr_idx + 1
                continue

            engine = DownloadUtil.create_engine(downloader_cfg)
            if not engine:
                item.current_engine_index = curr_idx + 1
                continue

            # 링크 프로토콜(ed2k vs magnet) 지원 여부 판별하여 미지원 엔진은 즉시 다음 순위로 폴백
            is_ed2k = str(item.magnet).lower().startswith('ed2k://')
            target_proto = "ed2k" if is_ed2k else "magnet"
            supported_protos = getattr(engine, 'SUPPORTED_PROTOCOLS', ['magnet'])
            if target_proto not in supported_protos:
                logger.debug(f"[DownloadDispatch] [{engine_name}] {target_proto} 프로토콜 미지원 -> 다음 순위 엔진으로 즉시 폴백: {item.title}")
                item.current_engine_index = curr_idx + 1
                continue

            success, task_id, err = engine.add_magnet(item.magnet, title=item.title)
            if success:
                item.status = 'downloading'
                item.current_engine_name = engine_name
                item.engine_task_id = str(task_id)
                item.engine_added_time = datetime.now()
                item.last_status_time = datetime.now()
                item.error_message = None
                logger.info(f"[DownloadDispatch] [{engine_name}] 작업 추가 성공: {item.title} (TaskID: {task_id})")
            else:
                logger.warning(f"[DownloadDispatch] [{engine_name}] 추가 실패 ({err}) -> 다음 엔진으로 폴백: {item.title}")
                item.current_engine_index = curr_idx + 1
                item.error_message = f"[{engine_name}] {err}"

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

            if hasattr(engine, 'refresh_cache'):
                engine.refresh_cache()

            task_ids = [it.engine_task_id for it in engine_items if it.engine_task_id]
            status_list, err = engine.get_status(task_ids=task_ids)
            if err:
                logger.warning(f"[DownloadPoll] [{engine_name}] 상태 조회 실패: {err}")
                continue

            status_map_by_tid = {str(s.get('task_id')): s for s in status_list if s.get('task_id')}
            status_map_by_hash = {str(s.get('hash')).lower(): s for s in status_list if s.get('hash')}

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

                    profile = FeederUtil.get_download_profile_by_feed(it.feed_name) or {}
                    dest_cfg = profile.get('destination', {})
                    dest_type = dest_cfg.get('type') or it.destination_type or 'local'
                    it.destination_type = dest_type
                    it.gdrive_upload_path = dest_cfg.get('upload_path', '')
                    it.gdrive_complete_path = dest_cfg.get('complete_path', '')
                    it.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''

                    transporter = DownloadUtil.get_transporter(dest_type)
                    is_relay_dest = bool(transporter and getattr(transporter, 'IS_RELAY_HANDLER', False))
                    is_remote_src = not os.path.exists(source_path) and (':' in source_path or source_path.startswith(('http://', 'https://')))

                    # 목적지 설정의 로컬 스테이징(임시 다운로드) 사용 여부 판별 (기본값: True)
                    use_local_staging = dest_cfg.get('use_local_staging')
                    if use_local_staging is None:
                        use_local_staging = profile.get('use_local_staging', True)
                    use_local_staging = str(use_local_staging).lower() in ['true', 'on', '1']

                    if is_relay_dest:
                        it.status = 'pending_relay'
                        transporter.transport(it, source_path, dest_cfg)
                        logger.info(f"[DownloadPoll] [{engine_name}] 완료 확인 -> 원격 릴레이 대기열({it.status}) 인계: {it.title}")
                    elif is_remote_src and not use_local_staging:
                        # 다이렉트(on-the-fly) 모드: 로컬 디스크 스테이징을 건너뛰고 곧바로 전송 대기열 인계
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 완료 확인 -> 다이렉트(on-the-fly) 전송 대기열 인계: {it.title}")
                    elif is_remote_src and use_local_staging:
                        it.status = 'pending_local_staging'
                        logger.info(f"[DownloadPoll] [{engine_name}] 원격 완료 확인 -> 로컬 스테이징 대기열 인계: {it.title}")
                    else:
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 다운로드 완료 확인 (이송 대기): {it.title}")

                elif std_status == 'error' or (stalled_hours > 0 and it.engine_added_time and (now - it.engine_added_time).total_seconds() > stalled_hours * 3600):
                    reason = "다운로더 에러" if std_status == 'error' else f"지연 제한시간({stalled_hours}시간) 초과"
                    logger.warning(f"[DownloadPoll] [{engine_name}] {reason} 감지 -> 다음 엔진 폴백: {it.title}")

                    try:
                        engine.delete_task(it.engine_task_id)
                    except Exception:
                        pass

                    it.current_engine_index = (it.current_engine_index or 0) + 1
                    it.status = 'pending'
                    it.engine_task_id = None
                    it.error_message = f"[{engine_name}] {reason}"

        db.session.commit()

    @staticmethod
    def process_local_staging():
        try:
            batch_limit = P.ModelSetting.get_int('download_batch_limit')
        except Exception:
            batch_limit = 50
        items = ModelDownload.get_list_by_status(['pending_local_staging'], limit=batch_limit)
        if not items:
            return

        staging_root = FeederUtil.get_global().get('local_staging_path') or os.path.join(FeederUtil.get_tmp_dir(), 'staging')
        os.makedirs(staging_root, exist_ok=True)

        for item in items:
            item.status = 'local_staging'
            db.session.commit()

            src_path = item.local_path
            raw_name = item.file_name or f"item_{item.id}"
            short_hash = (item.infohash[:6] if item.infohash else f"id_{item.id}")

            base_name, ext = os.path.splitext(raw_name)
            is_single_file = bool(ext and not raw_name.endswith(('/', '\\')))
            folder_base_name = base_name if is_single_file else raw_name

            profile = FeederUtil.get_download_profile_by_feed(item.feed_name)
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
                        ModelDownload.id != item.id,
                        ModelDownload.file_name == folder_base_name
                    )
                    .first()
                )
                if duplicate_item:
                    need_hash_suffix = True

            if need_hash_suffix:
                target_folder_name = f"{folder_base_name}_[{short_hash}]"
                logger.info(f"[LocalStaging] 동일 폴더명 감지 -> 해시 부여: {target_folder_name}")
            else:
                target_folder_name = folder_base_name

            # 경로 구조: 로컬 스테이징 경로 / 다운로드 프로필명 / 서브폴더(target_folder_name) / 파일
            profile_name = (profile.get('name') if profile else None) or 'default'
            item_staging_dir = os.path.join(staging_root, profile_name, target_folder_name)
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

            logger.info(f"[LocalStaging] 로컬 스테이징 다운로드 시작: {profile_name}/{target_folder_name} (명령: {rclone_cmd_type})")

            rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
            cmd = [
                "rclone", rclone_cmd_type, src_path, dest_download_path,
                "--stats", "10s", "--stats-one-line", "--log-level", "NOTICE",
                "--multi-thread-streams", "0",
                "--retries", "3"
            ]
            if rclone_conf:
                cmd.extend(["--config", rclone_conf])

            # 유저 설정 Rclone 확장 옵션 결합
            cmd.extend(FeederUtil.get_rclone_extra_options())

            success = False
            try:
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=1800)
                success = (res.returncode == 0)
                if not success:
                    logger.error(f"[LocalStaging] Rclone 다운로드 실패: {res.stderr.strip()}")
            except Exception as ex:
                logger.error(f"[LocalStaging] Rclone 실행 예외: {ex}")

            if success and os.path.exists(item_staging_dir):
                downloader_cfg = FeederUtil.get_downloader_by_name(item.current_engine_name)
                if downloader_cfg:
                    engine = DownloadUtil.create_engine(downloader_cfg)
                    if engine and item.engine_task_id:
                        engine.delete_task(item.engine_task_id)

                item.local_path = item_staging_dir
                item.file_name = target_folder_name
                item.status = 'downloaded'
                logger.info(f"[LocalStaging] 폴더 스테이징 완료: {target_folder_name} -> downloaded 전환")
            else:
                if os.path.exists(item_staging_dir):
                    shutil.rmtree(item_staging_dir, ignore_errors=True)
                item.status = 'pending_local_staging'
                item.error_message = "로컬 스테이징 Rclone 전송 실패"

            db.session.commit()

    @staticmethod
    def route_completed_downloads():
        try:
            batch_limit = P.ModelSetting.get_int('download_batch_limit')
        except Exception:
            batch_limit = 50
        if not batch_limit or batch_limit <= 0:
            batch_limit = 50

        items = ModelDownload.get_list_by_status(['downloaded'], limit=batch_limit)
        if not items:
            return

        now = datetime.now()

        for item in items:
            profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
            dest_cfg = profile.get('destination', {})
            dest_type = dest_cfg.get('type') or item.destination_type or 'local'
            item.destination_type = dest_type
            item.gdrive_upload_path = dest_cfg.get('upload_path', '')
            item.gdrive_complete_path = dest_cfg.get('complete_path', '')
            item.gdrive_remote_id = dest_cfg.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or ''

            transporter = DownloadUtil.get_transporter(dest_type)
            if not transporter:
                logger.warning(f"[RouteComplete] 등록되지 않은 이송 핸들러({dest_type}) -> 로컬 완료 대체: {item.title}")
                item.status = 'completed'
                item.completed_time = now
                continue

            # 구글 드라이브 계정 풀의 경우 멀티스레드 업로드 큐(pending_upload)로 넘겨 process_uploads에서 전담 처리
            if dest_type == 'gdrive_rotation':
                item.status = 'pending_upload'
                logger.info(f"[RouteComplete] Google Drive SA 계정 풀 업로드 큐(pending_upload) 인계: {item.title}")
                continue

            logger.info(f"[RouteComplete] 이송 핸들러 [{dest_type}] 호출 시작: {item.title}")
            try:
                success, next_status, msg = transporter.transport(item, item.local_path, dest_cfg)
                item.status = next_status

                if next_status == 'completed':
                    item.completed_time = now
                    item.error_message = None
                    logger.info(f"[RouteComplete] 최종 완료 확정: {item.title} ({msg})")
                elif next_status == 'failed':
                    item.error_message = msg
                    logger.warning(f"[RouteComplete] 이송 실패: {item.title} ({msg})")
                else:
                    logger.info(f"[RouteComplete] 상태 전이 ({next_status}): {item.title} ({msg})")

            except Exception as ex:
                logger.error(f"[RouteComplete] 이송 핸들러 예외 ({dest_type}): {ex}")
                item.status = 'failed'
                item.error_message = f"이송 핸들러 예외: {str(ex)}"

        db.session.commit()

    @staticmethod
    def process_uploads():
        try:
            batch_limit = P.ModelSetting.get_int('download_batch_limit')
        except Exception:
            batch_limit = 50
        items = ModelDownload.get_list_by_status(['pending_upload'], limit=batch_limit)
        if not items:
            return

        manager = UploadUtil.get_account_manager()
        for item in items:
            UploadUtil.execute_upload(item, manager)

    @staticmethod
    def retry_move_failed():
        try:
            batch_limit = P.ModelSetting.get_int('download_batch_limit')
        except Exception:
            batch_limit = 50
        one_hour_ago = datetime.now() - timedelta(hours=1)
        items = (
            db.session.query(ModelDownload)
            .filter(
                ModelDownload.status == 'move_failed',
                (ModelDownload.last_move_attempt_time.is_(None) | (ModelDownload.last_move_attempt_time < one_hour_ago))
            )
            .limit(batch_limit)
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
                dest_cfg = profile.get('destination', {})
                transporter = DownloadUtil.get_transporter(dest_type)

                if transporter and hasattr(transporter, 'resume_relay'):
                    transporter.resume_relay(dest_cfg)
                    triggered_dest_types.add(dest_type)

        except Exception as ex:
            logger.error(f"[DownloadPipeline] 잔여 릴레이 점검 중 예외: {ex}")
