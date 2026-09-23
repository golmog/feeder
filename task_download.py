# -*- coding: utf-8 -*-

import os
import re
import time
import shutil
import subprocess
from datetime import datetime, timedelta
from types import SimpleNamespace

from .setup import *
from .model_feed import ModelFeedBbs
from .model_download import ModelDownload, ModelDownloadStat
from .util_feed import FeedConfigUtil, FeedFilter, extract_info_hash, get_tmp_dir
from .util_download import DownloaderManager, TransporterManager
from .util_upload import GDriveAccountManager, GDriveUploadHandler


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

                profiles = FeedConfigUtil.get_download_profiles()
                if not profiles:
                    logger.debug("[DownloadPipeline] 등록된 다운로드 프로필(DOWNLOAD_PROFILES)이 없습니다.")
                    return

                from .util_upload import GDriveAccountManager
                GDriveAccountManager.drain_all_sa_mydrives()

                # 신규 피드 항목 동기화
                TaskDownload.sync_feed_items(profiles)

                # 대기 작업 다운로더 배정
                TaskDownload.dispatch_pending_downloads()

                # 진행 중 작업 상태 폴링 및 타임아웃 폴백 처리
                TaskDownload.poll_active_downloads()

                # 원격 다운로드 파일의 로컬 스테이징(버퍼 복사) 처리
                TaskDownload.process_local_staging()

                # 최종 목적지 라우팅 처리
                TaskDownload.route_completed_downloads()

                # 구글 드라이브 업로드 처리
                TaskDownload.process_uploads()

                # 이동 실패(move_failed) 항목 재시도 처리
                TaskDownload.retry_move_failed()

                logger.info("[DownloadPipeline] 다운로드 파이프라인 주기 완료")

            except Exception as e:
                logger.error(f"[DownloadPipeline] 파이프라인 처리 중 오류: {e}")
                logger.error(traceback.format_exc())

    @staticmethod
    def sync_feed_items(profiles: list[dict]):
        """DOWNLOAD_PROFILES에 매핑된 피드 항목을 감지하여 ModelDownload 큐에 등록"""
        global_cfg = FeedConfigUtil.get_global()
        all_feeds = FeedConfigUtil.get_feeds()
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

                sources = feed.get('sources', [])
                if not sources:
                    continue

                for src in sources:
                    site_name = src.get('site')
                    board_key = src.get('full_board_key') or src.get('board')
                    if not site_name or not board_key:
                        continue

                    # 프로필 개별 설정 또는 전역 기본 다운로드 기간(일) 적용
                    sync_days = profile.get('sync_days')
                    if sync_days is None:
                        sync_days = P.ModelSetting.get_int('download_feed_sync_days', 3)
                    else:
                        sync_days = int(sync_days)

                    query = db.session.query(ModelFeedBbs).filter_by(site=site_name, board=board_key)
                    if sync_days > 0:
                        limit_date = datetime.now() - timedelta(days=sync_days)
                        query = query.filter(ModelFeedBbs.created_time >= limit_date)

                    candidates = query.order_by(ModelFeedBbs.id.desc()).all()

                    for bbs in candidates:
                        bbs_dict = bbs.as_dict()
                        magnets = bbs_dict.get('magnet', [])
                        if not magnets:
                            continue

                        target_mag = magnets[0]
                        infohash = extract_info_hash(target_mag)

                        # 중복 다운로드 큐 등록 방지
                        existing = None
                        if infohash:
                            existing = ModelDownload.get_by_infohash(infohash)
                        if not existing:
                            existing = ModelDownload.get_by_magnet(target_mag)
                        if existing:
                            continue

                        # 피드 필터 규칙 검증
                        is_pass, _ = FeedFilter.evaluate(bbs_dict, feed, global_cfg)
                        if not is_pass:
                            continue

                        dl_item = ModelDownload(
                            feed_name=f_name,
                            title=bbs.title,
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
    def dispatch_pending_downloads():
        """pending 상태 작업을 현재 우선순위 체인 엔진에 할당"""
        batch_limit = P.ModelSetting.get_int('download_batch_limit', 50)
        items = ModelDownload.get_list_by_status(['pending'], limit=batch_limit)
        if not items:
            return

        for item in items:
            chain = item.priority_chain or []
            curr_idx = item.current_engine_index or 0

            # 모든 우선순위 체인을 소진한 경우
            if curr_idx >= len(chain):
                item.status = 'failed'
                item.error_message = '모든 다운로더 우선순위 체인 소진'
                logger.warning(f"[DownloadDispatch] 다운로더 체인 소진으로 실패 처리: {item.title}")
                continue

            engine_name = chain[curr_idx]
            downloader_cfg = FeedConfigUtil.get_downloader_by_name(engine_name)
            if not downloader_cfg or not downloader_cfg.get('enabled', True):
                logger.warning(f"[DownloadDispatch] 다운로더 [{engine_name}] 비활성 또는 미등록. 다음 순위로 전환: {item.title}")
                item.current_engine_index = curr_idx + 1
                continue

            engine = DownloaderManager.create_instance(downloader_cfg)
            if not engine:
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
        """downloading 상태 작업의 엔진 진행상태 폴링 및 지연 타임아웃 폴백 처리"""
        items = ModelDownload.get_list_by_status(['downloading'], limit=100)
        if not items:
            return

        # 엔진명 단위로 그룹화하여 일괄 상태 조회
        items_by_engine = {}
        for it in items:
            e_name = it.current_engine_name
            if e_name not in items_by_engine:
                items_by_engine[e_name] = []
            items_by_engine[e_name].append(it)

        now = datetime.now()

        for engine_name, engine_items in items_by_engine.items():
            downloader_cfg = FeedConfigUtil.get_downloader_by_name(engine_name)
            if not downloader_cfg:
                continue

            engine = DownloaderManager.create_instance(downloader_cfg)
            if not engine:
                continue

            # CD2 115 엔진인 경우 조회 전 가상경로 캐시 갱신 호출
            if hasattr(engine, 'refresh_cache'):
                engine.refresh_cache()

            task_ids = [it.engine_task_id for it in engine_items if it.engine_task_id]
            status_list, err = engine.get_status(task_ids=task_ids)
            if err:
                logger.warning(f"[DownloadPoll] [{engine_name}] 상태 조회 실패: {err}")
                continue

            # task_id 및 infohash 매핑 테이블 구성
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
                if fname:
                    it.file_name = fname
                if fsize:
                    it.file_size = fsize

                # 다운로드 완료 시 처리
                if std_status == 'completed':
                    source_path = matched_status.get('source_path', '')
                    it.local_path = source_path

                    # 다른 마그넷(릴그룹)의 동일 파일명이 DB에 이미 존재하는지 검사
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
                            logger.info(f"[DownloadPoll] 동일 파일명의 다른 릴그룹 감지! 충돌 방지를 위해 대상을 '{fname}'(으)로 분리: {it.title}")

                    profile = FeedConfigUtil.get_download_profile_by_feed(it.feed_name) or {}
                    dest_cfg = profile.get('destination', {})
                    dest_type = it.destination_type or dest_cfg.get('type', 'local')

                    # 목적지 유형별 수득 완료 분기 (Colab은 로컬 스테이징 완전 우회)
                    if dest_type == 'colab_gdrive':
                        transporter = TransporterManager.get_transporter('colab_gdrive')
                        if transporter:
                            transporter.transport(it, source_path, dest_cfg)
                        else:
                            it.status = 'pending_colab'
                        logger.info(f"[DownloadPoll] [{engine_name}] 원격 완료 확인 -> Colab 릴레이 대기열로 인계: {it.title}")
                    elif source_path.startswith(('ad:', 'http://', 'https://')):
                        it.status = 'pending_local_staging'
                        logger.info(f"[DownloadPoll] [{engine_name}] 원격 완료 확인 -> 로컬 스테이징 대기열로 인계: {it.title}")
                    else:
                        it.status = 'downloaded'
                        logger.info(f"[DownloadPoll] [{engine_name}] 다운로드 완료 확인 (이송 대기): {it.title}")

                # 에러 또는 지연 타임아웃 발생 시 다음 우선순위 엔진으로 폴백 (stalled_hours가 0 이하이면 시간 무제한)
                elif std_status == 'error' or (stalled_hours > 0 and it.engine_added_time and (now - it.engine_added_time).total_seconds() > stalled_hours * 3600):
                    reason = "다운로더 에러" if std_status == 'error' else f"지연 제한시간({stalled_hours}시간) 초과"
                    logger.warning(f"[DownloadPoll] [{engine_name}] {reason} 감지 -> 이전 작업 정리 및 다음 엔진 폴백: {it.title}")

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
        """AllDebrid 등 WebDAV/리모트 완료 파일을 작업별 고유 폴더 구조로 로컬 스테이징"""
        batch_limit = P.ModelSetting.get_int('download_batch_limit', 50)
        items = ModelDownload.get_list_by_status(['pending_local_staging'], limit=batch_limit)
        if not items:
            return

        staging_root = FeedConfigUtil.get_global().get('local_staging_path') or os.path.join(get_tmp_dir(), 'staging')
        os.makedirs(staging_root, exist_ok=True)

        for item in items:
            item.status = 'local_staging'
            db.session.commit()

            src_path = item.local_path
            raw_name = item.file_name or f"item_{item.id}"
            short_hash = (item.infohash[:6] if item.infohash else f"id_{item.id}")

            # 파일명에서 확장자를 제외한 기본 베이스명 추출
            base_name, ext = os.path.splitext(raw_name)
            # 폴더인지 단일 파일인지 판별 (확장자가 있고 원본 경로가 디렉터리가 아닌 경우)
            is_single_file = bool(ext and not raw_name.endswith(('/', '\\')))

            folder_base_name = base_name if is_single_file else raw_name

            # 프로필 설정에서 중복 시 해시 추가 옵션 확인 (기본값: True)
            profile = FeedConfigUtil.get_download_profile_by_feed(item.feed_name)
            append_hash_opt = True
            if profile and 'append_hash_on_conflict' in profile:
                append_hash_opt = bool(profile['append_hash_on_conflict'])
            else:
                append_hash_opt = bool(FeedConfigUtil.get_global().get('append_hash_on_conflict', True))

            # 옵션이 켜져 있고, DB에 동일한 폴더명이 이미 존재할 때만 해시를 붙여 충돌 방지
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
                logger.info(f"[LocalStaging] DB 내 동일 폴더명 감지 -> 해시 서픽스 부여: {target_folder_name} (ID: {item.id})")
            else:
                target_folder_name = folder_base_name
                logger.info(f"[LocalStaging] 폴더명 원본 유지(중복 없음): {target_folder_name} (ID: {item.id})")

            # 로컬 스테이징 내 고유 작업 디렉터리
            item_staging_dir = os.path.join(staging_root, target_folder_name)

            # 이전 비정상 중단 찌꺼기가 남아있다면 클린 삭제 후 재생성
            if os.path.exists(item_staging_dir):
                shutil.rmtree(item_staging_dir, ignore_errors=True)
            os.makedirs(item_staging_dir, exist_ok=True)

            # 단일 파일 토렌트인 경우 폴더 내부로 수신하도록 목적지 지정
            dest_download_path = os.path.join(item_staging_dir, raw_name) if is_single_file else item_staging_dir

            logger.info(f"[LocalStaging] 로컬 폴더 표준화 다운로드 시작: {target_folder_name} (ID: {item.id})")

            # 단일 파일이면 copyto, 폴더면 copy
            rclone_cmd_type = "copyto" if is_single_file else "copy"
            rclone_conf = P.ModelSetting.get('download_rclone_conf_path') or FeedConfigUtil.load_yaml().get('rclone', {}).get('conf_path', '')
            cmd = [
                "rclone", rclone_cmd_type, src_path, dest_download_path,
                "--stats", "10s", "--stats-one-line", "--log-level", "NOTICE",
                "--retries", "1", "--timeout", "15m"
            ]
            if rclone_conf:
                cmd.extend(["--config", rclone_conf])

            success = False
            try:
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=1800)
                success = (res.returncode == 0)
                if not success:
                    logger.error(f"[LocalStaging] Rclone 다운로드 실패: {res.stderr.strip()}")
            except Exception as ex:
                logger.error(f"[LocalStaging] Rclone 실행 예외: {ex}")

            if success and os.path.exists(item_staging_dir):
                # 다운로더 클라우드 원본 정리
                downloader_cfg = FeedConfigUtil.get_downloader_by_name(item.current_engine_name)
                if downloader_cfg:
                    engine = DownloaderManager.create_instance(downloader_cfg)
                    if engine and item.engine_task_id:
                        engine.delete_task(item.engine_task_id)

                # 모든 항목은 파이프라인에서 무조건 '고유 폴더' 경로로 통일
                item.local_path = item_staging_dir
                item.file_name = target_folder_name
                item.status = 'downloaded'
                logger.info(f"[LocalStaging] 폴더 표준화 스테이징 완료: {target_folder_name} -> downloaded 전환")
            else:
                # 전송 실패 시 불완전한 로컬 잔여물 완전 파기
                if os.path.exists(item_staging_dir):
                    shutil.rmtree(item_staging_dir, ignore_errors=True)
                item.status = 'pending_local_staging'
                item.error_message = "로컬 스테이징 Rclone 전송 실패 (불완전 잔여물 클린 파기)"

            db.session.commit()


    @staticmethod
    def route_completed_downloads():
        """downloaded 항목을 목적지 이송 핸들러(Transporter)에 전달하여 최종 이송 처리"""
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
            profile = FeedConfigUtil.get_download_profile_by_feed(item.feed_name) or {}
            dest_cfg = profile.get('destination', {})
            dest_type = item.destination_type or dest_cfg.get('type', 'local')

            transporter = TransporterManager.get_transporter(dest_type)
            if not transporter:
                logger.warning(f"[RouteComplete] 등록되지 않은 이송 핸들러({dest_type}) -> 로컬 완료로 대체 처리: {item.title}")
                item.status = 'completed'
                item.completed_time = now
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
                    logger.info(f"[RouteComplete] 중간 상태 전이 ({next_status}): {item.title} ({msg})")

            except Exception as ex:
                logger.error(f"[RouteComplete] 이송 핸들러 실행 중 예외 ({dest_type}): {ex}")
                item.status = 'failed'
                item.error_message = f"이송 핸들러 예외: {str(ex)}"

        db.session.commit()


    @staticmethod
    def process_uploads():
        """pending_upload 항목의 구글 드라이브 계정 로테이션 업로드 실행"""
        batch_limit = P.ModelSetting.get_int('download_batch_limit', 50)
        items = ModelDownload.get_list_by_status(['pending_upload'], limit=batch_limit)
        if not items:
            return

        manager = GDriveAccountManager()
        for item in items:
            GDriveUploadHandler.execute_upload(item, manager)

    @staticmethod
    def retry_move_failed():
        """move_failed 상태 항목의 서버사이드 이동 주기적 재시도"""
        batch_limit = P.ModelSetting.get_int('download_batch_limit', 50)
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

        manager = GDriveAccountManager()
        for item in items:
            item.last_move_attempt_time = datetime.now()
            db.session.commit()
            GDriveUploadHandler.execute_upload(item, manager)
