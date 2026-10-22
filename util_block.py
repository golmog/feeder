# -*- coding: utf-8 -*-
import os
import shutil
import threading
from sqlalchemy import event, select, or_, func

from .setup import *
from .model_download import ModelDownload, ModelDownloadBlock
from .util_base import FeederUtil


class DownloadBlock:
    """콘텐츠 차단과 실행 중인 작업의 임시 파일 정리를 담당한다."""
    _lock = threading.Lock()
    _running = set()

    @staticmethod
    def snapshot(item):
        state = dict(item.upload_state or {})
        profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest = FeederUtil.get_profile_destination(profile, item.current_engine_name)
        folder = item.output_folder_name or item.file_name
        if item.status == 'local_staging' and not state.get('staging_local_path') and folder:
            root = P.ModelSetting.get('download_local_staging_path') or FeederUtil.get_global().get('local_staging_path') or os.path.join(FeederUtil.get_tmp_dir(), 'staging')
            sub = (dest.get('staging_path') or profile.get('name') or 'default').strip('/\\')
            state['staging_local_path'] = os.path.join(root, sub, folder)
        # 기존 버전이 전송 시작 전에 저장하지 않았던 임시 경로도 계정 설정으로 복원한다.
        if item.gdrive_account and not state.get('incoming') and item.destination_type == 'gdrive_rotation':
            rclone = FeederUtil.load_yaml().get('rclone', {})
            pooled = P.ModelSetting.get_bool('download_use_gdrive_pool')
            impersonate = pooled and P.ModelSetting.get_bool('download_gdrive_use_impersonate')
            account = next((a for a in FeederUtil.get_gdrive_accounts()
                            if a.get('username') == item.gdrive_account), {}) if pooled else {}
            remote = dest.get('mydrive_remote_name') or P.ModelSetting.get('download_gdrive_mydrive_remote_name') or rclone.get('mydrive_remote_name') or 'gdrive_sa'
            if not pooled:
                remote = P.ModelSetting.get('download_rclone_upload_remote_name') or remote
            elif not impersonate:
                remote = account.get('remote_name') or remote
            drive = dest.get('shared_drive_id') or item.gdrive_remote_id or P.ModelSetting.get('download_shared_drive_id') or rclone.get('shared_drive_id')
            upload = (dest.get('upload_path') or item.gdrive_upload_path or 'incoming/default').strip('/')
            final = (dest.get('complete_path') or item.gdrive_complete_path or 'uploads/default').strip('/')
            if not folder or not drive or (pooled and not account.get('mydrive_rclone_id')):
                state['cleanup_error'] = '기존 업로드의 계정/임시 경로를 복원하지 못했습니다. 설정을 확인해주세요.'
            else:
                shared = f'{remote}:{{{drive}}}/{upload}/{folder}'
                state.pop('cleanup_error', None)
                state.update(incoming=f'{remote}:{{{account["mydrive_rclone_id"]}}}/{upload}/{folder}' if pooled else shared,
                    shared_incoming=shared, dest_parent=f'{remote}:{{{drive}}}/{final}', folder_name=folder,
                    rclone_conf=P.ModelSetting.get('download_rclone_conf_path') or rclone.get('conf_path', ''),
                    impersonate_arg=['--drive-impersonate', item.gdrive_account] if impersonate else [])
        return {'id': item.id, 'state': state,
                'local_path': item.local_path if item.status != 'completed' else None, 'pending': True}

    @staticmethod
    def initialize():
        engine = db.engines[ModelDownload.__bind_key__]
        ModelDownloadBlock.__table__.create(engine, checkfirst=True)
        for name in ('before_insert', 'before_update'):
            if not event.contains(ModelDownload, name, DownloadBlock._guard_write):
                event.listen(ModelDownload, name, DownloadBlock._guard_write)

    @staticmethod
    def _guard_write(mapper, connection, item):
        # 오래된 워커가 차단 상태를 대기/완료 상태로 덮어쓰지 못하게 한다.
        if item.status == 'blocked':
            return
        key = ModelDownloadBlock.key(item.infohash, item.magnet)
        exists = connection.execute(select(ModelDownloadBlock.content_key).where(
            ModelDownloadBlock.content_key == key)).first()
        if exists:
            raise ValueError('차단된 콘텐츠의 작업 상태 변경을 거부했습니다.')

    @staticmethod
    def stopped(item_id):
        # 세션 캐시 대신 별도 조회로 다른 프로세스에서 저장한 차단을 확인한다.
        engine = db.engines[ModelDownload.__bind_key__]
        with engine.connect() as connection:
            row = connection.execute(select(ModelDownload.status, ModelDownload.infohash,
                ModelDownload.magnet).where(ModelDownload.id == item_id)).first()
            if row is None or row[0] in ('blocked', 'cancelled'):
                return True
            key = ModelDownloadBlock.key(row[1], row[2])
            return connection.execute(select(ModelDownloadBlock.content_key).where(
                ModelDownloadBlock.content_key == key)).first() is not None

    @classmethod
    def block(cls, item):
        key = ModelDownloadBlock.key(item.infohash, item.magnet)
        record = db.session.get(ModelDownloadBlock, key)
        if record is None:
            record = ModelDownloadBlock(content_key=key, title=item.title)
            db.session.add(record)
        snapshots = {s['id']: s for s in (record.cleanup or [])}
        normalized_hash = FeederUtil.extract_info_hash(item.magnet or '') or item.infohash
        matches = db.session.query(ModelDownload).filter(or_(
            func.lower(ModelDownload.infohash) == (normalized_hash or '').lower(),
            ModelDownload.magnet == item.magnet, ModelDownload.infohash.is_(None))).all()
        ids = []
        for target in matches:
            FeederUtil.cooperate()
            if ModelDownloadBlock.key(target.infohash, target.magnet) != key:
                continue
            ids.append(target.id)
            if target.id not in snapshots:
                snapshots[target.id] = cls.snapshot(target)
            target.status = 'blocked'
            target.upload_state = dict(snapshots[target.id]['state'])
            target.completed_time = None
            target.error_message = '사용자 차단: 임시 파일 정리 대기'
        record.cleanup = list(snapshots.values())
        db.session.commit()
        # 정리 스레드를 기다리지 않고 우선 모든 전송을 즉시 중단한다.
        from .util_upload import UploadUtil
        for item_id in ids:
            UploadUtil.kill_item_rclone(item_id)
        cls.start_cleanup(key)

    @classmethod
    def start_cleanup(cls, key):
        with cls._lock:
            if key in cls._running:
                return
            cls._running.add(key)
        def work():
            try:
                with F.app.app_context():
                    cls.cleanup(key)
            except Exception as exc:
                logger.error(f'[차단] 임시 파일 정리 예외: {exc}')
            finally:
                with cls._lock:
                    cls._running.discard(key)
        threading.Thread(target=work, name='feeder-block-cleanup', daemon=True).start()

    @staticmethod
    def remove_staging(path):
        """스테이징 루트의 하위 경로만 제거하며 마운트 원본/최종 경로는 보존한다."""
        if not path or ':' in path:
            return
        root = os.path.realpath(P.ModelSetting.get('download_local_staging_path') or
            FeederUtil.get_global().get('local_staging_path') or
            os.path.join(FeederUtil.get_tmp_dir(), 'staging'))
        actual = os.path.realpath(path)
        if actual == root or os.path.commonpath([root, actual]) != root:
            return
        if os.path.isdir(actual):
            shutil.rmtree(actual)
        elif os.path.exists(actual):
            os.remove(actual)

    @classmethod
    def cleanup(cls, key):
        from .util_upload import UploadUtil
        from .util_download import DownloadUtil
        record = db.session.get(ModelDownloadBlock, key)
        if record is None:
            return
        snapshots = [dict(s) for s in (record.cleanup or [])]
        errors = []
        for snapshot in snapshots:
            if not snapshot.get('pending'):
                continue
            item_id = snapshot['id']
            try:
                UploadUtil.kill_item_rclone(item_id)
                state = snapshot.get('state') or {}
                if state.get('worker'):
                    from .util_remote_worker import RemoteWorker
                    # 종료 확인과 등록 전 취소 예약 이후에만 원본/임시 파일을 정리한다.
                    RemoteWorker.cancel_snapshot(state)
                    job = RemoteWorker.client(state['worker']['endpoint']).job(state['worker']['job_id'])
                    if item := db.session.get(ModelDownload, item_id):
                        UploadUtil.settle_usage(item, None, job.get('chargeable_bytes') if job else None)
                failures = []
                item = db.session.get(ModelDownload, item_id)
                if item:
                    try:
                        DownloadUtil.delete_remote_tasks(item, strict=True)
                    except Exception as exc:
                        failures.append(str(exc))
                if state.get('cleanup_error'):
                    # 설정 복구 후 재시도하면 경로 복원부터 다시 수행한다.
                    item = db.session.get(ModelDownload, item_id)
                    if item:
                        state = cls.snapshot(item)['state']
                        snapshot['state'] = state
                    if state.get('cleanup_error'):
                        raise ValueError(state['cleanup_error'])
                cls.remove_staging(state.get('staging_local_path'))
                cls.remove_staging(snapshot.get('local_path'))
                # 항목 전용 incoming 경로만 삭제한다. 최종 보관 경로는 절대 삭제하지 않는다.
                for path in set(filter(None, (state.get('incoming'), state.get('shared_incoming')))):
                    parent = str(state.get('dest_parent') or '').rstrip('/')
                    folder = state.get('folder_name')
                    if (not folder or not parent or not path.endswith('/' + folder)
                            or path == parent or path.startswith(parent + '/')):
                        raise ValueError('안전한 업로드 임시 경로를 확인하지 못했습니다.')
                    cmd = ['rclone', 'purge', path, '--config', state['rclone_conf'],
                           '--drive-use-trash=false'] + state.get('impersonate_arg', [])
                    success, output = UploadUtil.run_rclone(cmd, '차단 항목 임시 파일 정리', execution_timeout=60)
                    if not success and not any(text in output.lower() for text in
                            ('directory not found', 'object not found', "doesn't exist")):
                        failures.append(output.strip()[:200] or '원격 임시 파일 정리 실패')
                if failures:
                    raise RuntimeError('; '.join(failures))
                snapshot['pending'] = False
                snapshot.pop('error', None)
            except Exception as exc:
                snapshot['error'] = str(exc)[:300]
                errors.append(f'{item_id}: {snapshot["error"]}')
            item = db.session.get(ModelDownload, item_id)
            if item and item.status == 'blocked':
                item.error_message = ('차단됨: 임시 파일 정리 실패 (차단 목록에서 재시도 가능)'
                                      if snapshot.get('pending') else '차단됨: 임시 파일 정리 완료')
                if not snapshot.get('pending'):
                    item.local_path = None
                    item.upload_state = None
                    item.gdrive_account = None
            record.cleanup = [dict(s) for s in snapshots]
            record.error_message = '\n'.join(errors) or None
            db.session.commit()
        db.session.remove()

    @staticmethod
    def unblock(key):
        record = db.session.get(ModelDownloadBlock, key)
        if record is None:
            return {'ret': 'fail', 'msg': '차단 기록을 찾을 수 없습니다.'}
        if any(s.get('pending') for s in (record.cleanup or [])):
            return {'ret': 'fail', 'msg': '임시 파일 정리가 끝난 뒤 차단을 해제할 수 있습니다. 정리 재시도를 실행해주세요.'}
        db.session.delete(record)
        db.session.flush()
        for item in db.session.query(ModelDownload).filter_by(status='blocked').all():
            if ModelDownloadBlock.key(item.infohash, item.magnet) == key:
                item.status = 'cancelled'
                item.error_message = '차단 해제됨: 재시도 버튼으로 다시 등록할 수 있습니다.'
        db.session.commit()
        return {'ret': 'success', 'msg': '차단을 해제했습니다. 필요한 항목은 재시도해주세요.'}


# Celery가 이 모듈을 직접 로드하는 경우에도 오래된 상태 저장을 차단한다.
for _name in ('before_insert', 'before_update'):
    if not event.contains(ModelDownload, _name, DownloadBlock._guard_write):
        event.listen(ModelDownload, _name, DownloadBlock._guard_write)
