"""원격 복사의 소유권·복구·취소를 기존 업로드 파이프라인과 연결한다."""

import configparser
import os
import re
import threading
import time
import uuid

from .setup import *
from .util_base import FeederUtil
from .model_download import ModelDownload, ModelDownloadStat
from .remote_worker_client import WorkerClient, WorkerError, WorkerEvents
from .worker.rclone_logging import redact_log
from .util_download_activity import DownloadActivity


class RemoteWorker:
    _submit_lock = threading.Lock()

    @staticmethod
    def _write_settings(values, only_if_token_empty=False):
        """접속 설정을 함께 저장하고 DB 오류에 인증 값이 노출되지 않게 한다."""
        try:
            table = P.ModelSetting.__table__
            with F.app.app_context():
                engine = db.engines.get(P.package_name)
                if engine is None:
                    engine = db.engine
                with engine.begin() as connection:
                    if only_if_token_empty:
                        token = connection.execute(table.select().with_only_columns(table.c.value).where(
                            table.c.key == 'download_worker_token')).scalar()
                        if token:
                            return
                    for key, value in values.items():
                        result = connection.execute(table.update().where(table.c.key == key).values(value=value))
                        if result.rowcount == 0:
                            connection.execute(table.insert().values(key=key, value=value))
        except Exception:
            raise WorkerError('워커 연결 설정을 DB에 저장하지 못했습니다. 잠시 후 다시 시도해주세요.') from None

    @staticmethod
    def migrate_token():
        """기존 인증 파일을 설정 DB로 한 번 이전하고 원본 파일은 보존한다."""
        if P.ModelSetting.get('download_worker_token'):
            return
        path = P.ModelSetting.get('download_worker_token_path')
        if not path:
            return
        try:
            with open(path, encoding='utf-8') as stream:
                token = stream.read(1024).strip()
            if re.fullmatch(r'[A-Za-z0-9_.~-]{32,512}', token):
                RemoteWorker._write_settings({'download_worker_token': token,
                    'download_worker_token_path': ''}, only_if_token_empty=True)
        except (OSError, UnicodeError):
            logger.warning('기존 워커 인증 파일을 이전하지 못했습니다. 설정 화면에서 토큰을 입력해주세요.')

    @staticmethod
    def client(endpoint=None, token=None):
        if token is None:
            RemoteWorker.migrate_token()
            token = P.ModelSetting.get('download_worker_token') or ''
        address = P.ModelSetting.get('download_worker_url') if endpoint is None else endpoint
        if not address:
            raise WorkerError('원격 워커 주소를 입력해주세요.')
        return WorkerClient(address, token)

    @staticmethod
    def is_ad(item):
        engine = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
        return str(engine.get('engine_type') or '').lower() == 'alldebrid'

    @staticmethod
    def reserves_ad():
        rows = db.session.query(ModelDownload).filter(ModelDownload.destination_type == 'remote_worker',
            ModelDownload.status != 'completed').all()
        reserved = any(row.status in ('downloaded', 'pending_upload', 'uploading') or
                       bool((row.upload_state or {}).get('worker') and
                            not (row.upload_state or {})['worker'].get('settled')) for row in rows)
        db.session.rollback()
        return reserved

    @staticmethod
    def _relative(value):
        value = str(value or '').strip().strip('/')
        if (not value or any(part in ('', '.', '..') for part in value.split('/')) or
                any(ord(char) < 32 or char in '\\:' for char in value)):
            raise WorkerError('원격 워커의 임시/최종 경로를 확인해주세요.')
        return value

    @classmethod
    def prepare(cls, item, client, configuration, allocation=None):
        from .util_download import DownloadUtil
        from .util_block import DownloadBlock
        profile = FeederUtil.get_download_profile_by_feed(item.feed_name) or {}
        dest = FeederUtil.get_profile_destination(profile, item.current_engine_name)
        options = FeederUtil.get_upload_options(profile, item.current_engine_name)
        mode, account = allocation if allocation is not None else ('shareddrive', None)
        impersonate = []
        if options['use_gdrive_pool'] and allocation is not None:
            if not account or not account.get('username'):
                raise WorkerError('업로드에 사용할 계정 풀 항목이 없습니다.')
            if options['use_impersonate']:
                remote = dest.get('mydrive_remote_name') or dest.get('remote_name') or P.ModelSetting.get('download_gdrive_mydrive_remote_name')
                email = account['username']
                if len(email) > 254 or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._%+-]{0,63}@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?', email):
                    raise WorkerError('계정 풀의 위임 이메일을 확인해주세요.')
                impersonate = ['--drive-impersonate', account['username']]
            else:
                remote = account.get('remote_name')
        else:
            remote = dest.get('remote_name') or P.ModelSetting.get('download_rclone_upload_remote_name')
        remote = str(remote or '').strip()
        drive = str(dest.get('shared_drive_id') or P.ModelSetting.get('download_shared_drive_id') or '').strip()
        if not re.fullmatch(r'[A-Za-z0-9_-]+', remote) or not re.fullmatch(r'[A-Za-z0-9_-]+', drive):
            raise WorkerError('원격 서버와 로컬에 동일한 Drive 리모트명과 목적지 드라이브 ID가 필요합니다.')
        source = item.local_path or ''
        if not re.fullmatch(r'[A-Za-z0-9_-]+:.+', source) or source.startswith('/'):
            raise WorkerError('원격 워커는 WebDAV 리모트 소스만 전송할 수 있습니다.')
        incoming = cls._relative(dest.get('upload_path'))
        final = cls._relative(dest.get('complete_path'))
        if incoming == final or incoming.startswith(final + '/') or final.startswith(incoming + '/'):
            raise WorkerError('임시 업로드 경로와 최종 완료 경로가 서로 겹치지 않도록 지정해주세요.')
        conf = P.ModelSetting.get('download_rclone_conf_path') or FeederUtil.load_yaml().get('rclone', {}).get('conf_path', '')
        parsed = configparser.RawConfigParser()
        try:
            with open(conf, encoding='utf-8') as stream: parsed.read_file(stream)
        except (OSError, UnicodeError, configparser.Error):
            raise WorkerError('최종 이동에 필요한 로컬 Rclone 설정을 확인하지 못했습니다.') from None
        if parsed.get(remote, 'type', fallback='') != 'drive':
            raise WorkerError('원격 서버와 동일한 이름의 로컬 Drive 리모트가 필요합니다.')
        folder = DownloadUtil.reserve_output_folder(item)
        if not folder or folder in ('.', '..') or any(char in folder for char in '/\\'):
            raise WorkerError('안전한 작업 폴더명을 확인하지 못했습니다.')
        prefix = f'{remote}:{{{drive}}}'
        shared_target = f'{prefix}/{incoming}/{folder}'
        if mode == 'mydrive':
            account_drive = str((account or {}).get('mydrive_rclone_id') or '')
            if not re.fullmatch(r'[A-Za-z0-9_-]+', account_drive):
                raise WorkerError('선택한 계정의 MyDrive 폴더 ID를 확인해주세요.')
            target = f'{remote}:{{{account_drive}}}/{incoming}/{folder}'
        else:
            target = shared_target
        payload = {'job_id': f'feeder-{item.id}-{uuid.uuid4().hex}', 'source': source,
                   'destination': target, 'config_version': configuration['config_version'],
                   'exclude_patterns': FeederUtil.get_rclone_exclude_options()[1::2]}
        if impersonate:
            payload['drive_impersonate'] = impersonate[1]
        account_name = account['username'] if account else 'RemoteWorker'
        state = {'phase': 'copying', 'mode': mode, 'uses_pool': bool(account),
                 'account': account_name, 'remote': remote, 'rclone_conf': conf, 'impersonate_arg': impersonate,
                 'incoming': target, 'shared_incoming': shared_target, 'dest_parent': f'{prefix}/{final}',
                 'folder_name': folder, 'local_path': source, 'is_remote_source': True,
                 'engine_name': item.current_engine_name, 'engine_task_id': item.engine_task_id,
                 'worker': {'endpoint': client.endpoint, 'job_id': payload['job_id'], 'payload': payload, 'settled': False}}
        if DownloadBlock.stopped(item.id): raise WorkerError('차단 또는 취소된 작업입니다.')
        item.file_name = folder
        item.gdrive_account = account_name
        item.status = 'uploading'
        item.upload_state = state
        db.session.add(ModelDownloadStat(stat_type='gdrive_usage', stat_key='SHARED_DRIVE_UPLOAD',
            stat_value=float(item.file_size or 0), timestamp=int(time.time())))
        if account:
            db.session.add(ModelDownloadStat(stat_type='gdrive_usage', stat_key=account_name,
                stat_value=float(item.file_size or 0), timestamp=int(time.time())))
        db.session.commit()
        return state

    @classmethod
    def cancel_snapshot(cls, state, timeout=15):
        remote = (state or {}).get('worker')
        if not remote: return
        if remote.get('settled'): return
        client = cls.client(remote['endpoint'])
        job = client.cancel(remote['job_id'])
        deadline = time.monotonic() + timeout
        while job and job.get('status') in ('queued', 'running'):
            if time.monotonic() >= deadline:
                raise WorkerError('원격 전송 종료를 아직 확인하지 못했습니다. 파일 정리를 보류합니다.')
            time.sleep(.3)
            job = client.job(remote['job_id'])
            if job is None:
                job = client.cancel(remote['job_id'])
        if not job or job.get('status') not in ('completed', 'failed', 'cancelled'):
            raise WorkerError('원격 전송 종료를 확인하지 못해 파일 정리를 보류합니다.')

    @classmethod
    def prepare_action(cls, item, action):
        """원격 중단 확인 전에 재시도·삭제·수동 상태 변경을 진행하지 않는다."""
        state = dict(item.upload_state or {})
        if not state.get('worker'): return False
        previous = item.status
        item.status = 'cancelled'
        item.error_message = '원격 전송 중단을 확인 중입니다. 확인 전에는 파일을 정리하지 않습니다.'
        db.session.commit()
        from .util_upload import UploadUtil
        UploadUtil.kill_item_rclone(item.id)
        db.session.refresh(item)
        state = dict(item.upload_state or {})
        cls.cancel_snapshot(state)
        state['worker'] = dict(state['worker'], settled=True)
        item.upload_state = state
        db.session.commit()
        if action == 'retry' and state.get('phase') in ('uploaded', 'shared_incoming', 'moved'):
            item.status = 'pending_upload'
            item.error_message = None
            db.session.commit()
            return True
        if action in ('retry', 'delete') and state.get('phase') == 'copying':
            folder, path = state.get('folder_name'), state.get('incoming', '')
            relative = re.fullmatch(r'[A-Za-z0-9_-]+:\{[A-Za-z0-9_-]+\}/(.+)', path)
            destination = (state.get('worker', {}).get('payload') or {}).get('destination')
            if (not folder or folder in ('.', '..') or any(char in folder for char in '/\\') or
                    not relative or path != destination or not path.endswith('/' + folder) or
                    '/' not in cls._relative(relative.group(1))):
                raise WorkerError('원격 작업의 안전한 임시 경로를 확인하지 못했습니다.')
            success, output = UploadUtil.run_rclone(['rclone', 'purge', path, '--config', state['rclone_conf'],
                '--drive-use-trash=false'] + state.get('impersonate_arg', []), '원격 작업 임시 파일 정리', execution_timeout=60)
            if not success and not any(word in output.lower() for word in ('directory not found', 'object not found', "doesn't exist")):
                raise WorkerError('원격 임시 파일 정리가 끝나지 않았습니다. 다시 시도해주세요.')
            item.upload_state = None
            item.gdrive_account = None
            db.session.commit()
        item.status = previous
        db.session.commit()
        return False

    @staticmethod
    def _progress(item_id, job):
        data = job.get('progress') or {}
        speed = max(0, float(data.get('speed_bytes_per_sec') or 0))
        FeederUtil.report_rclone_progress(item_id, {
            'progress': float(data.get('percent') or 0),
            'speed_str': FeederUtil.format_bytes(speed) + '/s', 'download_speed': speed,
            'downloaded_bytes': max(0, int(data.get('bytes_transferred') or 0)),
            'total_bytes': max(0, int(data.get('total_bytes') or 0)),
            'eta': str(data.get('eta_seconds') or ''), 'worker_status': job.get('status')})

    @classmethod
    def execute(cls, item, manager):
        from .util_upload import UploadUtil
        from .util_block import DownloadBlock
        item_id, hub, held_account = item.id, None, None
        try:
            state = dict(item.upload_state or {})
            if state.get('worker') and state.get('phase') in ('uploaded', 'shared_incoming', 'moved'):
                return UploadUtil.resume_upload(item_id, manager)
            with cls._submit_lock:
                client = cls.client((state.get('worker') or {}).get('endpoint'))
                if not state.get('worker'):
                    while True:
                        if FeederUtil.is_download_paused() or DownloadBlock.stopped(item_id): return False
                        health, configuration = client.health(), client.configuration()
                        if tuple(int(n) for n in health.get('version', '0').split('.')[:3]) < (0, 7, 0):
                            raise WorkerError('Feeder 동시 전송 설정을 적용하려면 원격 워커 0.7.0 이상이 필요합니다.')
                        limit = max(1, P.ModelSetting.get_int('download_upload_workers'))
                        if health['active_jobs'] + health['queued_jobs'] < limit:
                            break
                        db.session.remove()
                        time.sleep(1)
                    target = db.session.get(ModelDownload, item_id)
                    if not target or DownloadBlock.stopped(item_id): return False
                    profile = FeederUtil.get_download_profile_by_feed(target.feed_name) or {}
                    options = FeederUtil.get_upload_options(profile, target.current_engine_name)
                    mode, account, reason = manager.allocate_account(target.file_size or 0, use_pool=options['use_gdrive_pool'])
                    if mode in ('wait', 'skip'):
                        return cls.defer(item_id, '업로드 계정·한도 대기: ' + reason)
                    held_account = account.get('username') if account else None
                    state = cls.prepare(target, client, configuration, (mode, account))
                elif state.get('uses_pool'):
                    if not manager.reserve_existing_account(state['account']):
                        return cls.defer(item_id, '이전 원격 작업의 계정 사용권을 기다립니다.')
                    held_account = state['account']
                remote = state['worker']
                if remote.get('settled'):
                    raise WorkerError('종료된 원격 작업입니다. 항목의 재시도를 실행해주세요.')
                db.session.remove()
                job = client.job(remote['job_id'])
                if job is None:
                    if FeederUtil.is_download_paused():
                        return cls.defer(item_id, '중단 예약으로 원격 작업의 신규 등록을 보류합니다.')
                    if DownloadBlock.stopped(item_id):
                        cls.cancel_snapshot(state)
                        return False
                    health, configuration = client.health(), client.configuration()
                    if health['active_jobs'] + health['queued_jobs'] >= max(1, P.ModelSetting.get_int('download_upload_workers')):
                        return cls.defer(item_id, 'Feeder 동시 전송 설정에 따라 기존 원격 작업의 완료를 기다립니다.')
                    job = client.submit(remote['payload'])
                target = db.session.get(ModelDownload, item_id)
                if (not target or DownloadBlock.stopped(item_id) or
                        (target.upload_state or {}).get('worker', {}).get('settled')):
                    cls.cancel_snapshot(state)
                    return False
                target.status = 'uploading'
                target.error_message = None
                db.session.commit(); db.session.remove()
            hub = WorkerEvents.acquire(client)
            generation = hub.generation
            while True:
                if DownloadBlock.stopped(item_id):
                    cls.cancel_snapshot(state)
                    return False
                job = client.job(remote['job_id'])
                if job is None:
                    raise WorkerError('등록했던 원격 작업이 없습니다. 같은 작업 ID를 유지하고 복구를 기다립니다.')
                if job.get('source') != remote['payload']['source'] or job.get('destination') != remote['payload']['destination']:
                    raise WorkerError('원격 작업의 소스/목적지가 저장된 요청과 일치하지 않습니다.')
                if job.get('drive_impersonate', '') != remote['payload'].get('drive_impersonate', ''):
                    raise WorkerError('원격 작업의 위임 계정이 저장된 요청과 일치하지 않습니다.')
                cls._progress(item_id, job)
                status = job.get('status')
                if status in ('completed', 'failed', 'cancelled'):
                    target = db.session.get(ModelDownload, item_id)
                    if (not target or DownloadBlock.stopped(item_id) or target.status != 'uploading' or
                            (target.upload_state or {}).get('worker', {}).get('settled') or
                            (target.upload_state or {}).get('worker', {}).get('job_id') != remote['job_id']): return False
                    state['worker'] = dict(remote, settled=True)
                    if status == 'completed':
                        state['phase'] = 'uploaded'
                        target.upload_state = state
                        db.session.commit(); db.session.remove()
                        return UploadUtil.resume_upload(item_id, manager, account_reserved=bool(held_account))
                    target.upload_state = state
                    target.status = 'failed'
                    if status == 'failed':
                        details = [redact_log(line, (P.ModelSetting.get('download_worker_token'),))
                                   for line in str(job.get('error_message') or '상세 오류 없음').splitlines()[-9:]]
                        message = f"원격 전송 실패 (종료 코드: {job.get('rclone_exit_code')})"
                        target.error_message = message + '\n' + '\n'.join(details)
                        logger.error('[RemoteWorker] 작업 %s: %s', item_id, target.error_message)
                        DownloadActivity.record('원격 전송 오류', f'작업 {item_id} · {message}', 'error')
                        for line in details:
                            DownloadActivity.record('원격 전송 오류', f'작업 {item_id} · {line}', 'error')
                    else:
                        target.error_message = '원격 전송이 취소됐습니다.'
                    db.session.commit()
                    return False
                if status not in ('queued', 'running'):
                    raise WorkerError('원격 작업 상태를 확인하지 못했습니다.')
                db.session.remove()
                generation = hub.wait(generation)
        except Exception as exc:
            db.session.rollback()
            message = str(exc) if isinstance(exc, WorkerError) else '원격 작업 처리 오류: 기존 작업 ID를 보존하고 재확인합니다.'
            logger.warning('[RemoteWorker] 작업 %s: %s', item_id, message)
            return cls.defer(item_id, message)
        finally:
            if hub: hub.release()
            if held_account: manager.release_account(held_account)
            FeederUtil.report_rclone_progress(item_id, {}, action='clear')
            db.session.remove()

    @staticmethod
    def defer(item_id, message):
        item = db.session.get(ModelDownload, item_id)
        if item and item.status not in ('completed', 'failed', 'blocked', 'cancelled'):
            item.status = 'pending_upload'
            item.error_message = message
            db.session.commit()
        return False
