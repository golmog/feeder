"""원격 복사의 소유권·복구·취소를 기존 업로드 파이프라인과 연결한다."""

import configparser
import os
import re
import threading
import time
import uuid
import hashlib
from datetime import datetime, timedelta
from contextlib import nullcontext

from .setup import *
from .util_base import FeederUtil
from .model_download import ModelDownload, ModelDownloadStat
from .remote_worker_client import WorkerClient, WorkerError, WorkerEvents
from .worker.rclone_logging import redact_log, validate_extra_args
from .util_download_activity import DownloadActivity


class RemoteWorker:
    _submit_lock = threading.Lock()
    _readiness = {}
    _boot_session = None
    _sessions = {}

    @staticmethod
    def settled_worker(remote, job):
        """종료 결과를 FF 체크포인트에 담아 승인 후에도 복구 근거를 남긴다."""
        result = {key: job.get(key) for key in ('status', 'error_type', 'rclone_exit_code', 'chargeable_bytes')}
        result['error_message'] = redact_log(str(job.get('error_message') or ''), limit=3600)
        if job.get('_missing'):
            result['status'] = 'missing'
        return dict(remote, settled=True, submission='accepted', result=result)

    @classmethod
    def acknowledge(cls, state):
        """FF 커밋 뒤에만 호출하며 승인 장애는 이미 확정한 전송 결과를 되돌리지 않는다."""
        remote = state.get('worker') or {}
        session_id = (remote.get('payload') or {}).get('session_id')
        result = remote.get('result') or {}
        if (not remote.get('settled') or remote.get('submission') == 'uncertain' or state.get('cleanup_pending') or
                (state.get('usage') and not state['usage'].get('settled')) or
                not isinstance(session_id, str) or not re.fullmatch(r'[a-f0-9]{32}', session_id) or
                not str(remote.get('job_id') or '').startswith(session_id + '-') or
                (result.get('status') not in ('completed', 'failed', 'cancelled') and
                 state.get('phase') not in ('uploaded', 'shared_incoming', 'moved'))):
            return False
        try:
            response = cls.client(remote['endpoint']).acknowledge(remote['job_id'], session_id)
            if (not isinstance(response, dict) or response.get('job_id') != remote['job_id'] or
                    not (response.get('removed') or response.get('already_absent'))):
                raise WorkerError('워커 결과 승인 응답 확인 실패')
            return True
        except Exception:
            logger.warning('[RemoteWorker] 작업 %s 결과 승인 미확인 · FF 정산/체크포인트 유지, 기존 재시도 또는 다음 세션 초기화에서 정리합니다.', remote.get('job_id'))
            return False

    @classmethod
    def begin_session(cls):
        """FF 로드에서만 세션을 발급하고 같은 실행의 중복 로드는 무시한다."""
        with cls._submit_lock:
            if cls._boot_session is not None:
                return False
            session_id = uuid.uuid4().hex
            P.ModelSetting.set('download_worker_session_id', session_id)
            cls._boot_session = session_id
            cls._sessions.clear()
            cls._readiness.clear()
            return True

    @classmethod
    def wait_for_worker(cls, client, exc, item_id=None):
        """워커 단위 대기를 기록하며 미접수 항목의 오류·재시도 기록은 바꾸지 않는다."""
        key, now = client.endpoint, time.monotonic()
        cached = cls._readiness.get(key)
        code = exc.code
        reconnecting = code in ('unavailable', 'invalid_response') and cached and (
            cached['code'] == 'updating' or cached.get('update_confirmed'))
        if reconnecting:
            code = 'updating'
        level, message = {
            'updating': ('info', '업데이트 후 재연결 대기' if reconnecting or (cached or {}).get('reconnecting') else '워커 업데이트 대기'),
            'checking': ('info', '워커 준비 확인 대기'),
            'full': ('info', '워커 전송 슬롯 대기'),
            'missing_job': ('warning', '원격 접수 작업' + (f' #{item_id}' if type(item_id) is int else '') +
                            ' 기록 없음(HTTP 404) · FF 미정산 상태 확인 중 · 연결 장애 아님'),
            'auth': ('error', '워커 인증 오류 · 연결 설정을 확인해주세요.'),
            'configuration': ('error', '워커 설정·버전 확인이 필요합니다.'),
            'update_failed': ('error', '워커 업데이트 실패 · 서비스 로그를 확인해주세요.'),
        }.get(code, ('warning', '워커 연결·서비스 장애 대기'))
        unchanged = cached and cached['code'] == code
        state = {'after': now + 10, 'message': message, 'status': exc.status, 'code': code, 'item_id': item_id,
                 'since': cached['since'] if unchanged else now,
                 'reported': cached['reported'] if unchanged else now,
                 'update_confirmed': code == 'updating' or bool(code == 'checking' and cached and cached.get('update_confirmed')),
                 'reconnecting': bool(reconnecting or (unchanged and cached.get('reconnecting')))}
        cls._readiness[key] = state
        if not unchanged or state['reconnecting'] != cached.get('reconnecting', False) or now - state['reported'] >= 900:
            delayed = f' · 대기 {int((now - state["since"]) / 60)}분 경과' if unchanged else ''
            if unchanged and state['reconnecting'] and now - state['reported'] >= 900:
                message = '업데이트 후 재연결 지연'
            detail = f'{key} · {message}{delayed} · 10초 후 재확인'
            DownloadActivity.record('원격 워커 대기', detail, level)
            getattr(logger, level)('[RemoteWorker] %s', detail)
            state['reported'] = now
        return WorkerError(state['message'], state['status'], state['code'])

    @classmethod
    def ready(cls, client):
        """신규 제출 전에만 확인하며 유지보수 중에는 엔드포인트별로 재조회를 제한한다."""
        key = client.endpoint
        now = time.monotonic()
        cached = cls._readiness.get(key)
        if cached and now < cached['after']:
            raise WorkerError(cached['message'], cached['status'], cached['code'])
        lease = 'worker_ready:' + hashlib.sha256(key.encode()).hexdigest()[:24]
        owner = uuid.uuid4().hex
        if not FeederUtil.acquire_runtime_lock(lease, owner, stale_after=10):
            raise WorkerError('다른 실행에서 워커 준비 상태를 확인 중입니다. 10초 후 재확인합니다.', 202, 'checking')
        try:
            health = client.ensure_version()
            configuration = client.configuration()
        except WorkerError as exc:
            # 실패 시 임대를 남겨 다른 프로세스도 즉시 같은 확인을 반복하지 않게 한다.
            raise cls.wait_for_worker(client, exc) from None
        except Exception:
            FeederUtil.release_runtime_lock(lease, owner)
            raise
        FeederUtil.release_runtime_lock(lease, owner)
        if cached and cached['code'] != 'full':
            DownloadActivity.record('원격 워커', f'{key} · {cached["message"]} 해제 · 준비 확인 완료')
        if not cached or cached['code'] != 'full':
            cls._readiness.pop(key, None)
        return health, configuration

    @classmethod
    def submission_ready(cls, client, manager=None):
        session_id = P.ModelSetting.get('download_worker_session_id')
        if not session_id:
            raise WorkerError('Feeder 실행 세션이 준비되지 않았습니다.', code='configuration')
        cached = cls._readiness.get(client.endpoint)
        if cached and time.monotonic() < cached['after']:
            raise WorkerError(cached['message'], cached['status'], cached['code'])
        if cls._sessions.get(client.endpoint) != session_id:
            cls.synchronize_session(client, session_id, manager)
        health, configuration = cls.ready(client)
        if health['active_jobs'] + health['queued_jobs'] >= max(1, P.ModelSetting.get_int('download_upload_workers')):
            raise cls.wait_for_worker(client, WorkerError('워커 전송 슬롯 대기', code='full'))
        cached = cls._readiness.pop(client.endpoint, None)
        if cached:
            DownloadActivity.record('원격 워커', f'{client.endpoint} · {cached["message"]} 해제 · 신규 제출 재개')
        return health, configuration

    @classmethod
    def synchronize_session(cls, client, session_id, manager=None):
        """이전 복사의 종료·정산·경로 복구 후에만 워커 작업 기록을 비운다."""
        from .util_upload import UploadUtil
        rows = db.session.query(ModelDownload).filter(ModelDownload.upload_state.isnot(None)).all()
        previous = {}
        for item in rows:
            remote = (item.upload_state or {}).get('worker') or {}
            if remote and (remote.get('payload') or {}).get('session_id') != session_id:
                endpoint = remote.get('endpoint')
                if not endpoint:
                    raise WorkerError('이전 원격 작업의 주소를 확인할 수 없습니다.', code='configuration')
                previous.setdefault(endpoint, []).append(item.id)
        db.session.remove()
        clients = {client.endpoint: client}
        for endpoint in previous:
            clients.setdefault(endpoint, cls.client(endpoint))
        pending = []
        for endpoint, connection in clients.items():
            if cls._sessions.get(endpoint) == session_id:
                continue
            for item_id in previous.get(endpoint, []):
                item = db.session.get(ModelDownload, item_id)
                state = dict(item.upload_state or {})
                remote = state['worker']
                if state.get('phase') in ('uploaded', 'shared_incoming', 'moved') and remote.get('settled'):
                    cls.acknowledge(state)
                    continue
                db.session.remove()
                job = connection.job(remote['job_id'])
                payload = remote.get('payload') or {}
                if job is not None and (job.get('job_id') != remote['job_id'] or
                        not payload.get('source') or not payload.get('destination') or
                        job.get('source') != payload['source'] or job.get('destination') != payload['destination']):
                    raise WorkerError('이전 작업의 ID·전송 경로가 저장한 요청과 일치하지 않습니다.', code='configuration')
                if job and job.get('status') in ('queued', 'running'):
                    job = cls.cancel_snapshot(state)
                if job is None:
                    payload = remote.get('payload') or {}
                    job = dict(payload, job_id=remote['job_id'], status='cancelled', chargeable_bytes=None, _missing=True)
                if item.status not in ('uploading', 'pending_upload', 'move_failed'):
                    if job.get('status') not in ('completed', 'failed', 'cancelled'):
                        raise WorkerError('이전 작업의 종료를 확인하지 못했습니다.', code='unavailable')
                    target = db.session.get(ModelDownload, item_id)
                    UploadUtil.settle_usage(target, manager, job.get('chargeable_bytes'))
                    target.upload_state = dict(target.upload_state or {}, worker=cls.settled_worker(remote, job))
                    db.session.commit()
                    cls.acknowledge(dict(target.upload_state or {}))
                    continue
                if job.get('status') == 'completed':
                    if not cls._complete_copy(item_id, state, job, manager):
                        raise WorkerError('이전 복사 완료 결과를 저장하지 못했습니다.', code='checking')
                elif job.get('status') in ('failed', 'cancelled'):
                    if state.get('phase') in ('uploaded', 'shared_incoming', 'moved') or item.status == 'move_failed':
                        # 이미 저장한 복사 완료 체크포인트는 원격 기록 부재로 되돌리지 않는다.
                        UploadUtil.settle_usage(db.session.get(ModelDownload, item_id), manager, job.get('chargeable_bytes'))
                        target = db.session.get(ModelDownload, item_id)
                        target.upload_state = dict(target.upload_state or {}, worker=cls.settled_worker(remote, job))
                        db.session.commit()
                        cls.acknowledge(dict(target.upload_state or {}))
                        continue
                    cls._restart_incomplete_copy(item_id, manager, state, job)
                    current = db.session.get(ModelDownload, item_id)
                    if current and (current.upload_state or {}).get('worker'):
                        raise WorkerError('이전 작업의 임시 경로 복구를 기다립니다.', code='checking')
                else:
                    raise WorkerError('이전 원격 작업의 종료를 확인하지 못했습니다.', code='unavailable')
                db.session.remove()
            # 상태 필터의 목록은 취소 시 줄어드므로 먼저 모든 페이지를 모은다.
            active = {}
            for status in ('queued', 'running'):
                offset = 0
                while True:
                    page = connection.jobs(limit=100, offset=offset, status=status)
                    jobs = page.get('jobs') if isinstance(page, dict) else None
                    if not isinstance(jobs, list):
                        raise WorkerError('이전 워커 실행 목록을 확인하지 못했습니다.', code='invalid_response')
                    for job in jobs:
                        if job.get('session_id') != session_id and job.get('status') in ('queued', 'running'):
                            active[job['job_id']] = job
                    if len(jobs) < 100:
                        break
                    offset += len(jobs)
            for job_id in active:
                cls.cancel_snapshot({'worker': {'endpoint': endpoint, 'job_id': job_id}})
            pending.append((endpoint, connection))
        for endpoint, connection in pending:
            # 이전 실행 취소를 먼저 끝내어 자동 업데이트의 draining을 해제한다.
            cls.ready(connection)
            result = connection.reset_session(session_id)
            if not isinstance(result, dict) or result.get('session_id') != session_id:
                raise WorkerError('워커 세션 초기화 결과를 확인하지 못했습니다.', code='invalid_response')
        for endpoint, _ in pending:
            cls._sessions[endpoint] = session_id
            DownloadActivity.record('원격 워커', f'{endpoint} · 이전 실행 정산 및 새 세션 초기화 완료')

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
        if FeederUtil.relative_path_error(value):
            raise WorkerError('원격 워커의 임시/최종 경로를 확인해주세요.')
        return value

    @classmethod
    def prepare(cls, item, client, configuration, allocation=None):
        from .util_download import DownloadUtil
        from .util_block import DownloadBlock
        profile = FeederUtil.get_item_profile(item) or {}
        dest = FeederUtil.get_profile_destination(profile, item.current_engine_name, item)
        options = FeederUtil.get_upload_options(profile, item.current_engine_name, item)
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
        payload = {'job_id': f"{P.ModelSetting.get('download_worker_session_id')}-feeder-{item.id}-{uuid.uuid4().hex}", 'source': source,
                   'session_id': P.ModelSetting.get('download_worker_session_id'),
                   'destination': target, 'config_version': configuration['config_version'],
                   'rclone_transfers': FeederUtil.get_rclone_transfers(),
                   'exclude_patterns': FeederUtil.get_rclone_exclude_options()[1::2]}
        try:
            # 로컬 copy/copyto와 동일한 추가 인자. --transfers만 전용 설정으로 관리한다.
            payload['rclone_extra_args'] = validate_extra_args(FeederUtil.get_rclone_transfer_options()[:-2])
        except ValueError as exc:
            raise WorkerError(str(exc), code='configuration') from None
        if impersonate:
            payload['drive_impersonate'] = impersonate[1]
        account_name = account['username'] if account else 'RemoteWorker'
        FeederUtil.pin_transfer_destination(item, profile, dest, options)
        state = {**(item.upload_state or {}), 'phase': 'copying', 'mode': mode, 'uses_pool': bool(account),
                 'account': account_name, 'remote': remote, 'rclone_conf': conf, 'impersonate_arg': impersonate,
                 'incoming': target, 'shared_incoming': shared_target, 'dest_parent': f'{prefix}/{final}',
                 'folder_name': folder, 'local_path': source, 'is_remote_source': True,
                 'engine_name': item.current_engine_name, 'engine_task_id': item.engine_task_id,
                 'worker': {'endpoint': client.endpoint, 'job_id': payload['job_id'], 'payload': payload,
                            'settled': False, 'submission': 'new'}}
        if DownloadBlock.stopped(item.id): raise WorkerError('차단 또는 취소된 작업입니다.')
        item.file_name = folder
        item.gdrive_account = account_name
        item.status = 'uploading'
        item.upload_retry_after = None
        item.upload_state = state
        from .util_upload import UploadUtil
        state['usage'] = UploadUtil.reserve_usage(account_name if account else None, item.file_size or 0,
                                                item.id, payload['job_id'])
        item.upload_state = dict(state)
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
        return job

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
        job = cls.cancel_snapshot(state)
        if job is None and not state['worker'].get('settled'):
            job = cls.client(state['worker']['endpoint']).job(state['worker']['job_id'])
        if action == 'retry' and state.get('phase') == 'copying' and job and job.get('status') == 'completed':
            # 원격에서 끝난 복사는 일반 재시도에서도 다시 지우거나 업로드하지 않는다.
            item.status = 'pending_upload'
            try:
                return cls._complete_copy(item.id, state, job, None)
            except Exception:
                db.session.rollback()
                raise
        state['worker'] = cls.settled_worker(state['worker'], job) if job else dict(state['worker'], settled=True)
        item.upload_state = state
        db.session.commit()
        UploadUtil.settle_usage(item, None, job.get('chargeable_bytes') if job else None)
        state = dict(item.upload_state or {})
        if action == 'retry' and state.get('phase') in ('uploaded', 'shared_incoming', 'moved'):
            item.status = 'pending_upload'
            item.error_message = None
            db.session.commit()
            cls.acknowledge(state)
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
            state.pop('cleanup_pending', None)
            FeederUtil.clear_transfer_state(item)
            item.gdrive_account = None
            db.session.commit()
        item.status = previous
        db.session.commit()
        cls.acknowledge(state)
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
    def _complete_copy(cls, item_id, state, job, manager):
        """완료 증거와 사용량을 저장한 뒤 원격 기록 없이도 최종 이동을 재개한다."""
        from .util_upload import UploadUtil
        remote = state.get('worker') or {}
        payload = remote.get('payload') or {}
        if (not payload.get('source') or not payload.get('destination') or
                job.get('job_id') != remote.get('job_id') or job.get('source') != payload.get('source') or
                job.get('destination') != payload.get('destination') or job.get('status') != 'completed'):
            raise WorkerError('이전 복사 완료 결과의 작업 ID·전송 경로가 일치하지 않습니다.', code='configuration')
        target = db.session.get(ModelDownload, item_id)
        if not target or target.status in ('blocked', 'cancelled', 'failed'):
            return False
        state = dict(target.upload_state or state)
        state['worker'] = cls.settled_worker(remote, job)
        state.pop('restart_recopy', None)
        if state.get('phase') not in ('uploaded', 'shared_incoming', 'moved'):
            state['phase'] = 'uploaded'
        target.upload_state = state
        target.source_unavailable_since = None
        target.transfer_error_type = None
        UploadUtil.settle_usage(target, manager, job.get('chargeable_bytes'))
        db.session.commit()
        cls.acknowledge(dict(target.upload_state or {}))
        return True

    @classmethod
    def _restart_incomplete_copy(cls, item_id, manager, state, job):
        """종료가 확인된 미완료 원격 복사는 임시 파일만 정리한 뒤 새 작업으로 재등록한다."""
        from .util_upload import UploadUtil
        remote = state.get('worker') or {}
        payload = remote.get('payload') or {}
        if (not payload.get('source') or not payload.get('destination') or
                job.get('status') not in ('failed', 'cancelled') or job.get('job_id') != remote.get('job_id') or
                job.get('source') != payload.get('source') or
                job.get('destination') != payload.get('destination')):
            raise WorkerError('이전 원격 작업의 종료 상태 또는 전송 경로를 확인하지 못했습니다.', code='configuration')
        target = db.session.get(ModelDownload, item_id)
        if not target or target.status not in ('uploading', 'pending_upload'):
            return
        # 중단된 이전 시도를 먼저 정산한 뒤 다음 시도는 새 예약/작업 ID를 사용한다.
        UploadUtil.settle_usage(target, manager, job.get('chargeable_bytes'))
        state = dict(target.upload_state or state)
        state['restart_recopy'] = True
        state['worker'] = cls.settled_worker(state['worker'], job)
        state['cleanup_pending'] = True
        target.upload_state = state
        target.status = 'pending_upload'
        target.upload_retry_after = None
        db.session.commit()
        if not UploadUtil.recover_copy_failure(item_id):
            # 경로 확인·정리가 실패하면 기존 스냅샷을 보존하고 다음 주기에 재확인한다.
            return
        target = db.session.get(ModelDownload, item_id)
        if target and target.status == 'pending_upload':
            cls.acknowledge(dict(target.upload_state or {}))
            FeederUtil.clear_transfer_state(target)
            target.gdrive_account = None
            target.error_message = None
            target.transfer_error_type = None
            target.upload_retry_after = None
            db.session.commit()
            DownloadActivity.record('업로드 재전송', f'작업 #{item_id} · 중단된 원격 복사 정리 완료, 새 전송 대기')

    @staticmethod
    def same_job(current, expected):
        """상태 스냅샷 대신 작업 ID·엔드포인트·소유 세션을 비교한다."""
        return (current.get('job_id') == expected.get('job_id') and
                current.get('endpoint') == expected.get('endpoint') and
                (current.get('payload') or {}).get('session_id') == (expected.get('payload') or {}).get('session_id'))

    @classmethod
    def pending_job(cls, item_id, expected):
        """이전 읽기 세션을 버리고 아직 같은 원격 작업을 소유하는 최신 커밋만 읽는다."""
        db.session.remove()
        current = db.session.get(ModelDownload, item_id)
        remote = ((current.upload_state or {}).get('worker') or {}) if current else {}
        if (current and current.status in ('pending_upload', 'uploading') and current.remote_submission() and
                cls.same_job(remote, expected)):
            return current
        return None

    @staticmethod
    def _validate_job(remote, job):
        """원격 응답의 소유권과 전송 옵션을 확인한 뒤에만 접수·정산 상태를 바꾼다."""
        if job.get('job_id') != remote['job_id']:
            raise WorkerError('원격 작업 ID가 저장된 요청과 일치하지 않습니다.', code='configuration')
        payload = remote['payload']
        if job.get('source') != payload['source'] or job.get('destination') != payload['destination']:
            raise WorkerError('원격 작업의 소스/목적지가 저장된 요청과 일치하지 않습니다.', code='configuration')
        if job.get('drive_impersonate', '') != payload.get('drive_impersonate', ''):
            raise WorkerError('원격 작업의 위임 계정이 저장된 요청과 일치하지 않습니다.', code='configuration')
        if job.get('rclone_transfers') != payload.get('rclone_transfers'):
            raise WorkerError('원격 작업의 파일 동시 전송 수가 저장된 요청과 일치하지 않습니다.', code='configuration')
        if job.get('rclone_extra_args') != payload.get('rclone_extra_args'):
            raise WorkerError('원격 작업의 Rclone 추가 옵션이 저장된 요청과 일치하지 않습니다.', code='configuration')

    @classmethod
    def execute(cls, item, manager, reconcile=False):
        from .util_upload import UploadUtil
        from .util_block import DownloadBlock
        item_id, hub, held_account = item.id, None, None
        keep_progress = False
        try:
            state = dict(item.upload_state or {})
            if reconcile and (not item.remote_submission() or state.get('restart_recopy') or
                              state.get('cleanup_pending') or (state.get('worker') or {}).get('recovery_required')):
                return None
            if (state.get('restart_recopy') or (state.get('worker') or {}).get('recovery_required')) and state.get('worker') and state.get('phase') == 'copying':
                # FF 재시작 이전 작업을 계속 전송하지 않는다. 원격 종료 확인 뒤 새 설정으로 재접수한다.
                remote = state['worker']
                client = cls.client(remote['endpoint'])
                job = client.job(remote['job_id'])
                if job is None:
                    # 재연결된 워커의 확정된 404는 이전 job이 없다는 뜻이다.
                    # 네트워크 단절은 client.job()에서 예외가 나므로 기존 ID를 보존한다.
                    payload = remote.get('payload') or {}
                    if not payload.get('source') or not payload.get('destination'):
                        raise WorkerError('이전 원격 작업의 경로 정보를 확인할 수 없습니다.', code='configuration')
                    cls._restart_incomplete_copy(item_id, manager, state,
                        dict(payload, job_id=remote['job_id'], status='cancelled', chargeable_bytes=None, _missing=True))
                    return False
                if job.get('status') in ('running', 'queued'):
                    cls.cancel_snapshot(state)
                    job = client.job(remote['job_id'])
                if not job or job.get('status') not in ('completed', 'failed', 'cancelled'):
                    raise WorkerError('이전 원격 작업의 종료를 확인하지 못했습니다.', code='unavailable')
                if job['status'] != 'completed':
                    cls._restart_incomplete_copy(item_id, manager, state, job)
                    return False
                # 원격 복사가 실제 완료됐다면 새 복사 대신 기존 체크포인트로 최종 이동한다.
                if not cls._complete_copy(item_id, state, job, manager):
                    return False
                return UploadUtil.resume_upload(item_id, manager)
            if state.get('cleanup_pending'):
                return False
            if state.get('worker') and state.get('phase') in ('uploaded', 'shared_incoming', 'moved'):
                cls.acknowledge(state)
                return UploadUtil.resume_upload(item_id, manager)
            with (nullcontext() if reconcile else cls._submit_lock):
                client = cls.client((state.get('worker') or {}).get('endpoint'))
                if not state.get('worker'):
                    if FeederUtil.is_download_paused() or DownloadBlock.stopped(item_id): return False
                    health, configuration = cls.submission_ready(client, manager)
                    if tuple(int(n) for n in health.get('version', '0').split('.')[:3]) < (0, 9, 0):
                        raise WorkerError('오류 분류와 파일 단위 사용량 정산에는 원격 워커 0.9.0 이상이 필요합니다.', code='configuration')
                    target = db.session.get(ModelDownload, item_id)
                    if not target or DownloadBlock.stopped(item_id): return False
                    profile = FeederUtil.get_item_profile(target)
                    previous_state = dict(target.upload_state or {})
                    FeederUtil.pin_transfer_destination(target, profile)
                    options = FeederUtil.get_upload_options(profile, target.current_engine_name, target)
                    mode, account, reason = manager.allocate_account(target.file_size or 0, use_pool=options['use_gdrive_pool'])
                    if mode in ('wait', 'skip'):
                        target.upload_state = previous_state or None
                        return cls.defer(item_id, '업로드 계정·한도 대기: ' + reason)
                    held_account = account.get('username') if account else None
                    state = cls.prepare(target, client, configuration, (mode, account))
                elif state.get('uses_pool') and not reconcile:
                    if not manager.reserve_existing_account(state['account']):
                        return cls.defer(item_id, '이전 원격 작업의 계정 사용권을 기다립니다.')
                    held_account = state['account']
                remote = state['worker']
                db.session.remove()
                try:
                    job = client.job(remote['job_id'])
                except WorkerError as exc:
                    raise cls.wait_for_worker(client, exc) from None
                if job is None:
                    if remote.get('settled'):
                        raise WorkerError('종료된 원격 작업의 결과를 찾지 못했습니다. 항목의 재시도를 실행해주세요.', code='job')
                    if remote.get('submission') not in ('new', 'rejected'):
                        if cls.pending_job(item_id, remote) is None:
                            current = db.session.get(ModelDownload, item_id)
                            return bool(current and current.status == 'completed')
                        raise cls.wait_for_worker(client, WorkerError('기존 원격 작업의 접수 여부 확인 대기', 404, 'missing_job'), item_id)
                    if FeederUtil.is_download_paused():
                        return cls.defer(item_id, '중단 예약으로 원격 작업의 신규 등록을 보류합니다.')
                    if DownloadBlock.stopped(item_id):
                        cls.cancel_snapshot(state)
                        return False
                    health, configuration = cls.submission_ready(client, manager)
                    remote = dict(remote, submission='uncertain')
                    state['worker'] = remote
                    target = db.session.get(ModelDownload, item_id)
                    target.upload_state = dict(state)
                    db.session.commit()
                    try:
                        job = client.submit(remote['payload'])
                    except WorkerError as exc:
                        if exc.status in (400, 401, 403, 409) or exc.code == 'updating':
                            remote = dict(remote, submission='rejected')
                            state['worker'] = remote
                            target.upload_state = dict(state)
                            db.session.commit()
                        cls.wait_for_worker(client, exc)
                        raise
                cls._validate_job(remote, job)
                remote = dict(remote, submission='accepted')
                state['worker'] = remote
                target = db.session.get(ModelDownload, item_id)
                if (not target or target.status not in ('pending_upload', 'uploading') or DownloadBlock.stopped(item_id) or
                        (target.upload_state or {}).get('worker', {}).get('settled') or
                        (target.upload_state or {}).get('worker', {}).get('job_id') != remote['job_id']):
                    if not reconcile:
                        cls.cancel_snapshot(state)
                    return False
                target.status = 'uploading'
                target.upload_state = dict(state)
                target.error_message = None
                db.session.commit(); db.session.remove()
            if not reconcile:
                hub = WorkerEvents.acquire(client)
                generation = hub.generation
            while True:
                if DownloadBlock.stopped(item_id):
                    if not reconcile:
                        cls.cancel_snapshot(state)
                    return False
                if not reconcile:
                    job = client.job(remote['job_id'])
                if job is None:
                    if cls.pending_job(item_id, remote) is None:
                        current = db.session.get(ModelDownload, item_id)
                        return bool(current and current.status == 'completed')
                    raise cls.wait_for_worker(client, WorkerError('기존 원격 작업의 접수 여부 확인 대기', 404, 'missing_job'), item_id)
                cls._validate_job(remote, job)
                cls._progress(item_id, job)
                status = job.get('status')
                if status in ('completed', 'failed', 'cancelled'):
                    target = db.session.get(ModelDownload, item_id)
                    if (not target or DownloadBlock.stopped(item_id) or target.status != 'uploading' or
                            (target.upload_state or {}).get('worker', {}).get('job_id') != remote['job_id']): return False
                    if status == 'completed':
                        if not cls._complete_copy(item_id, state, job, manager):
                            return False
                        db.session.remove()
                        return UploadUtil.resume_upload(item_id, manager, account_reserved=bool(held_account))
                    state['worker'] = cls.settled_worker(remote, job)
                    target.upload_state = state
                    db.session.commit()
                    interrupted = status == 'failed' and (
                        job.get('error_type') == 'WORKER_INTERRUPTED' or
                        job.get('error_message') == '워커가 재시작되어 이전 전송이 중단됐습니다.')
                    amount = job.get('chargeable_bytes')
                    UploadUtil.settle_usage(target, manager, amount)
                    state = dict(target.upload_state or {})
                    target.upload_state = state
                    if status == 'failed':
                        if interrupted:
                            cls._restart_incomplete_copy(item_id, manager, state, job)
                            return False
                        details = [redact_log(line, (P.ModelSetting.get('download_worker_token'),))
                                   for line in str(job.get('error_message') or '상세 오류 없음').splitlines()[-9:]]
                        message = f"원격 전송 실패 (종료 코드: {job.get('rclone_exit_code')})"
                        target.error_message = message + '\n' + '\n'.join(details)
                        logger.error('[RemoteWorker] 작업 %s: %s', item_id, target.error_message)
                        DownloadActivity.record('원격 전송 오류', f'작업 {item_id} · {message}', 'error')
                        for line in details:
                            DownloadActivity.record('원격 전송 오류', f'작업 {item_id} · {line}', 'error')
                        failure = target.error_message
                        db.session.commit()
                        outcome = UploadUtil.transfer_failed(item_id, failure, manager, job.get('chargeable_bytes'),
                                                             structured_type=job.get('error_type', 'UNKNOWN_ERROR'))
                        current = db.session.get(ModelDownload, item_id)
                        if current and not (current.upload_state or {}).get('cleanup_pending'):
                            cls.acknowledge(state)
                        return outcome
                    else:
                        target.status = 'cancelled'
                        target.error_message = '원격 전송이 취소됐습니다.'
                    db.session.commit()
                    cls.acknowledge(state)
                    return False
                if status not in ('queued', 'running'):
                    raise WorkerError('원격 작업 상태를 확인하지 못했습니다.')
                if reconcile:
                    keep_progress = True
                    return None
                db.session.remove()
                generation = hub.wait(generation)
        except Exception as exc:
            db.session.rollback()
            if isinstance(exc, WorkerError) and exc.code in ('updating', 'checking', 'full', 'unavailable',
                    'invalid_response', 'auth', 'configuration', 'update_failed', 'missing_job') and 'client' in locals():
                cached = cls._readiness.get(client.endpoint)
                if not cached or time.monotonic() >= cached['after']:
                    cls.wait_for_worker(client, exc)
                current = db.session.get(ModelDownload, item_id)
                expected = state.get('worker') or {}
                current_remote = ((current.upload_state or {}).get('worker') or {}) if current else {}
                if (current and current.status in ('pending_upload', 'uploading') and
                        not current_remote.get('settled') and cls.same_job(current_remote, expected)):
                    current.status = 'pending_upload'
                    reason = {
                        'updating': '원격 워커 업데이트 대기',
                        'checking': '원격 워커 준비 확인 대기',
                        'full': '원격 워커 전송 슬롯 대기',
                        'invalid_response': '원격 워커 응답 확인 불가',
                        'auth': '원격 워커 인증 확인 필요',
                        'configuration': '원격 워커 설정 확인 필요',
                        'update_failed': '원격 워커 업데이트 실패',
                        'missing_job': '기존 원격 작업 미확인(404) · 재등록 없이 확인 대기',
                    }.get(exc.code, '원격 워커 연결 확인 불가')
                    receipt = current.remote_submission()
                    if receipt:
                        prefix = '원격 접수됨' if receipt == 'accepted' else '원격 접수 확인 필요'
                        current.error_message = prefix + ' · ' + reason + ' · 상태 확인 대기'
                    else:
                        current.error_message = reason + ' · 다음 실행에서 재확인'
                    db.session.commit()
                return None
            message = str(exc) if isinstance(exc, WorkerError) else '원격 작업 처리 오류: 기존 작업 ID를 보존하고 재확인합니다.'
            logger.debug('[RemoteWorker] 작업 %s 일시 대기: %s', item_id, message)
            current = db.session.get(ModelDownload, item_id)
            if current and current.error_message != message and not (isinstance(exc, WorkerError) and exc.code in ('updating', 'checking')):
                DownloadActivity.record('원격 워커 일시 대기', f'작업 #{item_id} · {message}', 'warning')
            return cls.defer(item_id, message)
        finally:
            if hub: hub.release()
            if held_account: manager.release_account(held_account)
            if not keep_progress:
                FeederUtil.report_rclone_progress(item_id, {}, action='clear')
            db.session.remove()

    @staticmethod
    def defer(item_id, message):
        item = db.session.get(ModelDownload, item_id)
        if item and item.status not in ('completed', 'failed', 'blocked', 'cancelled'):
            item.status = 'pending_upload'
            item.error_message = message
            item.upload_retry_after = datetime.now() + timedelta(seconds=30)
            db.session.commit()
        return False
