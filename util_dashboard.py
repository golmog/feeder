"""대시보드 전용 읽기 모델과 엔진별 조회 격리. 다운로드 상태는 수정하지 않는다."""
import base64
import hashlib
import hmac
import json
import math
import threading
import time
import uuid
import zlib
from datetime import datetime, timedelta

from sqlalchemy import and_, func, or_
from .setup import F, P, db
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil
from .util_download import DownloadUtil
from .worker.rclone_logging import redact_log


TRANSFER_STAGES = {'uploading': 'pending_upload', 'local_staging': 'pending_local_staging',
                   'relay_transferring': 'pending_relay'}
RANKS = {'uploading': 0, 'local_staging': 1, 'relay_transferring': 2,
         'pending_upload': 3, 'upload_held': 4, 'pending_local_staging': 5, 'pending_relay': 6,
         'downloaded': 7, 'waiting_transporter': 8, 'downloading': 9, 'pending': 10}


class EngineBusy(Exception):
    pass


def dashboard_lock():
    """협력형 웹 스레드와 실제 조회 스레드 사이에서도 사용할 수 있는 락."""
    try:
        from gevent import monkey
        return monkey.get_original('_thread', 'allocate_lock')()
    except ImportError:
        return threading.Lock()


class EngineStatusCache:
    """프로세스 공유 single-flight. 느린 호출은 요청/SSE 스레드 밖에서 실행한다."""
    def __init__(self, clock=time.time):
        self.clock = clock
        self.lock = dashboard_lock()
        self.records = {}
        self.pool = None
        self.inflight_keys = set()

    @staticmethod
    def key(config):
        return hashlib.sha256(str(config.get('name', '')).encode()).hexdigest()[:24]

    def request(self, configs, fetch):
        with self.lock:
            enabled = {self.key(c): c for c in configs if c.get('enabled', True) not in (False, 'False', 'false', 0)}
            for key in list(self.records):
                if key not in enabled:
                    self.records.pop(key)
            for key, config in enabled.items():
                fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()
                record = self.records.get(key)
                if record and record['fingerprint'] == fingerprint:
                    record.pop('pending_fingerprint', None)
                if record and record['fingerprint'] != fingerprint and record['inflight']:
                    # 설정 변경도 기존 호출이 끝난 다음 적용해 같은 엔진의 중첩 조회를 막는다.
                    record['pending_fingerprint'] = fingerprint
                    continue
                if not record or record['fingerprint'] != fingerprint:
                    record = {'fingerprint': fingerprint, 'engine_key': key, 'engine_name': str(config.get('name', '')),
                              'items': {}, 'state': 'loading', 'complete': False, 'last_success_at': None,
                              'last_attempt_at': None, 'after': 0, 'inflight': False, 'error_type': None}
                    self.records[key] = record
                if key in self.inflight_keys or self.clock() < record['after'] or len(self.inflight_keys) >= 3:
                    continue
                if self.pool is None:
                    try:
                        from gevent import monkey
                        if not monkey.is_module_patched('threading'):
                            raise ImportError
                        from gevent.threadpool import ThreadPoolExecutor
                    except ImportError:
                        from concurrent.futures import ThreadPoolExecutor
                    self.pool = ThreadPoolExecutor(max_workers=3)
                record.update(inflight=True, last_attempt_at=self.clock())
                self.inflight_keys.add(key)
                try:
                    self.pool.submit(self.refresh, key, fingerprint, dict(config), fetch)
                except Exception:
                    record.update(inflight=False, state='error', after=self.clock() + 45, error_type='executor_unavailable')
                    self.inflight_keys.discard(key)

    def refresh(self, key, fingerprint, config, fetch):
        try:
            rows, complete = fetch(config)
            if not isinstance(rows, list) or not all(isinstance(row, dict) and (row.get('task_id') or row.get('hash')) for row in rows):
                raise ValueError('목록 형식 오류')
            now = self.clock()
            clean = {}
            for row in rows:
                identity = str(row['task_id']) if row.get('task_id') else str(row['hash']).lower()
                item = {k: row.get(k) for k in ('task_id', 'hash', 'filename', 'source_path', 'status')}
                for field in ('file_size', 'downloaded_bytes', 'download_speed', 'progress'):
                    value = row.get(field, 0)
                    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                        value = 0
                    item[field] = value
                # 표시용 0 보정과 실제 확인된 0%를 정렬에서 구별한다.
                progress = row.get('progress')
                if type(progress) not in (int, float) or not math.isfinite(progress) or not 0 <= progress <= 100:
                    size, done = row.get('file_size'), row.get('downloaded_bytes')
                    progress = (done / size * 100 if type(size) in (int, float) and math.isfinite(size) and size > 0
                                and type(done) in (int, float) and math.isfinite(done) and 0 <= done <= size else None)
                item['sort_progress'] = progress
                for field in ('task_id', 'hash', 'filename', 'source_path', 'status'):
                    item[field] = redact_log(item[field] or '', limit=2000)
                item['last_seen_at'] = now
                clean[identity] = item
            with self.lock:
                record = self.records.get(key)
                if not record or record['fingerprint'] != fingerprint:
                    return
                if not complete:
                    clean = dict(record['items'], **clean)
                record.update(items=clean, state='ready' if complete else 'stale', complete=bool(complete),
                              last_success_at=now, error_type=None, after=now + 30)
        except Exception as exc:
            with self.lock:
                record = self.records.get(key)
                if record and record['fingerprint'] == fingerprint:
                    busy = isinstance(exc, EngineBusy)
                    record.update(state='stale' if record['items'] else 'loading' if busy else 'error',
                                  complete=False, error_type='refresh_in_other_process' if busy else type(exc).__name__,
                                  after=self.clock() + (5 if busy else 45))
        finally:
            with self.lock:
                self.inflight_keys.discard(key)
                record = self.records.get(key)
                if record and record['fingerprint'] == fingerprint:
                    record['inflight'] = False

    def view(self):
        now = self.clock()
        with self.lock:
            snapshots = []
            for record in self.records.values():
                data = {key: record[key] for key in ('engine_key', 'engine_name', 'state', 'complete',
                        'last_success_at', 'last_attempt_at', 'error_type')}
                data['config_revision'] = record['fingerprint']
                if record['inflight']:
                    data['state'] = 'loading'
                if record.get('pending_fingerprint'):
                    data.update(config_revision=record['pending_fingerprint'], state='loading', complete=False, items=[])
                    snapshots.append(data)
                    continue
                record['items'] = {key: row for key, row in record['items'].items() if now - row['last_seen_at'] <= 300}
                items = []
                for row in record['items'].values():
                    stale = record['state'] != 'ready' and row['last_seen_at'] < (record['last_attempt_at'] or now)
                    stale = stale or record['inflight']
                    stale = stale or record['error_type'] is not None or now - row['last_seen_at'] > 60
                    item = dict(row, stale=stale)
                    if stale:
                        item['download_speed'] = 0
                    items.append(item)
                data['items'] = items
                snapshots.append(data)
            return snapshots


class Dashboard:
    engines = EngineStatusCache()
    ranks = RANKS
    _metric_lock = dashboard_lock()
    _metric = {'success': True, 'counts': None, 'state': 'loading', 'observed_at': 0}
    _metric_running = False
    _metric_after = 0

    @classmethod
    def upload_counts(cls, read):
        with cls._metric_lock:
            if not cls._metric_running and time.time() >= cls._metric_after:
                cls._metric_running = True
                # 전송/엔진 조회와 별개인 공유 슬롯 하나만 사용한다.
                if not hasattr(cls, '_metric_pool'):
                    try:
                        from gevent import monkey
                        if not monkey.is_module_patched('threading'):
                            raise ImportError
                        from gevent.threadpool import ThreadPoolExecutor
                    except ImportError:
                        from concurrent.futures import ThreadPoolExecutor
                    cls._metric_pool = ThreadPoolExecutor(max_workers=1)
                cls._metric_pool.submit(cls._read_upload_counts, read)
            return dict(cls._metric)

    @classmethod
    def _read_upload_counts(cls, read):
        try:
            with F.app.app_context():
                try:
                    counts = read()
                finally:
                    db.session.remove()
            with cls._metric_lock:
                cls._metric = {'success': True, 'counts': counts, 'state': 'ready', 'observed_at': time.time()}
        except Exception:
            with cls._metric_lock:
                cls._metric = dict(cls._metric, state='stale' if cls._metric['counts'] else 'error')
        finally:
            with cls._metric_lock:
                cls._metric_running = False
                cls._metric_after = time.time() + 15
    fields = ('id', 'created_time', 'updated_time', 'feed_name', 'title', 'file_name', 'file_size',
              'status', 'current_engine_name', 'engine_task_id', 'infohash', 'local_path', 'destination_type',
              'error_message', 'upload_retry_after', 'source_unavailable_since', 'last_status_time')

    @classmethod
    def serialize(cls, item):
        result = {field: getattr(item, field) for field in cls.fields}
        for key, value in result.items():
            if isinstance(value, datetime):
                result[key] = (value.isoformat(timespec='microseconds') if key in ('last_status_time', 'upload_retry_after')
                               else value.strftime('%Y-%m-%d %H:%M:%S'))
        result['error_message'] = redact_log(result['error_message'] or '', limit=3500)
        result['local_path'] = redact_log(result['local_path'] or '', limit=2000)
        # 경로 자체를 노출하지 않고 로컬 스테이징 결과 여부만 UI에 전달한다.
        staging_path = (item.upload_state or {}).get('staging_local_path')
        result['hold_reason'] = ((item.upload_state or {}).get('hold') or {}).get('reason')
        result['staging_completed'] = bool(item.status == 'downloaded' and staging_path and staging_path == item.local_path)
        result['row_key'] = 'db:' + str(item.id)
        result['remote_submission'] = item.remote_submission()
        result['row_version'] = item.updated_time.isoformat(timespec='microseconds') if item.updated_time else ''
        return result

    @staticmethod
    def cursor_key():
        secret = F.app.config.get('SECRET_KEY')
        if not secret:
            raise ValueError('cursor_secret_unavailable')
        return secret if isinstance(secret, bytes) else str(secret).encode()

    @staticmethod
    def cursor(ids, seen=None):
        if not ids:
            return None
        data = {'ids': ids, 'at': time.time()}
        if seen is not None:
            data['seen'] = seen
        raw = zlib.compress(json.dumps(data).encode())
        key = Dashboard.cursor_key()
        return base64.urlsafe_b64encode(hmac.digest(key, raw, 'sha256') + raw).decode()

    @staticmethod
    def read_cursor(value, with_seen=False):
        if not isinstance(value, str) or len(value) > 500000:
            raise ValueError('invalid_cursor')
        raw = base64.b64decode(value.encode(), altchars=b'-_', validate=True)
        signature, body = raw[:32], raw[32:]
        if not hmac.compare_digest(signature, hmac.digest(Dashboard.cursor_key(), body, 'sha256')):
            raise ValueError('invalid_cursor')
        decoder = zlib.decompressobj()
        decoded = decoder.decompress(body, 2000000)
        if not decoder.eof or decoder.unused_data:
            raise ValueError('invalid_cursor')
        data = json.loads(decoded)
        if not 0 <= time.time() - data['at'] <= 600:
            raise ValueError('snapshot_expired')
        ids = data['ids']
        def valid(key):
            return ((type(key) is int and key > 0) or (isinstance(key, str) and len(key) <= 4096 and
                    ((key.startswith('db:') and key[3:].isdigit() and int(key[3:]) > 0) or key.startswith('engine:'))))
        if (not isinstance(ids, list) or not all(valid(i) for i in ids) or
                not isinstance(data.get('seen', []), list) or not all(valid(i) for i in data.get('seen', []))):
            raise ValueError('invalid_cursor')
        return data if with_seen else ids

    @staticmethod
    def counts():
        grouped = dict(db.session.query(ModelDownload.status, func.count(ModelDownload.id)).group_by(ModelDownload.status).all())
        usage_rows = db.session.query(ModelDownloadStat.stat_key, func.sum(ModelDownloadStat.stat_value)).filter(
            ModelDownloadStat.stat_type == 'gdrive_usage', ModelDownloadStat.timestamp > int(time.time()) - 86400
        ).group_by(ModelDownloadStat.stat_key).all()
        usage = {key: int(value or 0) for key, value in usage_rows}
        total_download = db.session.query(func.sum(ModelDownload.file_size)).filter(ModelDownload.success_filter(),
            ModelDownload.completed_time >= datetime.now() - timedelta(hours=24)).scalar() or 0
        return {'pending': grouped.get('pending', 0) + grouped.get('pending_relay', 0),
                'downloading': grouped.get('downloading', 0),
                'staging': grouped.get('pending_local_staging', 0) + grouped.get('local_staging', 0),
                'completed': db.session.query(ModelDownload).filter(ModelDownload.success_filter()).count(),
                'failed': db.session.query(ModelDownload).filter(ModelDownload.failure_filter()).count(),
                'total_download_24h': int(total_download),
                'total_upload_24h': usage.get('SHARED_DRIVE_UPLOAD', sum(usage.values()))}

    @classmethod
    def stage_ranks(cls, active_stage):
        """실제 전송을 모두 앞세우고 각 그룹 안에서 현재 전송 단계를 우선한다."""
        stages = list(TRANSFER_STAGES)
        if active_stage in stages:
            stages.remove(active_stage)
            stages.insert(0, active_stage)
        order = stages + [TRANSFER_STAGES[stage] for stage in stages]
        order.insert(order.index('pending_upload') + 1, 'upload_held')
        order += ['downloaded', 'waiting_transporter', 'downloading', 'pending']
        return {status: rank for rank, status in enumerate(order)}

    @classmethod
    def queue(cls, cursor=None, known=None):
        """DB 정렬 인덱스와 기존 엔진 캐시를 합쳐 하나의 서명 커서로 40건씩 선택한다."""
        observed = time.time()
        sort_time = datetime.fromtimestamp(observed)
        active = ModelDownload.status.in_(list(RANKS))
        # 첫 페이지나 외부 진행률이 아닌 전체 활성 DB 상태로 현재 단계를 결정한다.
        active_rows = db.session.query(ModelDownload.id, ModelDownload.status,
                                       ModelDownload.last_status_time, ModelDownload.upload_retry_after).filter(active).all()
        states = {row.status for row in active_rows}
        active_stage = next((stage for stage in TRANSFER_STAGES if stage in states), None)
        ranks = cls.stage_ranks(active_stage)

        def sort_key(row):
            # 기존 실행기의 후보 우선순위를 표시 순서에 반영한다.
            deferred = row.status in ('pending_upload', 'pending_local_staging') and bool(
                row.upload_retry_after and row.upload_retry_after > sort_time)
            status_time = row.last_status_time if row.status == 'pending_upload' else None
            return ranks.get(row.status, 99), deferred, status_time is not None, status_time or datetime.min, row.id

        ids = [row.id for row in sorted(active_rows, key=sort_key)]
        cached = cls.engine_view(refresh=False)
        remote = {}
        for snapshot in cached['engines']:
            for item in snapshot['items']:
                if not item.get('registered'):
                    remote[item['row_key']] = (snapshot, item)
        keys = ['db:' + str(i) for i in ids] + sorted(remote)
        present = set(keys)
        known = known if isinstance(known, dict) else {}
        known = {('db:' + str(k) if str(k).isdigit() else str(k)): v for k, v in known.items()}
        counts = None
        counts_error = False
        if cursor:
            snapshot = cls.read_cursor(cursor, with_seen=True)
            seen = set(snapshot.get('seen', []))
            if 'seen' not in snapshot:
                seen = set('db:' + str(i) for i in ids).difference('db:' + str(i) for i in snapshot['ids'])
            removed = seen.difference(present)
            batch = [key for key in keys if key not in seen][:40]
            chosen = set(batch)
            seen.update(batch)
        else:
            seen = set(known).intersection(present)
            # 화면 내 갱신은 펼친 깊이를 유지하고 완료/소멸로 빈 자리만 보충한다.
            added = [key for key in keys if key not in seen][:max(0, max(40, len(known)) - len(seen))]
            seen.update(added)
            chosen = set(added)
            known_ids = [int(key[3:]) for key in seen if key.startswith('db:')]
            for row in db.session.query(ModelDownload.id, ModelDownload.updated_time).filter(active, ModelDownload.id.in_(known_ids)).all():
                version = row.updated_time.isoformat(timespec='microseconds') if row.updated_time else ''
                if known.get('db:' + str(row.id)) != version:
                    chosen.add('db:' + str(row.id))
            chosen.update(key for key in seen if key.startswith('engine:'))
            removed = set(known).difference(present)
            try:
                counts = cls.counts()
            except Exception:
                db.session.rollback()
                counts_error = True
        chosen_ids = [int(key[3:]) for key in chosen if key.startswith('db:')]
        rows = db.session.query(ModelDownload).filter(ModelDownload.id.in_(chosen_ids)).all() if chosen_ids else []
        details = {'db:' + str(row.id): cls.serialize(row) for row in rows}
        for key in chosen.intersection(remote):
            snapshot, item = remote[key]
            verified = item['match_verified']
            details[key] = dict(id=str(item.get('task_id') or item.get('hash'))[:10], row_key=key,
                task_id=item.get('task_id'), hash=item.get('hash'), title=item.get('filename'), file_name=item.get('filename'),
                file_size=item.get('file_size'), local_path=item.get('source_path'), feed_name=snapshot['engine_name'],
                current_engine_name=snapshot['engine_name'], status='engine_unmanaged' if verified else 'engine_unverified',
                raw_status=item.get('status'), engine_stale=bool(item.get('stale')), created_time='원격 보관',
                last_seen_at=item.get('last_seen_at'), row_version=str(item.get('last_seen_at')))
        remaining = [key for key in keys if key not in seen]
        unverified = sum(not item['match_verified'] for _, item in remote.values())
        return {'success': True, 'version': 3, 'auto_disabled_engines': FeederUtil.auto_disabled_engines(),
                'items': [details[key] for key in keys if key in details],
                'removed_ids': [int(key[3:]) for key in removed if key.startswith('db:')],
                'removed_keys': [key for key in removed if key.startswith('engine:')], 'counts': counts, 'counts_error': counts_error,
                'total': len(keys), 'db_total': len(ids), 'orphan_total': len(remote) - unverified, 'unverified_total': unverified,
                'has_more': bool(remaining), 'next_cursor': cls.cursor(remaining, sorted(seen)),
                'observed_at': observed, 'active_stage': active_stage,
                'queue_time': sort_time.isoformat(timespec='microseconds')}

    @staticmethod
    def fetch_engine(config):
        with F.app.app_context():
            owner = uuid.uuid4().hex
            fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()[:24]
            running = 'dashboard_running:' + fingerprint
            cooldown = 'dashboard_poll:' + fingerprint
            try:
                # A submitted refresh may outlive the enabled configuration it captured.
                current = FeederUtil.get_downloader_by_name(config.get('name', ''))
                if config.get('engine_type') == 'pikpak' and (current is None or
                        DownloadUtil.pikpak_config_revision(current) != DownloadUtil.pikpak_config_revision(config)):
                    return [], True
                if ((current is not None and current.get('enabled', True) in (False, 'False', 'false', 0)) or
                        not DownloadUtil.pikpak_dashboard_allowed(current or config) or
                        not DownloadUtil.pikpak_dashboard_allowed(config)):
                    return [], True
                if not FeederUtil.acquire_runtime_lock(cooldown, owner, stale_after=30):
                    raise EngineBusy
                if not FeederUtil.acquire_runtime_lock(running, owner, stale_after=120):
                    raise EngineBusy
                try:
                    engine = DownloadUtil.create_engine(config)
                    if not engine:
                        raise ValueError('engine_unavailable')
                    rows, error = engine.get_status(resolve_paths=False) if config.get('engine_type') == 'pikpak' else engine.get_status()
                    if error:
                        FeederUtil.engine_auth_error(engine, error, 'get_status')
                        raise RuntimeError('engine_status_error')
                    return rows, getattr(engine, 'status_list_complete', True)
                finally:
                    FeederUtil.release_runtime_lock(running, owner)
            finally:
                db.session.remove()

    @classmethod
    def engine_view(cls, refresh=True, visible=None):
        observed = time.time()
        configs = [c for c in FeederUtil.get_downloaders() if c.get('enabled', True) not in (False, 'False', 'false', 0)
                   and DownloadUtil.pikpak_dashboard_allowed(c)]
        if refresh:
            cls.engines.request(configs, cls.fetch_engine)
        allowed = {c['name'] for c in configs}
        snapshots = [s for s in cls.engines.view() if s['engine_name'] in allowed]
        # 콘텐츠 귀속은 유효한 infohash로만 판정하며 ID는 동일 해시 안의 동률만 해소한다.
        hashes = set()
        for snapshot in snapshots:
            for item in snapshot['items']:
                value = (FeederUtil.extract_info_hash('magnet:?xt=urn:btih:' + str(item.get('hash') or '')) or '').lower()
                item['hash'] = value if len(value) == 40 and all(c in '0123456789abcdef' for c in value) else ''
                if item['hash']:
                    hashes.add(item['hash'])
        match_verified = True
        try:
            rows = db.session.query(ModelDownload.id, ModelDownload.current_engine_name,
                                    ModelDownload.engine_task_id, ModelDownload.infohash).filter(
                                        func.lower(ModelDownload.infohash).in_(hashes)).all() if hashes else []
        except Exception:
            # DB 보강 실패를 정상 원격 캐시의 조회 실패로 확대하지 않는다.
            match_verified = False
            rows = []
            try:
                db.session.rollback()
            except Exception:
                try:
                    db.session.remove()
                except Exception:
                    pass
        by_hash = {}
        by_id = {row.id: row for row in rows}
        for row in rows:
            by_hash.setdefault(str(row.infohash).lower(), []).append(row)
        visible = set(visible) if visible is not None else None
        orphan_total = unverified_total = 0
        for snapshot in snapshots:
            snapshot['match_verified'] = match_verified
            name = snapshot['engine_name']
            speed, speed_known = 0, False
            for item in snapshot['items']:
                candidates = by_hash.get(item['hash'], [])
                pool = [r for r in candidates if r.current_engine_name == name] or candidates
                exact = [r for r in pool if item.get('task_id') and str(r.engine_task_id) == str(item['task_id'])]
                matched = pool[0] if len(pool) == 1 else exact[0] if len(exact) == 1 else None
                item['db_id'] = matched.id if matched else None
                item['registered'] = bool(candidates)
                item['match_verified'] = match_verified and bool(item['hash'])
                identity = str(item.get('task_id') or item['hash'])
                item['row_key'] = 'engine:' + str(snapshot.get('engine_key') or EngineStatusCache.key({'name': name})) + ':' + identity
                if not candidates:
                    if item['match_verified']:
                        orphan_total += 1
                    else:
                        unverified_total += 1
                stale = item.get('stale') or snapshot.get('state') == 'error' or snapshot.get('error_type') or observed - item.get('last_seen_at', 0) > 60
                value = item.get('download_speed')
                if not stale and item.get('status') == 'downloading' and type(value) in (int, float) and math.isfinite(value) and value >= 0:
                    speed += value
                    speed_known = True
            snapshot['download_speed_total'] = speed if speed_known else None
            if visible is not None:
                selected = {}
                for item in snapshot['items']:
                    row = by_id.get(item['db_id'])
                    key = item['row_key'] if item['row_key'] in visible else 'db:' + str(item['db_id'])
                    if key not in visible or (key.startswith('db:') and (not row or row.current_engine_name != name)):
                        continue
                    old = selected.get(key)
                    if old is None or (row and str(row.engine_task_id) == str(item.get('task_id'))):
                        selected[key] = item
                snapshot['items'] = list(selected.values())
        return {'success': True, 'version': 3, 'engines': snapshots,
                'orphan_total': orphan_total, 'unverified_total': unverified_total,
                'match_verified': match_verified, 'observed_at': observed}

    @classmethod
    def progress(cls, ids):
        items = []
        for item in db.session.query(ModelDownload).filter(ModelDownload.id.in_(ids[:200]),
                or_(ModelDownload.status.in_(['uploading', 'local_staging', 'relay_transferring']),
                    and_(ModelDownload.status == 'downloaded', ModelDownload.destination_type == 'rclone_simple'))).all():
            stat = FeederUtil.get_rclone_progress(item.id) or {}
            stale = bool(stat) and time.time() - stat.get('_updated_at', 0) > 30
            active = bool(stat) and not stale and (
                (stat.get('downloaded_bytes') or 0) > 0 or (stat.get('download_speed') or 0) > 0)
            if stale:
                stat = dict(stat, download_speed=0, speed_str='갱신 지연')
            items.append({'id': item.id, 'status': item.status, 'row_version': cls.serialize(item)['row_version'],
                          'file_size': stat.get('total_bytes') or item.file_size,
                          **{key: stat[key] for key in ('progress', 'download_speed', 'speed_str', 'downloaded_bytes') if key in stat},
                          'eta': (stat.get('eta') if stat.get('eta') is not None else '') if active else '',
                          'transfer_active': active, 'progress_stale': stale})
        return {'success': True, 'items': items, 'observed_at': time.time()}
