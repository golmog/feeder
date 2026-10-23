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

from sqlalchemy import and_, case, func, or_
from .setup import F, P, db
from .model_download import ModelDownload, ModelDownloadStat
from .util_base import FeederUtil
from .util_download import DownloadUtil
from .worker.rclone_logging import redact_log


RANKS = {'uploading': 0, 'local_staging': 1, 'relay_transferring': 2, 'downloading': 3,
         'pending_upload': 4, 'pending_local_staging': 5, 'pending_relay': 6,
         'downloaded': 7, 'waiting_transporter': 8, 'pending': 9}


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
              'error_message', 'upload_retry_after', 'source_unavailable_since')

    @classmethod
    def serialize(cls, item):
        result = {field: getattr(item, field) for field in cls.fields}
        for key, value in result.items():
            if isinstance(value, datetime):
                result[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        result['error_message'] = redact_log(result['error_message'] or '', limit=3500)
        result['local_path'] = redact_log(result['local_path'] or '', limit=2000)
        result['row_key'] = 'db:' + str(item.id)
        result['row_version'] = item.updated_time.isoformat(timespec='microseconds') if item.updated_time else ''
        return result

    @staticmethod
    def cursor_key():
        secret = F.app.config.get('SECRET_KEY')
        if not secret:
            raise ValueError('cursor_secret_unavailable')
        return secret if isinstance(secret, bytes) else str(secret).encode()

    @staticmethod
    def cursor(ids):
        if not ids:
            return None
        raw = zlib.compress(json.dumps({'ids': ids, 'at': time.time()}).encode())
        key = Dashboard.cursor_key()
        return base64.urlsafe_b64encode(hmac.digest(key, raw, 'sha256') + raw).decode()

    @staticmethod
    def read_cursor(value):
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
        if not isinstance(ids, list) or any(type(i) is not int or i <= 0 for i in ids):
            raise ValueError('invalid_cursor')
        return ids

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
    def queue(cls, cursor=None, known=None):
        """서명된 잔여 ID 스냅샷. OFFSET이나 외부 엔진/스테이징 디스크에 의존하지 않는다."""
        observed = time.time()
        active = ModelDownload.status.in_(list(RANKS))
        rank = case(*[(ModelDownload.status == status, value) for status, value in RANKS.items()], else_=99)
        ids = [i for (i,) in db.session.query(ModelDownload.id).filter(active).order_by(rank, ModelDownload.id).all()]
        removed = []
        counts = None
        counts_error = False
        if cursor:
            snapshot = cls.read_cursor(cursor)
            batch, remaining = snapshot[:40], snapshot[40:]
            chosen = set(batch).intersection(ids)
        else:
            known = known if isinstance(known, dict) else {}
            known = {int(key): value for key, value in known.items() if str(key).isdigit()}
            chosen = set(ids[:40])
            for row in db.session.query(ModelDownload.id, ModelDownload.updated_time).filter(active, ModelDownload.id.in_(list(known))).all():
                version = row.updated_time.isoformat(timespec='microseconds') if row.updated_time else ''
                if known.get(row.id) != version:
                    chosen.add(row.id)
            removed = list(set(known).difference(ids))
            remaining = [i for i in ids if i not in chosen and i not in known]
            try:
                counts = cls.counts()
            except Exception:
                db.session.rollback()
                counts_error = True
        rows = db.session.query(ModelDownload).filter(ModelDownload.id.in_(chosen)).all() if chosen else []
        items = sorted((cls.serialize(row) for row in rows), key=lambda item: (RANKS[item['status']], item['id']))
        return {'success': True, 'version': 2, 'items': items, 'removed_ids': removed, 'counts': counts, 'counts_error': counts_error,
                'total': len(ids), 'has_more': bool(remaining), 'next_cursor': cls.cursor(remaining),
                'observed_at': observed}

    @staticmethod
    def fetch_engine(config):
        with F.app.app_context():
            owner = uuid.uuid4().hex
            fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()[:24]
            running = 'dashboard_running:' + fingerprint
            cooldown = 'dashboard_poll:' + fingerprint
            try:
                if not FeederUtil.acquire_runtime_lock(cooldown, owner, stale_after=30):
                    raise EngineBusy
                if not FeederUtil.acquire_runtime_lock(running, owner, stale_after=120):
                    raise EngineBusy
                try:
                    engine = DownloadUtil.create_engine(config)
                    if not engine:
                        raise ValueError('engine_unavailable')
                    rows, error = engine.get_status()
                    if error:
                        raise RuntimeError('engine_status_error')
                    return rows, getattr(engine, 'status_list_complete', True)
                finally:
                    FeederUtil.release_runtime_lock(running, owner)
            finally:
                db.session.remove()

    @classmethod
    def engine_view(cls):
        observed = time.time()
        cls.engines.request(FeederUtil.get_downloaders(), cls.fetch_engine)
        snapshots = cls.engines.view()
        # 상태 변경/삭제 뒤에도 엔진 ID/hash만으로 다른 엔진의 행을 인수하지 않는다.
        matches = []
        for snapshot in snapshots:
            tids = [str(item['task_id']) for item in snapshot['items'] if item['task_id']]
            hashes = [str(item['hash']).lower() for item in snapshot['items'] if item['hash']]
            if tids or hashes:
                matches.append(and_(ModelDownload.current_engine_name == snapshot['engine_name'],
                    or_(ModelDownload.engine_task_id.in_(tids), func.lower(ModelDownload.infohash).in_(hashes))))
        match_verified = True
        try:
            rows = db.session.query(ModelDownload.id, ModelDownload.current_engine_name,
                                    ModelDownload.engine_task_id, ModelDownload.infohash).filter(or_(*matches)).all() if matches else []
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
        tids = {(name, str(tid)): i for i, name, tid, hash_value in rows if name and tid}
        hashes = {(name, str(hash_value).lower()): (i, tid) for i, name, tid, hash_value in rows if name and hash_value}
        for snapshot in snapshots:
            snapshot['match_verified'] = match_verified
            name = snapshot['engine_name']
            for item in snapshot['items']:
                matched = tids.get((name, str(item['task_id'])))
                fallback = hashes.get((name, str(item['hash']).lower()))
                if not matched and fallback and (not fallback[1] or not item['task_id']):
                    matched = fallback[0]
                item['db_id'] = matched
        return {'success': True, 'version': 2, 'engines': snapshots,
                'match_verified': match_verified, 'observed_at': observed}

    @classmethod
    def progress(cls, ids):
        items = []
        for item in db.session.query(ModelDownload).filter(ModelDownload.id.in_(ids[:200]),
                ModelDownload.status.in_(['uploading', 'local_staging', 'relay_transferring'])).all():
            stat = FeederUtil.get_rclone_progress(item.id) or {}
            if stat and time.time() - stat.get('_updated_at', 0) > 30:
                stat = dict(stat, download_speed=0, speed_str='갱신 지연')
            items.append({'id': item.id, 'status': item.status, 'row_version': cls.serialize(item)['row_version'],
                          'file_size': stat.get('total_bytes') or item.file_size,
                          **{key: stat[key] for key in ('progress', 'download_speed', 'speed_str', 'downloaded_bytes', 'eta') if key in stat}})
        return {'success': True, 'items': items, 'observed_at': time.time()}
