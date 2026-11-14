"""원래 패키지명과 영상 크기 목록으로 불필요한 전송만 생략한다."""
import os
import re
from datetime import datetime

from .setup import *
from .util_base import FeederUtil
from .util_download import DownloadUtil
from .model_download import ModelDownload
from .util_download_activity import DownloadActivity


class FileDedup:
    VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.m4v', '.wmv', '.ts', '.m2ts', '.webm', '.mpg', '.mpeg'}
    READY_STATES = ('downloading', 'downloaded', 'waiting_transporter', 'pending_local_staging', 'pending_upload')

    @staticmethod
    def minimum_bytes():
        value = P.ModelSetting.get('download_duplicate_min_video_size_mb')
        value = str(value if value is not None else '100').strip()
        if not re.fullmatch(r'[0-9]+', value):
            value = '100'
        try:
            return int(value) * 1024 * 1024
        except ValueError:
            return 100 * 1024 * 1024

    @classmethod
    def video_sizes(cls, tree):
        """영상 크기 하나라도 불명확하면 부분 목록으로 비교하지 않는다."""
        sizes, pending = [], [tree]
        while pending:
            FeederUtil.cooperate()
            nodes = pending.pop()
            if not isinstance(nodes, list):
                return None
            for node in nodes:
                if not isinstance(node, dict) or not isinstance(node.get('n'), str):
                    return None
                if 'e' in node:
                    pending.append(node['e'])
                elif os.path.splitext(node['n'])[1].lower() in cls.VIDEO_EXTENSIONS:
                    size = node.get('s')
                    if type(size) is not int or size < 0:
                        return None
                    sizes.append(size)
        return sorted(sizes)

    @staticmethod
    def selected_sizes(manifest, minimum):
        sizes = manifest.get('sizes')
        if not isinstance(sizes, list) or any(type(size) is not int or size < 0 for size in sizes):
            return []
        return sorted(size for size in sizes if size > 0 and size >= minimum)

    @classmethod
    def fetch_sizes(cls, config, task_id, infohash, source_name):
        """인증 정보·링크를 저장하지 않고 정확한 Ready 작업의 파일 크기만 읽는다."""
        import requests
        token = str(config.get('apikey') or '').strip()
        if not token or not re.fullmatch(r'[0-9]+', task_id):
            return None
        with requests.Session() as session:
            session.trust_env = False
            session.headers.update({'Authorization': 'Bearer ' + token})
            response = session.post('https://api.alldebrid.com/v4.1/magnet/status',
                                    data={'id': task_id}, timeout=(3, 15), allow_redirects=False)
            response.raise_for_status()
            status = response.json()
            if status.get('status') != 'success':
                return None
            magnets = status.get('data', {}).get('magnets', [])
            if isinstance(magnets, dict):
                magnets = [magnets] if 'id' in magnets else list(magnets.values())
            ready = next((m for m in magnets if str(m.get('id')) == task_id), None)
            if not ready or ready.get('statusCode') != 4 or ready.get('filename', '').casefold() != source_name.casefold():
                return None
            if ready.get('hash') and infohash and ready['hash'].lower() != infohash:
                return None
            response = session.post('https://api.alldebrid.com/v4/magnet/files',
                                    data={'id[]': task_id}, timeout=(3, 15), allow_redirects=False)
            response.raise_for_status()
            result = response.json()
            if result.get('status') != 'success':
                return None
            record = next((m for m in result.get('data', {}).get('magnets', []) if str(m.get('id')) == task_id), None)
            return cls.video_sizes(record['files']) if record and not record.get('error') and 'files' in record else None

    @classmethod
    def check(cls, item):
        """조회 실패는 기존 전송을 허용하며 시작된 전송은 변경하지 않는다."""
        try:
            config = FeederUtil.get_downloader_by_name(item.current_engine_name) or {}
            state = item.upload_state or {}
            if (config.get('engine_type') != 'alldebrid' or item.status not in cls.READY_STATES or
                    state.get('worker') or state.get('phase') or FeederUtil.is_download_paused()):
                return False
            raw = item.source_file_name or (item.file_name if not item.output_folder_name else '')
            if not raw:
                return False
            source_key = (os.path.splitext(raw)[0] if DownloadUtil.is_single_file(raw) else raw).casefold()
            task_id, engine_name, infohash = str(item.engine_task_id or ''), item.current_engine_name, (item.infohash or '').lower()
            manifest = item.video_manifest or {}
            same = (manifest.get('source_key') == source_key and manifest.get('engine_name') == engine_name and
                    manifest.get('infohash') == infohash and (not task_id or manifest.get('task_id') == task_id))
            if not same:
                if not config.get('apikey') or not re.fullmatch(r'[0-9]+', task_id):
                    return False
                db.session.commit()
                try:
                    sizes = cls.fetch_sizes(config, task_id, infohash, raw)
                except Exception:
                    sizes = None
                db.session.refresh(item)
                current = item.upload_state or {}
                if (item.status not in cls.READY_STATES or item.current_engine_name != engine_name or
                        str(item.engine_task_id or '') != task_id or (item.infohash or '').lower() != infohash or
                        current.get('worker') or current.get('phase')):
                    return False
                # 조회 불가도 작업 ID별로 보존하여 후속 단계의 반복 API 요청을 막는다.
                manifest = {'source_key': source_key, 'engine_name': engine_name, 'task_id': task_id,
                            'infohash': infohash, 'sizes': sizes, 'stored': False}
                item.source_file_name = raw
                item.video_manifest = manifest
                db.session.commit()
            minimum = cls.minimum_bytes()
            sizes = cls.selected_sizes(manifest, minimum)
            if not sizes:
                return False
            candidates = db.session.query(ModelDownload).filter(
                ModelDownload.id != item.id, ModelDownload.status == 'completed',
                ModelDownload.video_manifest['source_key'].as_string() == source_key,
                ModelDownload.video_manifest['stored'].as_boolean().is_(True),
                ModelDownload.video_manifest['duplicate_of'].as_integer().is_(None)
            ).order_by(ModelDownload.id.asc()).all()
            for original in candidates:
                FeederUtil.cooperate()
                if cls.selected_sizes(original.video_manifest or {}, minimum) != sizes:
                    continue
                db.session.refresh(original)
                db.session.refresh(item)
                proof = original.video_manifest or {}
                current = item.upload_state or {}
                if (original.status != 'completed' or not proof.get('stored') or proof.get('duplicate_of') or
                        proof.get('source_key') != source_key or cls.selected_sizes(proof, minimum) != sizes or
                        item.status not in cls.READY_STATES or item.current_engine_name != engine_name or
                        str(item.engine_task_id or '') != task_id or (item.infohash or '').lower() != infohash or
                        current.get('worker') or current.get('phase') or FeederUtil.is_download_paused()):
                    return False
                item.video_manifest = dict(manifest, duplicate_of=original.id, stored=False)
                item.status = 'completed'
                item.completed_time = datetime.now()
                item.error_message = (f'중복 · 전송 생략 (기존 #{original.id}) · 원폴더명·영상 크기 조합 기준(파일 해시 미검증) 판정입니다. '
                                      '새 목적지에는 파일을 복사하지 않았습니다.')
                db.session.commit()
                DownloadActivity.record('중복 전송 생략', f'작업 #{item.id} · {item.error_message}', 'warning')
                logger.info('[FileDedup] 작업 #%s: %s', item.id, item.error_message)
                return True
            return False
        except Exception:
            db.session.rollback()
            logger.warning('[FileDedup] 파일 크기 비교를 확인하지 못해 기존 전송을 유지합니다.')
            return False
