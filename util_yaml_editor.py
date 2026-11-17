"""Feeder가 관리하는 YAML 한 파일의 원문 편집과 충돌 검사를 담당한다."""

import codecs
import hashlib
import logging
import os
from pathlib import Path
import re
import stat
import tempfile
import threading

import yaml

from .util_base import FeederUtil


logger = logging.getLogger(__name__)


class FeederYamlEditor:
    _lock = threading.Lock()

    @staticmethod
    def _revision(raw, metadata):
        identity = f'{metadata.st_dev}:{metadata.st_ino}:{metadata.st_mtime_ns}:{metadata.st_mode}'.encode()
        return hashlib.sha256(identity + b'\0' + raw).hexdigest()

    @classmethod
    def _snapshot(cls):
        path = Path(FeederUtil.get_filepath()).resolve()
        with path.open('rb') as source:
            raw = source.read()
            metadata = os.fstat(source.fileno())
        return path, raw, metadata, cls._revision(raw, metadata)

    @classmethod
    def read(cls):
        try:
            _, raw, _, revision = cls._snapshot()
            content = raw.decode('utf-8-sig')
            return {'ret': 'success', 'content': content, 'revision': revision,
                    'newline': 'windows' if '\r\n' in content else 'unix'}
        except (OSError, UnicodeError):
            return {'ret': 'fail', 'msg': 'YAML 파일을 읽지 못했습니다. UTF-8 인코딩과 파일 접근 권한을 확인해주세요.'}

    @classmethod
    def save(cls, content, revision):
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError as exc:
            mark = getattr(exc, 'problem_mark', None)
            location = f' ({mark.line + 1}행 {mark.column + 1}열)' if mark is not None else ''
            return {'ret': 'fail', 'msg': 'YAML 문법 오류' + location + '를 수정해주세요.'}
        if not isinstance(parsed, dict):
            return {'ret': 'fail', 'msg': 'YAML 최상위 값은 키와 값으로 이루어진 매핑이어야 합니다.'}

        temporary = None
        try:
            with cls._lock:
                path, original, metadata, current = cls._snapshot()
                if not revision or revision != current:
                    return {'ret': 'conflict', 'msg': '다른 화면이나 작업에서 YAML을 변경했습니다. 편집 내용을 복사해 둔 뒤 다시 불러와주세요.'}
                old_text = original.decode('utf-8-sig')
                normalized = content.replace('\r\n', '\n').replace('\r', '\n')
                if normalized == old_text.replace('\r\n', '\n').replace('\r', '\n'):
                    raw = original
                else:
                    ending = re.search(r'\r\n|\r|\n', old_text)
                    text = normalized.replace('\n', ending.group() if ending else '\n')
                    raw = (codecs.BOM_UTF8 if original.startswith(codecs.BOM_UTF8) else b'') + text.encode('utf-8')
                descriptor, temporary = tempfile.mkstemp(prefix='.feeder_settings.', suffix='.tmp', dir=path.parent)
                with os.fdopen(descriptor, 'wb') as target:
                    target.write(raw)
                    own = os.fstat(target.fileno())
                    if (own.st_uid, own.st_gid) != (metadata.st_uid, metadata.st_gid):
                        os.fchown(target.fileno(), metadata.st_uid, metadata.st_gid)
                    os.fchmod(target.fileno(), stat.S_IMODE(metadata.st_mode))
                    target.flush()
                    os.fsync(target.fileno())
                    saved_metadata = os.fstat(target.fileno())
                # 임시 파일을 쓰는 동안 생긴 외부 변경도 교체 직전에 확인한다.
                latest_path, _, _, latest = cls._snapshot()
                if latest_path != path or latest != revision:
                    return {'ret': 'conflict', 'msg': '저장 중 YAML이 변경되어 덮어쓰지 않았습니다. 편집 내용을 보존하고 다시 불러와주세요.'}
                os.replace(temporary, path)
                temporary = None
                return {'ret': 'success', 'revision': cls._revision(raw, saved_metadata), 'msg': 'YAML을 저장했습니다.'}
        except (OSError, UnicodeError) as exc:
            logger.warning('YAML 원문 저장 실패 (%s)', type(exc).__name__)
            return {'ret': 'fail', 'msg': 'YAML을 저장하지 못했습니다. 기존 파일과 편집 내용은 유지됩니다. 접근 권한과 저장 공간을 확인해주세요.'}
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    logger.warning('YAML 편집 임시 파일을 정리하지 못했습니다.')
