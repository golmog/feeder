"""전송을 시작하지 않는 운영 검사와 온라인 DB 백업 명령."""

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import os
import sqlite3
import uuid

from . import create_app


def backup(database):
    directory = database.path.parent / 'backups'
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = directory / ('worker-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex + '.db')
    descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    try:
        with closing(database.connect()) as source, closing(sqlite3.connect(destination)) as target:
            source.backup(target, pages=128)
            if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise RuntimeError('백업 DB의 무결성을 확인하지 못했습니다.')
        return destination
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description='전송 없는 워커 점검과 일관된 SQLite 백업')
    parser.add_argument('command', choices=('check', 'backup'))
    args = parser.parse_args()
    app = create_app()
    try:
        if args.command == 'backup':
            print(json.dumps({'backup': str(backup(app.extensions['worker_database']))}))
        else:
            with closing(app.extensions['worker_database'].connect()) as connection:
                if connection.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                    raise RuntimeError('작업 DB의 무결성을 확인하지 못했습니다.')
            print(json.dumps({'status': 'ok', 'version': app.config['WORKER_VERSION'],
                'config_version': app.extensions['worker_configurations'].get()['config_version'],
                **app.extensions['worker_database'].counts()}))
    finally:
        app.extensions['worker_events'].close()


if __name__ == '__main__':
    main()
