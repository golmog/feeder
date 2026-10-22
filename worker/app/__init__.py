"""Feeder의 전송 정책과 분리된 원격 워커 애플리케이션."""

import subprocess
import os
from pathlib import Path
from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException

from .api import api
from .auth import install_auth
from .database import Database
from .settings import Settings
from .jobs import JobStore
from .runner import Worker
from .events import Events
from .configuration import Configurations
from .updates import Updates


VERSION = '0.9.0'


def create_app(settings: Settings | None = None) -> Flask:
    settings = Settings.load() if settings is None else settings
    settings.validate_paths()
    settings.validate_remotes()
    # 설치된 바이너리만 확인한다. 업그레이드나 원격 접속은 수행하지 않는다.
    try:
        result = subprocess.run(
            [str(settings.rclone_binary), '--config', str(settings.rclone_config), 'version'],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError('설치된 Rclone을 실행하지 못했습니다.') from None
    if result.returncode or not result.stdout.startswith('rclone v'):
        raise RuntimeError('설치된 Rclone의 버전을 확인하지 못했습니다.')
    database = Database(settings.database)
    database.initialize()
    app = Flask(__name__, static_folder=None)
    app.config.update(MAX_CONTENT_LENGTH=settings.max_request_bytes, WORKER_VERSION=VERSION)
    app.extensions['worker_settings'] = settings
    app.extensions['worker_database'] = database
    app.extensions['worker_rclone_version'] = result.stdout.splitlines()[0]
    events = Events(database, settings.event_retention, settings.sse_max_clients, settings.sse_heartbeat_seconds)
    app.extensions['worker_events'] = events
    configurations = Configurations(database, events)
    configurations.initialize(settings)
    app.extensions['worker_configurations'] = configurations
    app.extensions['worker_jobs'] = JobStore(database, events)
    app.extensions['worker_runner'] = Worker(settings, app.extensions['worker_jobs'])
    root = Path(os.environ.get('FEEDER_WORKER_ROOT') or Path(__file__).resolve().parents[1])
    app.extensions['worker_updates'] = Updates(root, settings, database, VERSION)
    install_auth(app, settings.api_token)
    app.register_blueprint(api)

    @app.after_request
    def response_headers(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        return jsonify(error={'code': error.name.lower().replace(' ', '_'), 'message': error.name}), error.code

    @app.errorhandler(Exception)
    def unexpected_error(error):
        # 요청 헤더/본문이나 예외 메시지에 비밀 정보가 섞여 있을 수 있으므로 종류만 기록한다.
        app.logger.error('요청 처리 실패: %s', type(error).__name__)
        return jsonify(error={'code': 'internal_error', 'message': '서버 내부 오류가 발생했습니다.'}), 500

    return app
