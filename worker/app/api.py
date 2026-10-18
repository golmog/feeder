"""인증된 상태 확인과 작업 등록·조회·취소 API."""

import os
import sqlite3
import re
from flask import Blueprint, current_app, jsonify, request, abort, Response
from .jobs import ConflictError, STATUSES, validate_payload
from .events import StreamLimitError


api = Blueprint('api', __name__, url_prefix='/api/v1')


@api.get('/config')
def get_configuration():
    version = request.args.get('version')
    if version is not None:
        if not re.fullmatch(r'[0-9]{1,10}', version) or not 1 <= int(version) <= 2**31-1:
            return jsonify(error={'code': 'invalid_request', 'message': '설정 버전이 유효하지 않습니다.'}), 400
        version = int(version)
    try:
        return jsonify(current_app.extensions['worker_configurations'].get(version))
    except ValueError:
        abort(404)


@api.put('/config')
def put_configuration():
    try:
        result = current_app.extensions['worker_configurations'].update(request.get_json(silent=True))
    except ConflictError as exc:
        return jsonify(error={'code': 'conflict', 'message': str(exc)}), 409
    except ValueError as exc:
        return jsonify(error={'code': 'invalid_request', 'message': str(exc)}), 400
    current_app.extensions['worker_runner'].notify()
    return jsonify(result)


@api.get('/health')
def health():
    settings = current_app.extensions['worker_settings']
    available = (settings.rclone_binary.is_file() and os.access(settings.rclone_binary, os.X_OK)
                 and settings.rclone_config.is_file() and os.access(settings.rclone_config, os.R_OK))
    database_available = True
    try:
        counts = current_app.extensions['worker_database'].counts()
    except (sqlite3.Error, OSError):
        current_app.logger.warning('상태 확인 중 작업 저장소에 접근하지 못했습니다.')
        database_available = False
        counts = {'active_jobs': None, 'queued_jobs': None}
    healthy = available and database_available and not current_app.extensions['worker_runner'].failure
    return jsonify(
        status='ok' if healthy else 'degraded', version=current_app.config['WORKER_VERSION'],
        rclone_available=available, database_available=database_available, **counts,
    ), 200 if healthy else 503


@api.post('/jobs')
def create_job():
    try:
        data = validate_payload(request.get_json(silent=True), current_app.extensions['worker_settings'])
        job = current_app.extensions['worker_jobs'].submit(data)
    except ConflictError as exc:
        return jsonify(error={'code': 'conflict', 'message': str(exc)}), 409
    except ValueError as exc:
        return jsonify(error={'code': 'invalid_request', 'message': str(exc)}), 400
    current_app.extensions['worker_runner'].notify()
    return jsonify(job), 202


@api.get('/jobs/<job_id>')
def get_job(job_id):
    job = current_app.extensions['worker_jobs'].get(job_id)
    if job is None:
        abort(404)
    return jsonify(job)


@api.get('/jobs')
def list_jobs():
    status = request.args.get('status') or None
    try:
        limit, offset = int(request.args.get('limit', 50)), int(request.args.get('offset', 0))
    except ValueError:
        return jsonify(error={'code': 'invalid_request', 'message': 'limit/offset은 정수여야 합니다.'}), 400
    if status not in (None, *STATUSES) or not 1 <= limit <= 200 or offset < 0:
        return jsonify(error={'code': 'invalid_request', 'message': '상태 또는 페이지 범위를 확인해주세요.'}), 400
    return jsonify(jobs=current_app.extensions['worker_jobs'].list(status, limit, offset), limit=limit, offset=offset)


@api.post('/jobs/<job_id>/cancel')
def cancel_job(job_id):
    try:
        job = current_app.extensions['worker_jobs'].cancel(job_id)
    except ValueError:
        return jsonify(error={'code': 'invalid_request', 'message': '작업 ID가 유효하지 않습니다.'}), 400
    if job is None:
        return jsonify(error={'code': 'not_found', 'message': '작업이 없으며 이후 등록도 차단했습니다.'}), 404, {'X-Worker-Cancel-Tombstone': '1'}
    current_app.extensions['worker_runner'].notify()
    return jsonify(job), 202 if job['status'] == 'running' else 200


@api.get('/events/state')
def event_state():
    return jsonify(current_app.extensions['worker_events'].state())


@api.get('/events')
def events():
    if request.method != 'GET':
        return jsonify(error={'code': 'method_not_allowed', 'message': 'GET만 사용할 수 있습니다.'}), 405
    raw = request.headers.get('Last-Event-ID')
    cursor = None
    if raw:
        if not re.fullmatch(r'[0-9]{1,19}', raw) or int(raw) > 2**63-1:
            return jsonify(error={'code': 'invalid_cursor', 'message': 'Last-Event-ID는 0 이상의 이벤트 번호여야 합니다.'}), 400
        cursor = int(raw)
    stream_id = request.args.get('stream_id')
    if stream_id is not None and not re.fullmatch(r'[a-f0-9]{32}', stream_id):
        return jsonify(error={'code': 'invalid_stream_id', 'message': 'stream_id가 유효하지 않습니다.'}), 400
    broker = current_app.extensions['worker_events']
    state = broker.state()
    try:
        lease = broker.acquire()
    except StreamLimitError:
        response = jsonify(error={'code': 'stream_limit', 'message': 'SSE 연결 상한 또는 서비스 종료 상태입니다.'})
        response.status_code = 429
        response.headers['Retry-After'] = '3'
        return response
    response = Response(broker.stream(lease, cursor, stream_id, state,
                        request.environ.get('waitress.client_disconnected')), mimetype='text/event-stream',
                        headers={'X-Accel-Buffering': 'no'})
    response.call_on_close(lease.release)
    return response
