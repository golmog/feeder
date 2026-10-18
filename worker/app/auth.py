"""제어 API 전 구간에 Bearer 토큰 인증을 적용한다."""

import hmac
from flask import Flask, jsonify, request


def install_auth(app: Flask, token: str) -> None:
    expected = token.encode('utf-8')

    @app.before_request
    def authenticate():
        if not (request.path == '/api/v1' or request.path.startswith('/api/v1/')):
            return None
        parts = request.headers.get('Authorization', '').split()
        supplied = parts[1].encode('utf-8') if len(parts) == 2 else b''
        authorized = hmac.compare_digest(expected, supplied)
        if len(parts) != 2 or parts[0].lower() != 'bearer' or not authorized:
            response = jsonify(error={'code': 'unauthorized', 'message': '인증이 필요합니다.'})
            response.status_code = 401
            response.headers['WWW-Authenticate'] = 'Bearer realm="feeder-worker"'
            return response
        if request.content_length is not None and request.content_length > app.config['MAX_CONTENT_LENGTH']:
            return jsonify(error={'code': 'request_too_large', 'message': '요청 본문 크기 제한을 초과했습니다.'}), 413
        return None
