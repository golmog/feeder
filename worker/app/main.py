"""개발 서버 대신 Waitress로 실행한다."""

import logging
import signal
from waitress import create_server

from . import VERSION, create_app
from .settings import Settings


def main() -> None:
    settings = Settings.load()
    logging.basicConfig(level=settings.log_level, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    app = create_app(settings)
    logging.getLogger(__name__).info('Feeder Remote Worker %s 시작 (%s:%s)', VERSION, settings.bind, settings.port)
    worker = app.extensions['worker_runner']
    previous = {kind: signal.getsignal(kind) for kind in (signal.SIGTERM, signal.SIGINT)}
    def terminate(signum, frame):
        app.extensions['worker_events'].close()
        raise KeyboardInterrupt
    for kind in previous:
        signal.signal(kind, terminate)
    server = None
    try:
        # 포트 충돌이나 Tailscale 준비 실패로 큐의 작업이 먼저 실행되는 일을 막는다.
        server = create_server(
            app, host=settings.bind, port=settings.port, threads=settings.threads,
            max_request_body_size=settings.max_request_bytes, channel_timeout=30,
            expose_tracebacks=False, clear_untrusted_proxy_headers=True,
            ident='FeederRemoteWorker',
            outbuf_high_watermark=262144, outbuf_overflow=1048576,
            channel_request_lookahead=1,
        )
        worker.start()
        server.run()
    finally:
        try:
            app.extensions['worker_events'].close()
            worker.stop()
        finally:
            try:
                if server is not None:
                    server.task_dispatcher.shutdown()
                    server.close()
            finally:
                for kind, handler in previous.items():
                    signal.signal(kind, handler)


if __name__ == '__main__':
    main()
