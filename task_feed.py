# -*- coding: utf-8 -*-
import traceback

from .setup import *
from .util_feed import FeedUtil


class TaskFeedBase:

    @F.celery.task(bind=True, acks_late=False)
    def start(self, *args):
        delivery_info = getattr(self.request, 'delivery_info', {}) or {}
        is_redelivered = delivery_info.get('redelivered') or getattr(self.request, 'redelivered', False)

        if is_redelivered:
            logger.warning("[TaskFeed] 이전 세션 비정상 종료로 재전송된 태스크 취소")
            return

        TaskFeed.sync_all_feeds()


class TaskFeed:

    @staticmethod
    def sync_all_feeds():
        with F.app.app_context():
            try:
                logger.info("[TaskFeed] 피드 DB 동기화 및 공유 RSS XML 갱신 작업 시작")
                new_items_count = FeedUtil.sync_all_feeds()
                updated_count = FeedUtil.save_all_rss_files()
                logger.info(f"[TaskFeed] 피드 동기화 완료: 신규 {new_items_count}건 DB 적재, RSS 파일 {updated_count}개 갱신")
            except Exception as e:
                logger.error(f"[TaskFeed] 피드 동기화 중 오류: {e}")
                logger.error(traceback.format_exc())
            finally:
                try:
                    db.session.remove()
                except Exception:
                    pass
