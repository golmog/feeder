# -*- coding: utf-8 -*-
from .setup import *
from .util_feed import FeedRssFileWriter


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
                logger.info("[TaskFeed] 공유 RSS 피드 XML 파일 일괄 동기화 시작")
                updated_count = FeedRssFileWriter.save_all_rss_files()
                logger.info(f"[TaskFeed] 공유 RSS 피드 동기화 완료 (총 {updated_count}개 갱신됨)")
            except Exception as e:
                logger.error(f"[TaskFeed] 피드 동기화 중 오류: {e}")
                logger.error(traceback.format_exc())
