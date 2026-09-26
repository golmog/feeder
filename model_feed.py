# -*- coding: utf-8 -*-
import unicodedata
from datetime import datetime
from sqlalchemy import and_, or_, func, desc

from .setup import *
from .util_base import FeederUtil

PACKAGE_NAME = P.package_name


class ModelFeedItem(ModelBase):
    P = P
    __tablename__ = f'{PACKAGE_NAME}_feed'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = PACKAGE_NAME

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime, default=datetime.now, index=True)

    # 소속 피드 식별자
    feed_name = db.Column(db.String, index=True, nullable=False)

    # 소스 출처 유형 (crawl: 내부 수집기, rss: 외부 RSS 피드)
    source_type = db.Column(db.String, default='crawl', index=True)
    source_name = db.Column(db.String, index=True)

    title = db.Column(db.String, index=True)
    url = db.Column(db.String)
    magnet_count = db.Column(db.Integer, default=0)
    file_count = db.Column(db.Integer, default=0)
    magnet = db.Column(db.String, index=True)
    files = db.Column(db.String)
    torrent_info = db.Column(db.JSON, nullable=True)
    broadcast_status = db.Column(db.String, default='')

    def __init__(self, feed_name, source_type='crawl', source_name=''):
        self.created_time = datetime.now()
        self.feed_name = feed_name
        self.source_type = source_type
        self.source_name = source_name
        self.broadcast_status = ''

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time else ''
        ret['magnet'] = FeederUtil.split_magnets(self.magnet) if ret.get('magnet') else []
        if ret.get('files'):
            ret['files'] = [item.split('|') for item in self.files.split('||') if item]
        else:
            ret['files'] = []
        return ret

    @classmethod
    def get_last_item(cls, feed_name):
        try:
            return db.session.query(cls).filter_by(feed_name=feed_name).order_by(cls.id.desc()).first()
        except Exception as e:
            logger.error(f"ModelFeedItem.get_last_item error: {e}")
            return None

    @classmethod
    def make_query(cls, req, feed_name=None, order='desc', search='', status_filter='all'):
        query = db.session.query(cls)

        if feed_name and feed_name != 'all':
            query = query.filter(cls.feed_name == feed_name)

        if status_filter and status_filter != 'all':
            if status_filter == 'magnet':
                query = query.filter(cls.magnet.like('%magnet:%'))
            elif status_filter == 'ed2k':
                query = query.filter(cls.magnet.like('%ed2k://%'))
            elif status_filter == 'has_files':
                query = query.filter(cls.files.isnot(None), cls.files != '')
            elif status_filter in ['download_completed', 'download_active', 'not_downloaded']:
                query = FeederUtil.apply_download_status_filter(query, cls.magnet, status_filter)
                if status_filter == 'download_completed':
                    target_statuses = ['completed']
                elif status_filter == 'download_active':
                    target_statuses = ['downloading', 'pending', 'local_staging', 'uploading', 'colab_transferring']
                else:
                    target_statuses = None

                if target_statuses:
                    subq = db.session.query(ModelDownload.id).filter(
                        ModelDownload.status.in_(target_statuses),
                        ModelDownload.infohash.isnot(None),
                        cls.magnet.like(func.concat('%', ModelDownload.infohash, '%'))
                    ).exists()
                    query = query.filter(subq)
                else:
                    subq = db.session.query(ModelDownload.id).filter(
                        ModelDownload.infohash.isnot(None),
                        cls.magnet.like(func.concat('%', ModelDownload.infohash, '%'))
                    ).exists()
                    query = query.filter(~subq)

        if search:
            query = query.filter(or_(
                cls.title.like(f"%{search}%"),
                cls.files.like(f"%{search}%"),
                cls.magnet.like(f"%{search}%")
            ))

        if order == 'asc':
            query = query.order_by(cls.id.asc())
        else:
            query = query.order_by(desc(cls.id))

        return query

    @classmethod
    def web_list(cls, req):
        try:
            raw_feed = req.form.get('feed_select') or req.values.get('feed_select') or ''
            feed_name = unicodedata.normalize('NFC', raw_feed.strip())

            try:
                page = int(req.form.get('page') or req.values.get('page') or 1)
            except Exception:
                page = 1

            try:
                page_size = int(req.form.get('page_size') or req.values.get('page_size') or 25)
            except Exception:
                page_size = 25

            search = (req.form.get('search_word') or req.values.get('search_word') or '').strip()
            order = req.form.get('order') or req.values.get('order') or 'desc'
            status_filter = req.form.get('status_filter') or req.values.get('status_filter') or 'all'

            query = cls.make_query(req, feed_name=feed_name, order=order, search=search, status_filter=status_filter)

            count = query.count()
            items = query.limit(page_size).offset((page - 1) * page_size).all()
            item_dicts = FeederUtil.attach_download_info(items)

            return {
                'success': True,
                'list': item_dicts,
                'paging': FeederUtil.get_paging_info(count, page, page_size)
            }
        except Exception as e:
            logger.error(f"[ModelFeedItem] web_list error: {e}")
            logger.error(traceback.format_exc())
            return {'success': False, 'list': [], 'paging': None}
        finally:
            try:
                db.session.remove()
            except Exception:
                pass
