# -*- coding: utf-8 -*-
from datetime import datetime
from sqlalchemy import desc, or_

from .setup import *
from .util_base import FeederUtil

PACKAGE_NAME = P.package_name


class ModelDownload(ModelBase):
    P = P
    __tablename__ = f'{PACKAGE_NAME}_download'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = PACKAGE_NAME

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime, default=datetime.now)
    updated_time = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    completed_time = db.Column(db.DateTime, nullable=True)

    feed_name = db.Column(db.String, index=True)
    title = db.Column(db.String, index=True)
    magnet = db.Column(db.String, index=True)
    infohash = db.Column(db.String, index=True, nullable=True)
    file_name = db.Column(db.String, nullable=True)
    file_size = db.Column(db.BigInteger, default=0)

    status = db.Column(db.String, index=True, default='pending')

    priority_chain = db.Column(db.JSON)
    current_engine_index = db.Column(db.Integer, default=0)
    current_engine_name = db.Column(db.String, nullable=True)
    engine_task_id = db.Column(db.String, nullable=True)

    local_path = db.Column(db.String, nullable=True)
    destination_type = db.Column(db.String, default='local')
    gdrive_upload_path = db.Column(db.String, nullable=True)
    gdrive_complete_path = db.Column(db.String, nullable=True)
    gdrive_remote_id = db.Column(db.String, nullable=True)
    gdrive_account = db.Column(db.String, nullable=True)

    engine_added_time = db.Column(db.DateTime, nullable=True)
    last_status_time = db.Column(db.DateTime, nullable=True)
    last_move_attempt_time = db.Column(db.DateTime, nullable=True)

    error_message = db.Column(db.Text, nullable=True)

    def __init__(self, feed_name, title, magnet, infohash=None):
        self.created_time = datetime.now()
        self.updated_time = datetime.now()
        self.feed_name = feed_name
        self.title = title
        self.magnet = magnet
        self.infohash = infohash
        self.status = 'pending'
        self.priority_chain = []
        self.current_engine_index = 0

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time else ''
        ret['updated_time'] = self.updated_time.strftime('%Y-%m-%d %H:%M:%S') if self.updated_time else ''
        ret['completed_time'] = self.completed_time.strftime('%Y-%m-%d %H:%M:%S') if self.completed_time else ''
        ret['engine_added_time'] = self.engine_added_time.strftime('%Y-%m-%d %H:%M:%S') if self.engine_added_time else ''
        ret['last_status_time'] = self.last_status_time.strftime('%Y-%m-%d %H:%M:%S') if self.last_status_time else ''
        ret['last_move_attempt_time'] = self.last_move_attempt_time.strftime('%Y-%m-%d %H:%M:%S') if self.last_move_attempt_time else ''
        return ret

    @classmethod
    def get_by_magnet(cls, magnet: str):
        try:
            return db.session.query(cls).filter_by(magnet=magnet).first()
        except Exception as e:
            logger.error(f"ModelDownload.get_by_magnet 오류: {e}")
            return None

    @classmethod
    def get_by_infohash(cls, infohash: str):
        if not infohash:
            return None
        try:
            return db.session.query(cls).filter_by(infohash=infohash.lower()).first()
        except Exception as e:
            logger.error(f"ModelDownload.get_by_infohash 오류: {e}")
            return None

    @classmethod
    def get_list_by_status(cls, statuses: list[str], limit: int = 50):
        try:
            return db.session.query(cls).filter(cls.status.in_(statuses)).order_by(cls.id.asc()).limit(limit).all()
        except Exception as e:
            logger.error(f"ModelDownload.get_list_by_status 오류: {e}")
            return []

    @classmethod
    def web_list(cls, req):
        try:
            page = int(req.form.get('page', 1))
            page_size = int(req.form.get('page_size', 25))
            status_filter = req.form.get('status_filter', 'all')
            search_word = req.form.get('search_word', '').strip()

            query = db.session.query(cls)

            if status_filter != 'all':
                if status_filter == 'active':
                    query = query.filter(cls.status.in_([
                        'pending', 'downloading', 'pending_local_staging',
                        'local_staging', 'pending_upload', 'uploading',
                        'pending_colab', 'colab_transferring'
                    ]))
                else:
                    query = query.filter(cls.status == status_filter)

            if search_word:
                query = query.filter(or_(
                    cls.title.like(f"%{search_word}%"),
                    cls.file_name.like(f"%{search_word}%")
                ))

            total_count = query.count()
            items = query.order_by(desc(cls.id)).limit(page_size).offset((page - 1) * page_size).all()

            return {
                'success': True,
                'list': [it.as_dict() for it in items],
                'paging': FeederUtil.get_paging_info(total_count, page, page_size)
            }
        except Exception as e:
            logger.error(f"[ModelDownload] web_list 쿼리 오류: {e}")
            return {'success': False, 'list': [], 'paging': None}


class ModelDownloadStat(ModelBase):
    P = P
    __tablename__ = f'{PACKAGE_NAME}_download_stat'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = PACKAGE_NAME

    id = db.Column(db.Integer, primary_key=True)
    stat_type = db.Column(db.String, index=True)
    stat_key = db.Column(db.String, index=True)
    stat_value = db.Column(db.Float, default=0.0)
    timestamp = db.Column(db.BigInteger, index=True)

    def __init__(self, stat_type: str, stat_key: str, stat_value: float, timestamp: int):
        self.stat_type = stat_type
        self.stat_key = stat_key
        self.stat_value = float(stat_value)
        self.timestamp = int(timestamp)
