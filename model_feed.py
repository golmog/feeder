# -*- coding: utf-8 -*-
from datetime import datetime
from sqlalchemy import and_, or_, func, desc
from .setup import *

PACKAGE_NAME = P.package_name


class ModelFeedSite(db.Model):
    __tablename__ = f'{PACKAGE_NAME}_site'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = PACKAGE_NAME

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime, default=datetime.now)
    reserved = db.Column(db.JSON)

    info_type = db.Column(db.String)
    name = db.Column(db.String, index=True)
    info = db.Column(db.JSON)
    content = db.Column(db.String)

    def __init__(self, info_type, info, content):
        self.created_time = datetime.now()
        self.info_type = info_type
        self.name = info.get('NAME', '')
        self.info = info
        self.content = content

    def get_options(self) -> dict:
        """사이트별 프록시/FlareSolverr/Selenium/토렌트정보 기본 설정값 해석"""
        info = self.info or {}
        extra = info.get('EXTRA', [])
        return {
            'use_proxy': bool(info.get('USE_PROXY', False) or 'USE_PROXY' in extra),
            'use_flaresolverr': bool(info.get('USE_FLARESOLVERR', False) or 'USE_FLARESOLVERR' in extra),
            'use_selenium': bool(info.get('USE_SELENIUM', False) or 'USE_SELENIUM' in extra),
            'use_torrent_info': bool(info.get('USE_TORRENT_INFO', False) or 'USE_TORRENT_INFO' in extra),
        }

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time else ''
        ret['options'] = self.get_options()
        return ret

    @classmethod
    def get(cls, site_id=None, name=None, by_dict=False):
        try:
            query = db.session.query(cls)
            if site_id is not None:
                query = query.filter_by(id=int(site_id))
            if name is not None:
                query = query.filter_by(name=name)
            entity = query.first()
            return entity.as_dict() if (entity and by_dict) else entity
        except Exception as e:
            logger.error(f"ModelFeedSite.get error: {e}")
            return None

    @classmethod
    def get_list(cls, by_dict=False):
        try:
            items = db.session.query(cls).order_by(cls.id.asc()).all()
            return [x.as_dict() for x in items] if by_dict else items
        except Exception as e:
            logger.error(f"ModelFeedSite.get_list error: {e}")
            return []

    @classmethod
    def delete(cls, site_id):
        try:
            site = cls.get(site_id=site_id)
            if not site:
                return False
            db.session.query(ModelFeedBbs).filter_by(site=site.name).delete()
            db.session.delete(site)
            db.session.commit()
            return True
        except Exception as e:
            logger.error(f"ModelFeedSite.delete error: {e}")
            db.session.rollback()
            return False


class ModelFeedBbs(ModelBase):
    P = P
    __tablename__ = f'{PACKAGE_NAME}_bbs'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = PACKAGE_NAME

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime, default=datetime.now)
    site = db.Column(db.String, index=True)
    board = db.Column(db.String, index=True)
    board_id = db.Column(db.Integer, index=True, nullable=True)
    board_char_id = db.Column(db.String, index=True, nullable=True)
    title = db.Column(db.String, index=True)
    url = db.Column(db.String)
    magnet_count = db.Column(db.Integer, default=0)
    file_count = db.Column(db.Integer, default=0)
    magnet = db.Column(db.String, index=True)
    files = db.Column(db.String)
    torrent_info = db.Column(db.JSON, nullable=True)
    broadcast_status = db.Column(db.String, default='')

    def __init__(self, site_name, board_id):
        self.created_time = datetime.now()
        self.site = site_name
        self.board = board_id
        self.broadcast_status = ''

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time else ''
        ret['magnet'] = [m for m in self.magnet.split('|') if m] if ret.get('magnet') else []
        if ret.get('files'):
            ret['files'] = [item.split('|') for item in self.files.split('||') if item]
        else:
            ret['files'] = []
        return ret

    @classmethod
    def get(cls, id=None, site=None, board=None, board_id=None, board_char_id=None):
        try:
            query = db.session.query(cls)
            if id is not None:
                query = query.filter_by(id=int(id))
            if site is not None:
                query = query.filter_by(site=site)
            if board is not None:
                query = query.filter_by(board=board)
            if board_id is not None:
                query = query.filter_by(board_id=int(board_id))
            if board_char_id is not None:
                query = query.filter_by(board_char_id=str(board_char_id))
            return query.first()
        except Exception as e:
            logger.error(f"ModelFeedBbs.get error: {e}")
            return None

    @classmethod
    def get_last_bbs(cls, site, board):
        try:
            return db.session.query(cls).filter_by(site=site, board=board).order_by(cls.id.desc()).first()
        except Exception as e:
            logger.error(f"ModelFeedBbs.get_last_bbs error: {e}")
            return None

    @classmethod
    def is_exist_magnet(cls, magnet_list: list[str]) -> bool:
        if not magnet_list:
            return False
        try:
            from .util_feed import extract_info_hash
            for mag in magnet_list:
                info_hash = extract_info_hash(mag)
                if info_hash:
                    exist = db.session.query(cls.id).filter(cls.magnet.like(f"%{info_hash}%")).first()
                    if exist:
                        return True
                else:
                    exist = db.session.query(cls.id).filter(cls.magnet.like(f"%{mag}%")).first()
                    if exist:
                        return True
            return False
        except Exception as e:
            logger.error(f"ModelFeedBbs.is_exist_magnet error: {e}")
            return False

    @classmethod
    def make_query(cls, req, order='desc', search='', site_select='all', board_select='all', search_select='title'):
        with F.app.app_context():
            query = F.db.session.query(cls)

            if site_select and site_select != 'all':
                query = query.filter(cls.site == site_select)
            if board_select and board_select != 'all':
                query = query.filter(cls.board == board_select)

            # 검색 키워드 필터링
            if search:
                if search_select == 'title':
                    if '|' in search:
                        or_terms = [t.strip() for t in search.split('|') if t.strip()]
                        query = query.filter(or_(*[cls.title.like(f"%{term}%") for term in or_terms]))
                    elif ',' in search:
                        and_terms = [t.strip() for t in search.split(',') if t.strip()]
                        for term in and_terms:
                            query = query.filter(cls.title.like(f"%{term}%"))
                    else:
                        query = query.filter(or_(
                            cls.title.like(f"%{search}%"),
                            func.cast(cls.torrent_info, db.String).like(f"%{search}%")
                        ))
                elif search_select == 'filename':
                    query = query.filter(cls.files.like(f"%{search}%"))
                elif search_select == 'magnet':
                    query = query.filter(cls.magnet.like(f"%{search}%"))

            if order == 'asc':
                query = query.order_by(cls.id.asc())
            else:
                query = query.order_by(desc(cls.id))

            return query


    @classmethod
    def web_list(cls, req):
        try:
            ret = {}
            page = 1
            if 'page' in req.form:
                page = int(req.form['page'])

            try:
                page_size = int(req.form.get('page_size', 25))
            except Exception:
                page_size = 25

            search = ''
            if 'search_word' in req.form:
                search = req.form['search_word'].strip()

            order = req.form.get('order', 'desc')
            site_select = req.form.get('site_select', 'all')
            board_select = req.form.get('board_select', 'all')
            search_select = req.form.get('search_select', 'title')

            query = cls.make_query(
                req,
                order=order,
                search=search,
                site_select=site_select,
                board_select=board_select,
                search_select=search_select
            )

            count = query.count()
            query = query.limit(page_size).offset((page - 1) * page_size)
            lists = query.all()

            ret['list'] = [item.as_dict() for item in lists]
            ret['paging'] = cls.get_paging_info(count, page, page_size)

            feed_mod = P.get_module('feed')
            if feed_mod and hasattr(feed_mod, 'get_search_form_info'):
                ret['info'] = feed_mod.get_search_form_info()
            else:
                ret['info'] = {'site': [], 'board': {}}

            return ret
        except Exception as e:
            logger.error(f"[Feeder] ModelFeedBbs.web_list error: {e}")
            logger.error(traceback.format_exc())
            return {'ret': 'error', 'msg': str(e)}

