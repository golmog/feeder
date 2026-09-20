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
    magnet = db.Column(db.String)
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
    def make_query(cls, req, order='desc', search='', site_radio='site', site_select='all', board_select='all', group_select='all', search_select='title'):
        with F.app.app_context():
            query = F.db.session.query(cls)

            # 사이트 또는 그룹 필터링
            if site_radio == 'group':
                if group_select and group_select != 'all':
                    group_entity = F.db.session.query(ModelFeedGroup).filter_by(groupname=group_select).first()
                    if group_entity:
                        boards = group_entity.get_boards()
                        if len(boards) == 0:
                            query = query.filter(cls.site == '__empty_group__')
                        elif len(boards) == 1:
                            query = query.filter(cls.site == boards[0].get('site_name'), cls.board == boards[0].get('board_id'))
                        else:
                            group_conditions = []
                            for b in boards:
                                group_conditions.append(and_(cls.site == b.get('site_name'), cls.board == b.get('board_id')))
                            query = query.filter(or_(*group_conditions))

                            # 마그넷 존재 시 중복 제거, 비어있으면 고유 ID로 개별 보존
                            group_key = func.coalesce(
                                func.nullif(cls.magnet, ''),
                                func.cast(cls.id, db.String)
                            )
                            subq = (
                                F.db.session.query(func.max(cls.id).label("max_id"))
                                .filter(or_(*group_conditions))
                                .group_by(group_key)
                                .subquery()
                            )
                            query = query.join(subq, cls.id == subq.c.max_id)
            else:
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
            site_radio = req.form.get('site_radio', req.form.get('radio', 'site'))
            site_select = req.form.get('site_select', 'all')
            board_select = req.form.get('board_select', 'all')
            group_select = req.form.get('group_select', 'all')
            search_select = req.form.get('search_select', 'title')

            query = cls.make_query(
                req,
                order=order,
                search=search,
                site_radio=site_radio,
                site_select=site_select,
                board_select=board_select,
                group_select=group_select,
                search_select=search_select
            )

            count = query.count()
            query = query.limit(page_size).offset((page - 1) * page_size)
            lists = query.all()

            ret['list'] = [item.as_dict() for item in lists]
            ret['paging'] = cls.get_paging_info(count, page, page_size)

            # 검색 폼 동기화용 데이터 조회
            feed_mod = P.get_module('feed')
            if feed_mod and hasattr(feed_mod, 'get_search_form_info'):
                ret['info'] = feed_mod.get_search_form_info()
            else:
                ret['info'] = {'group': [], 'site': [], 'board': {}}

            return ret
        except Exception as e:
            logger.error(f"[Feeder] ModelFeedBbs.web_list error: {e}")
            logger.error(traceback.format_exc())
            return {'ret': 'error', 'msg': str(e)}


class ModelFeedGroup(db.Model):
    __tablename__ = f'{PACKAGE_NAME}_group'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = PACKAGE_NAME

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime, default=datetime.now)
    reserved = db.Column(db.JSON)

    groupname = db.Column(db.String, unique=True, index=True)
    query = db.Column(db.String, nullable=True)

    def __init__(self, groupname):
        self.created_time = datetime.now()
        self.groupname = groupname
        self.reserved = {'boards': []}

    def get_boards(self) -> list[dict]:
        if self.reserved and isinstance(self.reserved, dict):
            return self.reserved.get('boards', [])
        return []

    def set_boards(self, boards_list: list[dict]):
        from sqlalchemy.orm.attributes import flag_modified
        data = dict(self.reserved) if (self.reserved and isinstance(self.reserved, dict)) else {}
        data['boards'] = boards_list
        self.reserved = data
        flag_modified(self, 'reserved')

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time else ''
        ret['boards'] = self.get_boards()
        return ret

    @classmethod
    def get_list(cls, by_dict=False):
        try:
            items = db.session.query(cls).order_by(cls.id.asc()).all()
            return [x.as_dict() for x in items] if by_dict else items
        except Exception as e:
            logger.error(f"ModelFeedGroup.get_list error: {e}")
            return []
