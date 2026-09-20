# -*- coding: utf-8 -*-
from plugin import *

setting = {
    'filepath': __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': 'feed',
    'menu': {
        'uri': __package__,
        'name': 'Feeder',
        'list': [
            {
                'uri': 'feed',
                'name': 'FEED',
                'list': [
                    {'uri': 'setting', 'name': '설정 및 관리'},
                    {'uri': 'list', 'name': '토렌트 리스트'}
                ]
            },
            {
                'uri': 'manual',
                'name': '매뉴얼',
                'list': [
                    {'uri': 'README.md', 'name': '매뉴얼'},
                ]
            },
            {'uri': 'log', 'name': '로그'},
        ]
    },
    'default_route': 'normal',
}

P = create_plugin_instance(setting)
logger = P.logger
PLUGIN_ROOT = os.path.dirname(__file__)

try:
    from .model_feed import ModelFeedSite, ModelFeedBbs, ModelFeedGroup
    from .mod_feed import ModuleFeed

    P.set_module_list([ModuleFeed])
except Exception as e:
    P.logger.error(f'Exception: {str(e)}')
    P.logger.error(traceback.format_exc())


def plugin_load_celery():
    logger.info(f"[{P.package_name}] Celery 워커 플러그인 초기화 완료")
