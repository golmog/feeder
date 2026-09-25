# -*- coding: utf-8 -*-
import os
import traceback
from plugin import *

setting = {
    'filepath': __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': 'crawl',
    'menu': {
        'uri': __package__,
        'name': 'FEEDER',
        'list': [
            {
                'uri': 'crawl',
                'name': 'CRAWL',
                'list': [
                    {'uri': 'setting', 'name': '설정 및 관리'},
                    {'uri': 'list', 'name': '수집 리스트'}
                ]
            },
            {
                'uri': 'feed',
                'name': 'FEED',
                'list': [
                    {'uri': 'setting', 'name': '설정 및 관리'},
                    {'uri': 'list', 'name': '피드 리스트'}
                ]
            },
            {
                'uri': 'download',
                'name': 'DOWNLOAD',
                'list': [
                    {'uri': 'setting', 'name': '설정 및 관리'},
                    {'uri': 'queue', 'name': '다운로드 큐 및 대시보드'},
                    {'uri': 'list', 'name': '다운로드 리스트'}
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
    from .model_crawl import ModelCrawlSite, ModelCrawlItem
    from .model_download import ModelDownload, ModelDownloadStat
    from .mod_crawl import ModuleCrawl
    from .mod_feed import ModuleFeed
    from .mod_download import ModuleDownload

    P.set_module_list([ModuleCrawl, ModuleFeed, ModuleDownload])
except Exception as e:
    P.logger.error(f'Exception: {str(e)}')
    P.logger.error(traceback.format_exc())


def plugin_load_celery():
    logger.info(f"[{P.package_name}] Celery 워커 플러그인 초기화 완료")
