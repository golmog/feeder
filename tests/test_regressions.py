"""Regression tests run without importing the FlaskFarm plugin or contacting services.

Loads the repository's class ASTs with host interfaces supplied by the harness;
uses real SQLAlchemy/SQLite for queries, transactions, and persisted checkpoints.
Run: python3 -m unittest discover -s tests -v
"""
import ast
import hashlib
import base64
import contextlib
from contextlib import contextmanager
import uuid
import sqlite3
import traceback
import configparser
import copy
import datetime
import inspect
import importlib.util
import contextvars
import itertools
import io
import json
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
import unicodedata
import unittest
from unittest.mock import Mock, patch
from xml.etree import ElementTree as ET
import __future__
from urllib.parse import urlparse
from html import unescape

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
NS = types.SimpleNamespace
runtime_spec = importlib.util.spec_from_file_location('feeder_runtime_tests', ROOT/'util_runtime.py')
Runtime = importlib.util.module_from_spec(runtime_spec)
runtime_spec.loader.exec_module(Runtime)
def load_helper(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
logging_helpers = load_helper('feeder_log_test', 'worker/rclone_logging.py')
worker_client = load_helper('feeder_client_test', 'remote_worker_client.py')
Base = declarative_base()
db = NS(Model=Base, Column=sa.Column, Integer=sa.Integer, String=sa.String,
        JSON=sa.JSON, DateTime=sa.DateTime, BigInteger=sa.BigInteger,
        Text=sa.Text, Float=sa.Float, text=sa.text)

class Settings:
    values = {}
    @classmethod
    def get(cls, key, default=None): return cls.values.get(key, default)
    @classmethod
    def get_int(cls, key): return int(cls.get(key, 0))
    @classmethod
    def get_bool(cls, key): return str(cls.get(key, False)).lower() in ['true', '1', 'on']
    @classmethod
    def set(cls, key, value): cls.values[key] = value

class HTTPError(Exception):
    pass

def abort(code):
    raise HTTPError(code)

P = NS(package_name='feeder_test', ModelSetting=Settings)
F = NS(app=NS(app_context=contextlib.nullcontext), db=db)
NAMESPACES = []
COMMON = dict(unescape=unescape, sqlite3=sqlite3, traceback=traceback, configparser=configparser,
              WorkerError=worker_client.WorkerError, WorkerClient=worker_client.WorkerClient, WorkerEvents=worker_client.WorkerEvents,
              redact_log=logging_helpers.redact_log, classify_error=logging_helpers.classify_error, FileQuota=logging_helpers.FileQuota,
              contextmanager=contextmanager, uuid=uuid, hashlib=hashlib, base64=base64, event=sa.event, select=sa.select, cooperate=Runtime.cooperate, os=os, re=re, io=io, sys=sys, time=time, json=json,
              threading=threading, shutil=shutil, subprocess=subprocess,
              inspect=inspect, unicodedata=unicodedata,
              datetime=datetime.datetime, timedelta=datetime.timedelta,
              urlparse=urlparse, SimpleNamespace=NS, P=P, F=F, db=db, ModelBase=Base,
              PluginModuleBase=object, logger=logging.getLogger('feeder-tests'),
              path_data='/tmp', PACKAGE_NAME='feeder_test', HTTPException=HTTPError, abort=abort,
              jsonify=lambda value: value, Response=lambda value, **kw: value,
              func=sa.func, and_=sa.and_, or_=sa.or_, desc=sa.desc, case=sa.case)

def load_class(filename, name):
    env = dict(COMMON, __name__='feeder_test.'+filename[:-3], __package__='feeder_test')
    tree = ast.parse((ROOT / filename).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), filename, 'exec',
                 flags=__future__.annotations.compiler_flag), env)
    NAMESPACES.append(env)
    return env[name]

FeederUtil = load_class('util_base.py', 'FeederUtil')
COMMON['FeederUtil'] = FeederUtil
ModelDownload = load_class('model_download.py', 'ModelDownload')
ModelDownloadBlock = load_class('model_download.py', 'ModelDownloadBlock')
ModelDownloadStat = load_class('model_download.py', 'ModelDownloadStat')
ModelFeedItem = load_class('model_feed.py', 'ModelFeedItem')
ModelCrawlItem = load_class('model_crawl.py', 'ModelCrawlItem')
COMMON.update(ModelDownloadBlock=ModelDownloadBlock, ModelDownload=ModelDownload, ModelDownloadStat=ModelDownloadStat,
              ModelFeedItem=ModelFeedItem, ModelCrawlItem=ModelCrawlItem)
DownloadActivity = load_class('util_download_activity.py', 'DownloadActivity')
COMMON['DownloadActivity'] = DownloadActivity
UploadUtil = load_class('util_upload.py', 'UploadUtil')
REAL_RUN_RCLONE = UploadUtil.run_rclone
DownloadUtil = load_class('util_download.py', 'DownloadUtil')
CrawlUtil = load_class('util_crawl.py', 'CrawlUtil')
FeedUtil = load_class('util_feed.py', 'FeedUtil')
COMMON.update(UploadUtil=UploadUtil, DownloadUtil=DownloadUtil, CrawlUtil=CrawlUtil, FeedUtil=FeedUtil)
RemoteWorker = load_class('util_remote_worker.py', 'RemoteWorker')
COMMON['RemoteWorker'] = RemoteWorker
DownloadBlock = load_class('util_block.py', 'DownloadBlock')
COMMON['DownloadBlock'] = DownloadBlock
TaskDownload = load_class('task_download.py', 'TaskDownload')
TaskCrawl = load_class('task_crawl.py', 'TaskCrawl')
ModuleDownload = load_class('mod_download.py', 'ModuleDownload')
ModuleFeed = load_class('mod_feed.py', 'ModuleFeed')
ModuleCrawl = load_class('mod_crawl.py', 'ModuleCrawl')
COMMON.update(TaskDownload=TaskDownload, TaskCrawl=TaskCrawl)
for env in NAMESPACES:
    env.update(COMMON)
    env.update(_CURL_CFFI_AVAILABLE=False, requests=NS(Session=Mock()), html=NS(fromstring=Mock()))

# Support the production methods' lazy relative imports.
package = types.ModuleType('feeder_test')
package.__path__ = []
sys.modules['feeder_test'] = package
for module, exports in [('model_download', {'ModelDownload': ModelDownload, 'ModelDownloadBlock': ModelDownloadBlock}),
                        ('util_block', {'DownloadBlock': DownloadBlock}),
                        ('util_upload', {'UploadUtil': UploadUtil}),
                        ('util_remote_worker', {'RemoteWorker': RemoteWorker}),
                        ('model_feed', {'ModelFeedItem': ModelFeedItem}),
                        ('model_crawl', {'ModelCrawlItem': ModelCrawlItem}),
                        ('util_download', {'DownloadUtil': DownloadUtil})]:
    mod = types.ModuleType('feeder_test.'+module)
    mod.__dict__.update(exports)
    sys.modules[mod.__name__] = mod

class Regressions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        for env in NAMESPACES:
            env['path_data'] = self.tmp.name
        self.addCleanup(self.tmp.cleanup)
        self.engine = sa.create_engine('sqlite:///'+str(Path(self.tmp.name)/'feeder.db'), connect_args={'check_same_thread':False})
        Base.metadata.create_all(self.engine)
        db.session = scoped_session(sessionmaker(bind=self.engine))
        db.get_engine = lambda **kw: self.engine
        db.engine = self.engine
        db.engines = {'feeder_test': self.engine}
        FeederUtil.init_runtime_locks()
        self.addCleanup(self.engine.dispose)
        self.addCleanup(db.session.remove)
        Settings.values = {'feed_feed_count': '2', 'feed_rss_file_items': '1',
                           'feed_rss_file_days': '14', 'download_shared_drive_id': 'drive',
                           'download_use_gdrive_pool': 'True'}
        defaults = {'load_yaml': {}, 'get_global': {}, 'get_global_config': {},
                    'get_download_profile_by_feed': {}, 'get_downloader_by_name': None,
                    'get_downloaders': [], 'get_rclone_exclude_options': [],
                    'get_rclone_extra_options': [], 'get_ddns': 'https://host',
                    'get_system_apikey': 'key'}
        for name, value in defaults.items():
            target = FeedUtil if name == 'get_global_config' else FeederUtil
            p = patch.object(target, name, return_value=value)
            p.start(); self.addCleanup(p.stop)
        p = patch.object(UploadUtil, 'run_rclone', side_effect=self.rclone)
        self.run = p.start(); self.addCleanup(p.stop)
        p = patch('time.sleep'); p.start(); self.addCleanup(p.stop)
        TaskDownload.failed_ids_in_session.clear()
        self.commands = []
        p = patch.object(logging_helpers.FileQuota, 'inspect_source', side_effect=lambda *a, **k: logging_helpers.FileQuota())
        p.start(); self.addCleanup(p.stop)

    def rclone(self, cmd, *args, **kw):
        self.commands.append(list(cmd))
        return (False, 'directory not found') if cmd[1] == 'lsjson' else (True, '')

    def download(self, status='pending_upload', local_path=None, title='download', infohash='a'*40):
        item = ModelDownload('f', title, 'magnet:?xt=urn:btih:'+infohash, infohash)
        item.status = status
        item.file_name = title
        item.local_path = local_path
        item.gdrive_remote_id = 'drive'
        db.session.add(item); db.session.commit()
        return item

    def prepare_block(self, item):
        DownloadBlock.initialize()
        with patch.object(DownloadBlock, 'start_cleanup'), patch.object(UploadUtil, 'kill_item_rclone') as kill:
            DownloadBlock.block(item)
            self.assertTrue(kill.called)
        return ModelDownloadBlock.key(item.infohash, item.magnet)

    def test_block_cleans_partial_staging_and_both_incoming_paths_only(self):
        root = Path(self.tmp.name)/'staging'
        partial = root/'profile'/'movie'
        partial.mkdir(parents=True)
        (partial/'partial.part').write_bytes(b'partial')
        final = Path(self.tmp.name)/'final.mkv'
        final.write_bytes(b'final')
        Settings.set('download_local_staging_path', str(root))
        item = self.download(status='uploading', local_path=str(partial))
        item.gdrive_account = 'account'
        item.upload_state = {'phase':'copying', 'incoming':'sa:{my}/incoming/movie',
            'shared_incoming':'sa:{drive}/incoming/movie', 'dest_parent':'sa:{drive}/final',
            'folder_name':'movie', 'rclone_conf':'/config', 'impersonate_arg':[]}
        db.session.commit()
        item_id = item.id
        key = self.prepare_block(item)
        with patch.object(DownloadUtil, 'delete_remote_tasks'):
            DownloadBlock.cleanup(key)
        self.assertFalse(partial.exists())
        self.assertTrue(final.exists())
        self.assertEqual({'sa:{my}/incoming/movie','sa:{drive}/incoming/movie'},
            {c[2] for c in self.commands if c[1] == 'purge'})
        item = db.session.get(ModelDownload, item_id)
        self.assertEqual('blocked', item.status)
        self.assertIsNone(item.gdrive_account)
        self.assertFalse(db.session.get(ModelDownloadBlock,key).cleanup[0]['pending'])

    def test_block_survives_row_deletion_and_rejects_direct_or_stale_registration(self):
        item = self.download()
        key = self.prepare_block(item)
        magnet = item.magnet
        db.session.delete(item)
        db.session.commit()
        self.assertTrue(ModelDownload.is_blocked(magnet=magnet+'&dn=different'))
        result = FeederUtil.add_direct_download('again',magnet+'&dn=different')
        self.assertEqual('fail',result['ret'])
        blocked = ModelDownload('other','stale',magnet)
        db.session.add(blocked)
        with self.assertRaisesRegex(ValueError,'차단'):
            db.session.commit()
        db.session.rollback()
        self.assertIsNotNone(db.session.get(ModelDownloadBlock,key))

    def test_cleanup_failure_persists_and_retry_allows_unblock(self):
        item = self.download()
        item.upload_state={'incoming':'sa:{my}/incoming/movie','folder_name':'movie',
            'dest_parent':'sa:{drive}/final','rclone_conf':'/config'}
        db.session.commit()
        item_id=item.id
        key=self.prepare_block(item)
        self.run.side_effect=lambda *a,**k:(False,'403 permission denied')
        DownloadBlock.cleanup(key)
        record=db.session.get(ModelDownloadBlock,key)
        self.assertTrue(record.cleanup[0]['pending'])
        self.assertIn('403',record.error_message)
        self.assertEqual('fail',DownloadBlock.unblock(key)['ret'])
        self.run.side_effect=self.rclone
        with patch.object(DownloadUtil,'delete_remote_tasks'):
            DownloadBlock.cleanup(key)
        self.assertEqual('success',DownloadBlock.unblock(key)['ret'])
        self.assertIsNone(db.session.get(ModelDownloadBlock,key))
        self.assertEqual('cancelled',db.session.get(ModelDownload,item_id).status)

    def test_block_preserves_final_path_even_with_invalid_checkpoint(self):
        item=self.download()
        item.upload_state={'incoming':'sa:{drive}/final/movie','folder_name':'movie',
            'dest_parent':'sa:{drive}/final','rclone_conf':'/config'}
        db.session.commit()
        key=self.prepare_block(item)
        DownloadBlock.cleanup(key)
        self.run.assert_not_called()
        self.assertTrue(db.session.get(ModelDownloadBlock,key).cleanup[0]['pending'])

    def test_staging_cleanup_does_not_follow_symlink_outside_root_or_remove_root(self):
        root=Path(self.tmp.name)/'staging'; root.mkdir()
        outside=Path(self.tmp.name)/'original'; outside.mkdir()
        (outside/'source').write_bytes(b'source')
        (root/'link').symlink_to(outside,target_is_directory=True)
        Settings.set('download_local_staging_path',str(root))
        DownloadBlock.remove_staging(str(root/'link'))
        DownloadBlock.remove_staging(str(root))
        self.assertTrue((outside/'source').exists())
        self.assertTrue(root.exists())

    def test_stale_worker_cannot_overwrite_blocked_status(self):
        item=self.download(status='uploading')
        item_id=item.id
        self.prepare_block(item)
        item.status='completed'
        with self.assertRaisesRegex(ValueError,'차단'):
            db.session.commit()
        db.session.rollback()
        self.assertEqual('blocked',db.session.get(ModelDownload,item_id).status)
        self.assertTrue(DownloadBlock.stopped(item_id))

    def test_rclone_cannot_start_after_block_or_during_pid_registration(self):
        item=self.download()
        with patch.object(DownloadBlock,'stopped',return_value=True), patch.object(subprocess,'Popen') as popen:
            self.assertFalse(REAL_RUN_RCLONE(['rclone','copy','source','dest'],item_id=item.id)[0])
            popen.assert_not_called()
        process=Mock(pid=123)
        with patch.object(DownloadBlock,'stopped',side_effect=[False,True]), patch.object(subprocess,'Popen',return_value=process), patch.object(UploadUtil,'register_rclone_pid'), patch.object(UploadUtil,'unregister_rclone_pid'):
            self.assertFalse(REAL_RUN_RCLONE(['rclone','copy','source','dest'],item_id=item.id)[0])
        process.kill.assert_called_once()
        process.communicate.assert_called_once()

    def test_block_during_staging_retains_block_and_cleans_partial_directory(self):
        root=Path(self.tmp.name)/'staging'
        Settings.set('download_local_staging_path',str(root))
        Settings.set('download_upload_workers','1')
        item=self.download(status='pending_local_staging',local_path='ad:movie',title='movie')
        item_id=item.id
        def copy_then_block(*args,**kwargs):
            staged=db.session.get(ModelDownload,item_id)
            path=Path(staged.upload_state['staging_local_path'])
            self.assertTrue(path.exists())
            (path/'partial.part').write_bytes(b'part')
            self.prepare_block(staged)
            return False,'cancelled'
        self.run.side_effect=copy_then_block
        TaskDownload.process_local_staging()
        self.assertEqual('blocked',db.session.get(ModelDownload,item_id).status)
        self.assertFalse(any(root.rglob('partial.part')))

    def test_block_during_remote_admission_removes_just_added_task(self):
        item=self.download(status='pending'); item.priority_chain=['q']; item_id=item.id; db.session.commit()
        engine=NS(get_status=lambda:([],''),SUPPORTED_PROTOCOLS=['magnet'],delete_task=Mock())
        config={'name':'q','engine_type':'qbittorrent','enabled':True}
        def add(*args,**kwargs):
            self.prepare_block(db.session.get(ModelDownload,item_id))
            return True,'new',''
        with patch.object(FeederUtil,'get_downloaders',return_value=[config]),patch.object(FeederUtil,'get_downloader_by_name',return_value=config),patch.object(DownloadUtil,'create_engine',return_value=engine),patch.object(DownloadUtil,'add_magnet',side_effect=add):
            TaskDownload.dispatch_pending_downloads()
        engine.delete_task.assert_called_once_with('new')
        self.assertEqual('blocked',db.session.get(ModelDownload,item_id).status)

    def test_legacy_upload_checkpoint_is_rebuilt_from_account_settings(self):
        item=self.download(status='uploading',local_path='ad:movie',title='movie')
        item.destination_type='gdrive_rotation'; item.gdrive_account='account'; db.session.commit()
        accounts=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts):
            key=self.prepare_block(item)
        state=db.session.get(ModelDownloadBlock,key).cleanup[0]['state']
        self.assertEqual('sa:{my}/incoming/default/movie',state['incoming'])
        self.assertEqual('sa:{drive}/incoming/default/movie',state['shared_incoming'])

    def test_failed_remote_engine_cleanup_does_not_skip_staging_or_remote_files(self):
        root=Path(self.tmp.name)/'staging'; partial=root/'movie'; partial.mkdir(parents=True)
        (partial/'part').write_bytes(b'part')
        Settings.set('download_local_staging_path',str(root))
        item=self.download(local_path=str(partial))
        item.upload_state={'incoming':'sa:{my}/incoming/movie','folder_name':'movie',
            'dest_parent':'sa:{drive}/final','rclone_conf':'/config'}
        db.session.commit()
        key=self.prepare_block(item)
        with patch.object(DownloadUtil,'delete_remote_tasks',side_effect=RuntimeError('engine unavailable')):
            DownloadBlock.cleanup(key)
        self.assertFalse(partial.exists())
        self.assertTrue(any(c[1]=='purge' for c in self.commands))
        self.assertTrue(db.session.get(ModelDownloadBlock,key).cleanup[0]['pending'])

    def test_simple_rclone_upload_records_cleanup_path_and_uses_cancellable_runner(self):
        base=load_class('util_download.py','BaseTransporter')
        COMMON['BaseTransporter']=base
        transporter=load_class('util_download.py','RcloneSimpleTransporter')()
        item=self.download(status='downloaded',local_path='ad:movie',title='movie')
        item_id=item.id
        db.session.remove()
        def run(cmd,*args,**kwargs):
            self.assertEqual(item_id,kwargs['item_id'])
            row=db.session.get(ModelDownload,item_id)
            self.assertEqual('sa:incoming/movie',row.upload_state['incoming'])
            return True,''
        self.run.side_effect=run
        success,status,_=transporter.transport(item,'ad:movie',{'remote_name':'sa','upload_path':'incoming','complete_path':'final'})
        self.assertTrue(success)
        self.assertEqual('completed',status)

    def test_unmanaged_remote_queue_item_can_be_blocked_without_adopting_profile(self):
        module=ModuleDownload.__new__(ModuleDownload); module.name='download'
        request=NS(form={'engine_name':'ad','task_id':'remote-task','hash':'c'*40,'title':'orphan'})
        with patch.object(FeederUtil,'get_downloader_by_name',return_value={'name':'ad'}), patch.object(DownloadBlock,'start_cleanup'), patch.object(UploadUtil,'kill_item_rclone'):
            result=module.process_ajax('block_orphan',request)
        self.assertEqual('success',result['ret'])
        item=ModelDownload.get_by_infohash('c'*40)
        self.assertEqual('blocked',item.status)
        self.assertEqual('remote-task',item.engine_task_id)
        self.assertTrue(ModelDownload.is_blocked('c'*40))

    def manager(self, threshold=100):
        manager = UploadUtil.AccountManager.__new__(UploadUtil.AccountManager)
        manager.lock = threading.Lock()
        manager.accounts = [{'username': 'account', 'remote_name': 'sa', 'mydrive_rclone_id': 'mydrive'}]
        manager.busy_accounts = set()
        manager.blocked_accounts = {}
        manager.usage_map = {}
        manager.shared_usage = 0
        manager.limit_shared = manager.limit_user = 2**60
        manager.threshold = threshold
        return manager

    def source(self, directory=True):
        path = Path(self.tmp.name)/'source'
        if directory:
            path.mkdir()
            (path/'movie.mkv').write_bytes(b'data')
        else:
            path.write_bytes(b'data')
        return str(path)

    def test_download_pause_persists_through_runtime_lock_initialization(self):
        self.assertFalse(FeederUtil.is_download_paused())
        FeederUtil.set_download_paused(True)
        db.session.remove()
        FeederUtil.init_runtime_locks()
        FeederUtil.reset_runtime_lock('download_pipeline')
        self.assertTrue(FeederUtil.is_download_paused())
        FeederUtil.set_download_paused(True)
        self.assertTrue(FeederUtil.is_download_paused())
        FeederUtil.set_download_paused(False)
        self.assertFalse(FeederUtil.is_download_paused())

    def test_download_control_waits_for_lock_and_active_transfers(self):
        self.assertEqual('idle',FeederUtil.get_download_control_state()['state'])
        FeederUtil.acquire_runtime_lock('download_pipeline','worker')
        self.assertEqual('running',FeederUtil.get_download_control_state()['state'])
        FeederUtil.set_download_paused(True)
        self.assertEqual('stopping',FeederUtil.get_download_control_state()['state'])
        item=self.download(status='uploading');item_id=item.id
        FeederUtil.release_runtime_lock('download_pipeline','worker')
        self.assertEqual('stopping',FeederUtil.get_download_control_state()['state'])
        item.status='completed';db.session.commit()
        self.assertEqual('paused',FeederUtil.get_download_control_state()['state'])
        self.assertEqual('completed',db.session.get(ModelDownload,item_id).status)

    def test_pause_endpoint_and_resume_dispatch_without_waiting_for_celery(self):
        module=ModuleDownload.__new__(ModuleDownload)
        module.name='download'
        task_namespace=next(env for env in NAMESPACES if env['__name__']=='feeder_test.mod_download')
        result=NS(get=Mock(side_effect=AssertionError('must not wait for task completion')))
        task=NS(apply_async=Mock(return_value=result))
        with patch.dict(task_namespace,TaskDownloadBase=NS(start=task)),patch.object(F,'config',{'use_celery':True},create=True),patch.object(module,'start_celery',create=True) as dispatch:
            response=module.process_ajax('pause_pipeline',NS(form={}))
            self.assertEqual('paused',response['control']['state'])
            self.assertTrue(FeederUtil.is_download_paused())
            response=module.process_ajax('pipeline_control_state',NS(form={}))
            self.assertEqual('paused',response['control']['state'])
            response=module.process_ajax('resume_pipeline',NS(form={}))
            self.assertEqual('success',response['ret'])
            self.assertFalse(FeederUtil.is_download_paused())
            task.apply_async.assert_called_once_with(args=('default',))
            result.get.assert_not_called()
            dispatch.assert_not_called()

    def test_resume_without_celery_returns_while_background_task_is_running(self):
        module=ModuleDownload.__new__(ModuleDownload);module.name='download'
        FeederUtil.set_download_paused(True)
        started=threading.Event();release=threading.Event();finished=threading.Event()
        def run():
            started.set()
            try:release.wait(5)
            finally:finished.set()
        with patch.object(F,'config',{'use_celery':False},create=True),patch.object(TaskDownload,'run_pipeline',side_effect=run):
            try:
                response=module.process_ajax('resume_pipeline',NS(form={}))
                self.assertEqual('success',response['ret'])
                self.assertTrue(started.wait(2))
                self.assertFalse(finished.is_set())
                self.assertFalse(FeederUtil.is_download_paused())
            finally:
                release.set()
                self.assertTrue(finished.wait(2))

    def test_cancel_stop_request_does_not_dispatch_a_duplicate_pipeline(self):
        module=ModuleDownload.__new__(ModuleDownload);module.name='download'
        FeederUtil.acquire_runtime_lock('download_pipeline','existing')
        FeederUtil.set_download_paused(True)
        task_namespace=next(env for env in NAMESPACES if env['__name__']=='feeder_test.mod_download')
        task=NS(apply_async=Mock())
        with patch.dict(task_namespace,TaskDownloadBase=NS(start=task)),patch.object(F,'config',{'use_celery':True},create=True):
            response=module.process_ajax('resume_pipeline',NS(form={}))
        self.assertEqual('success',response['ret'])
        self.assertEqual('running',response['control']['state'])
        task.apply_async.assert_not_called()
        self.assertFalse(FeederUtil.is_download_paused())

    def test_celery_pause_lookup_has_application_context(self):
        FeederUtil.set_download_paused(True)
        active=[];observed=[]
        @contextlib.contextmanager
        def app_context():
            active.append(True)
            try:yield
            finally:active.pop()
        def engine(**kwargs):
            observed.append(bool(active))
            return self.engine
        with patch.object(F.app,'app_context',side_effect=app_context), patch.object(db,'get_engine',side_effect=engine):
            TaskDownload.run_pipeline()
        self.assertEqual([True],observed)

    def test_paused_pipeline_blocks_scheduler_manual_runs_and_uploads(self):
        FeederUtil.set_download_paused(True)
        module=ModuleDownload.__new__(ModuleDownload);module.name='download'
        with patch.object(TaskDownload,'_run_pipeline_locked') as run, patch.object(module,'start_celery',create=True) as dispatch, patch.object(module,'one_execute',create=True) as manual, patch.object(UploadUtil,'execute_upload') as upload:
            TaskDownload.run_pipeline()
            module.scheduler_function()
            self.assertEqual('warning',module.process_ajax('one_execute',NS(form={}))['ret'])
            TaskDownload.process_uploads()
            run.assert_not_called();dispatch.assert_not_called();manual.assert_not_called();upload.assert_not_called()
        self.assertFalse(TaskDownload._run_mutex.locked())

    def test_upload_in_progress_finishes_move_and_cleanup_after_pause(self):
        path=self.source();item=self.download(local_path=path);item_id=item.id
        def run(cmd,*args,**kwargs):
            self.commands.append(list(cmd))
            if cmd[1]=='copy':FeederUtil.set_download_paused(True)
            return (False,'directory not found') if cmd[1]=='lsjson' else (True,'')
        self.run.side_effect=run
        self.assertTrue(UploadUtil.execute_upload(item,self.manager()))
        self.assertTrue(FeederUtil.is_download_paused())
        self.assertEqual('completed',db.session.get(ModelDownload,item_id).status)
        self.assertTrue(any(c[1]=='move' for c in self.commands))
        self.assertTrue(any(c[1:3]==['backend','chpar'] for c in self.commands))
        self.assertFalse(os.path.exists(path))

    def test_parallel_upload_pause_finishes_only_admitted_workers(self):
        ids=[self.download(title='task'+str(i),infohash=format(i,'040x')).id for i in range(8)]
        db.session.remove()
        Settings.values['download_upload_workers']='2'
        started=[];completed=[];errors=[]
        ready=threading.Event();release=threading.Event();lock=threading.Lock()
        def upload(item,manager):
            item_id=item.id
            item.status='uploading';db.session.commit()
            with lock:
                started.append(item_id)
                if len(started)==2:ready.set()
            if not release.wait(5):raise RuntimeError('release timed out')
            item=db.session.get(ModelDownload,item_id)
            item.status='completed';db.session.commit()
            with lock:completed.append(item_id)
            return True
        def run():
            try:TaskDownload.process_uploads()
            except Exception as ex:errors.append(ex)
            finally:db.session.remove()
        with patch.object(UploadUtil,'get_account_manager',return_value=object()),patch.object(UploadUtil,'execute_upload',side_effect=upload):
            thread=threading.Thread(target=run)
            thread.start()
            try:
                self.assertTrue(ready.wait(5),'two upload workers must start')
                FeederUtil.set_download_paused(True)
                self.assertTrue(thread.is_alive())
                self.assertEqual('stopping',FeederUtil.get_download_control_state()['state'])
            finally:
                release.set();thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual(set(ids[:2]),set(started))
        self.assertEqual(set(started),set(completed))
        self.assertEqual(6,db.session.query(ModelDownload).filter_by(status='pending_upload').count())
        self.assertEqual('paused',FeederUtil.get_download_control_state()['state'])
        with patch.object(UploadUtil,'execute_upload') as upload:
            TaskDownload.process_uploads()
            upload.assert_not_called()

    def test_bounded_workers_do_not_submit_after_pause_and_continue_after_resume(self):
        FeederUtil.set_download_paused(True)
        worker=Mock()
        self.assertEqual([],list(TaskDownload._worker_results(worker,range(20),2)))
        worker.assert_not_called()
        FeederUtil.set_download_paused(False)
        results=list(TaskDownload._worker_results(worker,range(5),2))
        self.assertEqual(5,len(results))
        self.assertEqual(5,worker.call_count)
        for _,future in results:future.result()

    def test_feed_sync_does_not_share_output_count_or_xml_age_limits(self):
        for i in range(105):
            item = ModelCrawlItem('site', 'board')
            item.title = str(i); item.url = 'https://site/'+str(i)
            item.created_time = datetime.datetime.now()-datetime.timedelta(days=90)
            item.magnet = 'magnet:?xt=urn:btih:'+format(i, '040x')
            item.infohash = format(i, '040x'); item.files = ''
            db.session.add(item)
        db.session.commit()
        feed = {'name': 'f', 'feed_count': 1, 'rss_file_items': 1, 'rss_file_days': 1,
                'sources': [{'site': 'site', 'board': 'board'}]}
        self.assertEqual(105, FeedUtil.sync_feed(feed))
        self.assertEqual(0, FeedUtil.sync_feed(feed))
        self.assertEqual(105, db.session.query(ModelFeedItem).count())
        added = ModelCrawlItem('site','board'); added.title='new'; added.url='https://site/new'
        added.magnet='magnet:?xt=urn:btih:'+'f'*40
        db.session.add(added); db.session.commit()
        self.assertEqual(1, FeedUtil.sync_feed(feed))

    def feed_item(self):
        item = ModelFeedItem('f'); item.title = 'feed item'; item.url = 'https://site/post'
        item.magnet = '\n'.join('magnet:?xt=urn:btih:'+x*40 for x in ('a','b'))
        item.files = 'https://file/attachment|attachment.txt|NONE'
        db.session.add(item); db.session.commit()
        return item

    def test_rss_api_and_xml_caps_are_independent_and_count_emitted_items(self):
        item = self.feed_item()
        module = ModuleFeed.__new__(ModuleFeed); module.name='feed'
        config = {'name':'f', 'feed_count':2, 'rss_file_items':1, 'rss_file_path':self.tmp.name}
        with patch.object(FeederUtil, 'get_feed', return_value=config):
            xml = module._handle_feed_rss(feed_id=1)
        self.assertEqual(2, len(ET.fromstring(xml).findall('.//item')))
        with patch.object(FeedUtil, 'sync_feed', return_value=0), patch.object(P, 'get_module', create=True, return_value=module):
            self.assertTrue(FeedUtil.save_rss_file(config, force=True))
        self.assertEqual(1, len(ET.parse(Path(self.tmp.name)/'f.xml').findall('.//item')))
        # A record may expand into multiple items, but each output observes its own cap.
        self.assertEqual(1, db.session.query(ModelFeedItem).count())

    def test_feed_attachment_uses_feed_table_even_when_crawl_ids_collide(self):
        item = self.feed_item(); item.magnet=''; db.session.commit(); item_id=item.id
        crawl = ModelCrawlItem('site','board'); crawl.id=item_id; crawl.title='wrong'
        crawl.files='https://wrong/file|wrong.txt|NONE'
        db.session.add(crawl); db.session.commit()
        feed = ModuleFeed.__new__(ModuleFeed)
        xml = feed.generate_rss_feed('f', [item])
        self.assertIn('/api/feed/download?id='+str(item_id)+'_0', xml)
        crawl_module = ModuleCrawl.__new__(ModuleCrawl); crawl_module.name='crawl'
        env = next(e for e in NAMESPACES if e.get('ModuleCrawl') is ModuleCrawl)
        with patch.object(FeederUtil,'get_feed_by_name',return_value={}), patch.object(CrawlUtil,'download_file_stream',return_value=io.BytesIO(b'feed')) as stream, patch.dict(env,send_file=lambda payload,**kw:payload.read()):
            self.assertEqual(b'feed',crawl_module._handle_download_stream(str(item_id)+'_0',source_type='feed'))
            self.assertEqual('https://file/attachment',stream.call_args.args[0])
            with self.assertRaises(HTTPError):
                crawl_module._handle_download_stream(str(item_id)+'_-1',source_type='feed')

    def test_relay_false_reports_do_not_complete_or_delete_source(self):
        module=ModuleDownload.__new__(ModuleDownload); module.name='download'
        module._verify_api_auth=lambda req:True
        item=self.download(status='relay_transferring'); item_id=item.id
        engine=NS(delete_task=Mock())
        with patch.object(DownloadUtil,'create_engine',return_value=engine):
            for value in [False, 'false', 'False', '0', 'off', 'no', '', 0]:
                item=db.session.get(ModelDownload,item_id); item.status='relay_transferring'; db.session.commit()
                req=NS(get_json=lambda **kw:None, form={'id':str(item_id),'success':value})
                module._handle_relay_report(req)
                self.assertEqual('failed', db.session.get(ModelDownload,item_id).status)
            engine.delete_task.assert_not_called()
            req=NS(get_json=lambda **kw:{'id':item_id,'success':'invalid'},form={})
            self.assertEqual(400,module._handle_relay_report(req)[1])

    def test_pool_off_works_with_no_accounts(self):
        Settings.values['download_use_gdrive_pool']='False'
        manager=self.manager(); manager.accounts=[]
        item=self.download(local_path=self.source()); item_id=item.id
        self.assertTrue(UploadUtil.execute_upload(item,manager))
        self.assertEqual('completed',db.session.get(ModelDownload,item_id).status)
        self.assertFalse(manager.busy_accounts)
        copy_cmd=next(c for c in self.commands if c[1]=='copy')
        self.assertIn('{drive}',copy_cmd[3])
        self.assertNotIn('--drive-impersonate',copy_cmd)

    def test_accounts_released_in_both_modes(self):
        for threshold in [0,100]:
            with self.subTest(threshold=threshold):
                path=str(Path(self.tmp.name)/str(threshold)); Path(path).mkdir(); (Path(path)/'file').write_bytes(b'data')
                manager=self.manager(threshold=threshold)
                item=self.download(local_path=path,title='folder'+str(threshold))
                self.assertTrue(UploadUtil.execute_upload(item,manager))
                self.assertFalse(manager.busy_accounts)

    def test_move_retry_keeps_account_and_never_reuploads_or_recharges(self):
        manager=self.manager(); item=self.download(local_path=self.source()); item_id=item.id
        failing=True
        def run(cmd,*args,**kw):
            if cmd[1:3]==['backend','chpar'] and failing:
                self.commands.append(list(cmd)); return False,'temporary move failure'
            return self.rclone(cmd,*args,**kw)
        self.run.side_effect=run
        self.assertFalse(UploadUtil.execute_upload(item,manager))
        state=copy.deepcopy(db.session.get(ModelDownload,item_id).upload_state)
        self.assertEqual('shared_incoming',state['phase'])
        self.assertEqual(1,sum(c[1]=='move' for c in self.commands))
        failing=False
        # A new manager/session simulates the next pipeline run.
        db.session.remove()
        retry_manager=self.manager()
        self.assertTrue(UploadUtil.resume_upload(item_id,retry_manager))
        self.assertEqual(1,sum(c[1]=='copy' for c in self.commands))
        self.assertEqual(1,sum(c[1]=='move' for c in self.commands))
        self.assertEqual({'SHARED_DRIVE_UPLOAD', 'account'}, {row.stat_key for row in db.session.query(ModelDownloadStat).all()})
        self.assertEqual(1, len({row.attempt_id for row in db.session.query(ModelDownloadStat).all()}))
        self.assertFalse(retry_manager.busy_accounts)

    def test_cleanup_retry_skips_remote_copy_and_move(self):
        manager=self.manager(); source=self.source()
        item=self.download(local_path=source); item_id=item.id
        original=shutil.rmtree
        def fail_source(path,*args,**kwargs):
            if str(path)==source: raise PermissionError('busy source')
            return original(path,*args,**kwargs)
        with patch.object(shutil,'rmtree',side_effect=fail_source):
            self.assertFalse(UploadUtil.execute_upload(item,manager))
        self.assertEqual('moved',db.session.get(ModelDownload,item_id).upload_state['phase'])
        before=len(self.commands)
        self.assertTrue(UploadUtil.resume_upload(item_id,self.manager()))
        self.assertFalse(any(c[1] in ['copy','move','backend'] for c in self.commands[before:]))
        self.assertFalse(os.path.exists(source))

    def test_legacy_move_failure_with_source_reuploads(self):
        source=self.source(); item=self.download(status='move_failed',local_path=source); item_id=item.id
        def missing_then_upload(cmd,*args,**kw):
            if cmd[1]=='lsjson':
                self.commands.append(list(cmd)); return False,'directory not found'
            return self.rclone(cmd,*args,**kw)
        self.run.side_effect=missing_then_upload
        self.assertTrue(UploadUtil.execute_upload(item,self.manager()))
        self.assertEqual(1,sum(c[1]=='copy' for c in self.commands))
        self.assertEqual('completed',db.session.get(ModelDownload,item_id).status)

    def test_failed_upload_releases_account_and_preserves_remote_partial_files(self):
        self.run.side_effect=lambda cmd,*a,**kw: (False,'network error')
        item=self.download(local_path=self.source()); item_id=item.id; manager=self.manager()
        self.assertFalse(UploadUtil.execute_upload(item,manager))
        self.assertFalse(manager.busy_accounts)
        self.assertFalse(any(c.args[0][1] in ['purge','delete','cleanup'] for c in self.run.call_args_list))
        self.assertEqual('pending_upload',db.session.get(ModelDownload,item_id).status)

    def test_single_file_size_and_cleanup(self):
        source=self.source(directory=False); item=self.download(local_path=source); item_id=item.id
        self.assertTrue(UploadUtil.execute_upload(item,self.manager()))
        self.assertEqual(4,db.session.get(ModelDownload,item_id).file_size)
        self.assertFalse(os.path.exists(source))

    def test_queue_limit_only_applies_to_alldebrid(self):
        for engine_type in ['cd2','qbittorrent','custom','alldebrid']:
            self.assertEqual(3 if engine_type=='alldebrid' else 0,
                             DownloadUtil.get_queue_limit({'engine_type':engine_type,'max_active_tasks':3}))

    def test_full_remote_queue_can_still_adopt_existing_task(self):
        config={'name':'ad','engine_type':'alldebrid','enabled':True,'max_active_tasks':1}
        engine=NS(get_status=lambda:([{'task_id':'remote','hash':'a'*40,'status':'downloading'}],''),SUPPORTED_PROTOCOLS=['magnet'])
        item=self.download(status='pending'); item.priority_chain=['ad']; item_id=item.id; db.session.commit()
        with patch.object(FeederUtil,'get_downloaders',return_value=[config]), patch.object(FeederUtil,'get_downloader_by_name',return_value=config), patch.object(DownloadUtil,'create_engine',return_value=engine), patch.object(DownloadUtil,'add_magnet') as add:
            TaskDownload.dispatch_pending_downloads()
            add.assert_not_called()
        self.assertEqual('downloading',db.session.get(ModelDownload,item_id).status)
        self.assertEqual('remote',db.session.get(ModelDownload,item_id).engine_task_id)

    def test_dashboard_upload_count_displays_fifty_current_out_of_113_total(self):
        Settings.values['download_max_staging_items']='50'
        for i in range(113):
            status='pending_upload' if i<48 else 'uploading' if i<50 else ['downloaded','pending_local_staging','waiting_transporter','relay_transferring'][(i-50)%4]
            self.download(status=status,local_path='remote:'+str(i),title='task'+str(i),infohash=format(i,'040x'))
        self.download(status='pending',title='not downloaded')
        self.download(status='downloading',title='downloading')
        module=ModuleDownload.__new__(ModuleDownload);module.name='download'
        expected={'uploading':113,'upload_current':50,'upload_total':113}
        self.assertEqual(expected,module.get_upload_queue_counts())
        response=module.process_ajax('pipeline_control_state',NS(form={}))
        self.assertEqual(expected,response['counts'])
        # SSE must carry the same numbers as the lightweight fallback endpoint.
        namespace=next(env for env in NAMESPACES if env['__name__']=='feeder_test.mod_download')
        with patch.object(namespace['logger'],'debug',side_effect=RuntimeError('unexpected SSE error')):
            stream=module.generate_sse_stream()
            try:message=next(stream)
            finally:stream.close()
        payload=json.loads(message.removeprefix('data: ').strip())
        self.assertEqual(50,payload['counts']['upload_current'])
        self.assertEqual(113,payload['counts']['upload_total'])

    def test_dashboard_upload_count_preserves_actual_overflow_and_deduplicates_buffers(self):
        root=Path(self.tmp.name)/'staging';root.mkdir()
        Settings.values.update(download_local_staging_path=str(root),download_max_staging_items='2')
        for i,status in enumerate(['pending_upload','move_failed','downloaded']):
            folder=root/str(i);folder.mkdir()
            self.download(status=status,local_path=str(folder),title='buffer'+str(i))
        self.download(status='downloaded',local_path='remote:waiting',title='waiting')
        module=ModuleDownload.__new__(ModuleDownload)
        self.assertEqual({'uploading':4,'upload_current':3,'upload_total':4},module.get_upload_queue_counts())

    def test_dashboard_upload_count_still_counts_current_queue_when_unlimited(self):
        Settings.values['download_max_staging_items']='0'
        self.download(status='pending_upload',local_path='remote:a',title='queued')
        self.download(status='uploading',local_path='remote:b',title='running')
        self.download(status='downloaded',local_path='remote:c',title='waiting')
        module=ModuleDownload.__new__(ModuleDownload)
        self.assertEqual({'uploading':3,'upload_current':2,'upload_total':3},module.get_upload_queue_counts())

    def test_upload_queue_combines_direct_uploads_staging_and_retained_files_once(self):
        root=Path(self.tmp.name)/'staging';root.mkdir()
        Settings.values.update(download_local_staging_path=str(root),download_max_staging_items='10')
        held=[]
        for i,status in enumerate(['local_staging','pending_upload','uploading','move_failed']):
            path=root/str(i)
            if status in ['pending_upload','move_failed']:path.mkdir()
            held.append(self.download(status=status,local_path=str(path),title=str(i)).id)
        self.download(status='completed',local_path=str(root/'gone'),title='gone')
        self.download(status='downloaded',local_path='remote:waiting',title='not admitted')
        self.download(status='pending_local_staging',local_path='remote:waiting2',title='staging wait')
        limit,occupied=TaskDownload._get_upload_queue_state()
        self.assertEqual(10,limit)
        self.assertEqual(set(held),occupied)

    def test_direct_uploads_admit_only_available_queue_slots_then_refill(self):
        Settings.values['download_max_staging_items']='1'
        ids=[self.download(status='downloaded',local_path='remote:'+str(i),title='direct'+str(i),infohash=format(i,'040x')).id for i in range(3)]
        profile={'destination':{'type':'gdrive_rotation'}}
        with patch.object(FeederUtil,'get_download_profile_by_feed',return_value=profile),patch.object(DownloadUtil,'get_transporter',return_value=object()):
            TaskDownload.route_completed_downloads()
            self.assertEqual(1,db.session.query(ModelDownload).filter_by(status='pending_upload').count())
            self.assertEqual(2,db.session.query(ModelDownload).filter_by(status='downloaded').count())
            first=db.session.query(ModelDownload).filter_by(status='pending_upload').one()
            first.status='completed';db.session.commit()
            TaskDownload.route_completed_downloads()
        self.assertEqual(1,db.session.query(ModelDownload).filter_by(status='pending_upload').count())
        self.assertEqual(1,db.session.query(ModelDownload).filter_by(status='downloaded').count())
        self.assertEqual(3,len(ids))

    def test_buffered_item_can_advance_when_direct_upload_queue_is_full(self):
        root=Path(self.tmp.name)/'staging';root.mkdir();buffer=root/'buffer';buffer.mkdir()
        Settings.values.update(download_local_staging_path=str(root),download_max_staging_items='1')
        direct=self.download(status='downloaded',local_path='remote:direct',title='direct');direct_id=direct.id
        staged=self.download(status='downloaded',local_path=str(buffer),title='staged');staged_id=staged.id
        profile={'destination':{'type':'gdrive_rotation'}}
        with patch.object(FeederUtil,'get_download_profile_by_feed',return_value=profile),patch.object(DownloadUtil,'get_transporter',return_value=object()):
            TaskDownload.route_completed_downloads()
        self.assertEqual('downloaded',db.session.get(ModelDownload,direct_id).status)
        self.assertEqual('pending_upload',db.session.get(ModelDownload,staged_id).status)
        self.assertEqual({staged_id},TaskDownload._get_upload_queue_state()[1])

    def test_shared_upload_queue_limit_blocks_staging_and_new_remote_retries(self):
        root=Path(self.tmp.name)/'staging'
        Settings.values.update(download_local_staging_path=str(root),download_max_staging_items='1')
        self.download(status='pending_upload',local_path='remote:queued',title='queued')
        pending=self.download(status='pending_local_staging',local_path='remote:stage',title='stage');pending_id=pending.id
        failed=self.download(status='move_failed',local_path='remote:failed',title='failed');failed_id=failed.id
        with patch.object(TaskDownload,'_worker_results',return_value=iter([])) as workers, patch.object(UploadUtil,'get_account_manager',return_value=object()),patch.object(UploadUtil,'retry_failed_upload') as retry:
            TaskDownload.process_local_staging()
            TaskDownload.retry_move_failed()
            workers.assert_not_called();retry.assert_not_called()
        self.assertEqual('pending_local_staging',db.session.get(ModelDownload,pending_id).status)
        self.assertIsNone(db.session.get(ModelDownload,failed_id).last_move_attempt_time)

    def test_staging_and_upload_use_same_workers_and_share_queue_slots(self):
        root=Path(self.tmp.name)/'staging'
        Settings.values.update(download_local_staging_path=str(root),download_max_staging_items='3',download_staging_workers='9',download_upload_workers='2')
        for i in range(2):self.download(status='pending_upload',local_path='remote:'+str(i),title='queued'+str(i))
        for i in range(4):self.download(status='pending_local_staging',local_path='remote:stage'+str(i),title='stage'+str(i))
        calls=[]
        def workers(worker,ids,count,**kwargs):
            calls.append((list(ids),count))
            return iter([])
        with patch.object(TaskDownload,'_worker_results',side_effect=workers),patch.object(UploadUtil,'get_account_manager',return_value=object()):
            TaskDownload.process_local_staging()
            TaskDownload.process_uploads()
        self.assertEqual([1,2],[len(ids) for ids,_ in calls])
        self.assertEqual([2,2],[count for _,count in calls])

    def test_zero_upload_queue_limit_admits_all_direct_uploads(self):
        Settings.values['download_max_staging_items']='0'
        for i in range(3):self.download(status='downloaded',local_path='remote:'+str(i),title='direct'+str(i))
        with patch.object(FeederUtil,'get_download_profile_by_feed',return_value={'destination':{'type':'gdrive_rotation'}}),patch.object(DownloadUtil,'get_transporter',return_value=object()):
            TaskDownload.route_completed_downloads()
        self.assertEqual(3,db.session.query(ModelDownload).filter_by(status='pending_upload').count())

    def test_existing_upload_queue_over_limit_is_processed_without_dropping_items(self):
        Settings.values.update(download_max_staging_items='1',download_upload_workers='2')
        for i in range(3):self.download(local_path='remote:'+str(i),title='queued'+str(i))
        db.session.remove()
        def upload(item,manager):
            item.status='completed';db.session.commit()
            return True
        with patch.object(UploadUtil,'get_account_manager',return_value=object()),patch.object(UploadUtil,'execute_upload',side_effect=upload):
            TaskDownload.process_uploads()
        self.assertEqual(3,db.session.query(ModelDownload).filter_by(status='completed').count())

    def test_staging_cap_includes_failed_and_upload_waiting_files(self):
        root=Path(self.tmp.name)/'staging'; root.mkdir()
        Settings.values.update(download_local_staging_path=str(root),download_max_staging_items='2')
        for i,status in enumerate(['pending_upload','move_failed']):
            path=root/str(i); path.mkdir()
            self.download(status=status,local_path=str(path),title='retained'+str(i))
        pending=self.download(status='pending_local_staging',local_path='remote:source',title='pending'); item_id=pending.id
        TaskDownload.process_local_staging()
        self.run.assert_not_called()
        self.assertEqual('pending_local_staging',db.session.get(ModelDownload,item_id).status)

    def test_requests_fallback_without_cffi(self):
        env=CrawlUtil.get_html.__func__.__globals__
        session=NS(get=Mock(return_value=NS(status_code=200,text='<html>ok</html>')), cookies=Mock())
        CrawlUtil._requests_sessions={}; CrawlUtil._cf_cookies={}; CrawlUtil._cf_user_agents={}
        cfg=NS(use_proxy=False,proxy_url='',use_selenium=False,use_flaresolverr=False,max_retries=1,delay=1)
        with patch.dict(env,requests=NS(Session=lambda:session)):
            self.assertEqual('<html>ok</html>',CrawlUtil.get_html('https://site',scheduler_instance=cfg))
        session.get.assert_called_once()

    def test_selenium_can_initialize_lazily_without_hook(self):
        cfg=NS(use_selenium=True)
        with patch.object(CrawlUtil,'get_hook',return_value=None), patch.object(CrawlUtil,'get_html',return_value=None) as get, patch.object(CrawlUtil,'close_sessions'):
            self.assertIsNone(TaskCrawl.execute_board_crawl({'NAME':'site'},'board',target_cfg=cfg))
            get.assert_called_once()

    def test_progress_sizes_support_iec_units(self):
        for value,expected in [('1.5 MiB',1572864),('2 GiB',2*1024**3),('1 KB',1024),('1 PiB',1024**5)]:
            self.assertEqual(expected,UploadUtil.parse_size_bytes(value))

    def test_crawl_status_filter_uses_shared_active_statuses(self):
        crawl=ModelCrawlItem('site','board'); crawl.title='post'; crawl.infohash='a'*40
        crawl.magnet='magnet:?xt=urn:btih:'+'a'*40
        db.session.add(crawl); db.session.commit()
        download=self.download(status='pending_upload'); item_id=download.id
        for status in ['pending_upload','downloaded','pending_local_staging','relay_transferring']:
            download=db.session.get(ModelDownload,item_id); download.status=status; db.session.commit()
            query=ModelCrawlItem.make_query(NS(form={'status_filter':'download_active'}))
            self.assertEqual(1,query.count())

    def test_pending_relay_resume_uses_its_engine(self):
        item=self.download(status='pending_relay'); item.destination_type='relay'; item.current_engine_name='engine'
        db.session.commit()
        transporter=NS(resume_relay=Mock())
        with patch.object(DownloadUtil,'get_transporter',return_value=transporter), patch.object(FeederUtil,'get_profile_destination',return_value={}) as dest:
            TaskDownload.check_pending_relays()
            dest.assert_called_once_with({},'engine')
            transporter.resume_relay.assert_called_once_with({})

    def test_cancelled_upload_is_not_revived_by_completion(self):
        source=self.source(); item=self.download(local_path=source); item_id=item.id
        manager=self.manager()
        def cancel_during_copy(cmd,*args,**kw):
            result=self.rclone(cmd,*args,**kw)
            if cmd[1]=='copy':
                db.session.query(ModelDownload).filter_by(id=item_id).update({'status':'cancelled'})
                db.session.commit(); db.session.remove()
            return result
        self.run.side_effect=cancel_during_copy
        self.assertFalse(UploadUtil.execute_upload(item,manager))
        self.assertEqual('cancelled',db.session.get(ModelDownload,item_id).status)
        self.assertFalse(manager.busy_accounts)
        self.assertFalse(any(c[1]=='move' for c in self.commands))

    def test_first_move_failure_retries_without_another_copy(self):
        source=self.source(); item=self.download(local_path=source); item_id=item.id
        failing=True
        def fail_move(cmd,*args,**kw):
            if cmd[1]=='move' and failing:
                self.commands.append(list(cmd)); return False,'temporary move failure'
            return self.rclone(cmd,*args,**kw)
        self.run.side_effect=fail_move
        self.assertFalse(UploadUtil.execute_upload(item,self.manager()))
        self.assertEqual('uploaded',db.session.get(ModelDownload,item_id).upload_state['phase'])
        failing=False
        self.assertTrue(UploadUtil.resume_upload(item_id,self.manager()))
        self.assertEqual(1,sum(c[1]=='copy' for c in self.commands))
        self.assertEqual({'SHARED_DRIVE_UPLOAD', 'account'}, {row.stat_key for row in db.session.query(ModelDownloadStat).all()})
        self.assertEqual(1, len({row.attempt_id for row in db.session.query(ModelDownloadStat).all()}))

    def test_drain_preserves_accounts_with_tracked_incoming(self):
        item=self.download(status='pending_upload'); item.gdrive_account='account'; db.session.commit()
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=[{'username':'account','remote_name':'sa'}]):
            UploadUtil.drain_all_sa_mydrives()
        self.run.assert_not_called()

    def test_remote_missing_grace_fails_in_same_engine_without_fallback(self):
        item=self.download(status='downloading'); item.current_engine_name='e'; item.engine_task_id='task'; item.priority_chain=['e','backup']; item_id=item.id; db.session.commit()
        engine=NS(get_status=lambda:([],''), status_list_complete=True)
        config={'name':'e','missing_timeout_hours':1}
        with patch.object(FeederUtil,'get_downloaders',return_value=[config]), patch.object(DownloadUtil,'create_engine',return_value=engine):
            TaskDownload.poll_active_downloads()
            item=db.session.get(ModelDownload,item_id)
            self.assertIsNotNone(item.remote_missing_since)
            item.remote_missing_since=datetime.datetime.now()-datetime.timedelta(hours=2); db.session.commit()
            TaskDownload.poll_active_downloads()
        item=db.session.get(ModelDownload,item_id)
        self.assertEqual('failed',item.status)
        self.assertEqual('e',item.current_engine_name)
        self.assertEqual(0,item.current_engine_index)

    def test_partial_list_and_query_errors_do_not_expire_missing_tasks(self):
        item=self.download(status='downloading'); item.current_engine_name='e'; item_id=item.id; db.session.commit()
        config={'name':'e','missing_timeout_hours':1}
        for complete,error in [(False,''),(True,'timeout')]:
            item=db.session.get(ModelDownload,item_id); item.remote_missing_since=datetime.datetime.now()-datetime.timedelta(days=3); db.session.commit()
            engine=NS(get_status=lambda:([],error),status_list_complete=complete)
            with patch.object(FeederUtil,'get_downloaders',return_value=[config]), patch.object(DownloadUtil,'create_engine',return_value=engine):
                TaskDownload.poll_active_downloads()
            item=db.session.get(ModelDownload,item_id)
            self.assertEqual('downloading',item.status)
            self.assertIsNone(item.remote_missing_since)

    def test_remote_reappearance_resets_grace(self):
        item=self.download(status='downloading'); item.current_engine_name='e'; item.engine_task_id='task'; item.remote_missing_since=datetime.datetime.now()-datetime.timedelta(hours=3); item_id=item.id; db.session.commit()
        engine=NS(get_status=lambda:([{'task_id':'task','status':'downloading'}],''),status_list_complete=True)
        with patch.object(FeederUtil,'get_downloaders',return_value=[{'name':'e','missing_timeout_hours':1}]), patch.object(DownloadUtil,'create_engine',return_value=engine):
            TaskDownload.poll_active_downloads()
        self.assertIsNone(db.session.get(ModelDownload,item_id).remote_missing_since)

    def test_cd2_page_limit_marks_list_as_partial_without_extra_requests(self):
        base=load_class('util_download.py','BaseDownloadEngine')
        COMMON['BaseDownloadEngine']=base
        cd2=load_class('util_download.py','CloudDrive2Engine')
        env=cd2.get_status.__globals__
        request=lambda **kw:NS(**kw)
        task=NS(infoHash='a'*40,name='Movie',size=4,status=1)
        stub=NS(ListAllOfflineFiles=Mock(return_value=NS(offlineFiles=[task],pageCount=5)))
        engine=cd2({'cd2_max_pages':1})
        engine._get_grpc_stub=lambda:(stub,[])
        with patch.dict(env,FeederUtil=FeederUtil,CloudDrive_pb2=NS(OfflineFileListAllRequest=request)):
            result,error=engine.get_status()
            self.assertEqual('',error); self.assertEqual(1,len(result))
            self.assertFalse(engine.status_list_complete)
            self.assertEqual(1,stub.ListAllOfflineFiles.call_count)
            stub.ListAllOfflineFiles.return_value.pageCount=1
            engine.get_status(); self.assertTrue(engine.status_list_complete)

    def test_historical_folder_collision_uses_stable_hash_suffix_even_without_files(self):
        previous=self.download(status='completed',title='Movie.2026.1080p')
        previous_id=previous.id
        self.assertEqual('Movie.2026.1080p',DownloadUtil.reserve_output_folder(previous))
        item=self.download(title='Movie.2026.1080p',infohash='b'*40)
        self.assertEqual('Movie.2026.1080p_[bbbbbb]',DownloadUtil.reserve_output_folder(item))
        self.assertEqual('Movie.2026.1080p_[bbbbbb]',DownloadUtil.reserve_output_folder(item))
        self.assertIsNone(db.session.get(ModelDownload,previous_id).local_path)

    def test_same_title_does_not_block_engine_dispatch(self):
        active=self.download(status='downloading',title='same'); active.current_engine_name='q'
        pending=self.download(status='pending',title='same',infohash='b'*40); pending.priority_chain=['q']; item_id=pending.id; db.session.commit()
        engine=NS(get_status=lambda:([],''),SUPPORTED_PROTOCOLS=['magnet'])
        config={'name':'q','engine_type':'qbittorrent','enabled':True}
        with patch.object(FeederUtil,'get_downloaders',return_value=[config]),patch.object(FeederUtil,'get_downloader_by_name',return_value=config),patch.object(DownloadUtil,'create_engine',return_value=engine),patch.object(DownloadUtil,'add_magnet',return_value=(True,'new','')) as add:
            TaskDownload.dispatch_pending_downloads(); add.assert_called_once()
        self.assertEqual('downloading',db.session.get(ModelDownload,item_id).status)

    def test_move_failed_without_source_is_successful_but_keeps_status(self):
        item=self.download(status='move_failed',local_path=str(Path(self.tmp.name)/'absent')); item_id=item.id
        self.assertTrue(UploadUtil.execute_upload(item,self.manager()))
        item=db.session.get(ModelDownload,item_id)
        self.assertEqual('move_failed',item.status)
        self.assertIsNotNone(item.completed_time)
        self.assertTrue(item.as_dict()['successful'])
        self.assertEqual(1,db.session.query(ModelDownload).filter(ModelDownload.success_filter()).count())
        self.assertEqual(0,db.session.query(ModelDownload).filter(ModelDownload.failure_filter()).count())
        self.run.assert_not_called()

    def test_remote_recovery_moves_orphans_to_final_path_and_never_deletes(self):
        item=self.download(status='move_failed',local_path=str(Path(self.tmp.name)/'absent')); item_id=item.id
        item.gdrive_upload_path='incoming/movies';item.gdrive_complete_path='final/movies'
        item.upload_state={'incoming':'sa:{mydrive}/incoming/movies/Movie','shared_incoming':'sa:{drive}/incoming/movies/Movie','dest_parent':'sa:{drive}/final/movies','folder_name':'Movie','remote':'sa','rclone_conf':'/tmp/rclone.conf','impersonate_arg':[]}
        db.session.commit()
        def run(cmd,*args,**kw):
            self.commands.append(list(cmd))
            return (True,'{"Name":"Movie","IsDir":true}') if cmd[1]=='lsjson' else (True,'')
        self.run.side_effect=run
        self.assertTrue(UploadUtil.recover_failed_remote_files(item_id))
        moves=[c for c in self.commands if c[1]=='move']
        self.assertEqual(2,len(moves))
        self.assertTrue(all(c[3]=='sa:{drive}/final/movies/Movie' for c in moves))
        self.assertFalse(any(c[1] in ['purge','delete','cleanup'] for c in self.commands))
        count=len(self.commands)
        self.assertTrue(UploadUtil.recover_failed_remote_files(item_id))
        self.assertEqual(count,len(self.commands))

    def test_remote_recovery_uses_profile_routes_for_untracked_files(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        profile={'destination':{'type':'gdrive_rotation','upload_path':'incoming/movies','complete_path':'final/movies','shared_drive_id':'drive'}}
        def run(cmd, *args, **kwargs):
            self.commands.append(list(cmd))
            return (True, json.dumps([{'Name': 'movie.bin', 'Path': 'incoming/movies/movie.bin', 'IsDir': False}])) if cmd[1] == 'lsjson' else (True, '')
        self.run.side_effect = run
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'mydrive'}]),patch.object(FeederUtil,'get_download_profiles',return_value=[profile]):
            self.assertTrue(UploadUtil.drain_all_sa_mydrives())
        self.assertTrue(any(c[1:4]==['move','sa:{mydrive}/incoming/movies','sa:{drive}/final/movies'] for c in self.commands))
        self.assertFalse(any(c[1] in ['purge','delete','cleanup'] for c in self.commands))

    def test_drain_lists_each_drive_once_across_accounts_and_staging_roots(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        Settings.values['download_gdrive_mydrive_remote_name']='sa'
        Settings.values['download_gdrive_use_impersonate']='True'
        profiles=[{'destination':{'type':'gdrive_rotation','upload_path':f'{"incoming" if i % 2 else "imcoming"}/group{i}',
                                  'complete_path':f'final/group{i}'}} for i in range(13)]
        self.run.return_value=(True,'[]')
        self.run.side_effect=None
        accounts=[{'username':f'account{i}','remote_name':'sa','mydrive_rclone_id':f'my{i}'} for i in range(20)]
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts), patch.object(FeederUtil,'get_download_profiles',return_value=profiles):
            self.assertTrue(UploadUtil.drain_all_sa_mydrives())
        self.assertEqual(21,sum(c.args[0][1]=='lsjson' for c in self.run.call_args_list))
        lists=[c.args[0] for c in self.run.call_args_list if c.args[0][1]=='lsjson']
        self.assertEqual(1,sum(cmd[2]=='sa:{drive}' for cmd in lists))
        self.assertTrue(all('/incoming/**' in cmd and '/imcoming/**' in cmd for cmd in lists))
        self.assertFalse(any(c.args[0][1]=='rmdirs' for c in self.run.call_args_list))
        self.assertTrue(all(c.kwargs['execution_timeout']==60 for c in self.run.call_args_list))

    def test_drain_removes_empty_category_and_task_folders_without_moving(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        self.run.side_effect=None
        self.run.return_value=(True,json.dumps([{'Name':'default','Path':'incoming/default','IsDir':True},
                                                {'Name':'Task','Path':'incoming/default/Task','IsDir':True}]))
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]):
            self.assertTrue(UploadUtil.drain_all_sa_mydrives())
        lists=[c.args[0] for c in self.run.call_args_list if c.args[0][1]=='lsjson']
        self.assertEqual(2,len(lists))
        self.assertTrue(all('--recursive' in cmd and '/incoming/**' in cmd for cmd in lists))
        self.assertEqual(['lsjson','lsjson','rmdirs','rmdirs'],[c.args[0][1] for c in self.run.call_args_list])
        cleanups = [c.args[0] for c in self.run.call_args_list if c.args[0][1] == 'rmdirs']
        self.assertIn('sa:{my}/incoming', [cmd[2] for cmd in cleanups])
        self.assertTrue(all('--leave-root' in cmd for cmd in cleanups if cmd[2].startswith('sa:{drive}/')))

    def test_drain_errors_do_not_count_as_empty(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        accounts=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts):
            for result, expected in [((False,'directory not found'),True),
                                     ((False,'403 permission denied'),False),
                                     ((False,'원격 유지보수 실행 시간 만료'),False),
                                     ((True,'invalid json'),False)]:
                self.run.side_effect=None
                self.run.return_value=result
                self.run.reset_mock()
                self.assertEqual(expected,UploadUtil.drain_all_sa_mydrives())
                self.assertTrue(all(c.args[0][1]=='lsjson' for c in self.run.call_args_list))

    def test_drain_move_failure_blocks_new_uploads(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        accounts=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]
        def run(cmd,*args,**kwargs):
            return (True,'[{"Name":"file.bin","Path":"incoming/default/file.bin","IsDir":false}]') if cmd[1]=='lsjson' else (False,'move failed')
        self.run.side_effect=run
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts):
            self.assertFalse(UploadUtil.drain_all_sa_mydrives())
        self.assertTrue(any(c.args[0][1]=='move' for c in self.run.call_args_list))

    def test_drain_cleans_parent_once_after_all_files_are_moved(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        profiles=[{'destination':{'type':'gdrive_rotation','upload_path':f'incoming/{name}',
                                  'complete_path':f'final/{name}'}} for name in ['movies','tv']]
        def run(cmd,*args,**kwargs):
            self.commands.append(list(cmd))
            return (True,json.dumps([{'Path':f'incoming/{name}/file.bin','Name':'file.bin','IsDir':False}
                                      for name in ['movies','tv']] + [{'Name':'incoming','Path':'incoming','IsDir':True}])) if cmd[1]=='lsjson' else (True,'')
        self.run.side_effect=run
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]), patch.object(FeederUtil,'get_download_profiles',return_value=profiles):
            self.assertTrue(UploadUtil.drain_all_sa_mydrives())
        moves=[i for i,c in enumerate(self.commands) if c[1]=='move']
        cleanups=[i for i,c in enumerate(self.commands) if c[1]=='rmdirs']
        self.assertEqual(4,len(moves))
        self.assertEqual(1,len(cleanups))
        self.assertGreater(min(cleanups),max(moves))
        self.assertEqual({'sa:{my}/incoming'}, {self.commands[i][2] for i in cleanups})
        self.assertTrue(all('--drive-use-trash=false' in self.commands[i] and '--leave-root' not in self.commands[i] for i in cleanups))

    def test_drain_does_not_clean_paths_with_failed_moves_or_unknown_files(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        accounts=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]
        for category, move_ok, expected in [('default',False,False),('unregistered',True,True)]:
            self.run.reset_mock()
            def run(cmd,*args,**kwargs):
                return (True,json.dumps([{'Name':'file.bin','Path':'incoming/'+category+'/file.bin','IsDir':False}])) if cmd[1]=='lsjson' else (move_ok,'move failed')
            self.run.side_effect=run
            with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts):
                self.assertEqual(expected,UploadUtil.drain_all_sa_mydrives())
            self.assertFalse(any(c.args[0][1] in ['rmdirs','purge'] for c in self.run.call_args_list))

    def test_drain_cleanup_failure_blocks_upload_but_absent_parent_is_ok(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        accounts=[{'username':'account','remote_name':'sa','mydrive_rclone_id':'my'}]
        for error, expected in [('403 permission denied',False),('directory not found',True)]:
            self.run.side_effect=lambda cmd,*args,**kwargs: (True,'[{"Name":"incoming","Path":"incoming","IsDir":true}]') if cmd[1]=='lsjson' else (False,error)
            with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts):
                self.assertEqual(expected,UploadUtil.drain_all_sa_mydrives())

    def test_drain_protects_shared_incoming_even_when_using_another_account(self):
        Settings.values['download_rclone_conf_path']='/tmp/rclone.conf'
        Settings.values['download_gdrive_mydrive_remote_name']='sa'
        Settings.values['download_gdrive_use_impersonate']='True'
        item=self.download()
        item.gdrive_account='protected'
        item.upload_state={'shared_incoming':'sa:{drive}/incoming/default/Task'}
        db.session.commit()
        accounts=[{'username':'protected','mydrive_rclone_id':'protected-id'},
                  {'username':'idle','mydrive_rclone_id':'idle-id'}]
        self.run.side_effect=None
        self.run.return_value=(True,json.dumps([{'Name':'incoming','Path':'incoming','IsDir':True},
                                                {'Name':'file.bin','Path':'incoming/default/Task/file.bin','IsDir':False}]))
        with patch.object(FeederUtil,'get_gdrive_accounts',return_value=accounts):
            self.assertTrue(UploadUtil.drain_all_sa_mydrives())
        cmds=[c.args[0] for c in self.run.call_args_list]
        self.assertEqual(1,sum(c[1]=='lsjson' and c[2]=='sa:{drive}' for c in cmds))
        self.assertFalse(any(c[1] in ['move','rmdirs'] and c[2].startswith('sa:{drive}') for c in cmds))
        self.assertFalse(any('protected-id' in c[2] for c in cmds))

    def test_pipeline_requires_recovery_before_new_uploads(self):
        self.download()
        events=[]
        names=['sync_feed_items','sync_active_items_with_profiles','dispatch_pending_downloads',
               'poll_active_downloads','process_local_staging','route_completed_downloads',
               'process_uploads','retry_move_failed','check_pending_relays']
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(FeederUtil,'get_download_profiles',return_value=[{}]))
            for name in names:
                stack.enter_context(patch.object(TaskDownload,name,side_effect=lambda *args,n=name:events.append(n)))
            recover=stack.enter_context(patch.object(UploadUtil,'drain_all_sa_mydrives',side_effect=lambda:events.append('recover') or False))
            TaskDownload._run_pipeline_locked()
            self.assertEqual(['recover'],events)
            events.clear()
            recover.side_effect=lambda:events.append('recover') or True
            TaskDownload._run_pipeline_locked()
            self.assertEqual('recover',events[0])
            self.assertIn('process_uploads',events)

    def test_missing_transporter_waits_and_resumes_after_configuration_recovers(self):
        source=self.source(); item=self.download(status='downloaded',local_path=source); item_id=item.id
        with patch.object(DownloadUtil,'get_transporter',return_value=None):
            TaskDownload.route_completed_downloads()
        item=db.session.get(ModelDownload,item_id)
        self.assertEqual('waiting_transporter',item.status)
        self.assertIsNone(item.completed_time)
        transporter=NS(transport=Mock(return_value=(True,'completed','moved')))
        with patch.object(DownloadUtil,'get_transporter',return_value=transporter):
            TaskDownload.route_completed_downloads()
        self.assertEqual('completed',db.session.get(ModelDownload,item_id).status)
        self.assertTrue(os.path.exists(source))

    def test_waiting_transporter_can_be_manually_failed_without_deleting_source(self):
        source=self.source(); item=self.download(status='waiting_transporter',local_path=source); item_id=item.id
        module=ModuleDownload.__new__(ModuleDownload);module.name='download'
        req=NS(form={'command':'item_action','id':str(item_id),'action':'force_fail'})
        with patch.object(UploadUtil,'kill_item_rclone'):
            self.assertEqual('success',module.process_ajax('item_action',req)['ret'])
        self.assertEqual('failed',db.session.get(ModelDownload,item_id).status)
        self.assertTrue(os.path.exists(source))

    def test_local_transport_uses_hash_name_from_history_with_no_existing_destination(self):
        COMMON['BaseTransporter']=load_class('util_download.py','BaseTransporter')
        transporter_cls=load_class('util_download.py','LocalTransporter')
        transporter_cls.transport.__globals__['DownloadUtil']=DownloadUtil
        previous=self.download(status='completed',title='Movie',infohash='a'*40)
        DownloadUtil.reserve_output_folder(previous)
        source=self.source(); item=self.download(status='downloaded',title='Movie',infohash='b'*40,local_path=source)
        output=DownloadUtil.reserve_output_folder(item)
        target=str(Path(self.tmp.name)/'final')
        self.assertEqual((True,'completed'),transporter_cls().transport(item,source,{'complete_path':target})[:2])
        self.assertEqual('Movie_[bbbbbb]',output)
        self.assertTrue((Path(target)/output/'movie.mkv').exists())

    def test_remote_missing_timeout_zero_disables_failure(self):
        item=self.download(status='downloading'); item.current_engine_name='e'; item.remote_missing_since=datetime.datetime.now()-datetime.timedelta(days=10); item_id=item.id;db.session.commit()
        engine=NS(get_status=lambda:([],''),status_list_complete=True)
        with patch.object(FeederUtil,'get_downloaders',return_value=[{'name':'e','missing_timeout_hours':0}]),patch.object(DownloadUtil,'create_engine',return_value=engine):
            TaskDownload.poll_active_downloads()
        item=db.session.get(ModelDownload,item_id)
        self.assertEqual('downloading',item.status)
        self.assertIsNone(item.remote_missing_since)

    def test_relay_claim_uses_distinct_source_and_output_folder_names(self):
        old=self.download(status='completed',title='Movie',infohash='a'*40)
        DownloadUtil.reserve_output_folder(old)
        item=self.download(status='pending_relay',title='Movie',infohash='b'*40,local_path='ad:Movie'); item_id=item.id
        module=ModuleDownload.__new__(ModuleDownload);module.name='download';module._verify_api_auth=lambda req:True
        result=module._handle_relay_claim(NS())
        self.assertTrue(result['has_task'])
        self.assertEqual('ad:Movie',result['item']['remote_source_path'])
        self.assertTrue(result['item']['dest_path'].endswith('/Movie_[bbbbbb]'))
        self.assertEqual('Movie_[bbbbbb]',db.session.get(ModelDownload,item_id).output_folder_name)

    def test_waiting_transporter_can_be_completed_or_deleted(self):
        module=ModuleDownload.__new__(ModuleDownload);module.name='download'
        for action,expected in [('force_complete','completed'),('delete','cancelled')]:
            source=str(Path(self.tmp.name)/action);Path(source).mkdir()
            item=self.download(status='waiting_transporter',local_path=source,title=action);item_id=item.id
            req=NS(form={'command':'item_action','id':str(item_id),'action':action})
            with patch.object(UploadUtil,'kill_item_rclone'),patch.object(DownloadUtil,'delete_remote_tasks'):
                self.assertEqual('success',module.process_ajax('item_action',req)['ret'])
            self.assertEqual(expected,db.session.get(ModelDownload,item_id).status)

    def test_manual_failure_during_transport_is_not_overwritten(self):
        source=self.source(); item=self.download(status='waiting_transporter',local_path=source); item_id=item.id
        def transport(*args):
            db.session.query(ModelDownload).filter_by(id=item_id).update({'status':'failed','error_message':'manual'})
            db.session.commit()
            return True,'completed','moved'
        with patch.object(DownloadUtil,'get_transporter',return_value=NS(transport=transport)):
            TaskDownload.route_completed_downloads()
        self.assertEqual('failed',db.session.get(ModelDownload,item_id).status)

    def test_all_skipped_feed_rows_still_yield_without_changing_results(self):
        for index in range(60):
            item=ModelCrawlItem('site','board'); item.title='no links'; item.url='https://site/'+str(index)
            db.session.add(item)
        db.session.commit()
        token=Runtime._LAST_YIELD.set(None)
        try:
            with patch.object(Runtime,'pause') as pause, patch.object(Runtime.time,'monotonic',side_effect=itertools.count(10,0.003)):
                count=FeedUtil.sync_feed({'name':'f','sources':[{'site':'site','board':'board'}]})
            self.assertEqual(0,count)
            self.assertEqual(0,db.session.query(ModelFeedItem).count())
            self.assertGreater(pause.call_count,1)
            self.assertLess(pause.call_count,60)
        finally:
            Runtime._LAST_YIELD.reset(token)

    def test_schema_upgrade_is_additive_and_repeatable(self):
        engine=sa.create_engine('sqlite://')
        self.addCleanup(engine.dispose)
        with engine.begin() as conn:
            conn.execute(sa.text('CREATE TABLE feeder_test_download (id INTEGER PRIMARY KEY)'))
            conn.execute(sa.text('INSERT INTO feeder_test_download VALUES (7)'))
        with patch.object(db,'get_engine',return_value=engine):
            FeederUtil.migrate_db(); FeederUtil.migrate_db()
        self.assertTrue({'upload_state','source_file_name','output_folder_name','remote_missing_since'} <= {c['name'] for c in sa.inspect(engine).get_columns('feeder_test_download')})
        with engine.connect() as conn:
            self.assertEqual(7,conn.execute(sa.text('SELECT id FROM feeder_test_download')).scalar())


class RcloneExecutionTimeoutTests(unittest.TestCase):
    def test_silent_process_is_killed_on_execution_deadline(self):
        started=time.monotonic()
        success,out=UploadUtil.run_rclone([sys.executable,'-c','import time; time.sleep(10)'],
                                           log_output=False,execution_timeout=0.1)
        self.assertFalse(success)
        self.assertIn('실행 시간 만료',out)
        self.assertLess(time.monotonic()-started,3)

    def test_successful_command_cancels_deadline(self):
        success,out=UploadUtil.run_rclone([sys.executable,'-c','print("[]")'],
                                           log_output=False,execution_timeout=3)
        self.assertTrue(success)
        self.assertEqual('[]',out)


class RuntimeCooperationTests(unittest.TestCase):
    def setUp(self):
        token = Runtime._LAST_YIELD.set(None)
        self.addCleanup(Runtime._LAST_YIELD.reset, token)

    def test_hot_checkpoints_are_throttled_and_use_positive_delay(self):
        with patch.object(Runtime, 'pause') as pause, patch.object(Runtime.time, 'monotonic', side_effect=[10,10,10.01,10.021,10.021]):
            Runtime.cooperate()
            Runtime.cooperate()
            Runtime.cooperate()
        self.assertEqual(2, pause.call_count)
        self.assertGreater(Runtime._YIELD_DELAY, 0)

    def test_independent_contexts_do_not_suppress_each_others_yields(self):
        first = contextvars.Context()
        second = contextvars.Context()
        with patch.object(Runtime, 'pause') as pause, patch.object(Runtime.time, 'monotonic', return_value=10):
            first.run(Runtime.cooperate)
            second.run(Runtime.cooperate)
            first.run(Runtime.cooperate)
        self.assertEqual(2, pause.call_count)
        self.assertIsNone(Runtime._LAST_YIELD.get())

    def test_missing_gevent_uses_native_sleep(self):
        with patch.dict(sys.modules, {'gevent': None}), patch.object(Runtime.time, 'sleep') as sleep:
            Runtime.pause()
        sleep.assert_called_once_with(0.001)

    def test_installed_but_inactive_gevent_does_not_change_native_worker(self):
        gevent = NS(monkey=NS(is_module_patched=lambda name: False), sleep=Mock())
        with patch.dict(sys.modules, {'gevent': gevent}), patch.object(Runtime.time, 'sleep') as sleep:
            Runtime.pause()
        sleep.assert_called_once_with(0.001)
        gevent.sleep.assert_not_called()

    def test_active_gevent_yields_to_hub_for_socket_or_time_patching(self):
        for patched_module in ['socket', 'time']:
            with self.subTest(patched_module=patched_module):
                gevent = NS(monkey=NS(is_module_patched=lambda name: name == patched_module), sleep=Mock())
                with patch.dict(sys.modules, {'gevent': gevent}), patch.object(Runtime.time, 'sleep') as sleep:
                    Runtime.pause()
                gevent.sleep.assert_called_once_with(0.001)
                sleep.assert_not_called()

    def test_directory_scan_preserves_size_and_does_not_follow_directory_symlinks(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            (path/'a').write_bytes(b'123')
            (path/'sub').mkdir()
            (path/'sub'/'b').write_bytes(b'12345')
            (path/'link').symlink_to(path/'sub', target_is_directory=True)
            self.assertEqual(8, FeederUtil.directory_size(root))


if __name__=='__main__':
    unittest.main()
