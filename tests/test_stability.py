"""운영 서비스를 사용하지 않는 2026-10-22 안정화 회귀 검증."""
import contextlib
import datetime
import types
import io
import json
import threading
import base64
import zlib
import hashlib
import unittest
from unittest.mock import Mock, patch

import test_regressions as h


class SourceGraceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = h.Regressions()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.cfg = {'name': 'ad', 'engine_type': 'alldebrid', 'webdav_failure_timeout_hours': 6}
        p = patch.object(h.FeederUtil, 'get_downloader_by_name', return_value=self.cfg)
        p.start(); self.addCleanup(p.stop)

    def item(self):
        item = self.fixture.download()
        item.current_engine_name = 'ad'; item.engine_task_id = 'old-ad'
        item.engine_added_time = datetime.datetime.now() - datetime.timedelta(hours=40)
        item.priority_chain = ['ad', 'next']; item.local_path = 'ad:source'
        h.db.session.commit()
        return item

    def test_registration_age_is_not_webdav_age(self):
        item = self.item(); item.source_unavailable_since = datetime.datetime.now()
        item.transfer_error_type = 'SOURCE_UNAVAILABLE'; h.db.session.commit()
        self.assertFalse(h.UploadUtil.expire_source_failure(item, confirmed=True))
        self.assertEqual('pending_upload', item.status)

    def test_old_source_failure_without_new_confirmation_does_not_expire(self):
        item = self.item(); item.source_unavailable_since = datetime.datetime.now() - datetime.timedelta(hours=7)
        item.transfer_error_type = 'SOURCE_UNAVAILABLE'; h.db.session.commit()
        self.assertFalse(h.UploadUtil.expire_source_failure(item))

    def test_destination_error_preserves_first_source_failure(self):
        item = self.item(); first = datetime.datetime.now() - datetime.timedelta(hours=2)
        item.source_unavailable_since = first; h.db.session.commit(); item_id = item.id
        h.UploadUtil.transfer_failed(item_id, 'googleapi: Error 502 Internal Error')
        self.assertEqual(first, h.db.session.get(h.ModelDownload, item_id).source_unavailable_since)

    def test_ad_only_schema_is_injected_without_editing_engine(self):
        engine = types.SimpleNamespace(CONFIG_SCHEMA=[], ENGINE_NAME='AD')
        with patch.object(h.DownloadUtil, '_engine_classes', {'alldebrid': engine, 'other': engine}):
            schemas = h.DownloadUtil.get_engine_schemas()
        self.assertTrue(any(f['name'] == 'webdav_failure_timeout_hours' for f in schemas[0]['config_schema']))
        self.assertFalse(any(f['name'] == 'webdav_failure_timeout_hours' for f in schemas[1]['config_schema']))

    def expired(self):
        item = self.item()
        item.source_unavailable_since = datetime.datetime.now() - datetime.timedelta(hours=7)
        item.transfer_error_type = 'SOURCE_UNAVAILABLE'
        h.db.session.commit()
        return item

    def test_confirmed_expiry_advances_only_after_verified_deletion(self):
        item = self.expired()
        engine = types.SimpleNamespace(delete_task=Mock(return_value=True))
        with patch.object(h.DownloadUtil, 'create_engine', return_value=engine):
            self.assertTrue(h.UploadUtil.expire_source_failure(item, confirmed=True))
        engine.delete_task.assert_called_once_with('old-ad')
        self.assertEqual('pending', item.status)
        self.assertEqual(1, item.current_engine_index)
        self.assertIsNone(item.source_unavailable_since)
        self.assertIsNone(item.engine_task_id)

    def test_last_engine_expiry_is_final_failure(self):
        item = self.expired(); item.priority_chain = ['ad']; h.db.session.commit()
        with patch.object(h.DownloadUtil, 'create_engine', return_value=types.SimpleNamespace(delete_task=lambda _: True)):
            self.assertTrue(h.UploadUtil.expire_source_failure(item, confirmed=True))
        self.assertEqual('failed', item.status)

    def test_delete_false_or_exception_preserves_original_id_and_time(self):
        for error in (False, RuntimeError('delete unavailable')):
            item = self.expired(); first = item.source_unavailable_since
            delete = Mock(side_effect=error) if isinstance(error, Exception) else Mock(return_value=error)
            with patch.object(h.DownloadUtil, 'create_engine', return_value=types.SimpleNamespace(delete_task=delete)):
                self.assertFalse(h.UploadUtil.expire_source_failure(item, confirmed=True))
            self.assertEqual('old-ad', item.engine_task_id)
            self.assertEqual(first, item.source_unavailable_since)
            self.assertEqual(0, item.current_engine_index)

    def test_zero_unlimited_missing_default_and_invalid_legacy_value(self):
        item = self.expired()
        for value in (0, 24, -1, 'bad', '1.5', 100000):
            self.cfg['webdav_failure_timeout_hours'] = value
            self.assertFalse(h.UploadUtil.expire_source_failure(item, confirmed=True))
        self.cfg.pop('webdav_failure_timeout_hours')
        self.assertFalse(h.UploadUtil.expire_source_failure(item, confirmed=True))

    def test_unsettled_job_usage_and_cleanup_block_expiry(self):
        item = self.expired()
        for state in ({'worker': {'settled': False}}, {'cleanup_pending': True}, {'usage': {'settled': False}}):
            item.upload_state = state; h.db.session.commit()
            self.assertFalse(h.UploadUtil.expire_source_failure(item, confirmed=True))
            self.assertEqual('old-ad', item.engine_task_id)

    def test_new_source_failure_preserves_first_for_both_staging_and_direct(self):
        for retry in ('pending_upload', 'pending_local_staging'):
            item = self.item(); first = datetime.datetime.now() - datetime.timedelta(hours=2)
            item.source_unavailable_since = first; h.db.session.commit(); item_id = item.id
            h.UploadUtil.transfer_failed(item_id, 'failed to open source object: 404 Not Found', retry_status=retry)
            current = h.db.session.get(h.ModelDownload, item_id)
            self.assertEqual(first, current.source_unavailable_since)
            self.assertEqual(retry, current.status)

    def test_network_or_mismatched_worker_code_does_not_start_source_timer(self):
        for text, code in [('timeout EOF connection reset', None),
                           ('failed to open source object: unexpected EOF', None),
                           ('failed to open source object: connection reset', 'SOURCE_UNAVAILABLE'),
                           ('failed to open source object: 404 Not Found', 'DRIVE_SERVER_ERROR')]:
            item = self.item(); item_id = item.id
            h.UploadUtil.transfer_failed(item_id, text, structured_type=code)
            self.assertIsNone(h.db.session.get(h.ModelDownload, item_id).source_unavailable_since)

    def test_downloader_validation_and_setting_round_trip(self):
        data = {'DOWNLOADERS': [{'name': 'ad', 'engine_type': 'alldebrid', 'existing': 'preserved'}]}
        with patch.object(h.FeederUtil, 'load_yaml', return_value=data), patch.object(h.FeederUtil, 'save_yaml') as save:
            for value in (-1, '1.5', True, 8761, 'bad', ''):
                self.assertNotIn(h.FeederUtil.save_downloader(dict(self.cfg, webdav_failure_timeout_hours=value)), ('success', 'success_update'))
            save.assert_not_called()
            self.assertEqual('success_update', h.FeederUtil.save_downloader(dict(self.cfg, webdav_failure_timeout_hours='0')))
            self.assertEqual(0, save.call_args.args[0]['DOWNLOADERS'][0]['webdav_failure_timeout_hours'])
            self.assertEqual('preserved', save.call_args.args[0]['DOWNLOADERS'][0]['existing'])

    def test_expired_failure_is_confirmed_through_real_failure_handler(self):
        item = self.expired(); item_id = item.id
        with patch.object(h.DownloadUtil, 'create_engine', return_value=types.SimpleNamespace(delete_task=lambda _: True)):
            h.UploadUtil.transfer_failed(item_id, 'failed to open source object: 404 Not Found')
        current = h.db.session.get(h.ModelDownload, item_id)
        self.assertEqual('pending', current.status)
        self.assertIsNone(current.source_unavailable_since)

    def test_explicit_manual_new_attempt_clears_source_time(self):
        item = self.expired(); item_id = item.id
        module = h.ModuleDownload.__new__(h.ModuleDownload); module.name = 'download'
        req = types.SimpleNamespace(form={'command': 'item_action', 'id': str(item_id), 'action': 'retry'})
        with patch.object(h.FeederUtil, 'get_download_profiles', return_value=[]):
            result = module.process_ajax('item_action', req)
        self.assertEqual('success', result['ret'])
        self.assertIsNone(h.db.session.get(h.ModelDownload, item_id).source_unavailable_since)

    def test_verified_direct_copy_clears_source_timer_even_if_final_move_fails(self):
        item = self.item(); item.local_path = self.fixture.source(); item_id = item.id
        item.source_unavailable_since = datetime.datetime.now() - datetime.timedelta(hours=2)
        h.db.session.commit()
        original = self.fixture.rclone
        def run(command, *args, **kwargs):
            if command[1] == 'move': return False, 'temporary move failure'
            return original(command, *args, **kwargs)
        self.fixture.run.side_effect = run
        self.assertFalse(h.UploadUtil.execute_upload(item, self.fixture.manager()))
        current = h.db.session.get(h.ModelDownload, item_id)
        self.assertIsNone(current.source_unavailable_since)
        self.assertEqual('uploaded', current.upload_state['phase'])

    def test_verified_local_staging_clears_source_timer_and_keeps_unremoved_ad_id(self):
        item = self.item(); item_id = item.id
        item.status = 'pending_local_staging'; item.source_unavailable_since = datetime.datetime.now() - datetime.timedelta(hours=2)
        h.db.session.commit()
        h.Settings.set('download_local_staging_path', str(h.Path(self.fixture.tmp.name) / 'staging'))
        def workers(worker, ids, count, **kwargs):
            for item_id in ids:
                worker(item_id)
                yield item_id, types.SimpleNamespace(result=lambda: None)
        with patch.object(h.TaskDownload, '_worker_results', side_effect=workers), patch.object(h.DownloadUtil, 'create_engine', return_value=types.SimpleNamespace(delete_task=Mock(return_value=False))):
            h.TaskDownload.process_local_staging()
        current = h.db.session.get(h.ModelDownload, item_id)
        self.assertEqual('downloaded', current.status)
        self.assertIsNone(current.source_unavailable_since)
        self.assertEqual('old-ad', current.engine_task_id)

    def test_other_engine_keeps_legacy_source_expiry_policy(self):
        item = self.expired()
        self.cfg.update(engine_type='other', stalled_timeout_hours=6)
        with patch.object(h.DownloadUtil, 'create_engine', return_value=types.SimpleNamespace(delete_task=lambda _: True)):
            self.assertTrue(h.UploadUtil.expire_source_failure(item))
        self.assertEqual('pending', item.status)

    def test_non_ad_delete_false_none_exception_keeps_legacy_fallback(self):
        self.cfg.update(engine_type='other', stalled_timeout_hours=6)
        for result in (False, None, RuntimeError('delete unavailable'), 'no-engine'):
            with self.subTest(result=type(result).__name__):
                item = self.expired(); item_id = item.id
                delete = Mock(side_effect=result) if isinstance(result, Exception) else Mock(return_value=result)
                engine = None if result == 'no-engine' else types.SimpleNamespace(delete_task=delete)
                with patch.object(h.DownloadUtil, 'create_engine', return_value=engine):
                    self.assertTrue(h.UploadUtil.expire_source_failure(item))
                h.db.session.remove()
                current = h.db.session.get(h.ModelDownload, item_id)
                self.assertEqual('pending', current.status)
                self.assertEqual(1, current.current_engine_index)
                self.assertIsNone(current.engine_task_id)
                self.assertIsNone(current.source_unavailable_since)

    def test_non_ad_cleanup_and_unsettled_guards_still_block_fallback(self):
        self.cfg.update(engine_type='other', stalled_timeout_hours=6)
        item = self.expired(); item_id = item.id
        for state in ({'cleanup_pending': True}, {'worker': {'settled': False}}, {'usage': {'settled': False}}):
            item.upload_state = state; h.db.session.commit()
            with patch.object(h.DownloadUtil, 'create_engine') as create:
                self.assertFalse(h.UploadUtil.expire_source_failure(item))
            create.assert_not_called()
            h.db.session.remove(); item = h.db.session.get(h.ModelDownload, item_id)
            self.assertEqual('old-ad', item.engine_task_id)
            self.assertEqual(0, item.current_engine_index)

    def test_ad_delete_commit_failure_keeps_id_and_defers_without_absence_proof(self):
        item = self.expired(); item_id = item.id; first = item.source_unavailable_since
        state = {'phase': 'copying'}
        item.upload_state = state; h.db.session.commit()
        engine = types.SimpleNamespace(delete_task=Mock(side_effect=[True, False]))
        def failed_commit():
            h.db.session.flush()
            raise h.sa.exc.SQLAlchemyError('simulated commit failure')
        with patch.object(h.DownloadUtil, 'create_engine', return_value=engine):
            with patch.object(h.db.session, 'commit', side_effect=failed_commit):
                with self.assertRaises(h.sa.exc.SQLAlchemyError):
                    h.UploadUtil.expire_source_failure(item, confirmed=True)
            h.db.session.rollback(); h.db.session.remove()
            current = h.db.session.get(h.ModelDownload, item_id)
            self.assertFalse(h.UploadUtil.expire_source_failure(current, confirmed=True))
        self.assertEqual(2, engine.delete_task.call_count)
        h.db.session.remove(); current = h.db.session.get(h.ModelDownload, item_id)
        self.assertEqual('old-ad', current.engine_task_id)
        self.assertEqual(first, current.source_unavailable_since)
        self.assertEqual(state, current.upload_state)
        self.assertEqual(0, current.current_engine_index)
        self.assertEqual('pending_upload', current.status)


class CookieTests(unittest.TestCase):
    def test_underscore_cookie_and_path_are_preserved(self):
        cookies = h.CrawlUtil.normalize_cookies([{'name': '_login', 'value': 'value',
            'domain': 'example.test', 'path': '/private'}], 'https://example.test/private/page')
        self.assertEqual('/private', cookies[0]['path'])
        self.assertEqual('_login', cookies[0]['name'])

    def test_other_domain_and_invalid_path_are_filtered(self):
        cookies = h.CrawlUtil.normalize_cookies([{'name': 'a', 'value': 'b', 'domain': 'other.test'},
            {'name': 'x', 'value': 'y', 'path': 'bad'}], 'https://example.test/private/page')
        self.assertEqual([], cookies)

    def test_same_clearance_never_extends_original_lifetime(self):
        c = h.CrawlUtil
        with c.execution_scope('cookie-expiry'), patch.object(h.time, 'time', return_value=1000):
            sol = {'cookies': [{'name': 'cf_clearance', 'value': 'a', 'path': '/', 'expires': 1100}]}
            c._store_solution('example.test', sol, 'https://example.test/')
            with patch.object(h.time, 'time', return_value=1090):
                c._store_solution('example.test', sol, 'https://example.test/')
            with patch.object(h.time, 'time', return_value=1101):
                self.assertFalse(c._has_valid_clearance('example.test'))
                c._store_solution('example.test', {'cookies': [{'name': '_login', 'value': 'b'}]}, 'https://example.test/')
                self.assertFalse(c._has_valid_clearance('example.test'))
            c.finish_execution()

    def test_cookie_path_secure_and_cross_host_attachment_isolation(self):
        c = h.CrawlUtil
        with c.execution_scope('cookie-path'):
            c._store_solution('example.test', {'cookies': [{'name': '_login', 'value': 'a', 'path': '/private', 'secure': True}]}, 'https://example.test/private/page')
            self.assertEqual(1, len(c._cached_cookies('example.test', 'https://example.test/private/file')))
            for url in ('https://other.test/private/file', 'https://example.test/public', 'http://example.test/private/file'):
                self.assertEqual([], c._cached_cookies('example.test', url))
            c.finish_execution()

    def test_http_cookies_preserve_login_while_explicit_value_overrides_auto(self):
        import requests
        c = h.CrawlUtil
        session = requests.Session()
        self.addCleanup(session.close)
        session.cookies.set('session_login', 'kept', domain='example.test', path='/private')
        with c.execution_scope('cookie-header'):
            c._store_solution('example.test', {'cookies': [{'name': '_login', 'value': 'auto', 'path': '/'}]}, 'https://example.test/private/page')
            value = c._http_cookie_header('example.test', 'https://example.test/private/page', '_login=explicit', session)
            self.assertIn('session_login=kept', value)
            self.assertIn('_login=explicit', value)
            self.assertNotIn('_login=auto', value)
            c.finish_execution()


class FakeFlareSolverr:
    def __init__(self):
        self.sessions = {'feeder_example_test': ''}
        self.calls = []
        self.failures = []

    def post(self, url, json=None, **kwargs):
        self.calls.append(json.copy())
        command = json['cmd']
        if command == 'sessions.list':
            data = {'status': 'ok', 'sessions': list(self.sessions)}
        elif command == 'sessions.create':
            name = json['session']
            exists = name in self.sessions
            self.sessions[name] = json.get('proxy', {}).get('url', '')
            data = {'status': 'ok', 'session': name, 'message': 'Session already exists.' if exists else 'created'}
        elif command == 'sessions.destroy':
            self.sessions.pop(json['session'], None)
            data = {'status': 'ok'}
        else:
            if self.failures:
                failure = self.failures.pop(0)
                if isinstance(failure, Exception):
                    raise failure
                data = failure if isinstance(failure, dict) else {'status': 'error', 'message': failure}
            else:
                data = {'status': 'ok', 'message': 'Challenge not detected!', 'solution': {
                    'status': 200, 'response': '<html>OK</html>', 'cookies': [], 'userAgent': 'test-agent'}}
        return types.SimpleNamespace(status_code=200, json=lambda: data)


class CrawlSessionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = h.Regressions(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        h.Settings.set('crawl_flaresolverr_url', 'http://fs.invalid')
        p = patch.object(h.CrawlUtil, '_proxy_index', 0); p.start(); self.addCleanup(p.stop)
        self.fake = FakeFlareSolverr()
        self.env = h.CrawlUtil.get_html.__func__.__globals__
        p = patch.dict(self.env, requests=types.SimpleNamespace(post=self.fake.post, Session=Mock()))
        p.start(); self.addCleanup(p.stop)
        self.scope = h.CrawlUtil.execution_scope(h.uuid.uuid4().hex)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.addCleanup(h.CrawlUtil.finish_execution)

    def request(self, proxies=None):
        return h.CrawlUtil.get_by_flaresolverr('https://example.test/page', proxies=proxies)

    def crawl(self, retries=2, proxies='http://one.invalid,http://two.invalid'):
        scheduler=types.SimpleNamespace(use_proxy=bool(proxies),proxy_url=proxies,use_flaresolverr=True,use_selenium=False)
        return h.CrawlUtil.get_html('https://example.test/page?private=hidden',scheduler_instance=scheduler,max_retries=retries,retry_interval=.5)

    def test_fs_failure_rotates_without_unauthenticated_http(self):
        self.fake.failures=['API unavailable']*2
        direct=Mock(); direct.headers={}; direct.cookies=[]; direct.get.return_value=types.SimpleNamespace(status_code=403,text='blocked')
        with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct),requests=types.SimpleNamespace(post=self.fake.post,Session=lambda:direct)):
            self.assertIsNone(self.crawl())
        direct.get.assert_not_called()
        requests=[x for x in self.fake.calls if x['cmd']=='request.get']
        self.assertEqual(['http://one.invalid','http://two.invalid'],[x['proxy']['url'] for x in requests])

    def test_fs_second_proxy_success_then_cffi_fast_path(self):
        self.fake.failures=['API unavailable']
        direct=Mock(); direct.headers={}; direct.cookies=[]; direct.get.return_value=types.SimpleNamespace(status_code=200,text='<html>fast</html>')
        with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct)):
            self.assertEqual('<html>OK</html>',self.crawl())
            direct.get.assert_not_called()
            self.assertEqual('<html>fast</html>',self.crawl())
        self.assertEqual(2,len([x for x in self.fake.calls if x['cmd']=='request.get']))
        self.assertEqual('test-agent',direct.get.call_args.kwargs['headers']['User-Agent'])

    def test_non_fs_keeps_cffi_then_requests_fallback(self):
        curl=Mock(headers={},cookies=[]); curl.get.return_value=types.SimpleNamespace(status_code=403,text='blocked')
        direct=Mock(headers={},cookies=[]); direct.get.return_value=types.SimpleNamespace(status_code=200,text='<html>requests</html>')
        scheduler=types.SimpleNamespace(use_proxy=False,proxy_url='',use_flaresolverr=False,use_selenium=False)
        with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:curl),requests=types.SimpleNamespace(post=Mock(side_effect=AssertionError('FS 호출')),Session=lambda:direct)):
            self.assertEqual('<html>requests</html>',h.CrawlUtil.get_html('https://example.test/page',scheduler_instance=scheduler,max_retries=1))
        self.assertEqual(1,curl.get.call_count); self.assertEqual(1,direct.get.call_count)

    def test_authenticated_http_block_rotates_to_fs_before_any_new_http(self):
        curl=Mock(headers={},cookies=[]); curl.get.return_value=types.SimpleNamespace(status_code=403,text='blocked')
        direct=Mock()
        with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:curl),requests=types.SimpleNamespace(post=self.fake.post,Session=lambda:direct)):
            self.assertTrue(self.crawl()); self.fake.failures=['API unavailable']
            self.assertIsNone(self.crawl())
        self.assertEqual(1,curl.get.call_count); direct.get.assert_not_called()
        calls=[x for x in self.fake.calls if x['cmd']=='request.get']
        self.assertEqual(['http://one.invalid','http://two.invalid'],[x['proxy']['url'] for x in calls])
        self.assertFalse(h.CrawlUtil._http_ready)

    def test_destroy_timeout_preserves_old_ownership_and_new_proxy_works(self):
        c=h.CrawlUtil; self.request({'http':'http://one.invalid'}); old=next(iter(c._fs_sessions.values()))['id']
        real=self.fake.post
        def post(url,json=None,**kwargs):
            if json['cmd']=='sessions.destroy' and json['session']==old: raise TimeoutError('destroy timeout')
            return real(url,json=json,**kwargs)
        with patch.dict(self.env,requests=types.SimpleNamespace(post=post)):
            self.assertTrue(self.request({'http':'http://two.invalid'})[1])
            new=next(iter(c._fs_sessions.values()))['id']; self.assertNotEqual(old,new)
            self.assertTrue(any(key[2]==old for key in c._fs_retired)); self.assertEqual(2,len(c._fs_owned))
            c.close_flaresolverr_sessions()
            self.assertEqual({old},{key[2] for key in c._fs_owned}); self.assertIn('feeder_example_test',self.fake.sessions)
        c.close_flaresolverr_sessions(); self.assertFalse(c._fs_owned)

    def test_request_and_create_timeout_allow_next_proxy_without_http(self):
        c=h.CrawlUtil
        for command in ['sessions.create','request.get']:
            with self.subTest(command=command):
                c.close_flaresolverr_sessions(); self.fake.calls.clear(); c._proxy_index=0
                real=self.fake.post; failed=[]
                def post(url,json=None,**kwargs):
                    if json['cmd']==command and not failed:
                        failed.append(json['session']); raise TimeoutError('timeout')
                    return real(url,json=json,**kwargs)
                direct=Mock()
                with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct),requests=types.SimpleNamespace(post=post,Session=lambda:direct)):
                    self.assertEqual('<html>OK</html>',self.crawl())
                direct.get.assert_not_called()
                current=next(iter(c._fs_sessions.values()))['id']; self.assertNotEqual(failed[0],current)
                self.assertTrue(any(x['cmd']=='request.get' and x['session']==current for x in self.fake.calls))

    def test_fs_down_list_failure_and_single_connection_obey_retry_budget(self):
        c=h.CrawlUtil
        for proxies in ['', 'http://one.invalid', 'http://one.invalid,http://two.invalid']:
            with self.subTest(proxies=bool(proxies)):
                post=Mock(side_effect=TimeoutError('token=hidden http://secret.invalid/?private=hidden'))
                direct=Mock()
                with patch.dict(self.env,requests=types.SimpleNamespace(post=post,Session=lambda:direct)), self.assertLogs('feeder-tests',level='WARNING') as logs:
                    self.assertIsNone(self.crawl(retries=2,proxies=proxies))
                self.assertEqual(2,post.call_count); direct.get.assert_not_called()
                self.assertNotIn('secret.invalid',' '.join(logs.output)); self.assertNotIn('token=hidden',' '.join(logs.output))

    def test_direct_clearance_rotates_after_timeout_and_keeps_return_contract(self):
        self.fake.failures=[TimeoutError('request timeout')]
        scheduler=types.SimpleNamespace(use_proxy=True,proxy_url='http://one.invalid,http://two.invalid')
        cookies,ua,ok=h.CrawlUtil._get_flaresolverr_clearance_result('https://example.test/page',scheduler,max_retries=2)
        self.assertTrue(ok); self.assertEqual([],cookies); self.assertEqual('test-agent',ua)
        calls=[x for x in self.fake.calls if x['cmd']=='request.get']
        self.assertEqual(2,len(calls)); self.assertNotEqual(calls[0]['session'],calls[1]['session'])

    def test_explicit_cookie_does_not_skip_fs_and_overrides_fs_cookie(self):
        c=h.CrawlUtil; real=self.fake.post
        def post(url,json=None,**kwargs):
            if json['cmd']=='request.get':
                return types.SimpleNamespace(status_code=200,json=lambda:{'status':'ok','solution':{'response':'<html>FS</html>','cookies':[{'name':'_login','value':'auto','path':'/'}],'userAgent':'fs-agent'}})
            return real(url,json=json,**kwargs)
        scheduler=types.SimpleNamespace(use_proxy=False,use_selenium=False,use_flaresolverr=True)
        site={'COOKIE':'_login=user; cf_clearance=manual'}
        direct=Mock(headers={},cookies=[]);direct.get.return_value=types.SimpleNamespace(status_code=200,text='<html>fast</html>')
        with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct),requests=types.SimpleNamespace(post=post)):
            self.assertEqual('<html>FS</html>',c.get_html('https://example.test/page',site_info=site,scheduler_instance=scheduler,max_retries=1))
            direct.get.assert_not_called()
            self.assertEqual('<html>fast</html>',c.get_html('https://example.test/page',site_info=site,scheduler_instance=scheduler,max_retries=1))
        self.assertIn('_login=user',direct.get.call_args.kwargs['headers']['Cookie'])
        self.assertNotIn('_login=auto',direct.get.call_args.kwargs['headers']['Cookie'])

    def test_proxy_switch_never_seeds_auto_cookies_from_previous_proxy(self):
        c=h.CrawlUtil; real=self.fake.post; payloads=[]
        def post(url,json=None,**kwargs):
            response=real(url,json=json,**kwargs)
            if json['cmd']=='request.get':
                payloads.append(json.copy()); value='one' if 'one' in json['proxy']['url'] else 'two'
                return types.SimpleNamespace(status_code=200,json=lambda:{'status':'ok','solution':{'response':'<html>'+value+'</html>','cookies':[{'name':'cf_clearance','value':value,'path':'/'}],'userAgent':value+'-agent'}})
            return response
        direct=Mock(headers={},cookies=[]);direct.get.side_effect=[types.SimpleNamespace(status_code=403,text='blocked'),types.SimpleNamespace(status_code=200,text='<html>fast</html>')]
        with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct),requests=types.SimpleNamespace(post=post)):
            self.assertEqual('<html>one</html>',self.crawl())
            self.assertEqual('<html>two</html>',self.crawl())
            self.assertEqual('<html>fast</html>',self.crawl())
        self.assertTrue(all('cookies' not in payload for payload in payloads))
        self.assertEqual('cf_clearance=two',direct.get.call_args.kwargs['headers']['Cookie'])
        self.assertEqual('two-agent',direct.get.call_args.kwargs['headers']['User-Agent'])

    def test_ownership_cap_and_sensitive_log_redaction(self):
        c=h.CrawlUtil
        with patch.object(c,'FS_OWNED_LIMIT',2), patch.object(c,'_destroy_fs_session',return_value=False):
            self.assertTrue(self.request({'http':'http://one.invalid'})[1])
            self.assertTrue(self.request({'http':'http://two.invalid'})[1])
            self.assertEqual((None,None),self.request({'http':'http://three.invalid'}))
            self.assertEqual(2,len(c._fs_owned));self.assertEqual(2,len(c._fs_retired))
            self.assertEqual(2,sum(x['cmd']=='sessions.create' for x in self.fake.calls))
        value=c._safe_network_log('token=secret http://user:pass@proxy.invalid/?q=secret <html>private-body</html>')
        for secret in ['secret','user:pass','proxy.invalid','private-body','<html>']:
            self.assertNotIn(secret,value)

    def test_expired_or_missing_success_history_starts_fs_even_at_early_clock(self):
        c=h.CrawlUtil
        direct=Mock(headers={},cookies=[]); direct.get.return_value=types.SimpleNamespace(status_code=200,text='<html>fast</html>')
        with patch.object(h.time,'time',return_value=1000), patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct)):
            self.assertEqual('<html>OK</html>',self.crawl(proxies=''))
            direct.get.assert_not_called()
            self.assertEqual('<html>fast</html>',self.crawl(proxies=''))
            for key in c._http_ready: c._http_ready[key]=1000-c.CF_COOKIE_EXPIRY-1
            self.assertEqual('<html>OK</html>',self.crawl(proxies=''))
        self.assertEqual(1,direct.get.call_count)

    def test_unsolved_challenge_empty_html_and_target_error_never_use_http(self):
        c=h.CrawlUtil
        for source,status in [('<title>Just a moment...</title>',200),('',200),('<html>Error</html>',403)]:
            with self.subTest(status=status,empty=not source):
                c._clear_network_state('example.test')
                self.fake.calls.clear(); self.fake.failures=[{'status':'ok','solution':{'response':source,'status':status,'cookies':[]}}]*2
                direct=Mock()
                with patch.dict(self.env,_CURL_CFFI_AVAILABLE=True,cffi_requests=types.SimpleNamespace(Session=lambda:direct),requests=types.SimpleNamespace(post=self.fake.post,Session=lambda:direct)):
                    self.assertIsNone(self.crawl())
                self.assertEqual(2,len([x for x in self.fake.calls if x['cmd']=='request.get']))
                direct.get.assert_not_called()

    def test_unknown_fixed_session_is_never_adopted_or_destroyed(self):
        self.assertTrue(self.request()[1])
        name = next(call['session'] for call in self.fake.calls if call['cmd'] == 'request.get')
        self.assertNotEqual('feeder_example_test', name)
        h.CrawlUtil.close_flaresolverr_sessions()
        self.assertIn('feeder_example_test', self.fake.sessions)

    def test_healthy_session_reused_without_cookie_reinjection(self):
        self.request(); self.request()
        calls = [call for call in self.fake.calls if call['cmd'] == 'request.get']
        self.assertEqual(calls[0]['session'], calls[1]['session'])
        self.assertTrue(all('cookies' not in call for call in calls))

    def test_proxy_to_direct_replaces_browser_without_using_error_budget(self):
        c = h.CrawlUtil
        c._fs_scope.request = {'recoveries': 0, 'blocked': False}
        self.addCleanup(setattr, c._fs_scope, 'request', None)
        self.request({'http': 'http://proxy.invalid:8000'}); self.request()
        self.assertEqual(0, c._fs_scope.request['recoveries'])
        calls = [call for call in self.fake.calls if call['cmd'] == 'request.get']
        self.assertNotEqual(calls[0]['session'], calls[1]['session'])
        self.assertNotIn('proxy', calls[1])

    def test_nested_errors_consume_only_one_recovery_per_page(self):
        c = h.CrawlUtil
        c._fs_scope.request = {'recoveries': 0, 'blocked': False}
        self.addCleanup(setattr, c._fs_scope, 'request', None)
        self.fake.failures = ['Browser session is stuck', 'Session bound to a different proxy']
        self.assertEqual((None, None), self.request())
        self.assertEqual((None, None), self.request())
        self.assertEqual(1, c._fs_scope.request['recoveries'])
        self.assertEqual(2, len([x for x in self.fake.calls if x['cmd'] == 'request.get']))

    def test_request_timeout_quarantines_and_stops_same_page_retries(self):
        self.fake.failures = [TimeoutError('timeout')]
        c = h.CrawlUtil
        c._fs_scope.request = {'recoveries': 0, 'blocked': False}
        self.addCleanup(setattr, c._fs_scope, 'request', None)
        self.assertEqual((None, None), self.request())
        self.assertEqual((None, None), self.request())
        self.assertEqual('unknown', next(iter(c._fs_sessions.values()))['state'])
        self.assertEqual(1, len([x for x in self.fake.calls if x['cmd'] == 'request.get']))
        old=next(iter(c._fs_sessions.values()))['id']
        self.assertTrue(self.request({'http':'http://other.invalid'})[1])
        calls=[x for x in self.fake.calls if x['cmd']=='request.get']
        self.assertEqual(2,len(calls));self.assertNotEqual(old,calls[-1]['session'])

    def test_different_owner_creates_and_cleans_only_its_session(self):
        self.request()
        first = next(iter(h.CrawlUtil._fs_sessions.values()))['id']
        with h.CrawlUtil.execution_scope('other-owner'):
            self.request()
            second = next(iter(h.CrawlUtil._fs_sessions.values()))['id']
            self.assertNotEqual(first, second)
            h.CrawlUtil.finish_execution()
        self.assertIn(first, self.fake.sessions)
        self.assertNotIn(second, self.fake.sessions)

    def test_child_process_context_does_not_inherit_parent_session_ownership(self):
        self.request()
        parent = next(iter(h.CrawlUtil._fs_sessions.values()))['id']
        pid = h.os.getpid()
        with patch.object(h.os, 'getpid', return_value=pid + 1), h.CrawlUtil.execution_scope():
            self.assertFalse(h.CrawlUtil._fs_owned)
            self.request()
            child = next(iter(h.CrawlUtil._fs_sessions.values()))['id']
            self.assertNotEqual(parent, child)
            h.CrawlUtil.finish_execution()
        self.assertIn(parent, self.fake.sessions)
        self.assertNotIn(child, self.fake.sessions)

    def test_explicit_cookie_is_seeded_once_with_valid_path(self):
        h.CrawlUtil._fs_scope.user_cookie = ('example.test', '_login=user; cf_clearance=manual')
        self.request(); self.request()
        calls = [call for call in self.fake.calls if call['cmd'] == 'request.get']
        self.assertTrue(all(c['path'] == '/' for c in calls[0]['cookies']))
        self.assertIn('_login', [c['name'] for c in calls[0]['cookies']])
        self.assertNotIn('cookies', calls[1])
        h.CrawlUtil._fs_scope.user_cookie = None

    def test_failed_cookie_seed_is_unknown_and_new_session_is_seeded_after_cleanup(self):
        c = h.CrawlUtil
        c._fs_scope.user_cookie = ('example.test', '_login=user')
        for failure in ('API failure', TimeoutError('timeout'),
                        {'status': 'ok', 'solution': {'status': 200, 'response': '', 'cookies': []}}):
            with self.subTest(failure=type(failure).__name__):
                self.fake.calls.clear(); self.fake.failures = [failure]
                self.assertEqual((None, None), self.request())
                old = next(iter(c._fs_sessions.values()))
                self.assertEqual('unknown', old['state'])
                self.assertFalse(old.get('seeded', False))
                self.assertTrue(self.request()[1])
                requests = [call for call in self.fake.calls if call['cmd'] == 'request.get']
                self.assertEqual(2, len(requests))
                self.assertNotEqual(requests[0]['session'], requests[1]['session'])
                self.assertTrue(all(call.get('cookies') for call in requests))
                commands = [call['cmd'] for call in self.fake.calls]
                self.assertLess(commands.index('sessions.destroy'), len(commands) - 1 - commands[::-1].index('sessions.create'))
                self.assertTrue(next(iter(c._fs_sessions.values()))['seeded'])
                c.close_flaresolverr_sessions()

    def test_failed_cookie_seed_cannot_replace_without_confirmed_destroy(self):
        c=h.CrawlUtil; c._fs_scope.user_cookie=('example.test','_login=user')
        self.fake.failures=['API failure']
        self.assertEqual((None,None),self.request({'http':'http://one.invalid'}))
        old=next(iter(c._fs_sessions.values()))['id']
        with patch.object(c,'_destroy_fs_session',return_value=False):
            self.assertTrue(self.request({'http':'http://two.invalid'})[1])
        calls=[x for x in self.fake.calls if x['cmd']=='request.get']
        self.assertNotEqual(old,calls[-1]['session']); self.assertTrue(calls[-1].get('cookies'))
        self.assertEqual(1,sum(x['session']==old for x in calls))
        self.assertTrue(any(key[2]==old for key in c._fs_retired))
        self.assertTrue(next(iter(c._fs_sessions.values()))['seeded'])

    def test_attachment_closes_response_and_session_after_stream_error(self):
        response = Mock(); response.iter_content.side_effect = IOError('stream failed')
        session = Mock(); session.get.return_value = response
        with patch.dict(self.env, requests=types.SimpleNamespace(Session=lambda: session)):
            with self.assertRaises(IOError):
                h.CrawlUtil.download_file_stream('https://example.test/file')
        response.close.assert_called_once(); session.close.assert_called_once()

    def test_finish_continues_after_remote_cleanup_exception(self):
        with patch.object(h.CrawlUtil, 'close_flaresolverr_sessions', side_effect=RuntimeError('cleanup failed')), patch.object(h.CrawlUtil, 'close_sessions') as close:
            h.CrawlUtil.finish_execution()
        close.assert_called_once()

    def test_create_timeout_does_not_start_page_request(self):
        real_post = self.fake.post
        def post(url, json=None, **kwargs):
            if json['cmd'] == 'sessions.create': raise TimeoutError('create timeout')
            return real_post(url, json=json, **kwargs)
        with patch.dict(self.env, requests=types.SimpleNamespace(post=post)):
            self.assertEqual((None, None), self.request())
        self.assertFalse(any(call['cmd'] == 'request.get' for call in self.fake.calls))
        self.assertEqual('unknown', next(iter(h.CrawlUtil._fs_sessions.values()))['state'])

    def test_already_existing_creation_response_never_proves_ownership(self):
        real_post = self.fake.post
        def post(url, json=None, **kwargs):
            if json['cmd'] == 'sessions.create':
                self.fake.sessions[json['session']] = 'other-context'
                return types.SimpleNamespace(status_code=200, json=lambda: {'status': 'ok', 'session': json['session'], 'message': 'Session already exists.'})
            return real_post(url, json=json, **kwargs)
        with patch.dict(self.env, requests=types.SimpleNamespace(post=post)):
            self.assertEqual((None, None), self.request())
        self.assertEqual(set(), h.CrawlUtil._fs_owned)
        h.CrawlUtil.close_flaresolverr_sessions()
        self.assertEqual(2, len(self.fake.sessions))

    def test_unconfirmed_destroy_prevents_new_generation(self):
        c=h.CrawlUtil; self.request({'http':'http://one.invalid'})
        old=next(iter(c._fs_sessions.values()))['id']
        with patch.object(c,'_destroy_fs_session',return_value=False):
            self.assertTrue(self.request({'http':'http://two.invalid'})[1])
        current=next(iter(c._fs_sessions.values()))['id']
        self.assertNotEqual(old,current)
        self.assertTrue(any(key[2]==old for key in c._fs_owned))
        self.assertEqual('unknown',next(iter(c._fs_retired.values()))['state'])
        calls=[x for x in self.fake.calls if x['cmd']=='request.get']
        self.assertEqual([old,current],[x['session'] for x in calls])

    def test_attachment_keeps_preceding_page_proxy_and_closes_after_http_error(self):
        c = h.CrawlUtil
        c._network_context['example.test'] = ('http://fs.invalid', 'http://selected.invalid:8000', '')
        response = Mock(); response.raise_for_status.side_effect = IOError('404')
        session = Mock(); session.get.return_value = response
        with patch.dict(self.env, requests=types.SimpleNamespace(Session=lambda: session)), patch.object(c, 'get_current_proxy_url', return_value='http://other.invalid:8000'):
            with self.assertRaises(IOError): c.download_file_stream('https://example.test/file', referer='https://example.test/page')
        session.proxies.update.assert_called_once_with({'http': 'http://selected.invalid:8000', 'https': 'http://selected.invalid:8000'})
        response.close.assert_called_once(); session.close.assert_called_once()

    def test_attachment_reuses_scoped_http_login_cookie(self):
        import requests
        previous = requests.Session(); self.addCleanup(previous.close)
        previous.cookies.set('page_login', 'kept', domain='example.test', path='/')
        h.CrawlUtil._requests_sessions['example.test'] = previous
        response = Mock(); response.iter_content.return_value = [b'data']
        session = Mock(); session.get.return_value = response
        with patch.dict(self.env, requests=types.SimpleNamespace(Session=lambda: session)):
            result = h.CrawlUtil.download_file_stream('https://example.test/file', referer='https://example.test/page')
        self.assertEqual(b'data', result.getvalue())
        self.assertIn('page_login=kept', session.headers.update.call_args.args[0]['Cookie'])
        response.close.assert_called_once(); session.close.assert_called_once()


class WorkerStabilityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = h.Regressions(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        h.RemoteWorker._readiness = {}
        self.client = h.worker_client.WorkerClient('http://127.0.0.1:8000', 'x' * 32)

    def response(self, status, data):
        response = Mock(status_code=status, headers={})
        response.raw.read.return_value = json.dumps(data).encode()
        return response

    def test_updating_503_auth_and_configuration_are_distinct_and_redacted(self):
        for status, code in ((503, 'updating'), (401, 'auth'), (403, 'auth'), (400, 'configuration'), (409, 'configuration'), (500, 'unavailable')):
            response = self.response(status, {'error': {'code': 'updating', 'message': 'secret-token'}})
            session = Mock(); session.request.return_value = response
            with patch.object(h.worker_client.requests, 'Session', return_value=session):
                with self.assertRaises(h.worker_client.WorkerError) as caught:
                    self.client.submit({'job_id': 'same-id'})
            self.assertEqual(code, caught.exception.code)
            self.assertEqual(status, caught.exception.status)
            self.assertNotIn('secret-token', str(caught.exception))
            response.close.assert_called_once(); session.close.assert_called_once()

    def test_network_failure_has_no_source_classification(self):
        session = Mock(); session.request.side_effect = h.worker_client.requests.ConnectionError('secret-token')
        with patch.object(h.worker_client.requests, 'Session', return_value=session):
            with self.assertRaises(h.worker_client.WorkerError) as caught:
                self.client.job('same-id')
        self.assertEqual('unavailable', caught.exception.code)
        self.assertNotIn('secret-token', str(caught.exception))

    def test_ten_waiters_check_readiness_and_report_update_only_once(self):
        client = Mock(endpoint='http://127.0.0.1:8000/api/v1')
        client.ensure_version.side_effect = h.worker_client.WorkerError('업데이트 대기', 202)
        with patch.object(h.DownloadActivity, 'record') as activity:
            for _ in range(10):
                with self.assertRaises(h.worker_client.WorkerError):
                    h.RemoteWorker.ready(client)
        client.ensure_version.assert_called_once(); client.configuration.assert_not_called()
        activity.assert_called_once()

    def test_database_lease_limits_readiness_across_process_cache_reset(self):
        client = Mock(endpoint='http://127.0.0.1:8000/api/v1')
        client.ensure_version.side_effect = h.worker_client.WorkerError('업데이트 대기', 202)
        with self.assertRaises(h.worker_client.WorkerError): h.RemoteWorker.ready(client)
        h.RemoteWorker._readiness = {}
        with self.assertRaises(h.worker_client.WorkerError) as caught: h.RemoteWorker.ready(client)
        self.assertEqual('checking', caught.exception.code)
        client.ensure_version.assert_called_once()

    def test_defer_preserves_source_timer_payload_id_and_reservation(self):
        item = self.fixture.download(); item_id = item.id
        first = datetime.datetime.now() - datetime.timedelta(hours=7)
        state = {'phase': 'copying', 'worker': {'job_id': 'same-id', 'payload': {'source': 'src:path'}},
                 'usage': {'reserved': 123, 'settled': False}}
        item.upload_state = state; item.source_unavailable_since = first; h.db.session.commit()
        self.assertFalse(h.RemoteWorker.defer(item_id, '워커 업데이트 중'))
        current = h.db.session.get(h.ModelDownload, item_id)
        self.assertEqual(state, current.upload_state)
        self.assertEqual(first, current.source_unavailable_since)
        self.assertIsNotNone(current.upload_retry_after)

    def existing_item(self, submission='accepted'):
        item = self.fixture.download(); item_id = item.id
        payload = {'job_id': 'same-id', 'source': 'src:path', 'destination': 'dst:path', 'rclone_transfers': 2}
        item.destination_type = 'remote_worker'
        item.upload_state = {'phase': 'copying', 'worker': {'endpoint': self.client.endpoint,
            'job_id': 'same-id', 'payload': payload, 'settled': False, 'submission': submission}}
        h.db.session.commit()
        return item, payload

    def test_missing_accepted_or_ambiguous_job_is_never_resubmitted(self):
        for submission in ('accepted', 'uncertain', None):
            item, payload = self.existing_item(submission)
            client = Mock(endpoint=self.client.endpoint); client.job.return_value = None
            with patch.object(h.RemoteWorker, 'client', return_value=client):
                self.assertFalse(h.RemoteWorker.execute(item, self.fixture.manager()))
            client.submit.assert_not_called(); client.ensure_version.assert_not_called()
            current = h.db.session.get(h.ModelDownload, item.id)
            self.assertEqual('same-id', current.upload_state['worker']['job_id'])

    def test_manual_retry_without_remote_cancel_confirmation_preserves_checkpoint(self):
        item, payload = self.existing_item(); item_id = item.id
        state = dict(item.upload_state)
        state['usage'] = {'reserved': 123, 'settled': False, 'stat_ids': []}
        item.upload_state = state; h.db.session.commit()
        client = Mock(endpoint=self.client.endpoint)
        client.cancel.side_effect = h.worker_client.WorkerError('원격 취소 미확인', 503)
        with patch.object(h.RemoteWorker, 'client', return_value=client), patch.object(h.UploadUtil, 'kill_item_rclone'), patch.object(h.UploadUtil, 'settle_usage') as settle:
            with self.assertRaises(h.worker_client.WorkerError):
                h.RemoteWorker.prepare_action(item, 'retry')
        settle.assert_not_called(); client.submit.assert_not_called(); self.fixture.run.assert_not_called()
        h.db.session.remove(); current = h.db.session.get(h.ModelDownload, item_id)
        self.assertEqual(state, current.upload_state)
        self.assertEqual('cancelled', current.status)
        self.assertIn('확인 중', current.error_message)

    def test_existing_running_job_completes_without_version_gate_during_drain(self):
        item, payload = self.existing_item()
        item.source_unavailable_since = datetime.datetime.now() - datetime.timedelta(hours=7)
        h.db.session.commit(); item_id = item.id
        client = Mock(endpoint=self.client.endpoint)
        running = dict(payload, status='running', drive_impersonate='')
        completed = dict(running, status='completed', chargeable_bytes=0)
        client.job.side_effect = [running, running, completed]
        hub = Mock(generation=0)
        with patch.object(h.RemoteWorker, 'client', return_value=client), patch.object(h.worker_client.WorkerEvents, 'acquire', return_value=hub), patch.object(h.UploadUtil, 'resume_upload', return_value=True) as resume:
            self.assertTrue(h.RemoteWorker.execute(item, self.fixture.manager()))
        client.ensure_version.assert_not_called(); client.submit.assert_not_called()
        resume.assert_called_once(); hub.release.assert_called_once()
        self.assertIsNone(h.db.session.get(h.ModelDownload, item_id).source_unavailable_since)

    def test_worker_interrupted_preserves_checkpoint_and_does_not_recopy(self):
        item, payload = self.existing_item()
        client = Mock(endpoint=self.client.endpoint)
        failed = dict(payload, status='failed', drive_impersonate='', error_type='UNKNOWN_ERROR',
                      error_message='워커가 재시작되어 이전 전송이 중단됐습니다.')
        client.job.return_value = failed
        with patch.object(h.RemoteWorker, 'client', return_value=client), patch.object(h.worker_client.WorkerEvents, 'acquire', return_value=Mock(generation=0)), patch.object(h.UploadUtil, 'transfer_failed') as transfer:
            self.assertFalse(h.RemoteWorker.execute(item, self.fixture.manager()))
            current = h.db.session.get(h.ModelDownload, item.id)
            self.assertFalse(h.RemoteWorker.execute(current, self.fixture.manager()))
        transfer.assert_not_called(); client.submit.assert_not_called()
        current = h.db.session.get(h.ModelDownload, item.id)
        self.assertEqual('WORKER_INTERRUPTED', current.transfer_error_type)
        self.assertTrue(current.upload_state['worker']['recovery_required'])

    def test_submission_response_loss_preserves_id_and_never_reserves_again(self):
        item, payload = self.existing_item('new'); item_id = item.id
        client = Mock(endpoint=self.client.endpoint); client.job.return_value = None
        client.submit.side_effect = h.worker_client.WorkerError('응답 유실')
        with patch.object(h.RemoteWorker, 'client', return_value=client), patch.object(h.RemoteWorker, 'ready', return_value=({'active_jobs': 0, 'queued_jobs': 0}, {})), patch.object(h.UploadUtil, 'reserve_usage') as reserve:
            self.assertFalse(h.RemoteWorker.execute(item, self.fixture.manager()))
            current = h.db.session.get(h.ModelDownload, item_id)
            self.assertEqual('uncertain', current.upload_state['worker']['submission'])
            self.assertFalse(h.RemoteWorker.execute(current, self.fixture.manager()))
        client.submit.assert_called_once_with(payload); reserve.assert_not_called()

    def test_definitely_rejected_submission_can_retry_same_id(self):
        item, payload = self.existing_item('new'); item_id = item.id
        client = Mock(endpoint=self.client.endpoint); client.job.return_value = None
        client.submit.side_effect = h.worker_client.WorkerError('업데이트 중', 503, 'updating')
        with patch.object(h.RemoteWorker, 'client', return_value=client), patch.object(h.RemoteWorker, 'ready', return_value=({'active_jobs': 0, 'queued_jobs': 0}, {})):
            self.assertFalse(h.RemoteWorker.execute(item, self.fixture.manager()))
        current = h.db.session.get(h.ModelDownload, item_id)
        self.assertEqual('rejected', current.upload_state['worker']['submission'])
        self.assertEqual(payload, current.upload_state['worker']['payload'])
        self.assertEqual('pending_upload', current.status)

    def test_readiness_gate_runs_before_new_account_allocation_and_prepare(self):
        item = self.fixture.download(); manager = self.fixture.manager()
        client = Mock(endpoint=self.client.endpoint)
        with patch.object(h.RemoteWorker, 'client', return_value=client), patch.object(h.RemoteWorker, 'ready', side_effect=h.worker_client.WorkerError('업데이트 중', 202)), patch.object(h.RemoteWorker, 'prepare') as prepare, patch.object(manager, 'allocate_account') as allocate, patch.object(h.UploadUtil, 'reserve_usage') as reserve:
            self.assertFalse(h.RemoteWorker.execute(item, manager))
        prepare.assert_not_called(); allocate.assert_not_called(); reserve.assert_not_called()

    def test_defer_finishes_after_finite_recheck_when_worker_recovers(self):
        client = Mock(endpoint=self.client.endpoint)
        client.ensure_version.side_effect = [h.worker_client.WorkerError('업데이트 중', 202), {'version': '0.9.0'}]
        client.configuration.return_value = {'config_version': 1}
        with patch.object(h.time, 'monotonic', return_value=100), patch.object(h.time, 'time', return_value=1000):
            with self.assertRaises(h.worker_client.WorkerError): h.RemoteWorker.ready(client)
        with patch.object(h.time, 'monotonic', return_value=131), patch.object(h.time, 'time', return_value=1031):
            health, config = h.RemoteWorker.ready(client)
        self.assertEqual({'config_version': 1}, config)
        self.assertEqual({}, h.RemoteWorker._readiness)
        self.assertEqual(2, client.ensure_version.call_count)

    def test_client_requests_update_once_for_repeated_old_version_health(self):
        h.worker_client.WorkerClient._update_attempts = {}
        with patch.object(self.client, 'health', return_value={'version': '0.8.0', 'self_update': True}), patch.object(self.client, '_request', return_value={}) as request, patch.object(h.worker_client, 'worker_update_package', return_value={}):
            for _ in range(10):
                with self.assertRaises(h.worker_client.WorkerError) as caught: self.client.ensure_version()
                self.assertEqual('updating', caught.exception.code)
        request.assert_called_once_with('POST', '/update', {})

    def test_known_failed_update_does_not_automatically_repeat(self):
        with patch.object(self.client, 'health', return_value={'version': '0.8.0', 'self_update': True,
            'update': {'status': 'failed', 'target_version': h.worker_client.bundled_worker_version()}}), patch.object(self.client, '_request') as request:
            for _ in range(3):
                with self.assertRaises(h.worker_client.WorkerError): self.client.ensure_version()
        request.assert_not_called()


class SeleniumStabilityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = h.Regressions(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.env = h.CrawlUtil.get_html.__func__.__globals__
        self.browser = Mock(current_url='https://example.test/', title='OK', page_source='<html>OK</html>')
        self.web = types.SimpleNamespace(ChromeOptions=Mock, Remote=Mock(return_value=self.browser))
        p = patch.dict(self.env, _SELENIUM_AVAILABLE=True, webdriver=self.web)
        p.start(); self.addCleanup(p.stop)
        self.scope = h.CrawlUtil.execution_scope(h.uuid.uuid4().hex); self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.addCleanup(h.CrawlUtil.finish_execution)
        self.site = {'NAME': 'example', 'TORRENT_SITE_URL': 'https://example.test/'}
        self.cfg = types.SimpleNamespace(use_flaresolverr=False, use_proxy=False)

    def test_driver_quits_when_timeout_setup_fails(self):
        self.browser.set_page_load_timeout.side_effect = RuntimeError('failed')
        self.assertIsNone(h.CrawlUtil.init_stealth_selenium(self.site, self.cfg))
        self.browser.quit.assert_called_once()
        self.assertIsNone(h.CrawlUtil._selenium_driver)

    def test_driver_quits_when_both_initial_navigations_fail(self):
        self.browser.get.side_effect = RuntimeError('navigation failed')
        self.assertIsNone(h.CrawlUtil.init_stealth_selenium(self.site, self.cfg))
        self.browser.quit.assert_called_once()

    def test_driver_quits_when_user_cookie_cannot_be_applied(self):
        self.browser.add_cookie.side_effect = RuntimeError('cookie failed')
        self.assertIsNone(h.CrawlUtil.init_stealth_selenium(dict(self.site, COOKIE='login=value'), self.cfg))
        self.browser.quit.assert_called_once()

    def test_same_context_reuses_driver_changed_auth_recreates_it(self):
        first = h.CrawlUtil.init_stealth_selenium(self.site, self.cfg)
        self.assertIs(first, h.CrawlUtil.init_stealth_selenium(self.site, self.cfg))
        self.web.Remote.assert_called_once()
        h.CrawlUtil.init_stealth_selenium(dict(self.site, COOKIE='login=value'), self.cfg)
        self.assertEqual(2, self.web.Remote.call_count)
        self.browser.quit.assert_called_once()

    def test_proxy_and_selenium_endpoint_changes_recreate_browser(self):
        h.CrawlUtil.init_stealth_selenium(self.site, self.cfg)
        cfg = types.SimpleNamespace(use_flaresolverr=False, use_proxy=True, proxy_url='http://proxy.invalid:8000')
        h.CrawlUtil.init_stealth_selenium(self.site, cfg)
        h.CrawlUtil.init_stealth_selenium(dict(self.site, SELENIUM_REMOTE_URL='http://other.invalid:4444'), cfg)
        self.assertEqual(3, self.web.Remote.call_count)

    def test_unconfirmed_browser_quit_prevents_new_context_creation(self):
        h.CrawlUtil.init_stealth_selenium(self.site, self.cfg)
        self.browser.quit.side_effect = RuntimeError('quit unavailable')
        self.assertIsNone(h.CrawlUtil.init_stealth_selenium(dict(self.site, COOKIE='new=value'), self.cfg))
        self.web.Remote.assert_called_once()
        self.assertIsNone(h.CrawlUtil._selenium_context)
        self.browser.quit.side_effect = None

    def test_failed_challenge_handler_never_returns_page(self):
        with patch.object(h.CrawlUtil, 'handle_turnstile_challenge', return_value=False):
            self.assertIsNone(h.CrawlUtil.get_by_remote_selenium('https://example.test/page', site_info=self.site, scheduler_instance=self.cfg))

    def test_missing_driver_and_challenge_after_failed_reinit_are_failures(self):
        self.assertFalse(h.CrawlUtil.handle_turnstile_challenge(None, 'https://example.test/'))
        self.browser.title = 'Just a moment...'
        self.browser.page_source = '<title>Just a moment...</title>'
        with patch.object(h.CrawlUtil, 'solve_turnstile_checkbox', return_value=False), patch.object(h.CrawlUtil, 'reinit_session', return_value=None):
            self.assertFalse(h.CrawlUtil.handle_turnstile_challenge(self.browser, 'https://example.test/'))

    def test_required_xpath_timeout_never_returns_normal_html(self):
        wait = Mock(); wait.until.side_effect = TimeoutError('not found')
        with patch.dict(self.env, WebDriverWait=lambda *a: wait, EC=Mock(), By=Mock()):
            self.assertIsNone(h.CrawlUtil.get_by_remote_selenium('https://example.test/page', wait_tag='//article', site_info=self.site, scheduler_instance=self.cfg))

    def test_reinit_performs_only_one_flaresolverr_preauth(self):
        cfg = types.SimpleNamespace(use_flaresolverr=True, use_proxy=False)
        with patch.object(h.CrawlUtil, '_get_flaresolverr_clearance_result', return_value=([], 'agent', True)) as clearance, patch.object(h.CrawlUtil, 'get_hook', return_value=None):
            self.assertIsNotNone(h.CrawlUtil.reinit_session(self.site, cfg))
        clearance.assert_called_once()


class CrawlLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.fixture = h.Regressions(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)

    def test_cleanup_failure_still_clears_owned_flags_before_releasing_locks(self):
        events = []
        real_set = h.Settings.set
        real_release = h.FeederUtil.release_runtime_lock
        def setting(key, value):
            if key == 'crawl_is_running': events.append(('flag', value))
            real_set(key, value)
        def release(name, owner):
            events.append(('release', owner)); real_release(name, owner)
        with patch.object(h.FeederUtil, 'get_crawlers', return_value=[]), patch.object(h.CrawlUtil, 'close_flaresolverr_sessions', side_effect=RuntimeError('cleanup failed')), patch.object(h.Settings, 'set', side_effect=setting), patch.object(h.FeederUtil, 'release_runtime_lock', side_effect=release):
            h.TaskCrawl.run_crawl()
        self.assertFalse(h.TaskCrawl._run_mutex.locked())
        self.assertLess(events.index(('flag', 'False')), next(i for i, event in enumerate(events) if event[0] == 'release'))
        self.assertTrue(h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'next-owner'))

    def test_failed_lease_never_cleans_other_execution_or_changes_running_flag(self):
        h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'other-owner')
        h.Settings.set('crawl_is_running', 'True')
        with patch.object(h.CrawlUtil, 'finish_execution') as cleanup:
            h.TaskCrawl.run_crawl()
        cleanup.assert_not_called()
        self.assertTrue(h.Settings.get_bool('crawl_is_running'))
        self.assertFalse(h.TaskCrawl._run_mutex.locked())

    def test_plugin_load_preserves_live_lease_and_flags(self):
        h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'other-owner')
        h.Settings.set('crawl_is_running', 'True')
        module = h.ModuleCrawl.__new__(h.ModuleCrawl); module.name = 'crawl'
        with patch.object(h.FeederUtil, 'migrate_db'), patch.object(h.FeederUtil, 'reset_runtime_lock') as reset:
            module.plugin_load()
        reset.assert_not_called()
        self.assertFalse(h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'next-owner'))
        self.assertTrue(h.Settings.get_bool('crawl_is_running'))

    def test_site_test_uses_same_lease_and_reports_total_failure(self):
        module = h.ModuleCrawl.__new__(h.ModuleCrawl); module.name = 'crawl'
        site = types.SimpleNamespace(info={'NAME': 'site'}, get_options=lambda: {})
        env = module.process_command.__func__.__globals__
        req = types.SimpleNamespace(form={'site_id': '1', 'board_id': 'board'})
        def execute(*args, **kwargs):
            self.assertTrue(h.TaskCrawl._run_mutex.locked())
            self.assertFalse(h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'other-owner'))
            return None
        with patch.dict(env, ModelCrawlSite=types.SimpleNamespace(get=lambda **kw: site)), patch.object(h.TaskCrawl, 'execute_board_crawl', side_effect=execute):
            result = module.process_command('test', '', '', '', req)
        self.assertEqual('fail', result['ret'])
        self.assertFalse(h.TaskCrawl._run_mutex.locked())
        self.assertTrue(h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'other-owner'))

    def test_stale_display_flag_does_not_override_absent_execution_lease(self):
        h.Settings.set('crawl_is_running', 'True')
        self.assertFalse(h.TaskCrawl.is_running())
        h.FeederUtil.acquire_runtime_lock('crawl_pipeline', 'other-owner')
        self.assertTrue(h.TaskCrawl.is_running())

    def test_site_test_exception_returns_failure_and_releases_lease(self):
        module = h.ModuleCrawl.__new__(h.ModuleCrawl); module.name = 'crawl'
        site = types.SimpleNamespace(info={'NAME': 'site'}, get_options=lambda: {})
        env = module.process_command.__func__.__globals__
        req = types.SimpleNamespace(form={'site_id': '1', 'board_id': 'board'})
        with patch.dict(env, ModelCrawlSite=types.SimpleNamespace(get=lambda **kw: site)), patch.object(h.TaskCrawl, 'execute_board_crawl', side_effect=ValueError('invalid HTML')):
            result = module.process_command('test', '', '', '', req)
        self.assertEqual('fail', result['ret'])
        self.assertFalse(h.TaskCrawl.is_running())

    def test_empty_success_is_different_from_total_html_failure(self):
        cfg = types.SimpleNamespace(use_selenium=False)
        tree = types.SimpleNamespace(xpath=lambda _: [])
        env = h.TaskCrawl.execute_board_crawl.__globals__
        with patch.object(h.CrawlUtil, 'get_hook', return_value=None), patch.dict(env, html=types.SimpleNamespace(fromstring=lambda _: tree)):
            with patch.object(h.CrawlUtil, 'get_html', return_value='<html>empty</html>'):
                self.assertEqual([], h.TaskCrawl.execute_board_crawl({'NAME': 'site'}, 'board', target_cfg=cfg))
                self.assertEqual('success', h.CrawlUtil._fs_scope.crawl_result['status'])
            with patch.object(h.CrawlUtil, 'get_html', return_value=None):
                self.assertIsNone(h.TaskCrawl.execute_board_crawl({'NAME': 'site'}, 'board', target_cfg=cfg))
                self.assertEqual('failed', h.CrawlUtil._fs_scope.crawl_result['status'])

    def test_later_html_failure_reports_partial_result(self):
        cfg = types.SimpleNamespace(use_selenium=False)
        element = types.SimpleNamespace(text_content=lambda: 'example', attrib={'href': '/view/123'})
        tree = types.SimpleNamespace(xpath=lambda _: [element])
        env = h.TaskCrawl.execute_board_crawl.__globals__
        with patch.object(h.CrawlUtil, 'get_hook', return_value=None), patch.object(h.ModelCrawlItem, 'get', return_value=object()), patch.dict(env, html=types.SimpleNamespace(fromstring=lambda _: tree)), patch.object(h.CrawlUtil, 'get_html', side_effect=['<html>empty</html>', None]):
            self.assertEqual([], h.TaskCrawl.execute_board_crawl({'NAME': 'site'}, 'board', max_page=2, target_cfg=cfg))
        self.assertEqual('partial', h.CrawlUtil._fs_scope.crawl_result['status'])


class WorkerServerContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = h.tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = h.Path(self.tmp.name)
        self.updates = h.load_helper('feeder_update_contract', 'worker/app/updates.py')

    def package(self):
        payload = h.worker_client.worker_update_package()
        files = json.loads(zlib.decompress(base64.b64decode(payload['archive'])))
        files['app/__init__.py'] = files['app/__init__.py'].replace("VERSION = '0.9.0'", "VERSION = '0.9.1'")
        raw = json.dumps(files, sort_keys=True, ensure_ascii=True, separators=(',', ':')).encode()
        return {'version': '0.9.1', 'sha256': hashlib.sha256(raw).hexdigest(),
                'archive': base64.b64encode(zlib.compress(raw)).decode()}

    def test_server_update_is_idempotent_while_draining(self):
        (self.root / 'requirements.txt').write_text((h.ROOT / 'worker/requirements.txt').read_text())
        update = self.updates.Updates(self.root, None, Mock(), '0.9.0'); update.callback = Mock()
        payload = self.package()
        thread = Mock()
        with patch.object(self.updates.threading, 'Thread', return_value=thread):
            first = update.receive(payload); second = update.receive(payload)
        self.assertEqual('draining', first['status'])
        self.assertEqual(first, second)
        thread.start.assert_called_once(); update.callback.assert_not_called()

    def test_validation_and_reloading_do_not_start_before_jobs_drain(self):
        database = Mock(); database.counts.side_effect = [{'active_jobs': 1, 'queued_jobs': 2}, {'active_jobs': 0, 'queued_jobs': 0}]
        update = self.updates.Updates(self.root, None, database, '0.9.0'); update.callback = Mock()
        with patch.object(self.updates.time, 'sleep') as sleep, patch.object(self.updates, 'release_path', side_effect=RuntimeError('stop before validation process')):
            update._prepare()
        self.assertEqual(2, database.counts.call_count)
        sleep.assert_called_once_with(.5); update.callback.assert_not_called()

    def test_restart_recovery_keeps_queued_and_reports_interruption_for_running(self):
        module = h.load_helper('feeder_database_contract', 'worker/app/database.py')
        database = module.Database(self.root / 'worker.sqlite'); database.initialize()
        with patch.dict(h.sys.modules, rclone_logging=h.logging_helpers):
            jobs_module = h.load_helper('feeder_jobs_contract', 'worker/app/jobs.py')
        with contextlib.closing(database.connect()) as connection:
            for status in ('running', 'queued'):
                connection.execute('INSERT INTO jobs(job_id,status,source,destination,created_at) VALUES (?,?,?,?,?)',
                                   (status, status, 'src:path', 'dst:path', '2026-10-22'))
            connection.commit()
        store = jobs_module.JobStore(database, Mock()); store.recover()
        self.assertEqual('queued', store.get('queued')['status'])
        running = store.get('running')
        self.assertEqual('failed', running['status'])
        self.assertNotEqual('SOURCE_UNAVAILABLE', running['error_type'])
        self.assertEqual('워커가 재시작되어 이전 전송이 중단됐습니다.', running['error_message'])


class DownloaderSaveUITests(unittest.TestCase):
    def test_save_response_and_transport_failure_keep_form_until_real_success(self):
        script = r'''
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const source = fs.readFileSync(process.argv[1], 'utf8');
const start = source.indexOf("$(document).on('click', '#downloader_save_btn'");
const end = source.indexOf("$(document).on('click', '.delete_dl_btn'", start);
assert(start >= 0 && end > start);
const validation = 'WebDAV 실패 유예는 0~8760 사이의 정수 시간이어야 합니다.';
const cases = [
  [{ret:'empty_name'}, false], [{ret:validation}, false], [{ret:'fail',msg:'secret-token'}, false],
  [null, false], [{}, false], [{ret:{toString:null}}, false], [{ret:'<script>secret-token</script>'}, false],
  [{ret:'success'}, false], [{ret:'success',downloaders:[]}, true],
  [{ret:'success_update',downloaders:[{name:'saved'}]}, true], ['transport', false]
];
for (const [response, success] of cases) {
  let click, options, requests=0, rendered=0, hidden=0, notices=[];
  const button = {disabled:false, prop(name,value) {if(arguments.length===1)return this[name]; this[name]=value; return this;}};
  const elements = {
    '#dl_name': {val:()=> 'ad'}, '#dl_engine_type': {val:()=> 'alldebrid'},
    '#dl_enabled': {is:()=> true}, '#downloader_modal': {modal:value=> {if(value==='hide') hidden++;}}
  };
  const document = {};
  const $ = value => value===document ? {on:(event,selector,handler)=> {click=handler;}} : value===button ? button : elements[value];
  $.ajax = value => {options=value; requests++;};
  const original=[{name:'original'}];
  const context = {document,$,package_name:'feeder',sub:'download',available_engine_schemas:[],
    current_downloaders:original,render_downloaders:()=>rendered++,notify:(message,level)=>notices.push({message,level})};
  vm.runInNewContext(source.slice(start,end),context);
  click.call(button,{preventDefault(){}});
  click.call(button,{preventDefault(){}});
  assert.strictEqual(requests,1,'중복 저장 요청');
  if(response==='transport') options.error(); else options.success(response);
  options.complete();
  assert.strictEqual(button.disabled,false);
  assert.strictEqual(hidden,success?1:0);
  assert.strictEqual(rendered,success?1:0);
  assert.strictEqual(notices.filter(n=>n.level==='success').length,success?1:0);
  assert(notices.every(n=>!n.message.includes('secret-token')));
  if(!success) assert.strictEqual(context.current_downloaders,original);
  assert.strictEqual(elements['#dl_name'].val(),'ad');
}
'''
        result = h.subprocess.run(['node', '-e', script, str(h.ROOT / 'static/feeder_download.js')],
                                  capture_output=True, text=True, timeout=10)
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == '__main__':
    unittest.main()
