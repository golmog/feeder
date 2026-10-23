"""대시보드 선택 검증. 실제 서비스/운영 DB를 사용하지 않는다."""
import ast
from contextlib import contextmanager, ExitStack
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import math
import json
import zlib
import threading
import time
import subprocess
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import test_regressions as h

source = ast.parse((h.ROOT / 'util_dashboard.py').read_text())
env = dict(h.COMMON, hmac=hmac, math=math, zlib=zlib)
exec(compile(ast.Module(body=[n for n in source.body if isinstance(n, (ast.ClassDef, ast.FunctionDef, ast.Assign))], type_ignores=[]), 'util_dashboard.py', 'exec'), env)
Dashboard = env['Dashboard']
Cache = env['EngineStatusCache']
Busy = env['EngineBusy']


@contextmanager
def metric_cache():
    with ExitStack() as stack:
        for key,value in [('_metric',{'success':True,'counts':None,'state':'loading','observed_at':0}),('_metric_running',False),('_metric_after',0)]:
            stack.enter_context(patch.object(Dashboard,key,value))
        pool=stack.enter_context(ThreadPoolExecutor(max_workers=1))
        stack.enter_context(patch.object(Dashboard,'_metric_pool',pool,create=True))
        yield


def wait_metric():
    deadline=time.monotonic()+2
    while Dashboard._metric_running and time.monotonic()<deadline:
        threading.Event().wait(.005)
    if Dashboard._metric_running:
        raise AssertionError('집계 스레드 완료 지연')


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.fixture = h.Regressions(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        p = patch.object(h.F.app, 'config', {'SECRET_KEY': 'isolated-tests'}, create=True)
        p.start(); self.addCleanup(p.stop)

    def test_785_rows_snapshot_and_reconcile_without_remote_or_disk(self):
        ranks = list(Dashboard.ranks)
        rows = []
        for i in range(785):
            row = h.ModelDownload('feed', 'title'+str(i), 'sensitive-magnet', str(i))
            row.status = ranks[i % len(ranks)]
            row.upload_state = {'secret': 'never send'}
            rows.append(row)
        h.db.session.add_all(rows); h.db.session.commit()
        expected = {r.id for r in rows}
        with patch.object(h.DownloadUtil, 'create_engine', side_effect=AssertionError('remote called')), patch.object(h.TaskDownload, '_get_upload_queue_state', side_effect=AssertionError('disk called')):
            first = Dashboard.queue()
            self.assertEqual(40, len(first['items'])); self.assertEqual(785, first['total'])
            self.assertEqual('uploading', first['items'][0]['status'])
            self.assertNotIn('upload_state', first['items'][0]); self.assertNotIn('magnet', first['items'][0])
            all_items = list(first['items']); cursor = first['next_cursor']
            # 단순 OFFSET이면 누락될 수 있는 정렬 변화를 중간에 넣는다.
            rows[780].status = 'uploading'; h.db.session.commit()
            while cursor:
                page = Dashboard.queue(cursor=cursor)
                all_items.extend(page['items']); cursor = page['next_cursor']
            ids = [i['id'] for i in all_items]
            self.assertEqual(expected, set(ids)); self.assertEqual(785, len(ids))
            known = {str(r['id']): r['row_version'] for r in all_items}
            removed = rows[1].id; rows[1].status = 'completed'
            added = self.fixture.download(status='uploading', infohash='new')
            result = Dashboard.queue(known=known)
            self.assertIn(removed, result['removed_ids'])
            next_items = Dashboard.queue(cursor=result['next_cursor'])['items'] if result['next_cursor'] else []
            self.assertIn(added.id, [r['id'] for r in result['items'] + next_items])
            self.assertEqual(785, result['total'])

    def test_counts_failure_preserves_page_and_cursor_validation(self):
        self.fixture.download()
        with patch.object(Dashboard, 'counts', side_effect=RuntimeError('stats only')):
            data = Dashboard.queue()
        self.assertTrue(data['success']); self.assertTrue(data['counts_error']); self.assertEqual(1, len(data['items']))
        cursor = Dashboard.cursor([1, 2, 3]); self.assertEqual([1, 2, 3], Dashboard.read_cursor(cursor))
        with self.assertRaises(ValueError): Dashboard.read_cursor('x'+cursor[1:])
        with patch.object(env['time'], 'time', return_value=time.time()+601):
            with self.assertRaisesRegex(ValueError, 'snapshot_expired'): Dashboard.read_cursor(cursor)

    def test_engine_identity_requires_engine_and_task_match(self):
        row = self.fixture.download(status='downloading')
        row.current_engine_name = 'cd2'; row.engine_task_id = 'same'; h.db.session.commit()
        fake = h.NS(request=Mock(), view=lambda:[{'engine_name': name, 'items':[{'task_id':tid,'hash':row.infohash}]} for name, tid in [('cd2','same'),('ad','same'),('cd2','different')]])
        with patch.object(Dashboard, 'engines', fake): result = Dashboard.engine_view()
        self.assertEqual([row.id, None, None], [s['items'][0]['db_id'] for s in result['engines']])

    def test_matching_db_failure_preserves_engines_and_recovers(self):
        ids={}
        for name in ['cd2','ad']:
            row=self.fixture.download(status='downloading',infohash='shared')
            row.current_engine_name=name; row.engine_task_id='same'; h.db.session.commit(); ids[name]=row.id
        def snapshots():
            return [{'engine_key':name,'engine_name':name,'state':'ready' if name=='ad' else 'stale',
                'items':[{'task_id':tid,'hash':'shared','filename':name,'last_seen_at':1000} for tid in ['same','other']]} for name in ['cd2','ad']]
        cache=h.NS(request=Mock(),view=snapshots)
        with patch.object(Dashboard,'engines',cache), patch.object(h.DownloadUtil,'create_engine',side_effect=AssertionError('외부 재조회')):
            with patch.object(h.db.session,'query',side_effect=RuntimeError('token=private')), patch.object(h.db.session,'rollback',wraps=h.db.session.rollback) as rollback:
                failed=Dashboard.engine_view()
                rollback.assert_called_once()
            self.assertTrue(failed['success']); self.assertFalse(failed['match_verified'])
            self.assertEqual(['stale','ready'],[s['state'] for s in failed['engines']])
            self.assertTrue(all(not s['match_verified'] for s in failed['engines']))
            self.assertTrue(all(i['db_id'] is None for s in failed['engines'] for i in s['items']))
            self.assertNotIn('private',json.dumps(failed))
            recovered=Dashboard.engine_view()
        self.assertTrue(recovered['match_verified'])
        for snapshot in recovered['engines']:
            self.assertTrue(snapshot['match_verified'])
            self.assertEqual([ids[snapshot['engine_name']],None],[i['db_id'] for i in snapshot['items']])
        self.assertEqual(2,cache.request.call_count)

    def test_sse_v2_first_message_never_touches_db_or_engine(self):
        module = h.ModuleDownload.__new__(h.ModuleDownload)
        with patch.object(h.F.app, 'app_context', side_effect=AssertionError('context entered')), patch.object(h.DownloadUtil, 'create_engine', side_effect=AssertionError('engine called')):
            stream = module.generate_sse_stream(version='2')
            self.assertIn('retry', next(stream)); self.assertIn('"kind": "refresh"', next(stream)); stream.close()

    def test_legacy_sse_keeps_envelope_without_external_queries(self):
        self.fixture.download()
        module=h.ModuleDownload.__new__(h.ModuleDownload)
        entered=threading.Event(); release=threading.Event()
        def read():
            entered.set(); release.wait(2)
            return {'uploading':0,'upload_current':0,'upload_total':0}
        with metric_cache(), patch.dict(module.generate_sse_stream.__globals__,Dashboard=Dashboard), patch.object(h.DownloadUtil,'create_engine',side_effect=AssertionError('외부 조회')), patch.object(h.FeederUtil,'get_download_control_state',return_value={'state':'paused'}), patch.object(module,'get_upload_queue_counts',side_effect=read) as count:
            stream=module.generate_sse_stream()
            try:
                self.assertTrue(next(stream).startswith('retry:'))
                frame=next(stream)
                self.assertTrue(entered.wait(1)); self.assertTrue(frame.startswith('event: upload_counts_state'))
                state=json.loads(frame.split('data: ',1)[1]); self.assertEqual('loading',state['state'])
                self.assertNotIn('counts',state)
                release.set(); wait_metric()
                payload=json.loads(next(stream).removeprefix('data: ').strip())
                self.assertEqual(1,count.call_count)
            finally:
                release.set(); stream.close()
        self.assertIn('active_list',payload); self.assertIn('control',payload)
        self.assertIn('total_download_24h',payload); self.assertEqual(0,payload['counts']['upload_current'])
        self.assertEqual('ready',payload['upload_counts_state'])

    def test_upload_count_failure_retains_last_good_and_reports_error(self):
        with metric_cache():
            Dashboard.upload_counts(Mock(side_effect=RuntimeError('private error'))); wait_metric()
            self.assertEqual('error',Dashboard._metric['state']); self.assertIsNone(Dashboard._metric['counts'])
            counts={'uploading':0,'upload_current':0,'upload_total':0}
            Dashboard._metric_after=0
            Dashboard.upload_counts(lambda:counts); wait_metric()
            self.assertEqual('ready',Dashboard._metric['state']); self.assertEqual(counts,Dashboard._metric['counts'])
            Dashboard._metric_after=0
            Dashboard.upload_counts(Mock(side_effect=RuntimeError('private error'))); wait_metric()
            self.assertEqual('stale',Dashboard._metric['state']); self.assertEqual(counts,Dashboard._metric['counts'])
            self.assertNotIn('private',json.dumps(Dashboard._metric))

    def test_blocked_engine_leaves_db_control_and_activity_usable(self):
        self.fixture.download(status='downloading')
        cache=Cache(); release=threading.Event(); entered=threading.Event()
        self.addCleanup(lambda:cache.pool.shutdown(wait=True)); self.addCleanup(release.set)
        def blocked(cfg):
            entered.set(); release.wait(2)
            raise TimeoutError('모의 30초 제한시간 경과')
        cache.request([{'name':'cd2'}],blocked); self.assertTrue(entered.wait(1))
        module=h.ModuleDownload.__new__(h.ModuleDownload); module.name='download'
        with patch.object(h.DownloadUtil,'create_engine',side_effect=AssertionError('동기 외부 조회')), patch.object(h.FeederUtil,'get_download_control_state',return_value={'state':'paused'}), patch.object(h.DownloadActivity,'read',return_value=[{'id':1,'message':'activity'}]):
            first=Dashboard.queue()
            control=module.process_ajax('pipeline_control_state',h.NS(form={'dashboard_version':'2'}))
            activity=module.process_api('activity',h.NS(args={'snapshot':'1'},headers={}))
        self.assertEqual(1,len(first['items'])); self.assertEqual(1,first['counts']['downloading'])
        self.assertEqual('paused',control['control']['state']); self.assertNotIn('counts',control)
        self.assertEqual(1,len(activity['entries'])); self.assertFalse(release.is_set())

    def test_upload_counts_single_flight_separate_from_db(self):
        from concurrent.futures import ThreadPoolExecutor
        release=threading.Event(); entered=threading.Event(); pool=ThreadPoolExecutor(max_workers=1)
        self.addCleanup(lambda:pool.shutdown(wait=True)); self.addCleanup(release.set)
        for key,value in [('_metric_pool',pool),('_metric',{'success':True,'counts':None,'state':'loading','observed_at':0}),('_metric_running',False),('_metric_after',0)]:
            p=patch.object(Dashboard,key,value,create=True); p.start(); self.addCleanup(p.stop)
        def read():
            entered.set(); release.wait(2)
            return {'upload_current':2,'upload_total':10}
        callback=Mock(side_effect=read)
        self.assertEqual('loading',Dashboard.upload_counts(callback)['state']); self.assertTrue(entered.wait(1))
        for _ in range(3): Dashboard.upload_counts(callback)
        self.assertEqual(1,callback.call_count)
        self.assertTrue(Dashboard.queue()['success'])
        release.set()
        deadline=time.monotonic()+2
        while Dashboard._metric_running and time.monotonic()<deadline: threading.Event().wait(.005)
        self.assertEqual(2,Dashboard.upload_counts(callback)['counts']['upload_current'])
        for _ in range(3): Dashboard.upload_counts(callback)
        self.assertEqual(1,callback.call_count)


    def test_existing_lease_suppresses_other_process(self):
        cfg = {'name':'cd2','engine_type':'fake'}
        engine = h.NS(status_list_complete=False, get_status=Mock(return_value=([{'task_id':'one'}], None)))
        with patch.object(h.DownloadUtil, 'create_engine', return_value=engine):
            self.assertFalse(Dashboard.fetch_engine(cfg)[1])
            with self.assertRaises(Busy): Dashboard.fetch_engine(cfg)
        engine.get_status.assert_called_once()


class CacheTests(unittest.TestCase):
    def cache(self):
        clock = [1000.0]
        cache = Cache(clock=lambda:clock[0])
        self.addCleanup(lambda: cache.pool.shutdown(wait=True) if cache.pool else None)
        return cache, clock

    def wait(self, predicate):
        deadline = time.monotonic()+2
        wake = threading.Event()
        while not predicate() and time.monotonic() < deadline: wake.wait(.005)
        self.assertTrue(predicate())

    def test_blocked_cd2_does_not_delay_ad_or_inverse(self):
        for blocked, healthy in [('cd2','ad'),('ad','cd2')]:
            cache, clock = self.cache(); release = threading.Event(); self.addCleanup(release.set)
            entered = threading.Event(); calls = []
            def fetch(cfg):
                calls.append(cfg['name'])
                if cfg['name'] == blocked:
                    entered.set(); release.wait(2)
                    raise TimeoutError('DEADLINE_EXCEEDED after 30 seconds')
                return [{'task_id':'ok','filename':healthy,'download_speed':17}], True
            configs = [{'name':blocked},{'name':healthy}]
            cache.request(configs, fetch); self.assertTrue(entered.wait(1))
            for _ in range(3): cache.request(configs, fetch)
            self.wait(lambda:any(s['engine_name']==healthy and s['state']=='ready' for s in cache.view()))
            self.assertEqual(1, calls.count(blocked)); self.assertEqual(1,calls.count(healthy))
            self.assertEqual('loading', next(s for s in cache.view() if s['engine_name']==blocked)['state'])
            clock[0] += 30; release.set()
            self.wait(lambda:not cache.records[cache.key(configs[0])]['inflight'])
            self.assertEqual('ready', next(s for s in cache.view() if s['engine_name']==healthy)['state'])
            self.assertEqual('error', next(s for s in cache.view() if s['engine_name']==blocked)['state'])

    def test_partial_error_empty_ttl_and_config_invalidation(self):
        cache, clock = self.cache(); cfg = {'name':'cd2'}; fetch = Mock(return_value=([{'task_id':'A','download_speed':50}], True))
        def run():
            cache.request([cfg],fetch)
            self.wait(lambda:not cache.records[cache.key(cfg)]['inflight'])
            return cache.view()[0]
        first=run(); self.assertEqual('ready', first['state'])
        run(); self.assertEqual(1, fetch.call_count)
        clock[0]+=31; fetch.return_value=([],False)
        partial=run(); self.assertEqual(1,len(partial['items'])); self.assertTrue(partial['items'][0]['stale']); self.assertEqual(0,partial['items'][0]['download_speed'])
        clock[0]+=31; fetch.side_effect=RuntimeError('network secret')
        failed=run(); self.assertEqual('stale',failed['state']); self.assertNotIn('secret',json.dumps(failed))
        run(); self.assertEqual(3,fetch.call_count)
        clock[0]+=46; fetch.side_effect=None; fetch.return_value=([],True)
        self.assertEqual([],run()['items'])
        cfg['token']='changed'; fresh=run()
        self.assertNotEqual(first['config_revision'],fresh['config_revision']); self.assertNotIn('changed',json.dumps(fresh))
        cache.request([],fetch); self.assertEqual([],cache.view())

    def test_refreshing_last_good_hides_old_speed(self):
        cache,clock=self.cache(); cfg={'name':'cd2'}
        cache.request([cfg],lambda _:([{'task_id':'old','download_speed':88}],True))
        self.wait(lambda:not cache.inflight_keys)
        release=threading.Event(); self.addCleanup(release.set)
        def blocked(_):
            release.wait(2)
            raise TimeoutError()
        clock[0]+=31; cache.request([cfg],blocked)
        snapshot=cache.view()[0]
        self.assertEqual('loading',snapshot['state'])
        self.assertTrue(snapshot['items'][0]['stale']); self.assertEqual(0,snapshot['items'][0]['download_speed'])
        release.set(); self.wait(lambda:not cache.inflight_keys)
        self.assertEqual('stale',cache.view()[0]['state'])


    def test_pool_bound_and_changed_config_never_overlap_engine(self):
        cache, clock=self.cache(); release=threading.Event(); self.addCleanup(release.set)
        cfgs=[{'name':str(i)} for i in range(5)]; calls=[]
        def fetch(cfg):
            calls.append(cfg['name']); release.wait(2)
            return [{'task_id':'ok'}],True
        cache.request(cfgs,fetch)
        self.wait(lambda:len(calls)==3)
        self.assertEqual(3,len(cache.inflight_keys))
        cfgs[0]['token']='changed'
        cache.request(cfgs,fetch)
        self.assertEqual(1,calls.count('0')); self.assertEqual(3,len(calls))
        self.assertEqual([],cache.view()[0]['items'])
        release.set(); self.wait(lambda:not cache.inflight_keys)
        cache.request(cfgs,fetch); self.wait(lambda:not cache.inflight_keys)
        self.assertEqual(2,calls.count('0')); self.assertIn('3',calls); self.assertIn('4',calls)


    def test_malformed_list_retains_last_good_and_expires(self):
        cache, clock = self.cache(); cfg={'name':'ad'}
        fetch=Mock(return_value=([{'task_id':'a','download_speed':99}],True))
        cache.request([cfg],fetch); self.wait(lambda:not cache.records[cache.key(cfg)]['inflight'])
        clock[0]+=31; fetch.return_value=([{}],True)
        cache.request([cfg],fetch); self.wait(lambda:not cache.records[cache.key(cfg)]['inflight'])
        self.assertEqual(1,len(cache.view()[0]['items'])); self.assertEqual('ValueError',cache.view()[0]['error_type'])
        clock[0]+=301; self.assertEqual([],cache.view()[0]['items'])


class BrowserTests(unittest.TestCase):
    def test_incremental_merge_fallback_actions_and_render(self):
        result=subprocess.run(['node', str(h.ROOT/'tests/dashboard_state_test.js')], capture_output=True,text=True,timeout=15)
        self.assertEqual(0,result.returncode,result.stdout+result.stderr)
        self.assertIn('dashboard checks passed',result.stdout)
