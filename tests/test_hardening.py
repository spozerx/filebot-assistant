import os
os.environ.update(TELETHON_API_ID='1', TELETHON_API_HASH='test-only-hash', WORKER_URL='https://worker.invalid', ASSISTANT_SHARED_SECRET='test-only-secret')
import asyncio
import importlib.util
from pathlib import Path
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
spec=importlib.util.spec_from_file_location('assistant_under_test',Path(__file__).parents[1]/'assistant.py')
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)
# Test clients are plain namespaces: route the single forward request through their
# forward_messages(dest, ids, source) fake, keeping the request (and its random_ids) visible.
async def _test_invoke_forward(client,req,dest):
    log=getattr(client,'requests',None)
    if isinstance(log,list):log.append((list(req.id),list(req.random_id)))
    return await client.forward_messages(dest,list(req.id),req.from_peer,drop_author=req.drop_author)
a._invoke_forward=_test_invoke_forward

class HTTPValidation(unittest.TestCase):
    def test_non_2xx_write_is_not_success(self):
        with patch.object(a.requests,'request',return_value=SimpleNamespace(status_code=503)):
            with self.assertRaises(a.WorkerAPIError): a.worker_request_sync('POST','refs',{})
    def test_application_failure_is_not_success(self):
        response=Mock(status_code=200);response.json.return_value={'ok':False}
        with patch.object(a.requests,'request',return_value=response):
            with self.assertRaises(a.WorkerAPIError): a.worker_request_sync('POST','ack',{})
    def test_invalid_json_is_not_success(self):
        response=Mock(status_code=200);response.json.side_effect=ValueError('sensitive body')
        with patch.object(a.requests,'request',return_value=response):
            with self.assertRaises(a.WorkerAPIError) as cm:a.worker_request_sync('POST','refs',{})
            self.assertNotIn('sensitive',str(cm.exception))
    def test_protocol_header(self):self.assertEqual(a.HEADERS['X-Filebot-Protocol'],'2')
    def test_threaded_http_server_is_used(self):
        self.assertIn('ThreadingHTTPServer',a.serve_api.__code__.co_names)

class AsyncSafety(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        a.OPERATIONS.clear();a.PENDING.clear();a.ACTIVE_JOB=None;a.SESSION_SWITCHING=False
        a.LOOP=asyncio.get_running_loop();a.WAKE=asyncio.Event()
    async def test_sync_network_is_offloaded(self):
        ticks=0
        def slow(*args):time.sleep(.05);return {'ok':True}
        async def ticker():
            nonlocal ticks
            for _ in range(5):await asyncio.sleep(.005);ticks+=1
        with patch.object(a,'worker_request_sync',side_effect=slow):
            await asyncio.gather(a.worker_request('GET','pull'),ticker())
        self.assertEqual(ticks,5)
    async def test_duplicate_login_request_runs_once(self):
        calls=0
        async def route(body):
            nonlocal calls
            calls+=1;await asyncio.sleep(.01);return {'ok':True,'id':'dummy'}
        with patch.dict(a.ROUTES,{'/test':route}):
            results=await asyncio.gather(a.dispatch_api('/test',{'phone':'dummy'},'key'),a.dispatch_api('/test',{'phone':'dummy'},'key'))
        self.assertEqual(calls,1);self.assertEqual(results[0],results[1])
    async def test_idempotency_key_reuse_with_different_body_rejected(self):
        with patch.dict(a.ROUTES,{'/test':AsyncMock(return_value={'ok':True})}):
            await a.dispatch_api('/test',{'x':1},'key')
            r=await a.dispatch_api('/test',{'x':2},'key')
        self.assertEqual(r['error'],'request_key_conflict')
    async def test_source_resolution_uses_accessible_dialogs(self):
        entity=object();client=SimpleNamespace(get_entity=AsyncMock(side_effect=ValueError()),get_dialogs=AsyncMock(return_value=[SimpleNamespace(id=-100123,entity=entity)]))
        self.assertIs(await a.resolve_source(client,-100123),entity)
    async def test_unresolved_source_is_permanent_and_does_not_bypass_access(self):
        client=SimpleNamespace(get_entity=AsyncMock(side_effect=ValueError()),get_dialogs=AsyncMock(return_value=[]))
        with self.assertRaises(a.PermanentJobError):await a.resolve_source(client,-100123)
    async def test_protected_source_is_not_forwarded(self):
        client=SimpleNamespace(forward_messages=AsyncMock())
        with patch.object(a,'resolve_source',AsyncMock(return_value=SimpleNamespace(noforwards=True))):
            with self.assertRaises(a.PermanentJobError):await a.fetch_range(client,{'chat':'test','from':1,'to':2,'pool':123,'code':'a','bot_id':1})
        client.forward_messages.assert_not_called()
    async def test_ack_retries_network_error_with_same_lease(self):
        request=AsyncMock(side_effect=[a.requests.ConnectionError(),{'ok':True}]);job={'id':1,'lease_token':'test-lease'}
        with patch.object(a,'worker_request',request),patch.object(a.asyncio,'sleep',AsyncMock()):await a.send_ack(job,True)
        self.assertEqual(request.await_count,2)
        for call in request.await_args_list:self.assertEqual(call.args[2]['lease_token'],'test-lease')
    async def test_stale_ack_is_not_retried(self):
        request=AsyncMock(side_effect=a.WorkerAPIError(409))
        with patch.object(a,'worker_request',request):
            with self.assertRaises(a.WorkerAPIError):await a.send_ack({'id':1,'lease_token':'old'},True)
        self.assertEqual(request.await_count,1)
    async def test_failed_session_persistence_does_not_swap_clients(self):
        old=SimpleNamespace(disconnect=AsyncMock());a.CLIENT=old;a.SESSION='previous'
        new=SimpleNamespace(session=SimpleNamespace(save=lambda:'test-session'),get_me=AsyncMock(return_value=SimpleNamespace(username='test',first_name='Test',id=1)))
        with patch.object(a,'worker_request',AsyncMock(side_effect=a.WorkerAPIError(503))):
            with self.assertRaises(a.WorkerAPIError):await a._finish(new)
        self.assertIs(a.CLIENT,old);self.assertEqual(a.SESSION,'previous');self.assertFalse(a.SESSION_SWITCHING)
    async def test_session_swap_disconnects_old_client(self):
        old=SimpleNamespace(disconnect=AsyncMock());a.CLIENT=old
        new=SimpleNamespace(session=SimpleNamespace(save=lambda:'test-session'),get_me=AsyncMock(return_value=SimpleNamespace(username='test',first_name='Test',id=1)))
        with patch.object(a,'worker_request',AsyncMock(return_value={'ok':True})):out=await a._finish(new)
        self.assertTrue(out['ok']);self.assertIs(a.CLIENT,new);old.disconnect.assert_awaited_once()
    async def test_session_swap_during_active_job_is_rejected(self):
        a.ACTIVE_JOB={'id':1};self.assertEqual((await a._finish(None))['error'],'assistant_busy_retry')
    async def test_expired_login_is_pruned_on_code_submission(self):
        client=SimpleNamespace(disconnect=AsyncMock());a.PENDING['expired']={'client':client,'at':time.time()-700}
        self.assertEqual((await a._login_code('expired','dummy'))['error'],'expired')
        await asyncio.sleep(.01);client.disconnect.assert_awaited_once()
    async def test_startup_session_fetch_can_be_retried(self):
        with patch.object(a,'worker_request',AsyncMock(side_effect=[a.WorkerAPIError(503),{'session':'test-session'}])):
            self.assertIsNone(await a.load_session());self.assertEqual(await a.load_session(),'test-session')

if __name__=='__main__':unittest.main()

class LocalHTTPServer(unittest.TestCase):
    def setUp(self):
        import threading
        a.CLIENT=None;a.SESSION=None
        self.server=a.ThreadingHTTPServer(('127.0.0.1',0),a.Api)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.base=f'http://127.0.0.1:{self.server.server_port}'
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join()
    def test_health_and_unknown_route(self):
        r=a.requests.get(self.base+'/health',timeout=2);self.assertEqual(r.status_code,200);self.assertEqual(r.json()['protocol'],2)
        self.assertEqual(a.requests.get(self.base+'/unknown',timeout=2).status_code,404)
    def test_unauthenticated_post_is_rejected(self):
        self.assertEqual(a.requests.post(self.base+'/wake',json={},timeout=2).status_code,401)
    def test_oversized_and_invalid_body_are_rejected(self):
        headers={'Authorization':'Bearer test-only-secret'}
        self.assertEqual(a.requests.post(self.base+'/wake',data='x'*17000,headers=headers,timeout=2).status_code,413)
        self.assertEqual(a.requests.post(self.base+'/wake',data='{',headers=headers,timeout=2).status_code,400)

class MongoImportValidation(unittest.TestCase):
    def test_atlas_uri_accepted_without_network(self):
        self.assertEqual(a.validate_mongo_uri('mongodb+srv://user:dummy@sample.mongodb.net/users'), 'users')
    def test_unsafe_hosts_plain_mongo_and_tls_disable_rejected(self):
        for uri in ['mongodb://user:dummy@localhost/db', 'mongodb+srv://user:dummy@localhost/db',
                    'mongodb+srv://user:dummy@mongodb.net.evil.invalid/db',
                    'mongodb+srv://user:dummy@sample.mongodb.net/db?tls=false']:
            with self.subTest(uri=uri):
                with self.assertRaises(a.PermanentJobError): a.validate_mongo_uri(uri)
    def test_invalid_uri_error_does_not_expose_credentials(self):
        with self.assertRaises(a.PermanentJobError) as cm:
            a.validate_mongo_uri('mongodb://sensitive-user:sensitive-password@127.0.0.1/db')
        self.assertNotIn('sensitive', str(cm.exception))
    def test_cancel_before_import_connects_to_nothing(self):
        stop=a.threading.Event();stop.set()
        with patch('pymongo.MongoClient') as client:
            a.import_mongo_sync({'uri':'mongodb+srv://user:dummy@sample.mongodb.net/db','bot_id':1},{},stop)
            client.assert_not_called()
