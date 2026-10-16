import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from test_hardening import a

class CheckpointStorage(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        a.ACTIVE_JOB={'id':7,'lease_token':'lease'}
        self.payload={'storage_version':1,'bot_id':1,'code':'newbatch','pool':-100,'chat':'@source','from':1,'to':100,'next_id':1,'published_count':0}
        self.published=[];self.sent=[];self.cursor=1;self.total=0
    def message(self,id,buttons=False):
        return SimpleNamespace(id=id,action=None,noforwards=False,reply_markup=object() if buttons else None)
    def client(self):
        async def get(source,ids):return [self.message(i) for i in reversed(ids)]
        async def forward(dest,ids,source,**kwargs):
            self.sent.extend(ids);return [self.message(1000+i) for i in ids]
        return SimpleNamespace(get_messages=AsyncMock(side_effect=get),forward_messages=AsyncMock(side_effect=forward))
    async def publish(self,method,path,body):
        self.published.append(body.copy())
        if body['expected_next_id']==self.cursor:
            self.total+=len(body['msg_ids']);self.cursor=body['next_id']
        return {'ok':True,'next_id':self.cursor,'total':self.total}
    async def execute(self,client,payload=None,request=None):
        with patch.object(a,'resolve_source',AsyncMock(return_value=SimpleNamespace(noforwards=False))),patch.object(a,'resolve_pool',AsyncMock(return_value='pinned-storage')),patch.object(a,'worker_request',request or AsyncMock(side_effect=self.publish)):
            await a.fetch_range(client,payload or self.payload)
    async def test_fast_ordered_batches_publish_only_confirmed_storage_ids_without_sleep(self):
        client=self.client()
        with patch.object(a.asyncio,'sleep',AsyncMock()) as sleep:await self.execute(client)
        sleep.assert_not_called();self.assertEqual(self.sent,list(range(1,101)));self.assertEqual(client.forward_messages.await_count,1)
        self.assertEqual([len(b['msg_ids']) for b in self.published],[100,0]);self.assertTrue(self.published[-1]['done'])
        self.assertEqual(self.published[-1]['next_id'],101);self.assertTrue(all(b['chat_id']==-100 and not b.get('reset') for b in self.published))
        self.assertEqual(a.HEADERS['X-Filebot-Storage-Protocol'],'2')
    async def test_retry_resumes_after_committed_prefix_instead_of_resetting(self):
        self.payload['to']=200
        client=self.client();original=client.forward_messages.side_effect
        async def fail_second(dest,ids,source,**kw):
            if ids[0]>100:raise RuntimeError('temporary Telegram failure')
            return await original(dest,ids,source,**kw)
        client.forward_messages.side_effect=fail_second
        with self.assertRaises(RuntimeError):await self.execute(client)
        self.assertEqual(self.cursor,101);self.assertEqual(self.sent,list(range(1,101)))
        await self.execute(self.client(),{**self.payload,'next_id':101,'published_count':100})
        self.assertEqual(self.sent,list(range(1,201)));self.assertTrue(self.published[-1]['done'])
    async def test_partial_forward_stops_without_claiming_later_files_stored(self):
        client=self.client();client.forward_messages=AsyncMock(return_value=[self.message(1001),None,self.message(1003)])
        with self.assertRaises(a.PermanentJobError):await self.execute(client,{**self.payload,'to':3})
        self.assertEqual(len(self.published),1);self.assertEqual(self.published[0]['source_ids'],[1]);self.assertEqual(self.cursor,2)
        self.assertFalse(any(b['done'] for b in self.published))
    async def test_short_forward_response_is_not_guessed_into_source_positions(self):
        client=self.client();client.forward_messages=AsyncMock(return_value=[self.message(1001),self.message(1003)])
        with self.assertRaises(a.PermanentJobError):await self.execute(client,{**self.payload,'to':3})
        self.assertEqual(self.published,[])
    async def test_lost_publication_response_retries_same_checkpoint_not_forward(self):
        client=self.client();first=True
        async def request(method,path,body):
            nonlocal first
            result=await self.publish(method,path,body)
            if first:first=False;raise a.requests.ConnectionError('lost response')
            return result
        with patch.object(a.asyncio,'sleep',AsyncMock()):await self.execute(client,{**self.payload,'to':2},AsyncMock(side_effect=request))
        self.assertEqual(client.forward_messages.await_count,1);self.assertEqual(self.total,2);self.assertEqual(self.published[0],self.published[1])
    async def test_empty_and_service_only_range_does_not_invent_storage_files(self):
        client=self.client();client.get_messages=AsyncMock(return_value=[None,SimpleNamespace(id=2,action=object())])
        await self.execute(client,{**self.payload,'to':2});client.forward_messages.assert_not_called();self.assertEqual(self.total,0);self.assertEqual(self.cursor,3)
    async def test_protected_source_is_not_forwarded_in_checkpointed_path(self):
        client=self.client()
        with patch.object(a,'resolve_source',AsyncMock(return_value=SimpleNamespace(noforwards=True))):
            with self.assertRaises(a.PermanentJobError):await a.fetch_range(client,self.payload)
        client.forward_messages.assert_not_called()
    async def test_keyboard_runs_keep_source_order_and_same_destination(self):
        client=self.client()
        async def get(source,ids):return [self.message(i,buttons=(i in [2,3])) for i in reversed(ids)]
        client.get_messages.side_effect=get;await self.execute(client,{**self.payload,'to':4})
        self.assertEqual(self.sent,[1,2,3,4]);self.assertEqual([call.args[1] for call in client.forward_messages.await_args_list],[[1,2,3,4]])
        self.assertTrue(all(call.args[0]=='pinned-storage' and call.kwargs['drop_author'] is False for call in client.forward_messages.await_args_list))

    async def test_metadata_reads_a_hundred_stored_files_in_one_call_without_forwarding(self):
        client=self.client();refs=[{'file_pk':i,'message_id':1000+i} for i in range(1,101)]
        request=AsyncMock(return_value={'ok':True})
        with patch.object(a,'resolve_pool',AsyncMock(return_value='storage')),patch.object(a,'worker_request',request):
            await a.hydrate_storage(client,{'bot_id':1,'code':'batch','pool':-100,'refs':refs})
        self.assertEqual(client.get_messages.await_count,1);client.forward_messages.assert_not_called();body=request.await_args.args[2];self.assertEqual(len(body['rows']),100);self.assertEqual(body['rows'][0]['file_pk'],1)

    async def test_worker_claims_next_storage_job_without_two_second_sleep(self):
        jobs=[{'id':1,'kind':'fetch_range','payload':{}},{'id':2,'kind':'hydrate_storage','payload':{}},a.asyncio.CancelledError()]
        with patch.object(a,'CLIENT',self.client()),patch.object(a,'SESSION_SWITCHING',False),patch.object(a,'pull_job',AsyncMock(side_effect=jobs)),patch.object(a,'fetch_range',AsyncMock(return_value=None)),patch.object(a,'hydrate_storage',AsyncMock(return_value=None)),patch.object(a,'send_ack',AsyncMock()) as ack,patch.object(a.asyncio,'sleep',AsyncMock()) as sleep:
            with self.assertRaises(a.asyncio.CancelledError):await a.worker_loop()
        self.assertEqual(ack.await_count,2);self.assertTrue(all(call.args[1] is True for call in ack.await_args_list));sleep.assert_not_called()

    async def test_missing_storage_metadata_message_is_not_falsely_completed(self):
        client=self.client();client.get_messages=AsyncMock(return_value=[])
        with patch.object(a,'resolve_pool',AsyncMock(return_value='storage')),patch.object(a,'worker_request',AsyncMock()) as request:
            with self.assertRaises(a.PermanentJobError):await a.hydrate_storage(client,{'bot_id':1,'code':'batch','pool':-100,'refs':[{'file_pk':1,'message_id':1001}]})
        request.assert_not_called()

    async def test_metadata_ack_retry_does_not_reread_already_committed_files(self):
        client=self.client()
        with patch.object(a,'resolve_pool',AsyncMock()) as resolve:await a.hydrate_storage(client,{'metadata_done':1})
        resolve.assert_not_called();client.get_messages.assert_not_called()

    async def test_empty_telegram_source_slots_are_skipped_not_forwarded(self):
        self.payload['to']=3;client=self.client()
        client.get_messages=AsyncMock(return_value=[self.message(3),a.MessageEmpty(id=2,peer_id=None),self.message(1)])
        await self.execute(client);self.assertEqual(self.sent,[1,3]);self.assertEqual(self.total,2);self.assertTrue(self.published[-1]['done'])

    async def test_empty_storage_metadata_slot_is_not_marked_hydrated(self):
        client=self.client();client.get_messages=AsyncMock(return_value=[a.MessageEmpty(id=1001,peer_id=None)])
        with patch.object(a,'resolve_pool',AsyncMock(return_value='storage')),patch.object(a,'worker_request',AsyncMock()) as request:
            with self.assertRaises(a.PermanentJobError):await a.hydrate_storage(client,{'bot_id':1,'code':'batch','pool':-100,'refs':[{'file_pk':1,'message_id':1001}]})
        request.assert_not_called()

    async def test_empty_forward_result_is_not_counted_as_a_stored_file(self):
        self.payload['to']=2;client=self.client();client.forward_messages=AsyncMock(return_value=[self.message(1001),a.MessageEmpty(id=1002,peer_id=None)])
        with self.assertRaises(a.PermanentJobError):await self.execute(client)
        self.assertEqual(self.total,1);self.assertFalse(any(x['done'] for x in self.published))

    async def test_legacy_mixed_keyboard_range_uses_one_full_batch_without_warmup(self):
        client=self.client();client.get_messages=AsyncMock(return_value=[self.message(i,buttons=i%2==0) for i in range(100,0,-1)])
        payload={**self.payload};payload.pop('storage_version')
        with patch.object(a,'resolve_source',AsyncMock(return_value=SimpleNamespace(noforwards=False))),patch.object(a,'resolve_pool',AsyncMock(return_value='storage')),patch.object(a,'worker_request',AsyncMock(return_value={'ok':True})),patch.object(a.asyncio,'sleep',AsyncMock()) as sleep:
            await a.fetch_range(client,payload)
        self.assertEqual(client.forward_messages.await_count,1);self.assertEqual(self.sent,list(range(1,101)));self.assertFalse(client.forward_messages.await_args.kwargs['drop_author']);sleep.assert_not_called()
