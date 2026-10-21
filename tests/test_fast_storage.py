import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from test_hardening import a


class FastStorage(unittest.IsolatedAsyncioTestCase):
    """Storage goes to a channel the account created (100 msgs in ~4 s), bot made admin there."""

    def setUp(self):
        a.ACTIVE_JOB = {'id': 9, 'lease_token': 'lease'}
        a.FAST_POOLS = None
        self.calls = []

    def chan(self, cid):
        return SimpleNamespace(id=cid, broadcast=True, creator=True, left=False, title=a.FAST_POOL_TITLE)

    def client(self, dialogs, admin_errors=None):
        admin_errors = list(admin_errors or [])
        async def iter_dialogs():
            for e in dialogs:
                yield SimpleNamespace(entity=e)
        async def call(req):
            self.calls.append(type(req).__name__)
            if type(req).__name__ == 'CreateChannelRequest':
                return SimpleNamespace(chats=[self.chan(777)])
            if type(req).__name__ == 'EditAdminRequest' and admin_errors:
                raise admin_errors.pop(0)
            return None
        c = AsyncMock(side_effect=call)
        c.iter_dialogs = iter_dialogs
        c.get_entity = AsyncMock(return_value=SimpleNamespace(bot=True, id=1))
        return c

    async def run_fast(self, client, replies):
        bodies = []
        async def worker(method, path, body):
            bodies.append((path, body)); return replies.pop(0)
        with patch.object(a, 'worker_request', AsyncMock(side_effect=worker)):
            r = await a._fast_storage_impl(client, {'bot_id': 1})
        return r, bodies

    async def test_existing_own_channel_with_bot_admin_is_used(self):
        other = SimpleNamespace(id=5, broadcast=True, creator=False, left=False, title=a.FAST_POOL_TITLE)
        client = self.client([other, self.chan(300)])
        r, bodies = await self.run_fast(client, [{'ok': True, 'accepted': True, 'pool': -1000000000300}])
        self.assertEqual(r[1], -1000000000300)
        self.assertEqual(bodies[0][0], 'repool'); self.assertEqual(bodies[0][1]['job_id'], 9)
        self.assertNotIn('CreateChannelRequest', self.calls); self.assertNotIn('EditAdminRequest', self.calls)

    async def test_bot_is_promoted_then_job_is_moved(self):
        client = self.client([self.chan(300)])
        r, bodies = await self.run_fast(client, [{'ok': True, 'accepted': False, 'need_admin': True, 'bot_username': 'clone_bot'},
                                                 {'ok': True, 'accepted': True}])
        self.assertEqual(r[1], -1000000000300); self.assertEqual(self.calls, ['EditAdminRequest'])
        client.get_entity.assert_awaited_with('clone_bot'); self.assertEqual(len(bodies), 2)

    async def test_no_own_channel_creates_one(self):
        client = self.client([])
        r, _ = await self.run_fast(client, [{'ok': True, 'accepted': True}])
        self.assertEqual(self.calls, ['CreateChannelRequest']); self.assertEqual(r[1], -1000000000777)

    async def test_full_channel_falls_through_to_a_new_one(self):
        client = self.client([self.chan(300)], admin_errors=[a.AdminsTooMuchError(request=None)])
        r, _ = await self.run_fast(client, [{'ok': True, 'accepted': False, 'need_admin': True, 'bot_username': 'b'},
                                            {'ok': True, 'accepted': False, 'need_admin': True, 'bot_username': 'b'},
                                            {'ok': True, 'accepted': True}])
        self.assertEqual(self.calls, ['EditAdminRequest', 'CreateChannelRequest', 'EditAdminRequest'])
        self.assertEqual(r[1], -1000000000777)

    async def test_wrong_bot_identity_or_failure_keeps_assigned_pool(self):
        client = self.client([self.chan(300)]); client.get_entity = AsyncMock(return_value=SimpleNamespace(bot=True, id=99))
        r, _ = await self.run_fast(client, [{'ok': True, 'accepted': False, 'need_admin': True, 'bot_username': 'x'}])
        self.assertIsNone(r); self.assertNotIn('EditAdminRequest', self.calls)
        a.FAST_POOLS = None
        client = self.client([self.chan(300)])
        with patch.object(a, 'worker_request', AsyncMock(side_effect=a.WorkerAPIError(503))):
            self.assertIsNone(await a._fast_storage_impl(client, {'bot_id': 1}))

    async def test_fetch_range_publishes_into_the_own_channel(self):
        published = []
        async def publish(method, path, body):
            published.append(body); return {'ok': True, 'next_id': body['next_id'], 'total': len(body['msg_ids'])}
        msgs = lambda ids: [SimpleNamespace(id=i, action=None, noforwards=False, reply_markup=None) for i in ids]
        client = SimpleNamespace(get_messages=AsyncMock(side_effect=lambda c, ids=None, limit=None, min_id=None: msgs(ids) if ids else []),
                                 forward_messages=AsyncMock(side_effect=lambda d, ids, s, **k: msgs([1000 + i for i in ids])))
        with patch.object(a, 'resolve_source', AsyncMock(return_value=SimpleNamespace(noforwards=False))), \
             patch.object(a, 'resolve_pool', AsyncMock(side_effect=AssertionError('old pool must not be used'))), \
             patch.object(a, 'fast_storage', AsyncMock(return_value=('own', -1000000000300))), \
             patch.object(a, 'worker_request', AsyncMock(side_effect=publish)):
            await a.fetch_range(client, {'storage_version': 1, 'bot_id': 1, 'code': 'c', 'pool': -100, 'chat': '@s', 'from': 1, 'to': 100})
        self.assertEqual(client.forward_messages.await_count, 1, '100 messages, one request')
        self.assertTrue(all(b['chat_id'] == -1000000000300 for b in published)); self.assertTrue(published[-1]['done'])
        self.assertEqual(len(published[0]['msg_ids']), 100)


if __name__ == '__main__':
    unittest.main()
