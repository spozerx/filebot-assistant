import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from test_hardening import a
import asyncio as _asyncio
asyncio_real_sleep = _asyncio.sleep

BUSY = a.tg_errors.WorkerBusyTooLongRetryError(request=None)


class ConfirmedForward(unittest.IsolatedAsyncioTestCase):
    """Telegram answers WORKER_BUSY / timeouts although copies may land: verify, never guess."""

    def setUp(self):
        a.ACTIVE_JOB = {'id': 7, 'lease_token': 'lease'}
        self.payload = {'storage_version': 1, 'bot_id': 1, 'code': 'b', 'pool': -100, 'chat': '@src',
                        'from': 1, 'to': 101, 'next_id': 1, 'published_count': 0}
        self.storage = []          # messages really present in the storage channel
        self.next_dest = 5000
        self.published = []; self.cursor = 1; self.total = 0
        self.calls = []            # sizes of forward requests
        self.real = set(range(1, 102, 2))  # 51 real messages, the rest are deleted ids (gaps)
        self.clock = 0.0

    async def fake_sleep(self, seconds):
        self.clock += seconds

    def src(self, i):
        return SimpleNamespace(id=i, action=None, noforwards=False, reply_markup=None, message=f'cap {i}',
                               media=SimpleNamespace(document=SimpleNamespace(id=900000 + i)))

    def land(self, ids):
        out = []
        for i in ids:
            self.next_dest += 1
            m = SimpleNamespace(id=self.next_dest, action=None, message=f'cap {i}', reply_markup=None,
                                media=SimpleNamespace(document=SimpleNamespace(id=900000 + i)), src=i)
            self.storage.append(m); out.append(m)
        return out

    def client(self, behaviour):
        async def get(chat, ids=None, limit=None, min_id=None):
            if ids is not None:
                return [self.src(i) if i in self.real else None for i in ids]
            msgs = [m for m in self.storage if m.id > (min_id or 0)]
            return sorted(msgs, key=lambda m: -m.id)[:limit]

        async def forward(dest, ids, source, **kw):
            self.calls.append(len(ids))
            return behaviour(list(ids))
        return SimpleNamespace(get_messages=AsyncMock(side_effect=get), forward_messages=AsyncMock(side_effect=forward),
                               _request_retries=5, requests=[])

    async def publish(self, method, path, body):
        self.published.append(body)
        if body['expected_next_id'] == self.cursor:
            self.total += len(body['msg_ids']); self.cursor = body['next_id']
        return {'ok': True, 'next_id': self.cursor, 'total': self.total}

    async def run_job(self, client, payload=None):
        with patch.object(a, 'resolve_source', AsyncMock(return_value=SimpleNamespace(noforwards=False))), \
             patch.object(a, 'resolve_pool', AsyncMock(return_value='pool')), \
             patch.object(a, 'worker_request', AsyncMock(side_effect=self.publish)), \
             patch.object(a.asyncio, 'sleep', AsyncMock(side_effect=self.fake_sleep)), \
             patch.object(a, '_monotonic', lambda: self.clock):
            await a.fetch_range(client, payload or self.payload)

    def stored_sources(self):
        ids = [x for b in self.published for x in b['msg_ids']]
        by = {m.id: m.src for m in self.storage}
        return [by[i] for i in ids]

    async def test_busy_but_landed_copies_are_adopted_without_forwarding_again(self):
        def busy_after_landing(ids):
            self.land(ids); raise BUSY
        client = self.client(busy_after_landing)
        await self.run_job(client)
        self.assertEqual(self.calls, [50, 1])  # 100-id chunk has 50 real msgs; id 101 in second chunk
        self.assertEqual(self.stored_sources(), sorted(self.real))
        self.assertEqual(len(self.storage), 51, 'no duplicate copies')
        self.assertTrue(self.published[-1]['done']); self.assertEqual(self.total, 51)

    async def test_telethon_valueerror_after_retries_is_verified_too(self):
        def gave_up(ids):
            self.land(ids); raise ValueError('Request was unsuccessful 6 time(s)')
        await self.run_job(self.client(gave_up))
        self.assertEqual(self.total, 51); self.assertEqual(len(self.storage), 51)

    async def test_busy_then_slow_landing_never_sends_a_different_request(self):
        # Telegram is still working (busy), copies trickle in over time: wait, never split.
        pending = []
        def busy(ids):
            # Telegram starts the forward once per random_id set; repeats change nothing.
            have = {m.src for m in self.storage} | set(pending)
            pending.extend(i for i in ids if i not in have)
            raise BUSY
        client = self.client(busy)
        orig = client.get_messages.side_effect
        async def trickle(chat, ids=None, limit=None, min_id=None):
            if ids is None and pending:
                self.land(pending[:7]); del pending[:7]   # ~7 copies land per storage check
            return await orig(chat, ids=ids, limit=limit, min_id=min_id)
        client.get_messages.side_effect = trickle
        await self.run_job(client)
        self.assertTrue(all(n == 50 for n in self.calls[:-1]), self.calls)   # only the full chunk, re-sent
        first_chunk = [r for r in client.requests if len(r[0]) == 50]
        self.assertTrue(all(r == first_chunk[0] for r in first_chunk), 'identical ids AND random_ids on every re-send')
        self.assertEqual(self.stored_sources(), sorted(self.real)); self.assertEqual(len(self.storage), 51, 'no duplicates')
        self.assertGreater(len([b for b in self.published if b['msg_ids']]), 3, 'progress published as copies land')
        self.assertTrue(self.published[-1]['done'])

    async def test_partly_landed_prefix_is_published_and_identical_request_finishes_it(self):
        state = {'first': True}
        def half(ids):
            if state['first']:
                state['first'] = False; self.land(ids[:10]); raise BUSY
            # Telegram dedups the repeated random_ids: it lands only what is still missing.
            have = {m.src for m in self.storage}
            self.land([i for i in ids if i not in have])
            by = {m.src: m for m in self.storage}
            return [by[i] for i in ids]
        client = self.client(half)
        await self.run_job(client)
        self.assertEqual(self.calls[:2], [50, 50])
        self.assertEqual(client.requests[0], client.requests[1])
        self.assertEqual(self.stored_sources(), sorted(self.real)); self.assertEqual(len(self.storage), 51)
        self.assertEqual(len(self.published[0]['msg_ids']), 10)

    async def test_checkpoint_cursor_stops_right_after_the_last_confirmed_source(self):
        state = {'first': True}
        def half(ids):
            if state['first']:
                state['first'] = False; self.land(ids[:3]); raise BUSY
            have = {m.src for m in self.storage}
            self.land([i for i in ids if i not in have])
            by = {m.src: m for m in self.storage}
            return [by[i] for i in ids]
        await self.run_job(self.client(half))
        first = self.published[0]
        self.assertEqual(first['source_ids'], [1, 3, 5]); self.assertEqual(first['next_id'], 6)

    async def test_persistent_failure_stops_with_retryable_error_and_keeps_progress(self):
        def never(ids): raise BUSY
        client = self.client(never)
        with self.assertRaises(a.StorageNotConfirmed): await self.run_job(client)
        self.assertTrue(all(n == 50 for n in self.calls)); self.assertEqual(len({tuple(map(tuple, r)) for r in client.requests}), 1)
        self.assertGreaterEqual(self.clock, a.FORWARD_STALL_SECONDS); self.assertEqual(self.published, [])
        self.assertEqual(client._request_retries, 5, 'client retry setting restored')

    async def test_each_chunk_is_one_request_with_its_own_random_ids(self):
        client = self.client(lambda ids: self.land(ids))
        await self.run_job(client)
        self.assertEqual(self.calls, [50, 1])
        ids = [x for r in client.requests for x in r[1]]
        self.assertEqual(len(ids), len(set(ids)), 'random_ids are unique per message and per chunk')

    async def test_progress_moves_while_telegram_still_holds_the_request(self):
        pending, answered = [], []
        client = self.client(lambda ids: None)
        async def held(dest, ids, source, **kw):
            self.calls.append(len(ids)); pending.extend(ids)
            while pending:                      # Telegram answers only when everything landed
                await a.asyncio.sleep(0)        # (patched sleep) give the poller a turn
                await asyncio_real_sleep(0.001)
            answered.append(len(self.published))
            by = {m.src: m for m in self.storage}
            return [by[i] for i in ids]
        client.forward_messages = held
        orig = client.get_messages.side_effect
        async def trickle(chat, ids=None, limit=None, min_id=None):
            if ids is None and pending:
                self.land(pending[:10]); del pending[:10]
            return await orig(chat, ids=ids, limit=limit, min_id=min_id)
        client.get_messages.side_effect = trickle
        with patch.object(a, 'FORWARD_POLL_SECONDS', 0.001):
            await self.run_job(client)
        self.assertEqual(answered, [], 'all copies confirmed from storage before Telegram even answered')
        self.assertGreater(len([b for b in self.published if b['msg_ids']]), 3, 'progress published in steps as copies land')
        self.assertEqual(self.calls, [50, 1]); self.assertEqual(len(self.storage), 51)
        self.assertEqual(self.stored_sources(), sorted(self.real)); self.assertTrue(self.published[-1]['done'])

    async def test_other_posts_in_storage_are_not_adopted(self):
        def busy(ids):
            other = SimpleNamespace(id=self.next_dest + 1, action=None, message='someone else', reply_markup=None,
                                    media=SimpleNamespace(document=SimpleNamespace(id=1)), src=None)
            self.next_dest += 1; self.storage.append(other)
            self.land(ids); raise BUSY
        await self.run_job(self.client(busy))
        self.assertNotIn(None, self.stored_sources()); self.assertEqual(self.total, 51)

    async def test_flood_wait_is_not_treated_as_unconfirmed(self):
        def flood(ids): raise a.FloodWaitError(request=None, capture=30)
        with self.assertRaises(a.FloodWaitError): await self.run_job(self.client(flood))
        self.assertEqual(self.calls, [50])

    async def test_worker_loop_retries_unconfirmed_storage_later_not_permanently(self):
        jobs = [{'id': 1, 'kind': 'fetch_range', 'payload': {}}, a.asyncio.CancelledError()]
        with patch.object(a, 'CLIENT', object()), patch.object(a, 'SESSION_SWITCHING', False), \
             patch.object(a, 'pull_job', AsyncMock(side_effect=jobs)), \
             patch.object(a, 'fetch_range', AsyncMock(side_effect=a.StorageNotConfirmed('x'))), \
             patch.object(a, 'send_ack', AsyncMock()) as ack, patch.object(a.asyncio, 'sleep', AsyncMock()):
            with self.assertRaises(a.asyncio.CancelledError): await a.worker_loop()
        self.assertEqual(ack.await_args.kwargs.get('retry_after'), 60)
        self.assertNotIn('permanent', ack.await_args.kwargs)

    def test_link_preview_messages_match_by_text(self):
        src = SimpleNamespace(message='hi', media=type('MessageMediaWebPage', (), {})())
        copy = SimpleNamespace(message='hi', media=None)
        self.assertEqual(a.copy_signature(src), a.copy_signature(copy))


if __name__ == '__main__':
    unittest.main()
