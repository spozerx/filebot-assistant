"""
FileBot Assistant — the one job the Worker cannot do itself.

A Telegram bot cannot add another bot to a channel; only a real user account
can. This tiny service runs a Telethon user session, polls the Worker for jobs,
and promotes newly created clone bots into the platform storage channel.

It also runs MongoDB imports, because Workers cannot speak the MongoDB wire
protocol.

Designed for Render's free tier: it sleeps when idle, wakes on an HTTP ping,
and rate-limits itself so Telegram never flags the account.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    UserAlreadyParticipantError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import EditAdminRequest, InviteToChannelRequest
from telethon.tl.types import ChatAdminRights

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("assistant")

# ---------------------------------------------------------------- config
API_ID = int(os.environ["TELETHON_API_ID"])
API_HASH = os.environ["TELETHON_API_HASH"]
WORKER_URL = os.environ["WORKER_URL"].rstrip("/")
SHARED_SECRET = os.environ["ASSISTANT_SHARED_SECRET"]

# The session is NOT an env var. It is created by logging in from the bot and
# stored in the Worker's database, because Render's free tier wipes the disk on
# every restart and an env var would mean redeploying to change accounts.
SESSION: str | None = None
CLIENT = None          # the signed-in Telethon client, shared by login + jobs
LOOP = None            # the asyncio loop, so the HTTP thread can submit work

POLL_IDLE = int(os.getenv("POLL_IDLE", "60"))      # seconds between empty polls
PROMOTE_GAP = int(os.getenv("PROMOTE_GAP", "45"))  # min seconds between promotions
PORT = int(os.getenv("PORT", "10000"))

HEADERS = {"Authorization": f"Bearer {SHARED_SECRET}"}

# Rights a clone bot needs in the storage channel: post, edit and delete its
# own stored messages. Nothing more — no member management, no invites.
BOT_RIGHTS = ChatAdminRights(
    post_messages=True,
    edit_messages=True,
    delete_messages=True,
    change_info=False,
    invite_users=False,
    ban_users=False,
    pin_messages=False,
    add_admins=False,
    anonymous=False,
    manage_call=False,
)

_last_promote = 0.0


# ---------------------------------------------------------------- worker API
def pull_job() -> dict | None:
    try:
        r = requests.get(f"{WORKER_URL}/assistant/pull", headers=HEADERS, timeout=20)
        if r.status_code != 200:
            log.warning("pull failed: HTTP %s", r.status_code)
            return None
        return r.json().get("job")
    except Exception as e:
        log.warning("pull error: %s", e)
        return None


def ack(job_id: int, ok: bool, error: str | None = None) -> None:
    try:
        requests.post(
            f"{WORKER_URL}/assistant/ack",
            headers=HEADERS,
            json={"job_id": job_id, "ok": ok, "error": error},
            timeout=20,
        )
    except Exception as e:
        log.warning("ack error: %s", e)


# ---------------------------------------------------------------- jobs
async def promote_clone(client: TelegramClient, payload: dict) -> None:
    """Add a clone bot to the storage channel and grant it posting rights."""
    global _last_promote

    username = payload["username"]
    pool = payload.get("pool")
    if not pool:
        raise RuntimeError("no pool channel assigned to this clone")

    # self-imposed rate limit: promoting many bots quickly looks like abuse
    wait = PROMOTE_GAP - (time.time() - _last_promote)
    if wait > 0:
        log.info("rate limit: sleeping %.0fs", wait)
        await asyncio.sleep(wait)

    channel = await client.get_entity(int(pool))
    bot = await client.get_entity(username)

    try:
        await client(InviteToChannelRequest(channel=channel, users=[bot]))
        log.info("added @%s to %s", username, pool)
    except UserAlreadyParticipantError:
        log.info("@%s already in %s", username, pool)

    await client(EditAdminRequest(
        channel=channel,
        user_id=bot,
        admin_rights=BOT_RIGHTS,
        rank="storage",
    ))
    log.info("promoted @%s", username)
    _last_promote = time.time()


async def fetch_range(client: TelegramClient, payload: dict) -> None:
    """
    Collect a message range from a channel the BOT cannot read.

    A bot can only copy from chats it belongs to. A user account can read any
    public channel without joining, and can forward up to 100 messages per
    call, so this is both the only way and the fast way.
    """
    chat = payload["chat"]          # @username
    first = int(payload["from"])
    last = int(payload["to"])
    pool = int(payload["pool"])
    code = payload["code"]
    bot_id = int(payload["bot_id"])

    source = await client.get_entity(chat)
    dest = await client.get_entity(pool)

    ids = list(range(first, last + 1))
    log.info("fetch_range %s %s..%s (%d messages)", chat, first, last, len(ids))

    sent_total = 0
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]

        # drop ids that do not exist, otherwise the whole call fails
        msgs = await client.get_messages(source, ids=chunk)
        real = [m.id for m in msgs if m is not None and not m.action]
        if not real:
            continue

        try:
            fwd = await client.forward_messages(dest, real, source)
        except FloodWaitError as e:
            log.warning("flood wait %ss mid-range", e.seconds)
            await asyncio.sleep(min(e.seconds, 300))
            fwd = await client.forward_messages(dest, real, source)

        new_ids = [m.id for m in (fwd if isinstance(fwd, list) else [fwd])]
        sent_total += len(new_ids)

        requests.post(
            f"{WORKER_URL}/assistant/refs",
            headers=HEADERS,
            json={
                "bot_id": bot_id,
                "code": code,
                "chat_id": pool,
                "msg_ids": new_ids,
                "done": i + 100 >= len(ids),
            },
            timeout=30,
        )
        await asyncio.sleep(1)

    log.info("fetch_range done: %d messages", sent_total)


async def import_mongo(payload: dict) -> None:
    """
    Copy a user list out of an existing MongoDB bot into our database.
    The URI is used once and never written to disk or logs.
    """
    from pymongo import MongoClient

    uri = payload["uri"]
    bot_id = payload["bot_id"]
    collection = payload.get("collection")

    cli = MongoClient(uri, serverSelectionTimeoutMS=15000)
    try:
        db_name = cli.list_database_names()
        target = None

        for name in db_name:
            if name in ("admin", "local", "config"):
                continue
            d = cli[name]
            for coll in d.list_collection_names():
                if collection and coll != collection:
                    continue
                sample = d[coll].find_one()
                if sample and ("id" in sample or "_id" in sample or "user_id" in sample):
                    target = d[coll]
                    break
            if target is not None:
                break

        if target is None:
            raise RuntimeError("no user-like collection found")

        ids: list[int] = []
        for doc in target.find({}, {"id": 1, "_id": 1, "user_id": 1}).limit(200000):
            raw = doc.get("id") or doc.get("user_id") or doc.get("_id")
            try:
                n = int(raw)
            except (TypeError, ValueError):
                continue
            if 10000 < n < 10**13:
                ids.append(n)

        log.info("found %d users to import", len(ids))

        # hand them back in batches; the Worker owns all DB writes
        for i in range(0, len(ids), 500):
            requests.post(
                f"{WORKER_URL}/assistant/import",
                headers=HEADERS,
                json={"bot_id": bot_id, "users": ids[i:i + 500]},
                timeout=30,
            )
    finally:
        cli.close()
        del uri


# ---------------------------------------------------------------- main loop
async def worker_loop() -> None:
    global CLIENT
    client = CLIENT
    if client is None:
        client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
        await client.connect()
        CLIENT = client

    me = await client.get_me()
    log.info("assistant signed in as %s (id %s)", me.username or me.first_name, me.id)

    while True:
        job = pull_job()
        if not job:
            await asyncio.sleep(POLL_IDLE)
            continue

        jid, kind, payload = job["id"], job["kind"], job["payload"]
        log.info("job %s: %s", jid, kind)

        try:
            if kind == "promote_clone":
                await promote_clone(client, payload)
            elif kind == "fetch_range":
                await fetch_range(client, payload)
            elif kind == "import_mongo":
                await import_mongo(payload)
            else:
                raise RuntimeError(f"unknown job kind: {kind}")
            ack(jid, True)
            log.info("job %s done", jid)

        except FloodWaitError as e:
            log.warning("flood wait %ss", e.seconds)
            ack(jid, False, f"flood wait {e.seconds}s")
            await asyncio.sleep(min(e.seconds, 300))

        except Exception as e:
            log.error("job %s failed: %s", jid, e)
            ack(jid, False, str(e)[:300])

        await asyncio.sleep(2)


# ---------------------------------------------------------------- health server
# ---------------------------------------------------------------- login API
#
# The owner signs in from inside the bot, so the phone number, the SMS code and
# the 2FA password never leave Telegram. The Worker relays each step here; this
# service is the only place a Telethon client exists.
#
# A login is a multi-step conversation that must keep the SAME client object
# alive between steps (Telethon ties the sent code to that connection), so the
# pending client is held in memory keyed by a random id.
PENDING: dict[str, dict] = {}
PENDING_TTL = 600


def _prune_pending() -> None:
    cut = time.time() - PENDING_TTL
    for k in [k for k, v in PENDING.items() if v["at"] < cut]:
        try:
            asyncio.run_coroutine_threadsafe(PENDING[k]["client"].disconnect(), LOOP)
        except Exception:
            pass
        PENDING.pop(k, None)


async def _login_start(phone: str) -> dict:
    _prune_pending()
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    sent = await client.send_code_request(phone)
    lid = os.urandom(8).hex()
    PENDING[lid] = {"client": client, "phone": phone, "hash": sent.phone_code_hash,
                    "at": time.time()}
    return {"ok": True, "id": lid}


async def _finish(client) -> dict:
    """Persist the session in the Worker and swap it in without a restart."""
    global SESSION, CLIENT
    session = client.session.save()
    me = await client.get_me()
    who = me.username or me.first_name or str(me.id)
    r = requests.post(f"{WORKER_URL}/assistant/session", headers=HEADERS,
                      json={"session": session, "user": who}, timeout=30)
    r.raise_for_status()
    SESSION = session
    CLIENT = client
    log.info("signed in as %s (id %s)", me.username or me.first_name, me.id)
    return {"ok": True, "user": me.username or me.first_name, "id": me.id}


async def _login_code(lid: str, code: str) -> dict:
    p = PENDING.get(lid)
    if not p:
        return {"ok": False, "error": "expired"}
    try:
        await p["client"].sign_in(phone=p["phone"], code=code,
                                  phone_code_hash=p["hash"])
    except SessionPasswordNeededError:
        return {"ok": False, "need_password": True}
    except PhoneCodeInvalidError:
        return {"ok": False, "error": "bad_code"}
    except PhoneCodeExpiredError:
        PENDING.pop(lid, None)
        return {"ok": False, "error": "expired"}
    out = await _finish(p["client"])
    PENDING.pop(lid, None)
    return out


async def _login_password(lid: str, password: str) -> dict:
    p = PENDING.get(lid)
    if not p:
        return {"ok": False, "error": "expired"}
    try:
        await p["client"].sign_in(password=password)
    except PasswordHashInvalidError:
        return {"ok": False, "error": "bad_password"}
    out = await _finish(p["client"])
    PENDING.pop(lid, None)
    return out


ROUTES = {
    "/login/start": lambda b: _login_start(b["phone"]),
    "/login/code": lambda b: _login_code(b["id"], b["code"]),
    "/login/password": lambda b: _login_password(b["id"], b["password"]),
}


class Api(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        self._send(200, {"ok": True, "signed_in": SESSION is not None})

    def do_POST(self):  # noqa: N802
        if self.headers.get("authorization") != f"Bearer {SHARED_SECRET}":
            return self._send(401, {"ok": False, "error": "unauthorized"})
        route = ROUTES.get(self.path)
        if not route:
            return self._send(404, {"ok": False, "error": "no such route"})
        try:
            n = int(self.headers.get("content-length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
            fut = asyncio.run_coroutine_threadsafe(route(body), LOOP)
            self._send(200, fut.result(timeout=90))
        except Exception as e:                      # noqa: BLE001
            log.exception("login step failed")
            self._send(200, {"ok": False, "error": str(e)[:200]})

    def log_message(self, *args):  # silence per-request logging
        pass


def serve_api() -> None:
    """Render web services must bind a port; this also wakes the dyno."""
    HTTPServer(("0.0.0.0", PORT), Api).serve_forever()


def load_session() -> str | None:
    try:
        r = requests.get(f"{WORKER_URL}/assistant/session", headers=HEADERS, timeout=30)
        return r.json().get("session")
    except Exception:                               # noqa: BLE001
        log.warning("could not reach the worker for a session yet")
        return None


async def main() -> None:
    global LOOP, SESSION
    LOOP = asyncio.get_running_loop()
    threading.Thread(target=serve_api, daemon=True).start()
    log.info("api listening on :%s", PORT)

    SESSION = load_session()
    if not SESSION:
        log.info("no session yet - waiting for a login from the bot")
    while not SESSION:
        await asyncio.sleep(5)

    await worker_loop()


if __name__ == "__main__":
    asyncio.run(main())
