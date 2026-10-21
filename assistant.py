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
import contextlib
import hmac
import hashlib
import socket
import json
import logging
import os
import re
import threading
import time
from urllib.parse import urlsplit, parse_qs, unquote
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from telethon import TelegramClient
from telethon import errors as tg_errors
from telethon.errors import (
    FloodWaitError,
    ChatForwardsRestrictedError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    AdminsTooMuchError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    UserAlreadyParticipantError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import EditAdminRequest
from telethon.tl.functions.messages import ForwardMessagesRequest, ImportChatInviteRequest
from telethon.tl.types import ChatAdminRights, MessageEmpty

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

POLL_IDLE = int(os.getenv("POLL_IDLE", "60"))      # fallback poll when nobody knocks

# Polling alone is far too slow to feel instant: a job queued one second after
# a poll waits a whole POLL_IDLE before anyone looks at it, and on Render's
# free tier the dyno may be asleep entirely. So the Worker knocks on /wake the
# moment it queues something -- that HTTP request both wakes the dyno and
# releases this event, which is the difference between ~60s and ~2s.
WAKE: "asyncio.Event | None" = None
PROMOTE_GAP = int(os.getenv("PROMOTE_GAP", "45"))  # min seconds between promotions
PORT = int(os.getenv("PORT", "10000"))

HEADERS = {"Authorization": f"Bearer {SHARED_SECRET}", "X-Filebot-Protocol": "2", "X-Filebot-Storage-Protocol": "2"}

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
async def pull_job() -> dict | None:
    try:
        return (await worker_request("GET", "pull")).get("job")
    except Exception:
        log.warning("job poll unavailable")
        return None


async def ack(job_id: int, ok: bool, error: str | None = None) -> None:
    job = ACTIVE_JOB
    if not job or job["id"] != job_id:
        raise RuntimeError("No matching active job")
    await send_ack(job, ok, error)


class WorkerAPIError(RuntimeError):
    def __init__(self, status=0):
        self.status = status
        super().__init__(f"Worker API request failed (HTTP {status})")


class PermanentJobError(RuntimeError):
    pass


ACTIVE_JOB = None
SESSION_SWITCHING = False
OPERATIONS = {}


def worker_request_sync(method, path, payload=None, timeout=20):
    # Do not expose response bodies, auth headers, Mongo URIs or login inputs in errors.
    r = requests.request(method, f"{WORKER_URL}/assistant/{path}", headers=HEADERS,
                         json=payload, timeout=timeout)
    if not 200 <= r.status_code < 300:
        raise WorkerAPIError(r.status_code)
    try:
        data = r.json()
    except ValueError:
        raise WorkerAPIError(r.status_code) from None
    if not isinstance(data, dict) or data.get("ok") is False:
        raise WorkerAPIError(r.status_code)
    return data


async def worker_request(method, path, payload=None, timeout=20):
    return await asyncio.to_thread(worker_request_sync, method, path, payload, timeout)


def job_fields():
    job = ACTIVE_JOB or {}
    return {"job_id": job.get("id"), "lease_token": job.get("lease_token")}


async def send_ack(job, ok, error=None, **extra):
    payload = {"job_id": job["id"], "lease_token": job.get("lease_token"),
               "ok": ok, "error": error, **extra}
    for attempt in range(3):
        try:
            return await worker_request("POST", "ack", payload)
        except WorkerAPIError as exc:
            if exc.status == 409 or (400 <= exc.status < 500 and exc.status != 429):
                raise
        except requests.RequestException:
            pass
        if attempt < 2:
            await asyncio.sleep(2 ** attempt)
    raise WorkerAPIError()


async def lease_heartbeat(job):
    last_ok = time.monotonic()
    while True:
        await asyncio.sleep(25)
        try:
            await worker_request("POST", "heartbeat",
                                 {"job_id": job["id"], "lease_token": job["lease_token"]}, timeout=10)
            last_ok = time.monotonic()
        except WorkerAPIError as exc:
            if exc.status == 409:
                raise RuntimeError("job lease lost") from None
            if time.monotonic() - last_ok > 80:
                raise RuntimeError("lease renewal unavailable") from None
        except requests.RequestException:
            if time.monotonic() - last_ok > 80:
                raise RuntimeError("lease renewal unavailable") from None


async def resolve_source(client, chat):
    try:
        return await client.get_entity(chat)
    except (ValueError, TypeError):
        if isinstance(chat, int) or str(chat).lstrip('-').isdigit():
            # StringSession does not persist the entity cache. Only inspect dialogs this account can access.
            dialogs = await client.get_dialogs(limit=200)
            for dialog in dialogs:
                if int(dialog.id) == int(chat):
                    return dialog.entity
        raise PermanentJobError("Source channel is not resolvable by the assistant. Check account access and channel reference.") from None


async def dispatch_api(path, body, key):
    # Work stays on the asyncio loop; concurrent/retried HTTP requests share one operation.
    for old in [k for k, v in OPERATIONS.items() if v[0] < time.monotonic()-600 and v[1].done()]:
        OPERATIONS.pop(old, None)
    if key and len(key) > 200:
        return {"ok": False, "error": "invalid_request_key"}
    fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    identity = (path, key)
    if key and identity in OPERATIONS:
        _, task, previous = OPERATIONS[identity]
        if previous != fingerprint:
            return {"ok": False, "error": "request_key_conflict"}
    else:
        if len(OPERATIONS) >= 128:
            return {"ok": False, "error": "busy_try_again"}
        task = asyncio.create_task(ROUTES[path](body))
        if key:
            OPERATIONS[identity] = (time.monotonic(), task, fingerprint)
    return await asyncio.shield(task)


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

    bot_id = int(payload.get("bot_id") or 0)
    channel = await resolve_pool(client, int(pool), bot_id)
    try:
        bot = await client.get_entity(username)
    except (ValueError, TypeError):
        raise PermanentJobError("Clone bot username cannot be resolved; check its current username.") from None
    if not getattr(bot, "bot", False) or int(bot.id) != bot_id:
        raise PermanentJobError("Clone username does not match the expected bot identity")

    # No invite step. Telegram refuses to add a bot to a channel as a plain
    # member -- "Bots can only be admins in channels" -- and EditAdmin below
    # both adds and promotes it in one call. Inviting first simply failed
    # every time and the clone never reached the storage channel.

    try:
        await client(EditAdminRequest(
        channel=channel,
        user_id=bot,
        admin_rights=BOT_RIGHTS,
        rank="storage",
    ))

    except AdminsTooMuchError:
        # 50 admins is a hard channel limit. Mark this pool full, take the
        # next one, and promote there -- otherwise every clone created after
        # the 50th would silently fail.
        log.warning("pool %s is out of admin slots, switching", pool)
        r = await worker_request("POST", "nextpool", {"full": int(pool), "bot_id": bot_id, **job_fields()})
        if not r.get("ok"):
            raise RuntimeError(r.get("error") or "no free storage channel")
        pool = r["pool"]
        channel = await resolve_pool(client, int(pool), bot_id)
        await client(EditAdminRequest(
        channel=channel,
        user_id=bot,
        admin_rights=BOT_RIGHTS,
        rank="storage",
    ))
    log.info("clone promotion complete")
    _last_promote = time.time()
    return {"pool": int(pool)}


async def resolve_pool(client: TelegramClient, pool: int, bot_id: int):
    """
    Get a usable entity for a storage channel.

    Telethon can only address a channel this account has seen before. A fresh
    session has seen nothing, so the very first job always failed with
    "Could not find the input entity". Rather than make the owner add the
    account by hand, ask the Worker for an invite link (the bot is already an
    admin there) and join.
    """
    try:
        return await client.get_entity(pool)
    except (ValueError, TypeError):
        pass

    log.info("not a member of pool %s yet - requesting an invite", pool)
    data = await worker_request("POST", "invite", {"chat": pool, "bot_id": bot_id, **job_fields()})
    if not data.get("ok"):
        raise RuntimeError(f"could not get an invite for {pool}: {data.get('error')}")

    invite = data["invite"]
    hash_ = invite.rstrip("/").split("/")[-1].lstrip("+")
    try:
        await client(ImportChatInviteRequest(hash_))
        log.info("joined pool %s", pool)
    except UserAlreadyParticipantError:
        log.info("already in pool %s, just needed the entity", pool)
    except FloodWaitError as e:
        log.warning("flood wait %ss joining pool", e.seconds)
        raise

    # the join populates the session cache, so this now resolves
    entity = await client.get_entity(pool)

    # Joining only makes us a member; a storage channel accepts posts from
    # admins only. Ask the bot to promote us -- it can if it was given "add
    # new admins", and if it was not the owner has to do it once by hand.
    me = await client.get_me()
    try:
        pr = await worker_request("POST", "promote", {"chat": pool, "user_id": me.id, "bot_id": bot_id, **job_fields()})
        if pr.get("ok"):
            log.info("promoted to admin in %s", pool)
        else:
            log.warning("could not self-promote in %s: %s", pool, pr.get("error"))
    except Exception as e:                          # noqa: BLE001
        log.warning("promotion request failed (%s)", type(e).__name__)

    return entity


QUALITY_RUNGS = (2160, 1440, 1080, 720, 480, 360, 240, 144)


def _quality(height, name, caption):
    """Same rules as the Worker: the name usually says it, height is a fallback."""
    hay = f"{name or ''} {caption or ''}"
    m = re.search(r"\b(\d{3,4})\s?[pP]\b", hay)
    if m:
        return f"{m.group(1)}p"
    if re.search(r"\b(4k|2160|uhd)\b", hay, re.I):
        return "2160p"
    if re.search(r"\b(fhd|full\s?hd)\b", hay, re.I):
        return "1080p"
    if re.search(r"\bhd\b", hay, re.I):
        return "720p"
    if not height:
        return None
    for h in QUALITY_RUNGS:
        if height >= h * 0.9:
            return f"{h}p"
    return f"{height}p"


def file_meta(m):
    """
    What the Worker needs for {file_name} and friends.

    Only this service ever sees these messages, so anything not collected
    here is gone -- captions on assistant-built links rendered empty because
    nothing was collected at all.
    """
    doc = getattr(m, "document", None)
    name = size = caption = height = duration = None
    caption = getattr(m, "message", None) or None

    if doc is not None:
        size = getattr(doc, "size", None)
        for attr in getattr(doc, "attributes", []) or []:
            if getattr(attr, "file_name", None):
                name = attr.file_name
            if getattr(attr, "h", None):
                height = attr.h
            if getattr(attr, "duration", None):
                duration = int(attr.duration)
    elif getattr(m, "photo", None) is not None:
        name = "photo.jpg"

    return {
        "name": name,
        "size": size,
        "caption": caption,
        "quality": _quality(height, name, caption),
        "duration": duration,
    }


async def publish_storage_checkpoint(body):
    # Retry publication, not Telegram forwarding: avoid re-copying accepted messages.
    for attempt in range(3):
        try:
            return await worker_request("POST", "refs", body)
        except WorkerAPIError as exc:
            if exc.status < 500 or attempt == 2:
                raise
        except (requests.RequestException, TimeoutError):
            if attempt == 2:
                raise
        await asyncio.sleep(0.25 * (2 ** attempt))



# ---------------------------------------------------------------- confirmed forwarding
# How the official Telegram apps forward 100 messages, and how this engine does it too:
#   * ONE messages.forwardMessages request for the whole chunk (up to 100 ids), carrying one
#     random_id per message. Telegram de-duplicates by random_id, so re-sending that exact
#     request can never create a second copy.
#   * Telegram may answer WORKER_BUSY_TOO_LONG_RETRY / time out while it is still carrying the
#     forward out server-side (slow, rate-limited accounts see this constantly). The apps simply
#     wait and repeat the identical request; they never send a different, smaller request for
#     "missing" messages, because those are usually still being processed.
# So: an unanswered forward is never guessed and never split. The storage channel is read
# back every few seconds; copies that really landed (matched to their source, in order) are
# published at once so the user's progress moves, and the identical request is repeated only
# while nothing new arrives. The earlier step-down (100 -> 20 -> 5 -> 1) sent NEW requests for
# copies Telegram had not finished yet, which produced duplicate storage copies and minutes of
# extra work; it is gone.
FORWARD_POLL_SECONDS = 2        # read the storage channel this often while Telegram works
FORWARD_RESEND_SECONDS = 12     # repeat the identical request after this long without new copies
FORWARD_STALL_SECONDS = 150     # give up (job retried later) after this long without any new copy
UNCONFIRMED_ERRORS = (
    ValueError,                      # Telethon: "Request was unsuccessful N time(s)"
    tg_errors.ServerError,           # WorkerBusyTooLongRetryError, RandomIdDuplicateError, ...
    tg_errors.RpcCallFailError,
    asyncio.TimeoutError,
    ConnectionError,
) + tuple(e for e in (getattr(tg_errors, "TimedOutError", None),
                      getattr(tg_errors, "InterdcCallErrorError", None)) if e is not None)


class StorageNotConfirmed(RuntimeError):
    """Telegram stopped making progress on a storage forward; the job is retried later."""


def _media_key(m):
    media = getattr(m, "media", None)
    # A link preview is not a stored file and may be rebuilt on copy: match by text only.
    if media is None or type(media).__name__ in ("MessageMediaWebPage", "MessageMediaEmpty"):
        return None
    for attr in ("photo", "document"):
        obj = getattr(media, attr, None)
        if obj is not None and getattr(obj, "id", None) is not None:
            return (attr, obj.id)
    return (type(media).__name__,)


def copy_signature(m):
    """Forwarded copies keep the same photo/document id and caption as the source."""
    return (_media_key(m), (getattr(m, "message", None) or "").strip())


def _real(messages):
    return sorted((m for m in (messages or []) if m is not None and not isinstance(m, MessageEmpty)
                   and not getattr(m, "action", None)), key=lambda m: m.id)


async def latest_message_id(client, dest) -> int:
    newest = await client.get_messages(dest, limit=1)
    newest = [m for m in (newest or []) if m is not None and not isinstance(m, MessageEmpty)]
    return int(newest[0].id) if newest else 0


async def landed_copies(client, dest, after_id: int, sources: list) -> list:
    """Copies of `sources` (in order) that reached `dest` after message `after_id`."""
    window = _real(await client.get_messages(dest, min_id=after_id, limit=min(400, len(sources) * 3 + 50)))
    found, j = [], 0
    for src in sources:
        sig = copy_signature(src)
        while j < len(window) and copy_signature(window[j]) != sig:
            j += 1
        if j >= len(window):
            break
        found.append(window[j])
        j += 1
    return found


@contextlib.contextmanager
def few_internal_retries(client, retries=0):
    # Telethon would re-send an unanswered request ~6 times (~40 s) before failing. Here each
    # attempt is followed by a storage read-back instead, so Telethon itself tries only once.
    old = getattr(client, "_request_retries", None)
    if old is not None:
        client._request_retries = retries
    try:
        yield
    finally:
        if old is not None:
            client._request_retries = old


def build_forward_request(source, dest, ids, keep_tag):
    """One request, fixed random_ids: every re-send is the same request to Telegram."""
    return ForwardMessagesRequest(from_peer=source, id=list(ids), to_peer=dest, drop_author=not keep_tag)


async def _invoke_forward(client, req, dest):
    with few_internal_retries(client):
        result = await client(req)
    copies = client._get_response_message(req, result, dest)
    return copies if isinstance(copies, list) else [copies]


def _monotonic():
    return time.monotonic()


def _forward_outcome(copied, part, landed):
    if len(copied) == len(part) and all(m is not None and not isinstance(m, MessageEmpty) for m in copied):
        return copied, "ok"
    prefix = []
    if len(copied) == len(part):
        for message in copied:
            if message is None or isinstance(message, MessageEmpty):
                break
            prefix.append(message)
    return (prefix if len(prefix) >= len(landed) else landed), "incomplete"


async def forward_chunk(client, dest, source, part: list, keep_tag: bool, after_id: int, progress=None):
    """
    Forward `part` (source Message objects, ascending, at most 100) with ONE request.
    While Telegram works, the storage channel is followed and `progress(copies)` is awaited
    for every newly confirmed prefix. Returns (copies, status):
      "ok"          every message copied; copies align with part
      "incomplete"  Telegram answered with gaps; copies is the aligned leading prefix
    Raises StorageNotConfirmed when Telegram makes no progress for FORWARD_STALL_SECONDS.
    """
    req = build_forward_request(source, dest, [m.id for m in part], keep_tag)
    landed: list = []
    last_new = sent_at = _monotonic()
    attempts, task = 0, None
    try:
        while True:
            if task is None:
                attempts += 1
                sent_at = _monotonic()
                task = asyncio.ensure_future(_invoke_forward(client, req, dest))
            # Let Telegram answer; meanwhile keep the user's progress moving from storage.
            await asyncio.wait({task}, timeout=FORWARD_POLL_SECONDS)
            if task.done():
                finished, task = task, None
                try:
                    copied = finished.result()
                except FloodWaitError:
                    raise
                except UNCONFIRMED_ERRORS as exc:
                    if attempts == 1:
                        log.info("storage forward of %d still running at Telegram (%s); following it in storage",
                                 len(part), type(exc).__name__)
                    await asyncio.sleep(FORWARD_POLL_SECONDS)
                else:
                    return _forward_outcome(copied, part, landed)
            found = await landed_copies(client, dest, after_id, part)
            if len(found) > len(landed):
                landed, last_new = found, _monotonic()
                if progress is not None:
                    await progress(landed)
                if len(landed) == len(part):
                    log.info("storage forward of %d confirmed in storage after %d request(s)", len(part), attempts)
                    return landed, "ok"
            if _monotonic() - last_new >= FORWARD_STALL_SECONDS:
                log.warning("storage forward stalled: %d of %d copies after %d request(s)", len(landed), len(part), attempts)
                raise StorageNotConfirmed("Telegram did not confirm the storage copy")
            if task is None and _monotonic() - max(sent_at, last_new) < FORWARD_RESEND_SECONDS:
                # No answer yet and copies may still be arriving: keep watching before repeating.
                while _monotonic() - max(sent_at, last_new) < FORWARD_RESEND_SECONDS:
                    await asyncio.sleep(FORWARD_POLL_SECONDS)
                    found = await landed_copies(client, dest, after_id, part)
                    if len(found) > len(landed):
                        landed, last_new = found, _monotonic()
                        if progress is not None:
                            await progress(landed)
                        if len(landed) == len(part):
                            log.info("storage forward of %d confirmed in storage after %d request(s)", len(part), attempts)
                            return landed, "ok"
                    if _monotonic() - last_new >= FORWARD_STALL_SECONDS:
                        log.warning("storage forward stalled: %d of %d copies after %d request(s)", len(landed), len(part), attempts)
                        raise StorageNotConfirmed("Telegram did not confirm the storage copy")
            # Loop: task None -> repeat the identical request (same random_ids: no duplicates).
    finally:
        if task is not None and not task.done():
            task.cancel()


async def fetch_range(client: TelegramClient, payload: dict) -> None:
    if payload.get("storage_version") != 1:
        return await fetch_range_legacy(client, payload)
    first, last = int(payload["from"]), int(payload["to"])
    cursor = int(payload.get("next_id", first))
    pool, bot_id, code = int(payload["pool"]), int(payload["bot_id"]), payload["code"]
    if first < 1 or last < first or last-first > 200000 or not first <= cursor <= last+1:
        raise PermanentJobError("Invalid storage range or checkpoint")
    source = await resolve_source(client, payload["chat"])
    if getattr(source, "noforwards", False):
        raise PermanentJobError("Telegram protects this source from forwarding")
    # Pinned destination for the entire batch; no round-robin storage.
    dest = await resolve_pool(client, pool, bot_id)
    published = int(payload.get("published_count", 0))
    after_id = None  # newest storage message before our next forward (for verification)

    async def publish(next_id, sources, copied, done=False):
        nonlocal cursor, published
        body = {"storage_version": 1, "bot_id": bot_id, "code": code, "chat_id": pool,
                "expected_next_id": cursor, "next_id": next_id, "source_ids": sources,
                "msg_ids": [m.id for m in copied], "meta": [file_meta(m) for m in copied],
                "done": done, **job_fields()}
        if payload.get("notice_msg") is not None:
            body["notice_msg"] = payload["notice_msg"]
        if payload.get("total") is not None:
            body["total"] = payload["total"]
        result = await publish_storage_checkpoint(body)
        cursor = int(result["next_id"])
        published = int(result["total"])

    while cursor <= last:
        size = 100
        end = min(last, cursor + size - 1)
        requested = list(range(cursor, end + 1))
        messages = await client.get_messages(source, ids=requested)
        usable = sorted((m for m in messages if m is not None and not isinstance(m, MessageEmpty) and not getattr(m, "action", None)), key=lambda m: m.id)
        if any(m.id not in requested for m in usable) or len({m.id for m in usable}) != len(usable):
            raise PermanentJobError("Unexpected source message ordering")
        if any(getattr(m, "noforwards", False) for m in usable):
            raise PermanentJobError("Telegram protects a source message from forwarding")
        if not usable:
            await publish(end + 1, [], [])
            continue
        # Keep one maximal operation even when some messages carry keyboards: forward the
        # whole chunk with attribution when needed to preserve those buttons.
        keep_tag = any(getattr(m, 'reply_markup', None) is not None for m in usable)
        if after_id is None:
            after_id = await latest_message_id(client, dest)
        done_in_chunk = 0

        async def confirm(copies):
            # Publish the newly confirmed prefix at once so Completed/Remaining move live.
            nonlocal done_in_chunk
            if len(copies) <= done_in_chunk:
                return
            if any(not isinstance(m.id, int) or m.id <= 0 or (i and m.id <= copies[i-1].id) for i, m in enumerate(copies)):
                raise PermanentJobError("Unexpected destination message ordering")
            n = len(copies)
            await publish(end + 1 if n == len(usable) else usable[n-1].id + 1,
                          [m.id for m in usable[done_in_chunk:n]], copies[done_in_chunk:n])
            done_in_chunk = n

        # One forward request for the whole chunk (Telegram's maximum is 100 messages).
        copied, status = await forward_chunk(client, dest, source, usable, keep_tag, after_id, confirm)
        await confirm(copied)
        if copied:
            after_id = max(after_id, copied[-1].id)
        if status == "incomplete":
            # Never silently count a partial forward as complete: only the confirmed
            # leading prefix was published; stop for an explicit operator-visible failure.
            raise PermanentJobError("Telegram returned an incomplete storage batch; remaining files were not marked stored")
    await publish(last+1, [], [], done=True)
    log.info("checkpointed storage complete: %d confirmed messages", published)


async def hydrate_storage(client, payload):
    if payload.get("metadata_done"):
        return
    dest = await resolve_pool(client, int(payload["pool"]), int(payload["bot_id"]))
    refs = payload["refs"]
    if not isinstance(refs, list) or not 1 <= len(refs) <= 100:
        raise PermanentJobError("Invalid metadata batch")
    messages = await client.get_messages(dest, ids=[ref["message_id"] for ref in refs])
    by_id = {m.id: m for m in messages if m is not None and not isinstance(m, MessageEmpty)}
    if any(ref["message_id"] not in by_id for ref in refs):
        raise PermanentJobError("A stored message is unavailable for metadata inspection")
    rows = [{**file_meta(by_id[ref["message_id"]]), **ref} for ref in refs]
    await worker_request("POST", "storage-meta", {"bot_id": payload["bot_id"], "code": payload["code"],
        "chat_id": payload["pool"], "rows": rows, **job_fields()})


async def fetch_range_legacy(client: TelegramClient, payload: dict) -> None:
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

    if first < 1 or last < first or last-first > 200000:
        raise PermanentJobError("Invalid or excessive message range")
    source = await resolve_source(client, chat)
    if getattr(source, "noforwards", False):
        raise PermanentJobError("Telegram protects this source from forwarding. Use an authorized, unrestricted source.")
    dest = await resolve_pool(client, pool, bot_id)

    ids = list(range(first, last + 1))
    log.info("fetch_range %s %s..%s (%d messages)", chat, first, last, len(ids))

    sent_total = 0
    first_publish = True
    # Legacy queued ranges also use maximum-size chunks, without a warm-up.
    def batches(seq: list[int]):
        for i in range(0,len(seq),100):
            yield seq[i:i+100]

    for chunk in batches(ids):

        # drop ids that do not exist, otherwise the whole call fails
        msgs = await client.get_messages(source, ids=chunk)
        usable = sorted((m for m in msgs if m is not None and not isinstance(m, MessageEmpty) and not getattr(m, "action", None)), key=lambda m:m.id)
        if any(getattr(m,"noforwards",False) for m in usable):
            raise PermanentJobError("Telegram protects a source message from forwarding")
        if not usable:
            continue

        # Keep one ordered operation per chunk. Preserve keyboards by retaining
        # attribution for the whole chunk when any source message needs it.
        runs=[(any(getattr(m,"reply_markup",None) is not None for m in usable),[m.id for m in usable])]

        for keep_tag, run_ids in runs:
            try:
                fwd = await client.forward_messages(
                    dest, run_ids, source, drop_author=not keep_tag,
                )
            except FloodWaitError as e:
                log.warning("flood wait %ss mid-range", e.seconds)
                raise

            # forward_messages returns None in the slots it could not deliver,
            # so the list is not 1:1 with run_ids and must be filtered
            got = [m for m in (fwd if isinstance(fwd, list) else [fwd]) if m is not None and not isinstance(m, MessageEmpty)]
            new_ids = [m.id for m in got]
            new_meta = [file_meta(m) for m in got]
            sent_total += len(new_ids)
            if not new_ids:
                continue

            # Publish each run as soon as it lands. Holding everything until
            # the end made the link unusable for the whole backfill; the user
            # should be receiving file 1 while file 200 is still copying.
            #
            # `reset` clears the placeholder references, so it must fire on
            # the first publish of THIS attempt and never again -- a retry
            # starts from a clean slate instead of duplicating rows.
            await worker_request("POST", "refs", {
                "bot_id": bot_id, "code": code, "chat_id": pool,
                "msg_ids": new_ids, "meta": new_meta, "done": False,
                "reset": first_publish, **job_fields(),
            })
            first_publish = False

    # Nothing left to add -- just mark it finished.
    await worker_request("POST", "refs", {"bot_id": bot_id, "code": code, "chat_id": pool,
              "msg_ids": [], "done": True, "reset": first_publish, **job_fields()})

    log.info("fetch_range done: %d messages", sent_total)


def validate_mongo_uri(uri):
    try:
        parsed = urlsplit(uri)
        host = parsed.hostname or ""
        query = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
        if (parsed.scheme != "mongodb+srv" or not re.fullmatch(r"[a-zA-Z0-9.-]+\.mongodb\.net", host)
                or parsed.port is not None or not parsed.username or not parsed.password
                or any(str(v[0]).lower() in ("false", "0") for k, v in query.items() if k in ("ssl", "tls"))):
            raise ValueError()
        return unquote(parsed.path.strip("/")) or None
    except (ValueError, TypeError):
        raise PermanentJobError("Use a TLS-enabled MongoDB Atlas mongodb+srv URI") from None


def import_mongo_sync(payload: dict, fields: dict, stop=None) -> None:
    """
    Copy a user list out of an existing MongoDB bot into our database.
    The URI is used once and never written to disk or logs.
    """
    from pymongo import MongoClient

    uri = payload["uri"]
    bot_id = payload["bot_id"]
    collection = payload.get("collection")

    database = validate_mongo_uri(uri)
    if stop is not None and stop.is_set(): return
    cli = MongoClient(uri, serverSelectionTimeoutMS=15000, connectTimeoutMS=15000, socketTimeoutMS=15000, tls=True)
    try:
        db_name = [database] if database else cli.list_database_names()
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
            if stop is not None and stop.is_set(): return
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
            if stop is not None and stop.is_set(): return
            worker_request_sync("POST", "import", {"bot_id": bot_id, "users": ids[i:i + 500], **fields}, timeout=30)
    finally:
        cli.close()
        del uri


async def import_mongo(payload: dict) -> None:
    stop = threading.Event()
    try:
        await asyncio.to_thread(import_mongo_sync, payload, job_fields(), stop)
    finally:
        stop.set()


# ---------------------------------------------------------------- main loop
async def worker_loop() -> None:
    global CLIENT, ACTIVE_JOB
    if CLIENT is None:
        candidate = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
        await candidate.connect()
        if not await candidate.is_user_authorized():
            await candidate.disconnect()
            raise RuntimeError("Stored Telegram session is not authorized")
        CLIENT = candidate
    while True:
        while SESSION_SWITCHING:
            await asyncio.sleep(0.2)
        job = await pull_job()
        if not job:
            try:
                await asyncio.wait_for(WAKE.wait(), timeout=POLL_IDLE)
            except asyncio.TimeoutError:
                pass
            WAKE.clear()
            continue
        while SESSION_SWITCHING:
            await asyncio.sleep(0.2)
        ACTIVE_JOB = job
        client = CLIENT  # refresh after every account/session change
        kind, payload = job["kind"], job["payload"]
        async def perform():
            if kind == "promote_clone": return await promote_clone(client, payload)
            if kind == "fetch_range": return await fetch_range(client, payload)
            if kind == "hydrate_storage": return await hydrate_storage(client,payload)
            if kind == "import_mongo": return await import_mongo(payload)
            raise PermanentJobError("Unknown job kind")
        task = asyncio.create_task(perform())
        heartbeat = asyncio.create_task(lease_heartbeat(job)) if job.get("lease_token") else None
        try:
            if heartbeat:
                done, _ = await asyncio.wait({task, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
                if heartbeat in done:
                    await heartbeat
            result = await task
            await send_ack(job, True, **(result or {}))
            log.info("job %s completed", job["id"])
        except asyncio.CancelledError:
            raise
        except FloodWaitError as exc:
            await send_ack(job, False, "Telegram rate limit; retry scheduled", retry_after=exc.seconds+5)
        except (ChatForwardsRestrictedError, ChannelPrivateError, ChatAdminRequiredError) as exc:
            await send_ack(job, False, "Telegram permissions/protection prevent this operation: "+type(exc).__name__, permanent=True)
        except PermanentJobError as exc:
            await send_ack(job, False, str(exc), permanent=True)
        except StorageNotConfirmed as exc:
            await send_ack(job, False, str(exc), retry_after=60)
        except Exception as exc:
            log.warning("job %s failed (%s)", job["id"], type(exc).__name__)
            with contextlib.suppress(Exception):
                await send_ack(job, False, "Job failed: " + type(exc).__name__)
        finally:
            for pending in (task, heartbeat):
                if pending is not None:
                    pending.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await pending
            ACTIVE_JOB = None
        if kind not in ("fetch_range", "hydrate_storage"):
            await asyncio.sleep(2)


# ---------------------------------------------------------------- health server
# ---------------------------------------------------------------- login API
#
# The owner signs in from the bot; the Worker relays login inputs here over HTTPS.
# Never log request bodies, login inputs or session values. This
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
    if not re.fullmatch(r"\+\d{8,15}", str(phone)):
        return {"ok": False, "error": "invalid_phone"}
    if len(PENDING) >= 8:
        return {"ok": False, "error": "too_many_pending_logins"}
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except Exception:
        await client.disconnect()
        raise
    lid = os.urandom(16).hex()
    PENDING[lid] = {"client": client, "phone": phone, "hash": sent.phone_code_hash, "at": time.time()}
    return {"ok": True, "id": lid}


async def _finish(client) -> dict:
    global SESSION, CLIENT, SESSION_SWITCHING
    if ACTIVE_JOB is not None or SESSION_SWITCHING:
        return {"ok": False, "error": "assistant_busy_retry"}
    SESSION_SWITCHING = True
    try:
        session = client.session.save()
        me = await client.get_me()
        if me is None:
            return {"ok": False, "error": "not_authorized"}
        who = me.username or me.first_name or str(me.id)
        await worker_request("POST", "session", {"session": session, "user": who}, timeout=20)
        old = CLIENT
        SESSION, CLIENT = session, client
        if old is not None and old is not client:
            await old.disconnect()
        if WAKE is not None: WAKE.set()
        return {"ok": True, "user": me.username or me.first_name, "id": me.id}
    finally:
        SESSION_SWITCHING = False


async def _login_code(lid: str, code: str) -> dict:
    _prune_pending()
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
        await p["client"].disconnect()
        return {"ok": False, "error": "expired"}
    out = await _finish(p["client"])
    if out.get("ok"): PENDING.pop(lid, None)
    return out


async def _login_password(lid: str, password: str) -> dict:
    _prune_pending()
    p = PENDING.get(lid)
    if not p:
        return {"ok": False, "error": "expired"}
    try:
        await p["client"].sign_in(password=password)
    except PasswordHashInvalidError:
        return {"ok": False, "error": "bad_password"}
    out = await _finish(p["client"])
    if out.get("ok"): PENDING.pop(lid, None)
    return out


async def _wake(_body: dict) -> dict:
    """Release the poll loop right now. Used by the Worker when it queues a job."""
    if WAKE is not None:
        WAKE.set()
    return {"ok": True, "signed_in": SESSION is not None}


async def _diag(body: dict) -> dict:
    """Owner diagnostics (shared-secret protected): measure how fast THIS account forwards."""
    client = CLIENT
    if client is None:
        return {"ok": False, "error": "not_signed_in"}
    action = body.get("action")
    if action == "me":
        me = await client.get_me()
        return {"ok": True, "id": me.id, "dc": client.session.dc_id, "premium": bool(getattr(me, "premium", False)),
                "restricted": bool(getattr(me, "restricted", False)), "scam": bool(getattr(me, "scam", False))}
    if action == "spambot":
        bot = await client.get_entity("SpamBot")
        sent = await client.send_message(bot, "/start")
        for _ in range(10):
            await asyncio.sleep(1.5)
            msgs = await client.get_messages(bot, limit=3, min_id=sent.id)
            if msgs:
                return {"ok": True, "reply": msgs[0].message}
        return {"ok": True, "reply": None}
    if action == "forward":
        src = await resolve_source(client, body["src"])
        dest_spec = body["dest"]
        dest = await client.get_input_entity("me") if dest_spec == "me" else await resolve_pool(client, int(dest_spec), int(body["bot_id"]))
        ids = [int(x) for x in body["ids"]][:100]
        req = build_forward_request(src, dest, ids, not body.get("drop_author", True))
        t = time.monotonic(); err = None; copies = []
        try:
            copies = await _invoke_forward(client, req, dest)   # Telethon default retries for timing
        except Exception as exc:
            err = type(exc).__name__
        first = time.monotonic() - t
        after = int(body.get("after_id", 0))
        if err:  # follow in storage until all land (max 70 s)
            srcs = _real(await client.get_messages(src, ids=ids))
            while time.monotonic() - t < 70:
                await asyncio.sleep(1)
                copies = await landed_copies(client, dest, after, srcs)
                if len(copies) >= len(srcs):
                    break
        elapsed = time.monotonic() - t
        made = [m.id for m in copies if m is not None]
        if body.get("cleanup", True) and made:
            try:
                await client.delete_messages(dest, made)
            except Exception:
                pass
        return {"ok": True, "n": len(ids), "landed": len(made), "first_answer_s": round(first, 2),
                "elapsed_s": round(elapsed, 2), "error": err}
    if action == "latest":
        dest = await resolve_pool(client, int(body["dest"]), int(body["bot_id"]))
        return {"ok": True, "latest": await latest_message_id(client, dest)}
    return {"ok": False, "error": "unknown_action"}


ROUTES = {
    "/diag": _diag,
    "/wake": _wake,
    "/login/start": lambda b: _login_start(b["phone"]),
    "/login/code": lambda b: _login_code(b["id"], b["code"]),
    "/login/password": lambda b: _login_password(b["id"], b["password"]),
}


class Api(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def _send(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        try:
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            pass

    def do_GET(self):
        if self.path not in ("/", "/health"):
            return self._send(404, {"ok": False})
        self._send(200, {"ok": True, "signed_in": bool(SESSION and CLIENT and CLIENT.is_connected()),
                         "protocol": 2, "storage_protocol": 2, "version": "no-dup-2026-10-21"})

    def do_POST(self):
        if not hmac.compare_digest(self.headers.get("authorization", ""), f"Bearer {SHARED_SECRET}"):
            return self._send(401, {"ok": False, "error": "unauthorized"})
        if self.path not in ROUTES:
            return self._send(404, {"ok": False, "error": "no such route"})
        try:
            n = int(self.headers.get("content-length", "0"))
            if n < 0 or n > 16384:
                return self._send(413, {"ok": False, "error": "body_too_large"})
            body = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(body, dict): raise ValueError()
        except (ValueError, socket.timeout):
            return self._send(400, {"ok": False, "error": "invalid_request"})
        try:
            fut = asyncio.run_coroutine_threadsafe(
                dispatch_api(self.path, body, self.headers.get("x-idempotency-key", "")), LOOP)
            self._send(200, fut.result(timeout=85))
        except TimeoutError:
            self._send(503, {"ok": False, "error": "operation_pending_retry_same_request"})
        except (KeyError, TypeError, ValueError):
            self._send(400, {"ok": False, "error": "invalid_request"})
        except Exception as exc:
            log.warning("API operation failed (%s)", type(exc).__name__)
            self._send(503, {"ok": False, "error": "operation_unavailable"})

    def log_message(self, *args):
        pass


def serve_api() -> None:
    ThreadingHTTPServer(("0.0.0.0", PORT), Api).serve_forever()


async def load_session() -> str | None:
    try:
        return (await worker_request("GET", "session")).get("session")
    except Exception:
        log.warning("session store unavailable; will retry")
        return None


async def main() -> None:
    global LOOP, SESSION, WAKE, CLIENT
    LOOP = asyncio.get_running_loop()
    WAKE = asyncio.Event()
    threading.Thread(target=serve_api, daemon=True).start()
    log.info("API listening on :%s", PORT)
    while True:
        if not SESSION:
            SESSION = await load_session()
        if not SESSION:
            await asyncio.sleep(15)
            continue
        try:
            await worker_loop()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("worker loop restarting (%s)", type(exc).__name__)
            if CLIENT is not None:
                with contextlib.suppress(Exception): await CLIENT.disconnect()
            CLIENT = None
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
