# FileBot Assistant — setup

This small service does the one thing the Cloudflare Worker cannot: **add a
clone bot as an admin to the storage channel.** The Telegram Bot API has no
method for a bot to add another bot to a channel, so a real user account has to
do it.

It also runs MongoDB imports, since Workers cannot speak the MongoDB protocol.

It sleeps when idle and wakes when a job appears, so Render's free tier is
plenty.

---

## 1. Create a spare Telegram account

**Do not use your main account.** Use a second number. If Telegram ever flags
this account for promoting lots of bots, you do not want to lose your personal one.

## 2. Get Telegram API credentials

1. Open <https://my.telegram.org> and log in with the **spare** number
2. Click **API development tools**
3. App title: `filebot-assistant`, short name: `filebot`
4. Create → copy **App api_id** and **App api_hash**

## 3. Generate the session string

On your own computer (not Render):

```bash
pip install telethon
python make_session.py
```

Enter the api_id and api_hash, then the phone number and the login code
Telegram sends you. It prints a long session string.

> That string is a **full login** to the account. Treat it like a password.
> Never paste it anywhere public.

## 4. Prepare the storage channel

In the storage channel (`Storage pool`):

1. Add the **spare account** as an admin
2. Give it **Add New Admins** permission — without this it cannot promote the
   clone bots
3. Also keep **Post** and **Delete** permissions on

## 5. Deploy on Render

1. Push this folder to a GitHub repo
2. <https://render.com> → **New** → **Web Service** → connect the repo
3. Runtime **Python 3**, Plan **Free**
   - Build command: `pip install -r requirements.txt`
   - Start command: `python assistant.py`
4. Add these environment variables:

| Key | Value |
|---|---|
| `TELETHON_API_ID` | from my.telegram.org |
| `TELETHON_API_HASH` | from my.telegram.org |
| `TELETHON_SESSION` | the string from step 3 |
| `WORKER_URL` | `https://filebot.kanxcer.workers.dev` |
| `ASSISTANT_SHARED_SECRET` | ask the operator — same value as the Worker's `WEBHOOK_SECRET` |

5. Deploy. The logs should show:

```
assistant signed in as <name> (id ...)
health server on :10000
```

---

## How it behaves

```
Worker: new clone created  →  job queued in D1
           ↓
Assistant polls /assistant/pull every 60s
           ↓
Telethon: add bot to channel  →  promote to admin
           ↓
POST /assistant/ack  →  clone marked 'active'
           ↓
Owner gets "your clone is ready"
```

**Built-in safety limits**

- one promotion every 45 seconds (`PROMOTE_GAP`), so Telegram does not see a burst
- `FloodWaitError` is caught, the job is requeued, and the service backs off
- a job that fails 3 times is marked `failed` and surfaced to the admin
- the clone bot only gets post/edit/delete rights — it cannot invite, ban, or
  add other admins

## If the assistant is down

Nothing breaks permanently. New clones simply sit in `pending`. The owner can
also connect manually: add the clone bot as an admin in the storage channel,
then send `/verifystorage` inside that clone.

## Free tier notes

Render free spins the service down after ~15 minutes of inactivity and takes
roughly a minute to wake. That is fine here — clone creation is rare and the
owner already sees an "activating…" message. No uptime pinger is needed.
