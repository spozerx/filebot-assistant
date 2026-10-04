# Deploying the FileBot assistant

The assistant is a tiny Python service that runs a **real Telegram user
account**. It exists for the two things a bot account simply cannot do:

* read a **public channel it was never added to** (`/batch` on someone else's
  channel), and
* **add a newly created clone bot** to the storage channel as an admin.

It holds no state. The session is created by signing in **from inside the main
bot** and is stored, encrypted, in the Worker's database — so Render's free
tier wiping its disk on every restart costs nothing.

---

## 1. Put these files in a public Git repo

Render deploys from a public GitHub / GitLab / Bitbucket URL — you do **not**
have to connect your GitHub account.

1. Create a new **public** repo (name it anything, e.g. `filebot-assistant`).
2. Upload these four files to the repo root:
   * `assistant.py`
   * `requirements.txt`
   * `render.yaml`
   * `README.md`
3. Copy the repo URL, e.g. `https://github.com/yourname/filebot-assistant`.

Give that URL to the agent and it will create the Render service for you, or
do step 2 yourself below.

## 2. Create the service (if doing it by hand)

New → Web Service → **Public Git Repository** → paste the URL.

| Setting | Value |
|---|---|
| Runtime | Python 3 |
| Build command | `pip install -r requirements.txt` |
| Start command | `python assistant.py` |
| Instance type | Free |

Environment variables:

| Key | Value |
|---|---|
| `TELETHON_API_ID` | your api_id from my.telegram.org |
| `TELETHON_API_HASH` | your api_hash |
| `WORKER_URL` | `https://filebot.kanxcer.workers.dev` |
| `ASSISTANT_SHARED_SECRET` | the Worker's `WEBHOOK_SECRET` |

There is deliberately **no** `TELETHON_SESSION` variable. Changing accounts is
a sign-in, not a redeploy.

## 3. Point the bot at it

In the main bot:

```
/set assistant_url https://your-service.onrender.com
```

## 4. Sign in

Main bot → `/settings` → **SESSION LOGIN 🔐** → Sign in → phone → code → 2FA.

Send the code **with spaces between the digits** (`1 2 3 4 5`). Telegram
invalidates any login code that appears as plain text in a chat; spacing it out
is the standard way around that.

## Notes

* Use a **spare number**. The session can read everything that account can.
* The free instance sleeps when idle. The first request after a sleep takes
  up to a minute — the bot already waits 90 s for it.
* To revoke: Telegram → Settings → Devices → terminate that session, then sign
  in again from the bot.
