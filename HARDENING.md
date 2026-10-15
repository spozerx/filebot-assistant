# Assistant hardening — 2026-10-15

Deploy the matching hardened Cloudflare Worker and its additive migration before this revision.

## Tests

```sh
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

The tests use dummy credentials, mocked Telegram clients and mocked Worker requests. They do not log into Telegram or perform live deliveries/payments.

## Corrections

- Worker API calls validate HTTP status, JSON and application success; failed writes cannot be acknowledged as completed work.
- Blocking HTTP and Mongo operations are moved off the asyncio event loop.
- Protocol-2 job leases have heartbeats, stale-lease cancellation and retry-safe ACKs.
- Permanent permission/protected-content failures stop retrying; Telegram FloodWait delays are respected rather than capped and retried too soon.
- Numeric source-channel references are resolved against dialogs the account already has permission to access. No access-control or content-protection bypass is attempted.
- Clone identity is checked against the expected Telegram bot ID. Actual destination pool is returned with completion so accounting does not update the previous full pool.
- HTTP server is threaded, with socket timeouts, body-size limits, constant-time shared-secret checks and safe error messages.
- Login steps use retry IDs, expire pending sessions, and only switch clients after session persistence succeeds; active work blocks a session swap.
- Startup retries temporary session-store failures; job-loop recovery does not require restarting the web service.
- Health response includes protocol and build identifiers; `signed_in` requires a connected client.
- Requests is updated to 2.32.5. No paid service or new hosting provider is required.

## Remaining boundaries

- Telethon still needs legitimate access to source/storage channels and the relevant admin rights.
- Session/account switching is refused while a job is active. Retry through the bot when work is idle.
- Thread cancellation cannot cancel an already-running synchronous Mongo read; subsequent writes are protected by the current lease check.
- Retry-safe queue completion is not a guarantee of exactly-once Telegram forwards across crashes. Historical failed jobs are not automatically replayed by this release.
- Login request idempotency is process-local; a Render restart loses pending login state. Start a fresh login if the bot reports an expired session.
- Never put API keys, OTPs, passwords or session strings in the repository or logs. Login inputs are relayed by the Worker over HTTPS, not confined to Telegram.
