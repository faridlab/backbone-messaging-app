# backbone-messaging-app

The messaging application — composes the [`backbone-mail`](../../modules/backbone-mail)
module (the Odoo mail core port) into a runnable backend-service. Application-level
composition only: every domain rule lives in the module.

## Surface

| Group | Mount | Auth |
|---|---|---|
| Thread/chatter fetches, post/follow/schedule | `module.messaging_routes()` | user-scope verifier → guest_context |
| Discuss channels, members, typing, reactions, edits | `module.discuss_routes()` | user-scope verifier → guest_context |
| Attachments (owner-gated, throttled) | `module.attachment_routes()` | user-scope verifier → guest_context |
| Guest mint/rename | `module.guest_routes()` | guest_context (dgid cookie IS the credential) |
| Public channel bootstrap (throttled 10/60s) | `module.public_routes()` | guest_context (anonymous may bootstrap) |
| Presence (manual status + proof-gated bus liveness) | `module.presence_routes()` | + `SessionSecret` extension |
| SSE stream `GET /mail/realtime/stream` | `module.realtime_routes()` | + `SessionSecret` extension |
| SMS webhook `POST /sms/status` | `module.webhook_routes()` | BARE — raw-body HMAC is the auth (503 fail-closed when `SMS_WEBHOOK_SECRET` is unset) |

**User-scope auth (ADR-0014 posture 4):** messaging is `company_fence: none`, so the
verifier proves the caller's **partner id** (`sub` on the Bearer JWT), never a company.
A missing Authorization header passes through to the guest/anonymous path; a present-but-
invalid token is a 401. Admin edges (author-or-admin gates) come from
`security.admin_partners` in config.

## Background workers

- **Job runners** (tokio spawn-interval, tracing target = job name): `sms::process_queue`,
  `mail::send_queue`, `mail_scheduled_notification::dispatch_due`, `mail_notification::gc`,
  `mail_presence::gc`, `mail_guest::gc`, `sms::sweep_stuck_process`.
- **Outbox relay** (`backbone-outbox` runner, schema `messaging`) — the carrier of record;
  marks rows `published_at`. The publish seam logs each event; wiring the real transport is
  the increment-3 composition step.
- **SSE tailer** — read-only per-replica poll of `outbox_events` into the module's
  broadcast registry. Never writes, never marks.

Graceful shutdown: SIGTERM/Ctrl-C drains HTTP connections, then fans out to the relay
(broadcast) and the tailer (abort).

## SSE behind a reverse proxy

The stream sets `X-Accel-Buffering: no`. For nginx also set `proxy_buffering off;`,
`proxy_read_timeout` ≥ the keepalive (15s), and `proxy_http_version 1.1;` with
`proxy_set_header Connection "";`. Events beyond a connection's allowlist are dropped
server-side (BUS-B2) — the client never learns a foreign channel exists.

## Run

```bash
docker compose up                      # postgres + app
# or
export DATABASE_URL=... JWT_SECRET=... MESSAGING_SESSION_SECRET=... [SMS_WEBHOOK_SECRET=...]
cargo run
```

Health: `curl :8080/health` · `cargo run -- healthcheck` (Docker HEALTHCHECK).

## Increment-3 seams (documented gaps)

- `jobs.sms_provider` only accepts `noop` — the gateway group (MAIL-M26/27) brings the
  real provider port.
- The relay publish seam logs; the durable transport wiring lands with
  backbone-notification composition.
- `sms::gc` from the plan roster has no module method yet — nothing to reap that the
  MAIL-B8 notification GC doesn't already bound; lands with the gateway group.

## Regen safety

`metaphor.codegen.yaml` declares the whole hand-written tree `user_owned` — a future
schema generate inside this project can never silently delete the bootstrap surface.
