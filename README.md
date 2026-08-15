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
| Inbound email `POST /mail/inbound/:server_id` | `module.inbound_routes()` | BARE — per-row token (SHA-256 hash + consteq) IS the auth; uniform 401 (no server-existence oracle), throttled 120/60s |

**User-scope auth (ADR-0014 posture 4):** messaging is `company_fence: none`, so the
verifier proves the caller's **partner id** (`sub` on the Bearer JWT), never a company.
A missing Authorization header passes through to the guest/anonymous path; a present-but-
invalid token is a 401. Admin edges (author-or-admin gates) come from
`security.admin_partners` in config.

## Background workers

- **Job runners** (tokio spawn-interval, tracing target = job name): `sms::process_queue`
  (through the selected provider port), `mail::send_queue` (through `SmtpMailApi`),
  `mail_scheduled_notification::dispatch_due`, `mail_notification::gc`,
  `mail_presence::gc`, `mail_guest::gc`, `sms::gc`, `sms::sweep_stuck_process`,
  plus the opt-in `notification::dispatch_pending` (see
  [backbone-notification composition](#backbone-notification-composition-sentinel-company)).
- **Outbox relay** (`backbone-outbox` runner, schema `messaging`) — the carrier of record;
  marks rows `published_at`. The publish seam's one transport consumer is **MAIL-B7**: a
  `UserEmailChanged` event stages the security warning to the **previous** address through
  the mail queue (single-tx `message_post`, so a relay retry can never double-send). A B7
  staging error leaves that row for the next pass — at-least-once, never silently dropped.
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
# sms provider 'http' additionally needs: SMS_HTTP_ENDPOINT SMS_HTTP_TOKEN
cargo run
```

Health: `curl :8080/health` · `cargo run -- healthcheck` (Docker HEALTHCHECK).

## Gateway adapters (increment 3)

- **`SmtpMailApi`** (`src/gateway/mail_gateway.rs`) — the app-side `MailApiPort` over
  backbone-email/lettre. Per send: the module's MAIL-M26 selection ladder picks one
  `MailServer` row from the envelope sender, `smtp_pass_ref` (an ENV VAR NAME, ADR-0024)
  resolves to the secret, and a per-row cached transport delivers. `certificate` auth is
  declared-but-unsupported → `mail_server` failure at send time.
- **`HttpSmsApi`** (`src/gateway/sms_gateway.rs`) — the generic IAP-shaped HTTP SMS
  provider. `jobs.sms_provider: noop | http`; `http` requires `gateway.sms_http_endpoint`
  + `gateway.sms_http_token` (env refs; empty under `http` is a boot failure). Provider
  swap is config, not code.

## backbone-notification composition (sentinel company)

Notification is company-fence RLS; this app is fence-none (ADR-0014 posture 4). The
composition resolves that tension with a **sentinel platform company**: the app is the
fence's one legitimate principal, and every notification row carries the configured
company id. No RLS bypass anywhere.

- **Opt-in:** `gateway.platform_company_id` (env ref `MESSAGING_PLATFORM_COMPANY_ID`).
  Empty/unset = composition OFF — routes unmounted (404), dispatch job not started.
  Set-but-unparseable is a boot failure.
- **Surface:** the module's `guarded_routes` (`/notification_templates` full CRUD,
  `/notifications` read-only) mount behind the user-scope verifier **plus** an
  admin gate (`middleware/notification_gate.rs`): no partner → 401, non-admin → 403,
  admin → the request runs inside `with_company_scope(platform_company)`.
- **Dispatch:** `notification::dispatch_pending` sweeps pending rows under the same
  scope and fans them out through **`QueueCommunicationPort`**
  (`src/gateway/notification_gateway.rs`) — the SAME gateway the messaging pump uses:
  `email` dispatches stage a mail row via `message_post` (the ack's `message_id` IS the
  mail row id — the correlation key `record_delivery` correlates on), `sms` dispatches
  enqueue an sms row tied back to the notification, anything else is an honest
  `channel_not_composed` rejection recorded on the row. Interval:
  `jobs.notification_dispatch_interval_seconds` (default 60).
- **Posture assertion at boot:** composing notification turns the outbox crate's
  `multi_tenant` feature on for the whole graph (cargo unifies features), which installs
  the company-isolation RLS fence on `messaging.outbox_events` too. Messaging is
  fence-none by declaration, so boot DROPS that policy from the messaging schema after
  the outbox migrate; the notification schema keeps its fence (its writers self-scope
  per row).
- **One duplicate-crate trap, documented in `Cargo.toml`:** backbone-mail pins
  backbone-orm at git tag v2.7.0 while backbone-notification pins branch `main` —
  unpatched that is TWO backbone-orm crates, each with its own company-scope task-local,
  and the fence silently fails at the task-local layer. The `[patch]` entry collapses
  every ref onto the one local crate.

## Seams (documented gaps)

- Push devices (MAIL-M13/M14, MAIL-B3/B4), link previews, canned responses,
  translations, wizards, and RTC remain 3b (module port-notes scope register).

## Regen safety

`metaphor.codegen.yaml` declares the whole hand-written tree `user_owned` — a future
schema generate inside this project can never silently delete the bootstrap surface.
