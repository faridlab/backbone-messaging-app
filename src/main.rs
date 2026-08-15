//! backbone-messaging-app — the messaging application.
//!
//! A runnable backend-service that composes the `backbone-mail` module (the
//! Odoo mail core port) into a deployable surface:
//!
//! - the guarded discuss/chatter HTTP groups behind the **user-scope
//!   verifier** (ADR-0014 posture 4 — messaging is company_fence none, so
//!   the verifier proves the caller's PARTNER id, never a company);
//! - the public bootstrap + guest self-service behind the module's
//!   `guest_context` (dgid cookie) — guests are first-class callers;
//! - the sms provider webhook (`POST /sms/status`) and the inbound email
//!   webhook (`POST /mail/inbound/:server_id`) mounted BARE — the raw-body
//!   HMAC / per-row token IS the auth;
//! - `GET /mail/realtime/stream` — SSE over the replica-local outbox tailer;
//! - the queue job runners (sms/mail queues through the gateway adapters,
//!   scheduled dispatch, the four GCs, the stuck-process sweep) on
//!   `tokio::spawn` interval loops;
//! - the durable outbox relay (`backbone-outbox` runner) — carrier of record
//!   for durable consumers; its publish seam stages the MAIL-B7
//!   previous-address security email on `UserEmailChanged`. The SSE tailer is
//!   a replica-local convenience on top of the same rows.
//! - the backbone-notification composition (opt-in via
//!   `gateway.platform_company_id`, empty = OFF): the notification module's
//!   guarded routes mount behind an admin + sentinel-company gate, and its
//!   `dispatch_pending` job fans notifications out through the SAME gateway
//!   (`QueueCommunicationPort` stages them as mail rows / sms rows) — one
//!   gateway, two producers.
//!
//! ## Subcommands
//!
//! - `serve` (default) — start the HTTP server + jobs + relay + tailer.
//! - `healthcheck` — probe `/health` and exit 0 on 2xx (Docker HEALTHCHECK).
//! - `migrate` — placeholder; real services delegate to `metaphor migration run-all`.
//!
//! ## Seams (documented gaps, not silent ones)
//!
//! - `jobs.sms_provider: noop` sends nothing (dev/tests); `http` routes
//!   through the generic IAP-shaped provider under `gateway:`.
//! - backbone-notification is composed under ONE configured platform company
//!   (the sentinel-company posture): notification is company-fence RLS and
//!   this app is fence-none, so the app is the fence's ONLY legitimate
//!   principal — every notification row carries the configured company id and
//!   the gate + dispatch job both scope to it. No RLS bypass anywhere.
//!   Boot also re-asserts messaging's fence-none posture by dropping the
//!   outbox company-isolation policy from `messaging.outbox_events` —
//!   composing notification turns the outbox crate's `multi_tenant` feature
//!   on for the whole graph (cargo unifies features), which would otherwise
//!   fence the fence-none messaging schema and fail every mail stage closed.
//! - Push devices, link previews, canned responses, translations, wizards, and
//!   RTC remain 3b (port-notes scope register).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::middleware::from_fn_with_state;
use axum::{routing::get, Router};
use backbone_health::{routes::health_routes, HealthChecker, HealthConfig};
use backbone_maintenance::{
    admin_toggle_handler, maintenance_middleware, status_handler, MaintenanceConfig,
    MaintenanceState,
};
use backbone_observability::audit::{audit_middleware, AuditConfig};
use backbone_outbox::runner::{run as relay_run, RelayConfig};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

mod configuration;
mod gateway;
mod middleware;

use backbone_mail::realtime::{session::SESSION_TTL_SECS, SessionSecret};
use backbone_mail::application::service::{GcService, NoopSmsApi};
use backbone_mail::application::service::sms_write_service::SmsWriteService;
use backbone_mail::MessagingModule;
use gateway::mail_gateway::SmtpMailApi;
use gateway::notification_gateway::QueueCommunicationPort;
use gateway::relay_b7::handle_relayed_event;
use gateway::sms_gateway::HttpSmsApi;
use configuration::AppConfig;
use middleware::notification_gate::{notification_gate, NotificationGate};
use middleware::user_scope::{user_scope, UserScope};

#[tokio::main]
async fn main() -> Result<()> {
    // Subcommand dispatch runs BEFORE config init so `healthcheck` stays
    // cheap (Docker re-invokes it on every interval).
    let mut args = std::env::args().skip(1);
    if let Some(cmd) = args.next() {
        match cmd.as_str() {
            "healthcheck" => {
                let port: u16 = std::env::var("APP_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
                return backbone_health::cli::run_healthcheck(port)
                    .await
                    .map_err(|e| anyhow::anyhow!("healthcheck failed: {e}"));
            }
            "migrate" => {
                eprintln!(
                    "WARN: backbone-messaging-app migrate is a placeholder. Delegate to \
                     `metaphor migration run-all` (applies module migrations in order). Exiting 0."
                );
                return Ok(());
            }
            "serve" => {}
            other => anyhow::bail!(
                "unknown subcommand '{}' (supported: serve, migrate, healthcheck)",
                other
            ),
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    info!("🌀 backbone-messaging-app starting");

    let app_config = AppConfig::load().map_err(|e| {
        error!("failed to load config: {e}");
        e
    })?;
    app_config.validate().map_err(|e| {
        error!("insecure configuration: {e}");
        anyhow::anyhow!("insecure configuration: {e}")
    })?;
    info!("✅ Config loaded (secrets from env, validated non-empty)");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(app_config.database.max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&app_config.database.url)
        .await
        .map_err(|e| {
            error!("database connection failed: {e}");
            anyhow::anyhow!("database connection failed: {e}")
        })?;
    info!("✅ Database connected");

    // The outbox table is owned by the outbox crate, not the module
    // migrations — bring it up (idempotent) before anything stages events.
    backbone_outbox::outbox::migrate(&pool, "messaging").await?;
    // ADR-0014 posture assertion: messaging is fence-NONE (posture 4), but
    // composing backbone-notification (company-fence) turns the outbox
    // crate's `multi_tenant` feature on for the WHOLE graph — cargo unifies
    // features — so the migrate above also installs the company-isolation
    // RLS fence on messaging.outbox_events. That silently breaks this app's
    // declared posture: mail stages outbox rows with the nil sentinel
    // company id and its relay/HTTP paths run unscooped, so every stage,
    // read and publish would fail closed. Remove the fence HERE — the
    // composition layer owns the posture decision. The notification schema
    // (armed below) KEEPS its fence: that module is company-fenced and its
    // writers self-scope per row.
    sqlx::query("DROP POLICY IF EXISTS outbox_events_company_isolation ON messaging.outbox_events")
        .execute(&pool)
        .await?;
    sqlx::query("ALTER TABLE messaging.outbox_events DISABLE ROW LEVEL SECURITY")
        .execute(&pool)
        .await?;
    info!("✅ Outbox schema present (messaging.outbox_events, fence-none per ADR-0014 posture 4)");

    // ── The domain module ─────────────────────────────────────────────────────
    let module = Arc::new(MessagingModule::builder().with_database(pool.clone()).build()?);
    // MAIL-B1: open the host's chatter models from config (default: none —
    // deny-by-default stands until this list says otherwise).
    module.set_static_thread_access(app_config.messaging.open_thread_models.clone());
    info!(
        "✅ backbone-mail mounted ({} open thread models, {} admin partners)",
        app_config.messaging.open_thread_models.len(),
        app_config.security.admin_partners.len()
    );

    // ── Router ────────────────────────────────────────────────────────────────
    let session_secret = SessionSecret(Arc::new(app_config.security.session_secret.clone().into_bytes()));
    let verifier = UserScope::new(&app_config.security.jwt_secret, app_config.security.admin_partners.clone());

    let maintenance_state = MaintenanceState::from_config(&MaintenanceConfig::default());
    let maintenance_router = Router::new()
        .route("/maintenance/status", get(status_handler))
        .route("/maintenance", axum::routing::post(admin_toggle_handler))
        .with_state(maintenance_state.clone());

    // The guarded surface: user-scope verifier (optional Bearer → partner)
    // THEN the module's guest_context (partner ext → dgid cookie → anonymous).
    // The session secret rides the presence + realtime groups (proof minting).
    // NOTE: `Router::layer` wraps only what is ALREADY merged — the shared
    // layers below must come after every merge, so the notification
    // composition is merged into `guarded` first, then the verifier +
    // guest_context wrap the whole thing.
    let mut guarded = Router::new()
        .merge(module.messaging_routes())
        .merge(module.discuss_routes())
        .merge(module.attachment_routes())
        .merge(module.guest_routes())
        .merge(module.public_routes())
        .merge(module.presence_routes().layer(axum::Extension(session_secret.clone())))
        .merge(module.realtime_routes().layer(axum::Extension(session_secret.clone())));

    // ── Notification composition (armed by a platform company id) ────────────
    // backbone-notification under ONE sentinel company: routes admin-gated and
    // company-scoped by the same layer; the dispatch job sweeps the same fence.
    // No id configured → the composition stays off (dev/test default).
    let mut notification = None;
    if let Some(company) = app_config.gateway.platform_company() {
        // The notification composition stages lifecycle events into
        // notification.outbox_events through backbone-outbox — bring it up
        // (idempotent) so the table always matches the crate's CURRENT shape.
        // (The module's own 2026-07 migration created an 11-column variant
        // without company_id; CREATE IF NOT EXISTS would let the stale table
        // shadow the crate's and every stage would fail.)
        backbone_outbox::outbox::migrate(&pool, "notification").await?;
        let notif = backbone_notification::NotificationModule::builder()
            .with_database(pool.clone())
            .build()
            .map_err(|e| {
                error!("notification module build failed: {e}");
                anyhow::anyhow!("notification module build failed: {e}")
            })?;
        let gate = NotificationGate { company };
        guarded = guarded.merge(
            notif.guarded_routes()
                .layer(from_fn_with_state(gate, notification_gate)),
        );
        info!("✅ backbone-notification mounted (platform company {company}, admin-gated)");
        notification = Some(notif);
    }

    let guarded = guarded
        .layer(from_fn_with_state(verifier, user_scope))
        .layer(axum::middleware::from_fn(backbone_mail::presentation::middleware::guest_context));

    let mut app = Router::new()
        .merge(health_routes(Arc::new(HealthChecker::new(HealthConfig::default()))))
        .merge(maintenance_router)
        // The webhooks mount BARE — no identity middleware; the raw-body HMAC
        // (sms) / per-row token consteq (inbound mail) IS the auth.
        .merge(module.webhook_routes())
        .merge(module.inbound_routes())
        .merge(guarded)
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(AuditConfig { trust_proxy_headers: false }),
            audit_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            maintenance_state.clone(),
            maintenance_middleware,
        ))
        .layer(TraceLayer::new_for_http());

    if let Some(cors) = cors_layer() {
        app = app.layer(cors);
    }

    // ── Jobs + relay + tailer ────────────────────────────────────────────────
    let jobs = &app_config.jobs;
    let gc = &app_config.gc;
    let sms_module = Arc::clone(&module);

    // sms::process_queue — the queue drainer on the provider port. The port is
    // selected ONCE at boot (noop for dev/tests; the generic IAP-shaped HTTP
    // provider for real deployments) — swapping providers is config, not code.
    let sms_port: Arc<dyn backbone_mail::application::service::sms_ports::SmsApiPort> =
        match app_config.jobs.sms_provider.as_str() {
            "http" => Arc::new(HttpSmsApi::new(
                app_config.gateway.sms_http_endpoint.clone(),
                app_config.gateway.sms_http_token.clone(),
            )),
            _ => Arc::new(NoopSmsApi::accepting()),
        };
    info!("✅ SMS provider port: {}", app_config.jobs.sms_provider);
    spawn_interval("sms::process_queue", secs(jobs.sms_process_interval_seconds), move || {
        let m = Arc::clone(&sms_module);
        let port = Arc::clone(&sms_port);
        async move {
            let outcome = m.sms_write_service.process_queue(port.as_ref(), 100, 1).await.map_err(|e| e.to_string())?;
            Ok(format!("{outcome:?}"))
        }
    });
    // mail::send_queue — claims outgoing mails and hands each row to the SMTP
    // port (SmtpMailApi: per-row server selection via the MAIL-M26 ladder,
    // per-server cached lettre transport, ADR-0024 env-ref passwords).
    let mail_api = Arc::new(SmtpMailApi::new(pool.clone()));
    let mail_module = Arc::clone(&module);
    spawn_interval("mail::send_queue", secs(jobs.mail_process_interval_seconds), move || {
        let m = Arc::clone(&mail_module);
        let port = Arc::clone(&mail_api);
        async move {
            let outcome = m.mail_queue_write_service.process_queue(port.as_ref(), 100, 1).await.map_err(|e| e.to_string())?;
            Ok(format!("{outcome:?}"))
        }
    });
    // mail_scheduled_notification::dispatch_due — arm notifications, then
    // fire the due post-later rows (permission re-checked at fire time).
    let sched_module = Arc::clone(&module);
    spawn_interval("mail_scheduled_notification::dispatch_due", secs(jobs.scheduled_dispatch_interval_seconds), move || {
        let m = Arc::clone(&sched_module);
        let poster = Arc::clone(&m.message_write_service);
        async move {
            let notified = m.schedule_write_service.dispatch_due_notify().await.map_err(|e| e.to_string())?;
            let (posted, skipped) = m.schedule_write_service.dispatch_due_scheduled(&poster).await.map_err(|e| e.to_string())?;
            Ok(format!("notified={notified} posted={posted} skipped={skipped}"))
        }
    });
    // The three GCs + the SM-B13 sweep (one GcService each — they are cheap
    // pool-holding structs and the jobs are far apart).
    let gc_pool = pool.clone();
    let gc_cfg = gc.clone();
    spawn_interval("mail_notification::gc", secs(jobs.notification_gc_interval_seconds), move || {
        let svc = GcService::new(gc_pool.clone(), gc_cfg.notification_retention_days, gc_cfg.presence_stale_hours, gc_cfg.guest_stale_minutes, gc_cfg.stuck_threshold_minutes);
        async move { Ok(format!("reaped={}", svc.notification_gc().await.map_err(|e| e.to_string())?)) }
    });
    let gc_pool = pool.clone();
    spawn_interval("mail_presence::gc", secs(jobs.presence_gc_interval_seconds), move || {
        let svc = GcService::new(gc_pool.clone(), gc_cfg.notification_retention_days, gc_cfg.presence_stale_hours, gc_cfg.guest_stale_minutes, gc_cfg.stuck_threshold_minutes);
        async move { Ok(format!("reaped={}", svc.presence_gc().await.map_err(|e| e.to_string())?)) }
    });
    let gc_pool = pool.clone();
    spawn_interval("mail_guest::gc", secs(jobs.guest_gc_interval_seconds), move || {
        let svc = GcService::new(gc_pool.clone(), gc_cfg.notification_retention_days, gc_cfg.presence_stale_hours, gc_cfg.guest_stale_minutes, gc_cfg.stuck_threshold_minutes);
        async move { Ok(format!("reaped={}", svc.guest_gc().await.map_err(|e| e.to_string())?)) }
    });
    let gc_pool = pool.clone();
    spawn_interval("sms::sweep_stuck_process", secs(jobs.stuck_sweep_interval_seconds), move || {
        let svc = GcService::new(gc_pool.clone(), gc_cfg.notification_retention_days, gc_cfg.presence_stale_hours, gc_cfg.guest_stale_minutes, gc_cfg.stuck_threshold_minutes);
        async move { Ok(format!("{:?}", svc.sweep_stuck_process().await.map_err(|e| e.to_string())?)) }
    });
    // sms::gc — reap terminal-state sms rows (sent/error/canceled) past the
    // retention bound; pending rows survive (still awaiting the DSN).
    let gc_pool = pool.clone();
    let sms_retention = gc_cfg.sms_retention_days;
    spawn_interval("sms::gc", secs(jobs.sms_gc_interval_seconds), move || {
        let svc = GcService::new(gc_pool.clone(), gc_cfg.notification_retention_days, gc_cfg.presence_stale_hours, gc_cfg.guest_stale_minutes, gc_cfg.stuck_threshold_minutes);
        async move { Ok(format!("reaped={}", svc.sms_gc(sms_retention).await.map_err(|e| e.to_string())?)) }
    });
    // notification::dispatch_pending — the stranded-`pending` reaper (the
    // (event_id, recipient) dedup slot makes a crash between INSERT and
    // dispatch unrecoverable WITHOUT this sweep). Runs under the sentinel
    // company: the write service reads `pending` under the AMBIENT scope, so
    // the sweep must fence exactly like the routes do. Only armed when the
    // notification composition is.
    if let (Some(notif), Some(company)) = (&notification, app_config.gateway.platform_company()) {
        let notif_ws = notif.write_service();
        let comm = Arc::new(QueueCommunicationPort::new(
            Arc::clone(&module.message_write_service),
            Arc::new(SmsWriteService::new(pool.clone())),
        ));
        spawn_interval("notification::dispatch_pending", secs(jobs.notification_dispatch_interval_seconds), move || {
            let ws = Arc::clone(&notif_ws);
            let port = Arc::clone(&comm);
            async move {
                let sink = backbone_notification::application::service::notification_events::LoggingSink;
                let dispatched = backbone_orm::company_scope::with_company_scope(
                    Some(company),
                    ws.dispatch_pending(100, port.as_ref(), &sink),
                )
                .await
                .map_err(|e| e.to_string())?;
                Ok(format!("dispatched={dispatched}"))
            }
        });
    }
    info!("✅ Job runners started (queues / scheduled dispatch / GCs / stuck sweep)");

    // The durable relay: drains messaging.outbox_events (marks published).
    // The publish seam's one transport consumer today is MAIL-B7 — a
    // UserEmailChanged event stages the security warning to the PREVIOUS
    // address through the mail queue. Everything else is a logged pass-through
    // (the notification composition seam is deferred to 3b; see README).
    let relay_pool = pool.clone();
    let relay_mails = Arc::clone(&module.message_write_service);
    let relay_shutdown = shutdown_receiver();
    tokio::spawn(async move {
        let publish = |rec: backbone_outbox::OutboxRecord| {
            let mails = Arc::clone(&relay_mails);
            async move { handle_relayed_event(&mails, &rec).await.map(|_| ()) }
        };
        if let Err(e) = relay_run(relay_pool, RelayConfig::new("messaging"), publish, relay_shutdown).await {
            error!(target: "outbox::relay", error = %e, "relay stopped on error");
        }
    });
    info!("✅ Outbox relay started (schema=messaging, MAIL-B7 consumer wired)");

    // The replica-local SSE tailer — publishes into the module's registry.
    let tailer = backbone_mail::realtime::tailer::spawn(
        pool.clone(),
        Arc::clone(&module.realtime_registry),
        backbone_mail::realtime::tailer::TailerConfig {
            window_seconds: app_config.realtime.window_seconds,
            poll_ms: app_config.realtime.poll_ms,
        },
    );
    info!(
        "✅ SSE tailer started (window={}s poll={}ms, session TTL={}s)",
        app_config.realtime.window_seconds, app_config.realtime.poll_ms, SESSION_TTL_SECS
    );

    let addr = app_config.server.addr();
    let listener = TcpListener::bind(&addr).await?;
    info!("🚀 Listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Fan shutdown out to the background workers (the relay listens to the
    // broadcast; the tailer is a poll loop — abort it).
    let _ = shutdown_tx().send(());
    tailer.abort();
    info!("👋 backbone-messaging-app stopped cleanly");
    Ok(())
}

// ── shutdown plumbing ────────────────────────────────────────────────────────

static SHUTDOWN_TX: std::sync::OnceLock<tokio::sync::broadcast::Sender<()>> =
    std::sync::OnceLock::new();

fn shutdown_tx() -> &'static tokio::sync::broadcast::Sender<()> {
    SHUTDOWN_TX.get_or_init(|| tokio::sync::broadcast::channel(1).0)
}

fn shutdown_receiver() -> impl std::future::Future<Output = ()> + Send + 'static {
    let mut rx = shutdown_tx().subscribe();
    async move {
        let _ = rx.recv().await;
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap_or_else(|e| panic!("failed to install SIGTERM handler: {e}"));
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    info!("shutdown signal received — draining connections");
}

// ── job helpers ──────────────────────────────────────────────────────────────

fn secs(s: u64) -> Duration {
    Duration::from_secs(s.max(1))
}

/// The spawn-interval job shape (backbone-application main.rs exemplar): a
/// repeating body returning Ok(summary) or Err(report); successes log the
/// summary, errors keep the cadence and never panic the task.
fn spawn_interval<F, Fut>(target: &'static str, every: Duration, mut body: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = std::result::Result<String, String>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        // First tick fires immediately — drain what's pending at boot.
        tick.tick().await;
        loop {
            tick.tick().await;
            match body().await {
                Ok(summary) if summary.contains("=0") => {
                    tracing::trace!(target, summary, "job pass");
                }
                Ok(summary) => info!(target, summary, "job pass"),
                Err(report) => error!(target, report, "job failed"),
            }
        }
    });
}

// ── CORS ─────────────────────────────────────────────────────────────────────

fn cors_layer() -> Option<tower_http::cors::CorsLayer> {
    let origin = std::env::var("CORS_ALLOW_ORIGIN").ok()?;
    if origin.is_empty() || origin == "*" {
        return None; // wide-open CORS is a decision, not a default
    }
    use axum::http::HeaderValue;
    let allowed: Vec<HeaderValue> = origin.split(',').filter_map(|o| o.trim().parse().ok()).collect();
    Some(tower_http::cors::CorsLayer::new().allow_origin(allowed))
}
