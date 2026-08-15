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
//! - the sms provider webhook (`POST /sms/status`) mounted BARE — the raw-body
//!   HMAC is the auth;
//! - `GET /mail/realtime/stream` — SSE over the replica-local outbox tailer;
//! - the queue job runners (sms/mail queues, scheduled dispatch, the three
//!   GCs, the stuck-process sweep) on `tokio::spawn` interval loops;
//! - the durable outbox relay (`backbone-outbox` runner) — carrier of record
//!   for durable consumers; the SSE tailer is a replica-local convenience on
//!   top of the same rows.
//!
//! ## Subcommands
//!
//! - `serve` (default) — start the HTTP server + jobs + relay + tailer.
//! - `healthcheck` — probe `/health` and exit 0 on 2xx (Docker HEALTHCHECK).
//! - `migrate` — placeholder; real services delegate to `metaphor migration run-all`.
//!
//! ## Increment-3 seams (documented gaps, not silent ones)
//!
//! - The sms provider is the module's `NoopSmsApi` until the gateway group
//!   (MAIL-M26/27) lands; `jobs.sms_provider` pins which port may be used
//!   and only `noop` exists today.
//! - The relay's publish seam LOGS each event (at-least-once, position
//!   preserved); wiring it to backbone-notification's transport is the
//!   increment-3 composition step.
//! - `sms::gc` from the plan's job roster has no module service method yet
//!   (nothing to reap that MAIL-B8's notification GC doesn't already bound);
//!   it lands with the gateway group.

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
mod middleware;

use backbone_mail::realtime::{session::SESSION_TTL_SECS, SessionSecret};
use backbone_mail::application::service::{GcService, NoopSmsApi};
use backbone_mail::MessagingModule;
use configuration::AppConfig;
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
    if app_config.jobs.sms_provider != "noop" {
        anyhow::bail!(
            "jobs.sms_provider {:?} is not available — only 'noop' until the increment-3 gateway group",
            app_config.jobs.sms_provider
        );
    }
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
    info!("✅ Outbox schema present (messaging.outbox_events)");

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
    let guarded = Router::new()
        .merge(module.messaging_routes())
        .merge(module.discuss_routes())
        .merge(module.attachment_routes())
        .merge(module.guest_routes())
        .merge(module.public_routes())
        .merge(module.presence_routes().layer(axum::Extension(session_secret.clone())))
        .merge(module.realtime_routes().layer(axum::Extension(session_secret.clone())))
        .layer(from_fn_with_state(verifier, user_scope))
        .layer(axum::middleware::from_fn(backbone_mail::presentation::middleware::guest_context));

    let mut app = Router::new()
        .merge(health_routes(Arc::new(HealthChecker::new(HealthConfig::default()))))
        .merge(maintenance_router)
        // The webhook mounts BARE — no identity middleware; the raw-body HMAC
        // is the auth (fail-closed 503 when SMS_WEBHOOK_SECRET is unset).
        .merge(module.webhook_routes())
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

    // sms::process_queue — the queue drainer on the (increment-3) port.
    spawn_interval("sms::process_queue", secs(jobs.sms_process_interval_seconds), move || {
        let m = Arc::clone(&sms_module);
        async move {
            let outcome = m.sms_write_service.process_queue(&NoopSmsApi::accepting(), 100, 1).await.map_err(|e| e.to_string())?;
            Ok(format!("{outcome:?}"))
        }
    });
    // mail::send_queue — claims outgoing mails, stages the dispatch request.
    let mail_module = Arc::clone(&module);
    spawn_interval("mail::send_queue", secs(jobs.mail_process_interval_seconds), move || {
        let m = Arc::clone(&mail_module);
        async move {
            let outcome = m.mail_queue_write_service.process_queue(100, 1).await.map_err(|e| e.to_string())?;
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
    info!("✅ Job runners started (queues / scheduled dispatch / GCs / stuck sweep)");

    // The durable relay: drains messaging.outbox_events (marks published).
    // The publish seam logs for now — increment-3 wires the transport.
    let relay_pool = pool.clone();
    let relay_shutdown = shutdown_receiver();
    tokio::spawn(async move {
        let publish = |rec: backbone_outbox::OutboxRecord| async move {
            tracing::debug!(
                target: "outbox::relay",
                event = %rec.event_type,
                aggregate = %rec.aggregate_id,
                "outbox event relayed"
            );
            Ok(())
        };
        if let Err(e) = relay_run(relay_pool, RelayConfig::new("messaging"), publish, relay_shutdown).await {
            error!(target: "outbox::relay", error = %e, "relay stopped on error");
        }
    });
    info!("✅ Outbox relay started (schema=messaging, carrier of record)");

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
            .expect("install SIGTERM handler");
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
