//! App configuration (YAML + `${VAR}` env overlay, ADR-0024 interim).
//!
//! Secrets are ENV-VAR REFERENCES ONLY — never literals. A missing secret is
//! fatal at boot (never an empty-string secret silently accepted): the JWT
//! secret proves the caller's partner id and the session secret mints the
//! SSE presence proofs, so a placeholder is a security boundary, not a knob.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub security: SecurityConfig,
    pub realtime: RealtimeConfig,
    pub jobs: JobsConfig,
    pub gc: GcConfig,
    pub messaging: MessagingConfig,
    #[serde(default)]
    pub gateway: GatewayConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    8080
}

impl ServerConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    /// `${JWT_SECRET}` — the app-auth signing secret (user-scope verifier).
    pub jwt_secret: String,
    /// `${MESSAGING_SESSION_SECRET}` — the SSE session-proof HMAC key.
    pub session_secret: String,
    /// Partners granted the admin edges (author-or-admin gates). Default none.
    #[serde(default)]
    pub admin_partners: Vec<uuid::Uuid>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RealtimeConfig {
    #[serde(default = "default_window")]
    pub window_seconds: i64,
    #[serde(default = "default_poll")]
    pub poll_ms: u64,
}

fn default_window() -> i64 {
    55
}
fn default_poll() -> u64 {
    1000
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsConfig {
    pub sms_process_interval_seconds: u64,
    pub mail_process_interval_seconds: u64,
    pub scheduled_dispatch_interval_seconds: u64,
    pub notification_gc_interval_seconds: u64,
    pub presence_gc_interval_seconds: u64,
    pub guest_gc_interval_seconds: u64,
    pub stuck_sweep_interval_seconds: u64,
    pub sms_gc_interval_seconds: u64,
    /// `notification::dispatch_pending` — re-drives stranded `pending`
    /// notification rows (the dedup-slot reaper). Only starts when the
    /// notification composition is armed (a platform company id is set).
    #[serde(default = "default_notification_dispatch_interval_seconds")]
    pub notification_dispatch_interval_seconds: u64,
    /// The sms transport port: `noop` | `http` (increment 3).
    #[serde(default = "default_sms_provider")]
    pub sms_provider: String,
}

fn default_notification_dispatch_interval_seconds() -> u64 {
    60
}

fn default_sms_provider() -> String {
    "noop".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct GcConfig {
    pub notification_retention_days: i64,
    pub presence_stale_hours: i64,
    pub guest_stale_minutes: i64,
    pub stuck_threshold_minutes: i64,
    pub sms_retention_days: i64,
}

/// The generic HTTP SMS provider (used when `jobs.sms_provider: http`).
/// Both fields are env-var REFERENCES (ADR-0024) — empty is fine while the
/// provider stays `noop`; selecting `http` with either empty is fatal at boot.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GatewayConfig {
    pub sms_http_endpoint: String,
    pub sms_http_token: String,
    /// The PLATFORM company id the notification composition runs under (env
    /// ref). backbone-notification is company-fence RLS; this app is fence-none
    /// (ADR-0014 posture 4) — the sentinel id is the one company whose rows
    /// live here. EMPTY (default) = the notification composition stays off
    /// (routes unmounted, job not started); set to a uuid to arm it. Set but
    /// unparseable is fatal at boot.
    #[serde(default)]
    pub platform_company_id: String,
}

impl GatewayConfig {
    /// The parsed sentinel company id, or None when the composition is off.
    pub fn platform_company(&self) -> Option<uuid::Uuid> {
        if self.platform_company_id.trim().is_empty() {
            return None;
        }
        self.platform_company_id.trim().parse().ok()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessagingConfig {
    /// Document models opened to every authenticated partner (the static
    /// MAIL-B1 resolver). Default none — chatter stays deny-by-default.
    #[serde(default)]
    pub open_thread_models: Vec<String>,
}

impl AppConfig {
    /// Load `config/application.yml` overlaid by `config/application-{env}.yml`
    /// (APP_ENV, default dev), with `${VAR}` expansion from the environment.
    pub fn load() -> anyhow::Result<Self> {
        let env = std::env::var("APP_ENV").unwrap_or_else(|_| "dev".into());
        let base = std::fs::read_to_string("config/application.yml")?;
        let overlay_path = format!("config/application-{env}.yml");
        let overlay = std::fs::read_to_string(&overlay_path)
            .map(Some)
            .unwrap_or_else(|e| {
                tracing::debug!(path = %overlay_path, error = %e, "no env overlay file");
                None
            });

        let base = Self::expand(&base)?;
        let mut merged: serde_yaml::Value = serde_yaml::from_str(&base)?;

        if let Some(overlay) = overlay {
            let overlay = Self::expand(&overlay)?;
            let overlay: serde_yaml::Value = serde_yaml::from_str(&overlay)?;
            // A comments-only overlay parses as Null — merging it would
            // clobber the base document.
            if overlay.is_mapping() {
                merge_yaml(&mut merged, overlay);
            }
        }

        Ok(serde_yaml::from_value(merged)?)
    }

    /// Expand `${VAR}` (and `${VAR:-default}`) references from the process env.
    /// Comment lines (`# …`) are passed through untouched — docs mention the
    /// syntax literally and must not be expanded.
    fn expand(raw: &str) -> anyhow::Result<String> {
        let mut out = String::with_capacity(raw.len());
        for line in raw.split_inclusive('\n') {
            if line.trim_start().starts_with('#') {
                out.push_str(line);
                continue;
            }
            out.push_str(&Self::expand_line(line)?);
        }
        Ok(out)
    }

    fn expand_line(line: &str) -> anyhow::Result<String> {
        let mut out = String::with_capacity(line.len());
        let mut rest = line;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find('}') else {
                anyhow::bail!("unterminated ${{ variable");
            };
            let var = &after[..end];
            let (name, default) = match var.split_once(":-") {
                Some((n, d)) => (n, Some(d)),
                None => (var, None),
            };
            let value = std::env::var(name).ok().or_else(|| default.map(str::to_string));
            out.push_str(&value.ok_or_else(|| anyhow::anyhow!("environment variable {name} is not set (config references it)"))?);
            rest = &after[end + 1..];
        }
        out.push_str(rest);
        Ok(out)
    }

    /// Boot-time security posture: empty/placeholder secrets are fatal in
    /// every environment (they are trust boundaries, not tuning).
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.security.jwt_secret.trim().is_empty() {
            anyhow::bail!("security.jwt_secret is empty — set JWT_SECRET before starting");
        }
        if self.security.session_secret.trim().is_empty() {
            anyhow::bail!("security.session_secret is empty — set MESSAGING_SESSION_SECRET before starting");
        }
        match self.jobs.sms_provider.as_str() {
            "noop" | "http" => {}
            other => anyhow::bail!(
                "jobs.sms_provider {other:?} is not a port this app implements (noop | http)"
            ),
        }
        if self.jobs.sms_provider == "http" {
            if self.gateway.sms_http_endpoint.trim().is_empty() {
                anyhow::bail!("jobs.sms_provider is 'http' but gateway.sms_http_endpoint is empty — set SMS_HTTP_ENDPOINT");
            }
            if self.gateway.sms_http_token.trim().is_empty() {
                anyhow::bail!("jobs.sms_provider is 'http' but gateway.sms_http_token is empty — set SMS_HTTP_TOKEN");
            }
        }
        if !self.gateway.platform_company_id.trim().is_empty()
            && self.gateway.platform_company().is_none()
        {
            anyhow::bail!(
                "gateway.platform_company_id {:?} is not a uuid — set MESSAGING_PLATFORM_COMPANY_ID \
                 to the platform company id (or leave it empty to keep the notification composition off)",
                self.gateway.platform_company_id
            );
        }
        Ok(())
    }
}

/// Deep-merge `overlay` into `base` (maps merge per-key; everything else replaces).
fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    match (base, overlay) {
        (serde_yaml::Value::Mapping(b), serde_yaml::Value::Mapping(o)) => {
            for (k, v) in o {
                match b.get_mut(&k) {
                    Some(slot) if slot.is_mapping() && v.is_mapping() => merge_yaml(slot, v),
                    _ => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (b, o) => *b = o,
    }
}
