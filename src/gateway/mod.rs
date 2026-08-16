//! Gateway wiring (increment 3) — the transport adapters live in the modules
//! now (promoted): backbone-mail's `gateway-smtp` / `gateway-sms-http`
//! features and backbone-notification's `gateway-queue`. This app keeps only
//! its own outbox relay handler (`relay_b7`); the adapters are re-exported so
//! main.rs's imports stay stable.

pub mod relay_b7;

pub use backbone_mail::infrastructure::gateway::{HttpSmsApi, SmtpMailApi};
pub use backbone_notification::infrastructure::gateway::QueueCommunicationPort;
