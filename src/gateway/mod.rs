//! Gateway wiring (increment 3) — the transport adapters live in the modules
//! now (promoted): backbone-mail's `gateway-smtp` / `gateway-sms-http`
//! features and backbone-notification's `gateway-queue`, plus the middleware
//! (UserScope, notification gate) and the MAIL-B7 relay consumer. Re-exported
//! here so main.rs's imports stay stable.

pub use backbone_mail::infrastructure::gateway::{HttpSmsApi, SmtpMailApi};
pub use backbone_notification::infrastructure::gateway::QueueCommunicationPort;
