//! Gateway adapters (increment 3) — the app-side impls of backbone-mail's
//! transport ports. The module owns the queue rows and state machines; these
//! adapters own the sockets: SMTP over `backbone-email`'s lettre service
//! (per-row server selection via the module's MAIL-M26 ladder), and the
//! generic HTTP JSON SMS provider (IAP-shaped, ADR-0024 env-ref secrets).

pub mod mail_gateway;
pub mod relay_b7;
pub mod sms_gateway;
