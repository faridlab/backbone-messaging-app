# backbone-messaging-app — multi-stage build.
FROM rust:1.83-slim AS builder
WORKDIR /app

# Cargo creds not needed: all deps are public git crates + workspace path deps.
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY config ./config

RUN cargo build --release --bin backbone-messaging-app

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/backbone-messaging-app /usr/local/bin/
COPY config ./config

# SSE: disable proxy buffering expectations and keep the connection window open.
ENV APP_ENV=prod
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD backbone-messaging-app healthcheck || exit 1

CMD ["backbone-messaging-app", "serve"]
