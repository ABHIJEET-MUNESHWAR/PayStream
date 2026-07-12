# ---- Build stage -----------------------------------------------------------
FROM rust:1.82-slim AS build
WORKDIR /build

# Cache dependencies against the manifests first.
COPY Cargo.toml Cargo.lock* ./
COPY crates/types/Cargo.toml crates/types/
COPY crates/resilience/Cargo.toml crates/resilience/
COPY crates/core/Cargo.toml crates/core/
COPY crates/infra/Cargo.toml crates/infra/
COPY crates/api/Cargo.toml crates/api/
COPY crates/node/Cargo.toml crates/node/
COPY . .
RUN cargo build --release -p paystream-node

# ---- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim AS runtime
WORKDIR /app
RUN groupadd --system pay && useradd --system --gid pay pay
COPY --from=build /build/target/release/paystream-node /usr/local/bin/paystream-node
USER pay
EXPOSE 8082
ENV RUST_LOG=info
ENTRYPOINT ["paystream-node"]
