FROM rust:1.97-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /nonexistent ingestorx
COPY --from=builder /src/target/release/xml_watcher /usr/local/bin/xml_watcher
COPY --from=builder /src/target/release/consumer /usr/local/bin/consumer
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/xml_watcher"]
