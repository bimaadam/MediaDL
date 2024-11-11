# syntax=docker/dockerfile:1

FROM rust:1.99.1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY src ./src
COPY migrations ./migrations
COPY config ./config
ENV CARGO_BUILD_JOBS=2
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /tmp media
ADD --chmod=755 https://github.com/yt-dlp/yt-dlp/releases/download/2026.10.26/yt-dlp_linux /usr/local/bin/yt-dlp
COPY --from=build /src/target/release/media-engine /usr/local/bin/media-engine
COPY config /app/config
WORKDIR /app
USER media
ENV MEDIA_ENGINE_CONFIG_DIR=/app/config \
    XDG_CACHE_HOME=/tmp \
    HOME=/tmp
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD ["media-engine", "healthcheck", "api"]
ENTRYPOINT ["media-engine"]
CMD ["api"]
