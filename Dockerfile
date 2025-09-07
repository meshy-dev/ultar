# syntax=docker/dockerfile:1.4
# Multi-stage build for Ultar HTTP server behind Caddy with HTTP/3 and compression

FROM caddy:alpine

WORKDIR /app

# Ensure Caddy has writable config/data dirs when running unprivileged
ENV XDG_DATA_HOME=/app/.local/share
ENV XDG_CONFIG_HOME=/app/.config

# Build args to locate the release asset
ARG GH_REPO="owner/repo"
ARG VERSION="latest"
ARG TRIPLET="auto"

# Install wget/ca-certs to fetch release binary
RUN apk add --no-cache wget ca-certificates && update-ca-certificates

# Download ultar_httpd from GitHub Releases; allow 'latest' which resolves via redirect
RUN set -eux; \
    arch="$(uname -m)"; \
    triplet="${TRIPLET}"; \
    if [ "$triplet" = "auto" ]; then \
      case "$arch" in \
        x86_64) triplet="x86_64-linux-gnu.2.28" ;; \
        aarch64) triplet="aarch64-linux-gnu.2.28" ;; \
        *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
      esac; \
    fi; \
    base_url="https://github.com/${GH_REPO}/releases"; \
    if [ "${VERSION}" = "latest" ]; then url="$base_url/latest/download/ultar_httpd-${triplet}"; else url="$base_url/download/${VERSION}/ultar_httpd-${triplet}"; fi; \
    wget -O /usr/local/bin/ultar_httpd "$url"; \
    chmod +x /usr/local/bin/ultar_httpd

# Copy Caddy configuration
COPY Caddyfile /etc/caddy/Caddyfile

# Create non-root user and setup writable dirs
RUN adduser -D -H -s /sbin/nologin ultar && \
    mkdir -p /app/.local/share/caddy /app/.config/caddy && \
    chown -R ultar:ultar /app

USER ultar

# Data directory can be mounted here
ENV DATA_PATH=/data
VOLUME ["/data"]

# Expose HTTP and HTTPS for Caddy (include UDP/443 for HTTP/3/QUIC)
EXPOSE 80/tcp 443/tcp 443/udp

# Healthcheck: query Caddy
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD wget -qO- http://127.0.0.1:80/ || exit 1

# Run ultar_httpd on 3000 and proxy via Caddy
CMD ["sh", "-c", "ultar_httpd --addr 0.0.0.0 --port 3000 & exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile"]


