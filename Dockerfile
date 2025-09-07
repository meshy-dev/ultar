FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget ca-certificates curl gnupg libcap2-bin caddy && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Ensure Caddy has writable config/data dirs when running unprivileged
ENV XDG_DATA_HOME=/app/.local/share
ENV XDG_CONFIG_HOME=/app/.config

# Build args to locate the release asset
ARG GH_REPO="meshy-dev/ultar"
ARG VERSION="latest"
ARG TRIPLET="auto"

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

# Allow Caddy (running as non-root) to bind to :80/:443
RUN /sbin/setcap cap_net_bind_service=+ep /usr/bin/caddy || true

# Copy Caddy configuration
COPY Caddyfile /etc/caddy/Caddyfile

# Data directory can be mounted here
ENV DATA_PATH=/data
VOLUME ["/data"]

# Expose HTTP and HTTPS for Caddy (include UDP/443 for HTTP/3/QUIC)
EXPOSE 80/tcp 443/tcp 443/udp

# Healthcheck: query Caddy
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD wget -qO- http://127.0.0.1:80/ || exit 1

# Run ultar_httpd on 3000 and proxy via Caddy
CMD ["sh", "-c", "ultar_httpd -d $DATA_PATH --addr 0.0.0.0 --port 3000 & exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile"]


