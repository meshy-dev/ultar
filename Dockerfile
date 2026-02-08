FROM alpine:3.21

ARG TARGETARCH

WORKDIR /app

# Prebuilt binary placed by CI (see .github/workflows/ci.yml)
COPY bin/${TARGETARCH}/ultar_httpd /usr/local/bin/ultar_httpd
RUN chmod +x /usr/local/bin/ultar_httpd

# Templates and static assets (TemplateCache reads relative to CWD)
COPY ultar_httpd/templates /app/ultar_httpd/templates
COPY ultar_httpd/static    /app/ultar_httpd/static

# Mount your tar/utix data here
VOLUME /data
ENV DATA_PATH=/data

EXPOSE 3000

ENTRYPOINT ["ultar_httpd"]
CMD ["--addr", "0.0.0.0", "--port", "3000"]
