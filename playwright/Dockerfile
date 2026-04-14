# The Playwright server (here) and client (from requirements.txt)
# must match down to the minor version. If they differ, expect to
# see a clear error when running the e2e tests.
ARG PLAYWRIGHT_VERSION=1.58.2

FROM mcr.microsoft.com/playwright:v${PLAYWRIGHT_VERSION}-jammy

# socat runs per-port TCP forwarders that translate localhost:8080 and
# localhost:8180 inside this container to airflow-apiserver:8080 and
# keycloak:8180 on the compose network. This lets Chromium drive the
# Airflow UI via the exact same localhost URLs a host-browser developer
# uses, so webserver_config.py and Keycloak's KC_HOSTNAME stay prod-parity.
RUN apt-get update \
    && apt-get install -y --no-install-recommends socat \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/playwright-sidecar-entrypoint.sh
RUN chmod +x /usr/local/bin/playwright-sidecar-entrypoint.sh

# Inherits from the top-level ARG
ARG PLAYWRIGHT_VERSION
ENV PLAYWRIGHT_VERSION=$PLAYWRIGHT_VERSION
RUN npm install -g playwright@${PLAYWRIGHT_VERSION}

ENTRYPOINT ["/usr/local/bin/playwright-sidecar-entrypoint.sh"]
CMD ["playwright", "run-server", "--port", "3000", "--host", "0.0.0.0", "--path", "/ws"]
