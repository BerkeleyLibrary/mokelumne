#!/bin/bash
set -euo pipefail

# Port forwarders: make localhost:<port> inside this container transparently
# reach the named compose services. See this container's Dockerfile for why.
socat TCP-LISTEN:8080,fork,reuseaddr TCP:airflow-apiserver:8080 &
socat TCP-LISTEN:8180,fork,reuseaddr TCP:keycloak:8180 &

exec "$@"
