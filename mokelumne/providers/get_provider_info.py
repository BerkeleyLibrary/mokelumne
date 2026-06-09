"""Airflow provider info for the Mokelumne provider.

A single distribution can only register one Airflow provider (Airflow keys
providers by distribution name), so both the TIND and LDC connection types are
declared together in this provider's ``provider.yaml``.
"""

from __future__ import annotations

from pathlib import Path
import yaml

# Fields defined in airflow's provider_info.schema.json runtime properties.
# Only these are valid at runtime — build-time-only keys from provider.yaml are excluded.
_RUNTIME_FIELDS = {
    "package-name",
    "name",
    "description",
    "hook-class-names",  # deprecated but still accepted
    "connection-types",
    "hooks",
    "operators",
    "sensors",
    "transfers",
    "triggers",
    "bundles",
    "integrations",
    "filesystems",
    "asset-uris",
    "dialects",
    "extra-links",
    "secrets-backends",
    "auth-backends",
    "auth-managers",
    "notifications",
    "executors",
    "config",
}


def get_provider_info() -> dict:
    """Parse provider yaml to pass to Airflow."""
    data = (Path(__file__).parent / "provider.yaml").read_text()
    raw = yaml.safe_load(data)
    return {k: v for k, v in raw.items() if k in _RUNTIME_FIELDS}
