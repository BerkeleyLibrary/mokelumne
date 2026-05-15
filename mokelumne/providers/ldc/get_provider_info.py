from __future__ import annotations

from pathlib import Path
import yaml

_RUNTIME_FIELDS = {
    "package-name",
    "name",
    "description",
    "hook-class-names",
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
    "auth-backends",
    "auth-managers",
    "notifications",
    "executors",
    "config",
}


def get_provider_info() -> dict:
    data = (Path(__file__).parent / "provider.yaml").read_text()
    raw = yaml.safe_load(data)
    return {k: v for k, v in raw.items() if k in _RUNTIME_FIELDS}
