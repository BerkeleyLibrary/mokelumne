"""Provides utilities for resolving AWS settings from Airflow connections."""

import logging
from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)

def get_aws_connection_settings(conn_id: str = "aws_default") -> dict[str, str]:
    """Return AWS kwargs from an Airflow connection."""
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        message = f"Failed to resolve AWS connection {conn_id}: {exc}"
        logger.info(message)
        raise RuntimeError(message) from exc

    extra = conn.extra_dejson or {}
    settings = {
        "aws_access_key_id": conn.login,
        "aws_secret_access_key": conn.password,
        "region_name": extra.get("region_name"),
        "endpoint_url": extra.get("endpoint_url")
    }
    return {key: str(value) for key, value in settings.items() if value}
