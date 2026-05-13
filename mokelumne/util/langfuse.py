"""Provides Langfuse prompt management routines."""

import importlib.metadata
import json
import logging

from collections import namedtuple
from os import environ as ENV

from airflow.sdk.bases.hook import BaseHook
from langfuse import Langfuse

Prompt = namedtuple('Prompt', ['prompt', 'version'])
Prompt.__doc__ += ': Contains a LLM prompt for generating image descriptions.'
Prompt.prompt.__doc__ = 'The LLM prompt.'
Prompt.version.__doc__ = 'The version of the prompt, for use in tracing and debugging.'

logger = logging.getLogger(__name__)


def _base_url(conn):
    """Normalize host by preserving scheme or defaulting to https."""
    host = conn.host
    if not host:
        return host
    return f'https://{host}'

def _get_langfuse_connection_settings(conn_id: str) -> tuple[str, str, str]:
    """Return host/public/secret key tuple from the Langfuse Airflow connection."""
    conn = BaseHook.get_connection(conn_id)
    base_url = _base_url(conn)
    extras = conn.extra_dejson
    raw = extras.get('extra')
    creds = json.loads(raw)
    public_key = creds.get('public_key')
    secret_key = creds.get('secret_key')
    if not public_key or not secret_key or not base_url:
        raise ValueError(
            f'Missing public_key/secret_key or base_url in Airflow connection {conn_id}. '
            'Please check AIRFLOW_CONN_LANGFUSE_DEFAULT.'
        )
    return base_url, public_key, secret_key

def get_langfuse_client(conn_id: str = 'langfuse_default') -> Langfuse:
    """Return a Langfuse client configured from the ``langfuse_default`` Airflow connection."""
    base_url, public_key, secret_key = _get_langfuse_connection_settings(conn_id)
    return Langfuse(
        base_url=base_url,
        public_key=public_key,
        secret_key=secret_key,
        release=importlib.metadata.version('mokelumne'),
        environment=ENV.get('DEPLOYMENT_ID', 'default'),
    )

def get_prompt(name: str, version_or_label: int | str) -> Prompt:
    """Return the current prompt to use."""
    langfuse = get_langfuse_client()
    if isinstance(version_or_label, int):
        logger.debug(
            f"Getting Langfuse prompt {name}, version {version_or_label}"
        )
        sys_prompt = langfuse.get_prompt(name, version=version_or_label)
    else:
        logger.debug(
            f"Getting Langfuse prompt {name}, label {version_or_label}"
        )
        sys_prompt = langfuse.get_prompt(name, label=version_or_label)

    return Prompt(prompt=sys_prompt.prompt, version=sys_prompt.version)
