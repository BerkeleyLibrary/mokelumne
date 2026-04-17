"""Provides Langfuse prompt management routines."""

import importlib.metadata
import logging

from collections import namedtuple
from os import environ as ENV

from langfuse import Langfuse


Prompt = namedtuple('Prompt', ['prompt', 'version'])
Prompt.__doc__ += ': Contains a LLM prompt for generating image descriptions.'
Prompt.prompt.__doc__ = 'The LLM prompt.'
Prompt.version.__doc__ = 'The version of the prompt, for use in tracing and debugging.'

logger = logging.getLogger(__name__)


def get_prompt(name: str, version_or_label: int | str) -> Prompt:
    """Return the current prompt to use."""
    langfuse = Langfuse(release=importlib.metadata.version('mokelumne'),
                        environment=ENV.get('DEPLOYMENT_ID', 'default'))
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
