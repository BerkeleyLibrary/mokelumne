"""Provides Langfuse prompt management routines."""

import importlib.metadata
from collections import namedtuple
from os import environ as ENV

from langfuse import Langfuse


Prompt = namedtuple('Prompt', ['prompt', 'version'])
Prompt.__doc__ += ': Contains a LLM prompt for generating image descriptions.'
Prompt.prompt.__doc__ = 'The LLM prompt.'
Prompt.version.__doc__ = 'The version of the prompt, for use in tracing and debugging.'


def get_prompt() -> Prompt:
    """Return the current prompt to use."""
    langfuse = Langfuse(release=importlib.metadata.version('mokelumne'),
                        environment=ENV.get('DEPLOYMENT_ID', 'default'))
    sys_prompt = langfuse.get_prompt(ENV.get('LANGFUSE_PROMPT', 'image-description'),
                                     label=ENV.get('LANGFUSE_PROMPT_LABEL', 'production'))
    return Prompt(prompt=sys_prompt.prompt, version=sys_prompt.version)
