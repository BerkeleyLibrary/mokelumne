"""Utility classes and routines for Mokelumne."""

__copyright__ = "© 2026 The Regents of the University of California.  MIT license."

import os
from typing import Any

from airflow.sdk import Param


def configurable_param(prefix: str, **kwargs: Any) -> Param:
    """Create a Param with TITLE, DESCRIPTION, and DEFAULT environment overrides.

    Append each uppercase option to ``prefix`` (e.g. ``PDF_SOURCE_DEFAULT``).
    Default overrides use str, int, or float for string, integer, or number
    types respectively. An unspecified type uses str. Empty strings are overrides.

    :param str prefix: Environment variable prefix, including any trailing separator.
    :param Any **kwargs: Keyword arguments passed to Param.
    :return: The configured Param.
    :rtype: Param
    """
    if (title := os.getenv(f"{prefix}TITLE")) is not None:
        kwargs["title"] = title

    if (description := os.getenv(f"{prefix}DESCRIPTION")) is not None:
        kwargs["description"] = description

    if (default := os.getenv(f"{prefix}DEFAULT")) is not None:
        converters = {"string": str, "integer": int, "number": float}
        kwargs["default"] = converters[kwargs.get("type", "string")](default)

    return Param(**kwargs)
