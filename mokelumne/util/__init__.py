"""Utility classes and routines for Mokelumne."""

__copyright__ = "© 2026 The Regents of the University of California.  MIT license."

import os
from typing import Any

from airflow.sdk import Param, Variable


def configurable_param(prefix: str, **kwargs: Any) -> Param:
    """
    Returns a Param whose title/description/default can be overridden by an Airflow Variable

    Given a prefix "foo_", the returned param can be overridden as follows:
    - Var.foo_title -> overrides the title
    - Var.foo_description -> overrides the description
    - Var.foo_default -> overrides the default value

    The default value is coerced by type, but only supports simple types (int, float, str).

    Airflow Variables can be set via the environment. In the example above, you could override
    the title by setting `AIRFLOW_VAR_FOO_TITLE`.
    """
    if (title := Variable.get(f"{prefix}title")) is not None:
        kwargs["title"] = title

    if (description := Variable.get(f"{prefix}description")) is not None:
        kwargs["description"] = description

    if (default := Variable.get(f"{prefix}default")) is not None:
        converters = {"string": str, "integer": int, "number": float}
        kwargs["default"] = converters[kwargs.get("type", "string")](default)

    return Param(**kwargs)
