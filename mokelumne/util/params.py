"""Helpers for working with Airflow Param objects"""

from typing import Any

from airflow.sdk import Param, Variable


def configurable_param(prefix: str, **kwargs: Any) -> Param:
    """
    Returns a Param whose title, description, and default can be overridden by Airflow Variables

    Given a prefix "foo_", the returned param can be overridden as follows:
    - Var=foo_title         -> overrides the title (literal string)
    - Var=foo_description   -> overrides the description (literal string)
    - Var=foo_default       -> overrides the default value (deserialized JSON)

    The default value is coerced via JSON deserialization. This is flexible but means that string
    values must be encoded and enclosed in double-quotes (e.g. `default="The \"staging\" default"`).

    Recall that Airflow Variables can be set via the environment. In the example above, you could override
    the title by setting `AIRFLOW_VAR_FOO_TITLE`.
    """

    if (title := Variable.get(f"{prefix}title", default=None)) is not None:
        kwargs["title"] = title

    if (description := Variable.get(f"{prefix}description", default=None)) is not None:
        kwargs["description"] = description

    if (default := Variable.get(f"{prefix}default", default=None, deserialize_json=True)) is not None:
        kwargs["default"] = default

    return Param(**kwargs)

