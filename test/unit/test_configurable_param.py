"""Tests for environment-configured Airflow Params."""

import pytest
from airflow.sdk import Param
from airflow.sdk.exceptions import ParamValidationError

from mokelumne.util import configurable_param


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """Isolate all supported override variables."""
    for option in ("TITLE", "DESCRIPTION", "DEFAULT", "TYPE", "MINIMUM"):
        monkeypatch.delenv(f"AIRFLOW_VAR_TEST_PARAM_{option}", raising=False)


def test_no_overrides():
    """Preserve Param defaults, metadata, and validation options."""
    options = {"default": 3, "type": "integer", "title": "Count",
               "description": "Help", "minimum": 1}
    actual = configurable_param("TEST_PARAM_", **options)
    expected = Param(**options)
    assert actual.dump() == expected.dump()
    assert actual.resolve() == 3a


def test_required_param():
    """Preserve an omitted default rather than introducing None."""
    actual = configurable_param("TEST_PARAM_", type="string")
    assert actual.dump() == Param(type="string").dump()
    with pytest.raises(ParamValidationError):
        actual.resolve()


@pytest.mark.parametrize("value", ["override", ""])
def test_metadata_overrides(monkeypatch, value):
    """Override title and description, including with empty strings."""
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_TITLE", value)
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_DESCRIPTION", value)
    actual = configurable_param("TEST_PARAM_", title="Original", description="Original")
    assert actual.schema["title"] == value
    assert actual.description == value


@pytest.mark.parametrize(("param_type", "raw", "expected"), [
    ("string", "42", "42"),
    ("string", "", ""),
    ("integer", "42", 42),
    ("integer", "0", 0),
    ("integer", "-7", -7),
    ("number", "1.25", 1.25),
    ("number", "1e2", 100.0),
])
def test_default_coercion(monkeypatch, param_type, raw, expected):
    """Convert defaults using the declared scalar JSON Schema type."""
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_DEFAULT", raw)
    actual = configurable_param("TEST_PARAM_", default=None, type=param_type)
    assert actual.resolve() == expected
    assert type(actual.value) is type(expected)


def test_unspecified_type_and_call_time_read(monkeypatch):
    """Read the environment at each call and treat untyped overrides as strings."""
    assert configurable_param("TEST_PARAM_", default=1).value == 1
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_DEFAULT", "2")
    assert configurable_param("TEST_PARAM_", default=1).value == "2"
    assert configurable_param("OTHER_PARAM_", default=1).value == 1


@pytest.mark.parametrize(("param_type", "raw"), [
    ("integer", "1.2"), ("number", "invalid"), ("integer", ""),
])
def test_invalid_default_override(monkeypatch, param_type, raw):
    """Reject defaults that cannot be converted to the declared numeric type."""
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_DEFAULT", raw)
    with pytest.raises(ValueError):
        configurable_param("TEST_PARAM_", type=param_type)


def test_unsupported_type_without_default_override(monkeypatch):
    """Allow metadata overrides on otherwise unsupported types."""
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_TITLE", "Enabled")
    assert configurable_param("TEST_PARAM_", default=True, type="boolean").resolve() is True


def test_other_options_and_validation(monkeypatch):
    """Ignore unrelated environment options and retain schema constraints."""
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_TYPE", "string")
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_MINIMUM", "0")
    monkeypatch.setenv("AIRFLOW_VAR_TEST_PARAM_DEFAULT", "2")
    actual = configurable_param("TEST_PARAM_", type="integer", minimum=3)
    assert actual.schema == {"type": "integer", "minimum": 3}
    with pytest.raises(ParamValidationError):
        actual.resolve()
