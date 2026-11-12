"""Tests for environment-configured Airflow Params."""

import json

import pytest

from airflow.sdk import Param

from mokelumne.util.params import configurable_param


@pytest.fixture()
def int_param_args():
    return {
        "default": 3,
        "type": "integer",
        "title": "Count",
        "description": "Number of loop iterations",
        "minimum": 1
    }


@pytest.fixture()
def string_param_args():
    return {
        "default": "MyLoop",
        "type": "string",
        "title": "Label",
        "description": "Label the loop iteration",
    }


@pytest.fixture()
def number_param_args():
    return {
        "default": 0.5,
        "type": "number",
        "title": "Temperature",
        "description": "Temperature for model invocation",
    }


def test_no_overrides(string_param_args):
    """Preserve Param string_param_args, metadata, and validation options."""

    param = configurable_param('test_prefix_', **string_param_args)
    expected = Param(**string_param_args)
    assert param.dump() == expected.dump()


def test_overrides_title(monkeypatch, string_param_args):
    """Override title via Airflow variable and assert only that changed"""

    monkeypatch.setenv(
        "AIRFLOW_VAR_TEST_PREFIX_TITLE",
        title := "Overrides the title"
    )

    param = configurable_param("test_prefix_", **string_param_args)
    expected = Param(**(string_param_args | {"title": title}))
    assert param.dump() == expected.dump()


def test_overrides_description(monkeypatch, string_param_args):
    """Override description via Airflow variable and assert only that changed"""

    monkeypatch.setenv(
        "AIRFLOW_VAR_TEST_PREFIX_DESCRIPTION",
        description := "The description has been overridden"
    )

    param = configurable_param("test_prefix_", **string_param_args)
    expected = Param(**(string_param_args | {"description": description}))
    assert param.dump() == expected.dump()


def test_overrides_string_default(monkeypatch, string_param_args):
    """Override default for a string-type parameter"""

    monkeypatch.setenv(
        "AIRFLOW_VAR_TEST_PREFIX_DEFAULT",
        json.dumps(default := "The default is overridden")
    )

    param = configurable_param("test_prefix_", **string_param_args)
    expected = Param(**(string_param_args | {"default": default}))
    assert param.dump() == expected.dump()


def test_overrides_integer_default(monkeypatch, int_param_args):
    """Override default for an integer-type parameter"""

    monkeypatch.setenv(
        "AIRFLOW_VAR_TEST_PREFIX_DEFAULT",
        json.dumps(default := 55)
    )

    param = configurable_param("test_prefix_", **int_param_args)
    expected = Param(**(int_param_args | {"default": default}))
    assert param.dump() == expected.dump()


def test_overrides_float_default(monkeypatch, number_param_args):
    """Override the default for a float-type parameter"""

    monkeypatch.setenv(
        "AIRFLOW_VAR_TEST_PREFIX_DEFAULT",
        json.dumps(default := 5.5)
    )

    param = configurable_param("test_prefix_", **number_param_args)
    expected = Param(**(number_param_args | {"default": default}))
    assert param.dump() == expected.dump()
