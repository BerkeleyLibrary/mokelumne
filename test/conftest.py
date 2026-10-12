"""
PyTest fixtures, helpers and hooks that apply to all tests
"""

import csv
import io

from pathlib import Path

import pytest


GOBI_DAG_VARIABLES = {
    "process_gobi_orders_input_dir": "/opt/airflow/files/gobi/in",
    "process_gobi_orders_output_dir": "/opt/airflow/files/gobi/out",
    "process_gobi_orders_processed_dir": "/opt/airflow/files/gobi/processed",
}
_gobi_dag_variables_patch = pytest.MonkeyPatch()


def pytest_sessionstart() -> None:
    """Set Variables needed by collection-time DagBag parsing."""

    for key, value in GOBI_DAG_VARIABLES.items():
        _gobi_dag_variables_patch.setenv(f"AIRFLOW_VAR_{key.upper()}", value)


def pytest_sessionfinish() -> None:
    """Restore the environment after the test session."""

    _gobi_dag_variables_patch.undo()


@pytest.fixture(scope="session")
def gobi_dag_variables() -> dict[str, str]:
    """Return the mocked Airflow Variable values used by Dag tests."""

    return GOBI_DAG_VARIABLES


def read_csv(path: Path | str) -> csv.DictReader:
    """
    Return a csv.DictReader over the rows of the given CSV.

    DictReader infers fieldnames from the first row, assumed to be headers.
    """
    with open(path, newline='', encoding='utf-8') as f:
        return csv.DictReader(io.StringIO(f.read()))


def pytest_collection_modifyitems(config, items):
    """
    Automatically marks tests with the name of their enclosing directory above ./test

    Uses Path to avoid cross-platform issues, and matches relative to the
    test's rootdir to avoid matching on directories that happen to be named
    "unit" (or whatever), e.g. in CI.
    """

    rootdir = Path(config.rootdir)
    for item in items:
        try:
            item_relpath = Path(item.fspath).relative_to(rootdir)
        except ValueError:
            # test was collected outside of the project's root directory
            continue

        try:
            itemdir = item_relpath.parts[1]
            item.add_marker(getattr(pytest.mark, itemdir))
        except IndexError:
            # test was in the top-level ./test directory
            continue
