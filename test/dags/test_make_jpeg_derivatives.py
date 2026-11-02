"""Dag structure and failure-semantics tests for JPEG derivative generation."""

from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import ParamValidationError

from mokelumne.dags import make_jpeg_derivatives as module


@pytest.fixture(scope="module", name="jpeg_dag")
def load_jpeg_dag():
    """Load the JPEG Dag without filesystem discovery."""
    with patch.object(module.derivatives, "initialize_run", side_effect=AssertionError):
        bag = DagBag(dag_folder=Path(module.__file__))
    assert not bag.import_errors
    return bag.dags["make_jpeg_derivatives"]


def test_schedule_parameters_and_taskflow_tasks(jpeg_dag):
    """Require explicit parameters and use TaskFlow-decorated tasks throughout."""
    assert jpeg_dag.schedule is None
    assert jpeg_dag.catchup is False
    assert jpeg_dag.max_active_runs == 1
    assert set(jpeg_dag.params) == {"input_root", "output_root", "log_name"}
    for name in jpeg_dag.params:
        with pytest.raises(ParamValidationError):
            jpeg_dag.params.get_param(name).resolve()
    valid = {"input_root": "/srv/source", "output_root": "/srv/output", "log_name": "collection"}
    for name, value in valid.items():
        assert jpeg_dag.params.get_param(name).resolve(value) == value
    for task in jpeg_dag.tasks:
        assert task.operator_name == "@task"


def test_mapping_topology_and_failure_guard(jpeg_dag):
    """Bound conversion and prevent finalization from masking failed tasks."""
    assert set(jpeg_dag.task_ids) == {
        "validate_inputs", "discover_inputs", "convert_batch", "finalize_reports", "complete",
    }
    assert jpeg_dag.get_task("discover_inputs").upstream_task_ids == {"validate_inputs"}
    conversion = jpeg_dag.get_task("convert_batch")
    assert conversion.is_mapped
    assert conversion.upstream_task_ids == {"discover_inputs"}
    assert conversion.max_active_tis_per_dag == 2
    finalization = jpeg_dag.get_task("finalize_reports")
    assert finalization.trigger_rule == "all_done"
    assert finalization.multiple_outputs is False
    assert finalization.upstream_task_ids == {"validate_inputs", "discover_inputs", "convert_batch"}
    completion = jpeg_dag.get_task("complete")
    assert completion.trigger_rule == "none_failed"
    assert completion.upstream_task_ids == {
        "validate_inputs", "discover_inputs", "convert_batch", "finalize_reports",
    }
    assert {task.task_id for task in jpeg_dag.leaves} == {"complete"}


def test_finalizer_fails_on_incomplete_infrastructure_work():
    """Fail report finalization when a batch has not completed."""
    context = {
        "params": {"input_root": "/srv/in", "output_root": "/srv/out", "log_name": "test"},
        "run_id": "run",
    }
    with patch.object(module, "get_current_context", return_value=context):
        with patch.object(
            module.derivatives, "finalize_reports", return_value={"state": "incomplete"},
        ):
            with pytest.raises(RuntimeError, match="Incomplete JPEG"):
                module.finalize_reports.function()


def test_per_file_errors_allow_successful_finalization():
    """Allow completion when all per-file failures have been accounted for."""
    context = {"params": {"output_root": "/srv/out", "log_name": "test"}, "run_id": "run"}
    summary = {"state": "complete_with_warnings", "counts": {"error": 3}}
    with patch.object(module, "get_current_context", return_value=context):
        with patch.object(module.derivatives, "finalize_reports", return_value=summary):
            assert module.finalize_reports.function() == summary
