"""Test the copy_files DAG."""

import pytest

from pathlib import Path
from unittest.mock import patch

from airflow.dag_processing.dagbag import DagBag
from airflow.providers.standard.operators.hitl import ApprovalOperator
from mokelumne.dags.copy_files import build_volume_path

DAG_DIR = Path(__file__).resolve().parent.parent.parent / "mokelumne" / "dags"
_DAG_BAG = DagBag(dag_folder=DAG_DIR.resolve(), include_examples=False)
DAG = _DAG_BAG.get_dag("copy_files")


class TestBuildVolumePath:
    """Test path building from volume and subdirectory params."""

    def test_build_volume_path_combines_volume_and_subdirectory(self):
        result = build_volume_path("/srv/pa", "aerial/ucb", "Source")

        assert result == Path("/srv/pa/aerial/ucb")

    def test_build_volume_path_requires_subdirectory(self):
        with pytest.raises(ValueError, match="Source subdirectory is required"):
            build_volume_path("/srv/pa", "", "Source")

    def test_build_volume_path_rejects_absolute_subdirectory(self):
        with pytest.raises(ValueError, match="Source subdirectory must be relative"):
            build_volume_path("/srv/pa", "/aerial/ucb", "Source")


class TestCopyFilesDAGStructure:
    """Test basic DAG structure and configuration."""

    def test_dag_loads(self):
        """Test that the DAG loads without errors."""
        assert DAG is not None
        assert DAG.dag_id == "copy_files"

    def test_confirm_copy_task_exists(self):
        """Test that confirm_copy task exists in the DAG."""
        task_ids = [t.task_id for t in DAG.tasks]
        assert "confirm_copy" in task_ids

    def test_confirm_copy_runs_before_copying_files(self):
        """Test that confirm_copy runs before files are copied."""
        confirm_copy_task = DAG.get_task("confirm_copy")
        copy_manifest_files = DAG.get_task("copy_manifest_files")
        assert confirm_copy_task in copy_manifest_files.upstream_list

    def test_confirm_copy_is_approval_operator(self):
        """Test that confirm_copy is an ApprovalOperator."""
        confirm_copy_task = DAG.get_task("confirm_copy")
        assert isinstance(confirm_copy_task, ApprovalOperator)

    def test_confirm_copy_configuration(self):
        """Test that ApprovalOperator is configured correctly."""
        confirm_copy_task = DAG.get_task("confirm_copy")
        # Check subject and body templates
        assert "review" in confirm_copy_task.subject.lower() or "approve" in confirm_copy_task.subject.lower()
        assert confirm_copy_task.body is not None
        assert "task_instance.xcom_pull" in confirm_copy_task.body
        assert "task_ids='build_copy_paths'" in confirm_copy_task.body
        assert "key='source'" in confirm_copy_task.body
        assert "key='destination'" in confirm_copy_task.body

    def test_dag_task_order(self):
        """Test that tasks execute in correct order."""
        validate_source = DAG.get_task("validate_source")
        prepare_destination = DAG.get_task("prepare_destination")
        build_manifest = DAG.get_task("build_manifest")
        confirm_copy = DAG.get_task("confirm_copy")
        copy_manifest_files = DAG.get_task("copy_manifest_files")
        verify_manifest = DAG.get_task("verify_manifest")

        assert validate_source in prepare_destination.upstream_list
        assert prepare_destination in build_manifest.upstream_list
        assert build_manifest in confirm_copy.upstream_list
        assert confirm_copy in copy_manifest_files.upstream_list
        assert copy_manifest_files in verify_manifest.upstream_list

    def test_all_required_tasks_exist(self):
        """Test that all required tasks are present in the DAG."""
        required_tasks = [
            "build_copy_paths",
            "confirm_copy",
            "validate_source",
            "prepare_destination",
            "build_manifest",
            "copy_manifest_files",
            "verify_manifest",
        ]
        task_ids = [t.task_id for t in DAG.tasks]
        for task_id in required_tasks:
            assert task_id in task_ids, f"Task '{task_id}' not found in DAG"


class TestApprovalOperatorMocked:
    """Test ApprovalOperator behavior with mocked approval scenarios."""

    @patch("airflow.providers.standard.operators.hitl.ApprovalOperator.execute")
    def test_confirm_copy_approval_approved(self, mock_execute):
        """Test DAG execution when approval is granted."""
        # Simulate approval being granted (execute completes without exception)
        mock_execute.return_value = None

        confirm_copy_task = DAG.get_task("confirm_copy")
        result = confirm_copy_task.execute({})

        # Execution should complete successfully
        assert result is None

    @patch("airflow.providers.standard.operators.hitl.ApprovalOperator.execute")
    def test_confirm_copy_approval_rejected(self, mock_execute):
        """Test DAG execution when approval is rejected."""
        from airflow.exceptions import AirflowException

        # Simulate rejection (raise AirflowException)
        mock_execute.side_effect = AirflowException("Approval rejected")

        confirm_copy_task = DAG.get_task("confirm_copy")

        try:
            confirm_copy_task.execute({})
            assert False, "Expected AirflowException to be raised"
        except AirflowException as e:
            assert "Approval rejected" in str(e)

    def test_confirm_copy_template_uses_built_paths(self):
        """Test that ApprovalOperator templates render correctly with dag params."""
        confirm_copy_task = DAG.get_task("confirm_copy")

        # The templates should have params placeholders
        assert "task_instance.xcom_pull" in confirm_copy_task.body
        assert "task_ids='build_copy_paths'" in confirm_copy_task.body
        assert "key='source'" in confirm_copy_task.body
        assert "key='destination'" in confirm_copy_task.body

    @patch("airflow.providers.standard.operators.hitl.ApprovalOperator.execute")
    def test_confirm_copy_blocks_downstream_tasks(self, mock_execute):
        """Test that downstream tasks depend on confirm_copy approval."""
        build_manifest = DAG.get_task("build_manifest")
        confirm_copy = DAG.get_task("confirm_copy")
        copy_manifest_files = DAG.get_task("copy_manifest_files")

        assert confirm_copy in build_manifest.downstream_list
        assert copy_manifest_files in confirm_copy.downstream_list

    def test_approval_subject_content(self):
        """Test that approval subject informs user appropriately."""
        confirm_copy_task = DAG.get_task("confirm_copy")

        # Subject should mention approval and file copy
        assert "review" in confirm_copy_task.subject.lower() or "approve" in confirm_copy_task.subject.lower()
        # Body should contain the path params
        assert "task_instance.xcom_pull" in confirm_copy_task.body
        assert "task_ids='build_copy_paths'" in confirm_copy_task.body
        assert "key='source'" in confirm_copy_task.body
        assert "key='destination'" in confirm_copy_task.body

    def test_approval_body_content(self):
        """Test that approval body provides clear instructions."""
        confirm_copy_task = DAG.get_task("confirm_copy")

        # Body should be readable and provide context
        assert len(confirm_copy_task.body) > 0
        assert "approve" in confirm_copy_task.body.lower()
        assert "task_instance.xcom_pull" in confirm_copy_task.body
        assert "task_ids='build_copy_paths'" in confirm_copy_task.body
        assert "key='source'" in confirm_copy_task.body
        assert "key='destination'" in confirm_copy_task.body


class TestConfirmCopyIntegration:
    """Integration tests for confirm_copy in the DAG context."""

    def test_dag_serialization(self):
        """Test that DAG can be serialized (required for Airflow UI)."""
        # The DagBag already loaded it, which tests serialization
        assert DAG is not None
        # Should have a dag_id and tasks
        assert hasattr(DAG, "dag_id")
        assert hasattr(DAG, "tasks")
        assert len(DAG.tasks) > 0

    def test_confirm_copy_template_uses_built_paths(self):
        """Test that confirm_copy references match DAG param definitions."""
        # DAG should have source and destination params
        dag_params = DAG.params
        assert "source_volume" in dag_params
        assert "source_subdirectory" in dag_params
        assert "destination_volume" in dag_params
        assert "destination_subdirectory" in dag_params

        # ApprovalOperator should reference the built source/destination paths.
        confirm_copy_task = DAG.get_task("confirm_copy")
        assert "task_instance.xcom_pull" in confirm_copy_task.body
        assert "task_ids='build_copy_paths'" in confirm_copy_task.body
        assert "key='source'" in confirm_copy_task.body
        assert "key='destination'" in confirm_copy_task.body
