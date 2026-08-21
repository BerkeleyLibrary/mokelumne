"""Test the pdf_creation DAG."""

from test.util.dag_helper import get_dag


DAG = get_dag("pdf_creation")


class TestPDFCreationDag:
    """Tests for the pdf_creation DAG."""

    def test_validate_inputs_task_exists(self):
        """Ensure validate_inputs task exists."""
        assert DAG.get_task("validate_inputs")

    def test_expected_params_exist(self):
        """Ensure expected DAG parameters exist."""
        assert "source" in DAG.params
        assert "destination" in DAG.params
        assert "language" in DAG.params

