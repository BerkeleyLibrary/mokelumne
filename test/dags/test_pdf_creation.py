"""Test the pdf_creation DAG."""

from test.util.dag_helper import get_dag


DAG = get_dag("pdf_creation")


class TestPDFCreationDag:
    """Tests for the pdf_creation DAG."""

    def test_validate_inputs_task_exists(self):
        """Ensure validate_inputs task exists."""
        assert DAG.get_task("validate_inputs")

    def test_discover_documents_task_exists(self):
        """Ensure discover_documents task exists."""
        assert DAG.get_task("discover_documents")

    def test_expected_params_exist(self):
        """Ensure expected DAG parameters exist."""
        assert "source" in DAG.params
        assert "destination" in DAG.params
        assert "language" in DAG.params

    def test_task_order(self):
        """Ensure DAG tasks execute in the expected order."""
        # TODO: Update this test as additional tasks are implemented.
        validate_inputs = DAG.get_task("validate_inputs")
        discover_documents = DAG.get_task("discover_documents")

        assert [task.task_id for task in validate_inputs.downstream_list] == ["discover_documents"]
        assert [task.task_id for task in discover_documents.downstream_list] == ["process_document"]

    def test_process_document_task_exists(self):
        """Ensure process_document task exists."""
        assert DAG.get_task("process_document")
