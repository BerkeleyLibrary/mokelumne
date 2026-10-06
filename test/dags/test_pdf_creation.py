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
        assert "run_base" in DAG.params
        assert "language" in DAG.params
        assert "max_poll_interval" in DAG.params
        assert "max_total_poll_time" in DAG.params
        assert "max_resolution" in DAG.params

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

    def test_polling_tasks_are_connected(self):
        """Ensure submissions flow through kwargs generation to the sensor."""
        submissions = DAG.get_task("submit_ocr_job")
        set_sensor_kwargs = DAG.get_task("set_sensor_kwargs")
        wait_for_pdf = DAG.get_task("wait_for_pdf")

        assert submissions in set_sensor_kwargs.upstream_list
        assert set_sensor_kwargs in wait_for_pdf.upstream_list

    def test_set_sensor_kwargs_converts_polling_params_to_seconds(self, monkeypatch):
        """Convert poll interval hours and total poll days to sensor seconds."""
        task = DAG.get_task("set_sensor_kwargs").python_callable
        monkeypatch.setitem(
            task.__globals__,
            "get_current_context",
            lambda: {
                "params": {
                    "max_poll_interval": 2,
                    "max_total_poll_time": 4,
                }
            },
        )

        result = task(["jobs/hash-id-1", "jobs/hash-id-2"])

        assert result == [
            {
                "endpoint": "jobs/hash-id-1",
                "max_wait": 2 * 60 * 60,
                "timeout": 4 * 24 * 60 * 60,
            },
            {
                "endpoint": "jobs/hash-id-2",
                "max_wait": 2 * 60 * 60,
                "timeout": 4 * 24 * 60 * 60,
            },
        ]
