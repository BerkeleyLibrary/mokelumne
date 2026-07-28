"""Test the s3_download DAG."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowFailException
from test.util.dag_helper import get_dag

with patch("mokelumne.util.s3_utils.list_bucket_files"), \
     patch("mokelumne.util.s3_utils.download_single_s3_file"):
    DAG = get_dag("s3_download")


class TestS3DownloadDAGStructure:
    """Test basic DAG structure, configurations, and upstream dependencies."""

    def test_dag_loads(self):
        """Test that the DAG loads without errors."""
        assert DAG is not None
        assert DAG.dag_id == "s3_download"

    def test_all_required_tasks_exist(self):
        """Test that all required tasks are present in the DAG."""
        required_tasks = [
            "validate_destination",
            "get_bucket_file_names",
            "retrieve_file_from_bucket",
        ]
        task_ids = [t.task_id for t in DAG.tasks]
        for task_id in required_tasks:
            assert task_id in task_ids, f"Task '{task_id}' not found in DAG"

    def test_dag_task_order(self):
        """Test that tasks execute in the correct linear order."""
        validate_dest = DAG.get_task("validate_destination")
        get_filenames = DAG.get_task("get_bucket_file_names")
        retrieve_file = DAG.get_task("retrieve_file_from_bucket")

        # Verify: validate_destination >> get_bucket_file_names >> retrieve_file_from_bucket
        assert validate_dest in get_filenames.upstream_list
        assert get_filenames in retrieve_file.upstream_list

    def test_dag_parameters_exist(self):
        """Verify that default DAG parameters are defined correctly."""
        dag_params = DAG.params
        assert "s3_bucket" in dag_params
        assert "destination_directory" in dag_params
        assert "file_prefix" in dag_params
        assert "file_extension" in dag_params
        assert "s3_conn" in dag_params


class TestValidateDestinationTask:
    """Test validate_destination task logic"""

    def test_validate_destination_success(self, tmp_path):
        """Should pass silently if the destination directory exists."""
        validate_task = DAG.get_task("validate_destination").python_callable
        params = {"destination_directory": str(tmp_path)}
        validate_task(params=params)

    def test_validate_destination_raises_error(self):
        """Should throw AirflowFailException if the directory is missing."""
        validate_task = DAG.get_task("validate_destination").python_callable
        params = {"destination_directory": "/nonexistent/absolute/path/to/folder"}
        with pytest.raises(AirflowFailException) as exc_info:
            validate_task(params=params)
        assert "Destination directory does not exist:" in str(exc_info.value)


class TestGetBucketFileNamesTask:
    """Test get_bucket_file_names task logic and utility filtering calls."""

    def test_get_bucket_file_names_success(self):
        """Should clean and pass configurations to list_bucket_files utility."""
        get_filenames_task = DAG.get_task("get_bucket_file_names").python_callable
        params = {
            "s3_bucket": "my-bucket",
            "file_prefix": " raw_data/ ",
            "file_extension": " .wav",
            "s3_conn": "AWS_S3_CITY_ARTS"
        }
        mock_list_files = MagicMock(return_value=["file1.wav", "file2.wav"])
        
        with patch.dict(get_filenames_task.__globals__, {"list_bucket_files": mock_list_files}):
            result = get_filenames_task(params=params)
            
        assert result == ["file1.wav", "file2.wav"]
        mock_list_files.assert_called_once_with(
            bucket_name="my-bucket",
            file_prefix="raw_data/",
            file_extension=".wav",
            conn_id="AWS_S3_CITY_ARTS"
        )


class TestRetrieveFileFromBucketTask:
    """Test retrieve_file_from_bucket task and download."""

    def test_retrieve_file_from_bucket_success(self):
        """Should test download utility with mappings."""
        retrieve_task = DAG.get_task("retrieve_file_from_bucket").python_callable
        params = {
            "s3_bucket": "my-bucket",
            "destination_directory": "/srv/pa/incoming",
            "s3_conn": "AWS_S3_CITY_ARTS"
        }
        mock_download_file = MagicMock()
        
        with patch.dict(retrieve_task.__globals__, {"download_single_s3_file": mock_download_file}):
            retrieve_task(file_key="data/audio_file.wav", params=params)
            
        mock_download_file.assert_called_once_with(
            file_key="data/audio_file.wav",
            bucket_name="my-bucket",
            dest_dir="/srv/pa/incoming",
            conn_id="AWS_S3_CITY_ARTS"
        )

    def test_retrieve_file_from_bucket_failure(self):
        """Should catch utility exceptions and re-raise them as AirflowFailException."""
        retrieve_task = DAG.get_task("retrieve_file_from_bucket").python_callable
        params = {
            "s3_bucket": "my-bucket",
            "destination_directory": "/srv/pa/incoming",
            "s3_conn": "AWS_S3_CITY_ARTS"
        }
        mock_download_file = MagicMock(side_effect=RuntimeError("Disk full error"))
        
        with patch.dict(retrieve_task.__globals__, {"download_single_s3_file": mock_download_file}):
            with pytest.raises(AirflowFailException) as exc_info:
                retrieve_task(file_key="data/audio_file.wav", params=params)
                
        assert "Task aborted. Details: Disk full error" in str(exc_info.value)

