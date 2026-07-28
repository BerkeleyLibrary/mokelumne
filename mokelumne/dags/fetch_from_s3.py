import io
import os
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

from airflow.sdk import Param, dag, task
from airflow.exceptions import AirflowFailException
from mokelumne.util.s3_utils import list_bucket_files, download_single_s3_file

logger = logging.getLogger(__name__)

@dag(
    dag_id="s3_download",
    description="Download files from AWS S3 bucket to srv directory",
    schedule=None,
    catchup=False,
    params={
        "s3_bucket": Param(
            default="cityartsmedia", 
            type="string", 
            description="The name of your S3 bucket."
        ),
        "s3_conn": Param(
            default="AWS_S3_CITY_ARTS",
            type="string",
            description="The S3 connection."
        ),
        "destination_directory": Param(
            default="/srv/pa/city_arts/incoming", 
            type="string", 
            description="The absolute directory path where files will be saved."
        ),
        "file_prefix": Param(
            default="", 
            type=["null", "string"], 
            description="File prefix. Default is any prefix"
        ),
        "file_extension": Param(
            default="", 
            type=["null", "string"], 
            description="File extension. Default is any extension"
        ),
    },
)
def retrieve_S3_bucket():

    @task
    def validate_destination(params: dict):
        """Checks that the destination path exists."""
        dest_path = params["destination_directory"]
        destination_path = Path(dest_path)
        if not destination_path.exists():
            raise AirflowFailException(
                f"Destination directory does not exist: {destination_path}"
            )

    @task
    def get_bucket_file_names(params: dict) -> list:
        """Get a list of files for a given bucket. Can be filtered by prefix and extension"""
        bucket = params["s3_bucket"]
        s3_conn = params["s3_conn"]
        file_prefix = params["file_prefix"]
        file_extension = params["file_extension"]

        prefix = file_prefix.strip() if file_prefix else None
        extension = file_extension.strip() if file_extension else None
        
        file_names = list_bucket_files(bucket_name=bucket, conn_id = s3_conn, file_prefix=prefix, file_extension=extension)
        if file_names:
            for name in file_names:
                logger.info("Found file: %s", name)
        else:
            logger.info("The bucket is empty!")
            
        return file_names

    @task(max_active_tis_per_dag=4)
    def retrieve_file_from_bucket(file_key: str, params: dict):
        """Download from S3"""
        bucket = params["s3_bucket"]
        s3_conn = params["s3_conn"]
        dest_dir = params["destination_directory"]
        
        try:
            download_single_s3_file(
                file_key=file_key,
                conn_id=s3_conn,
                bucket_name=bucket,
                dest_dir=dest_dir
            )
        except Exception as ex:
            raise AirflowFailException(f"Task aborted. Details: {str(ex)}")

    validated_destination = validate_destination()
    bucket_filenames = get_bucket_file_names()
    retrieved_files = retrieve_file_from_bucket.expand(file_key=bucket_filenames)

    validated_destination >> bucket_filenames >> retrieved_files

retrieve_S3_bucket()
