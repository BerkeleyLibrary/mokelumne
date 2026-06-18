"""
File copy DAG to transfer files from one location to another
"""

from __future__ import annotations

import logging
import shutil

from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

logger = logging.getLogger(__name__)


@dag(
    description="Transfers files from one location to another",
    schedule=None,
    catchup=False,
    params={
        "source": Param(
            default="",
            title="Source Directory",
            description="Directory where source files are found",
            type="string",
        ),
        "destination": Param(
            default="",
            title="Destination Directory",
            description="Directory where source files will be copied to",
            type="string",
        ),
    },
    tags=["file-transfer"],
)
def copy_files():
    """
    Copy files from a source directory to an empty destination directory.
    """

    @task
    def validate_source() -> str:
        """
        Checks that the source is a valid directory
        """
        logger.info("VALIDATE_SOURCE - VERSION 2")
        
        ctx = get_current_context()
        source = ctx["params"]["source"]

        if not source.strip():
            raise AirflowFailException("Source directory is required")
        
        source_path = Path(source)
        if not source_path.exists():
            raise AirflowFailException(f"Source directory does not exist: {source_path}")
        
        if not source_path.is_dir():
            raise AirflowFailException(f"Source is not a directory: {source_path}")

        logger.info("SOURCE IS: %s", source)

        return source
    
    @task
    def prepare_destination() -> str:
        """
        Prepare the destination directory
        
        If directory does not exist, create it
        If it exists and it contains files in it, fail!
        """
        logger.info("PREPARE_DESTINATION - VERSION 1")

        ctx = get_current_context()
        destination = ctx["params"]["destination"]

        if not destination.strip():
            raise AirflowFailException("Destination directory is required")
        
        destination_path = Path(destination)

        if not destination_path.exists():
            logger.info("Creating destination directory: %s", destination_path)
            destination_path.mkdir(parents=True)
        
        if not destination_path.is_dir():
            raise AirflowFailException(f"Destination is not a directory: {destination_path}")
        
        if any(destination_path.iterdir()):
            raise AirflowFailException(f"Destination directory contains files: {destination_path}")

        logger.info("DESTINATION IS: %s", destination)

        return destination

    @task
    def copy_source_files(source: str, destination: str) -> list[str]:
        """
        Copy all files and subdirectories from source directory to destination directory
        """
        logger.info("COPY_SOURCE_FILES - VERSION 3")

        source_path = Path(source)
        destination_path = Path(destination)

        copied_files = []

        for item in source_path.rglob("*"):
            relative_path = item.relative_to(source_path)
            destination_item = destination_path / relative_path

            if item.is_dir():
                logger.info("Creating destination subdirectory: %s", destination_item)
                destination_item.mkdir(parents=True, exist_ok=True)
            elif item.is_file():
                destination_item.parent.mkdir(parents=True, exist_ok=True)
                
                logger.info("Copying %s to %s", item,  destination_item)
                shutil.copy2(item, destination_item)

                copied_files.append(str(relative_path))

        if not copied_files:
            raise AirflowFailException(f"No files found to copy in source: {source_path}")
        
        logger.info("Copied %s file(s)", len(copied_files))

        return copied_files
        

    @task
    def verify_copy(destination: str, copied_files: list[str]) -> None:
        """
        Verify all copied files exist at the destination.
        """
        logger.info("VERIFY_COPY - VERSION 1")

        destination_path = Path(destination)

        for copied_file in copied_files:
            destination_file = destination_path / copied_file

            if not destination_file.exists():
                raise AirflowFailException(
                    f"Copied file missing from destination: {destination_file}"
                )

            if not destination_file.is_file():
                raise AirflowFailException(
                    f"Copied path exists but is not a file: {destination_file}"
                )

            logger.info("Verified copied file exists: %s", destination_file)

        logger.info("Verified %s copied file(s)", len(copied_files))
            


    validated_source = validate_source()
    prepared_destination = prepare_destination()
    copied_files = copy_source_files(validated_source, prepared_destination)
    verified_copy = verify_copy(prepared_destination, copied_files)

    validated_source >> prepared_destination >> copied_files >> verified_copy

copy_files() # pyright: ignore[reportUnusedExpression]
