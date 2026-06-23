"""
File copy DAG to transfer files from one location to another
Change this to use a manifest rather than just copying from one dir to another....

1. validate_source
2. prepare_destination
3. build_manifest     <--- need to create the manifest from the source
4. copy_from_manifest <--- need to refactor
5. verify_manifest
"""

from __future__ import annotations

import hashlib
import logging
import shutil

from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

def sha256_for_file(path: Path) -> str:
    """
    Calculate the checksum
    """
    sha256 = hashlib.sha256()

    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            sha256.update(chunk)
    
    return sha256.hexdigest()

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
    def build_manifest(source: str) -> list[dict[str, int | str]]:
        """
        Build a manifest of all files under the source directory.
        """
        logger.info("BUILD_MANIFEST - VERSION 5")

        source_path = Path(source)
        manifest = []

        for item in source_path.rglob("*"):
            if item.is_file():
                relative_path = item.relative_to(source_path)

                manifest.append(
                    {
                        "path": str(relative_path),
                        "size": item.stat().st_size,
                        "sha256": sha256_for_file(item),
                    }
                )
        
        if not manifest:
            raise AirflowFailException(f"No files found in source: {source_path}")
        
        logger.info("Manifest contains %s file(s)", len(manifest))

        return manifest


    @task
    def copy_manifest_files(source: str, destination: str, manifest: list[dict[str, int | str]]) -> list[str]:
        """
        Copy all files in manifest to destination directory
        """
        logger.info("COPY_SOURCE_FILES - VERSION 5")

        source_path = Path(source)
        destination_path = Path(destination)

        copied_files = []

        for entry in manifest:
            relative_path = Path(str(entry["path"]))
            source_file = source_path / relative_path
            destination_file = destination_path / relative_path

            if not source_file.exists():
                raise AirflowFailException(f"Manifest file missing from source: {source_file}")

            if not source_file.is_file():
                raise AirflowFailException(f"Manifest path is not a file: {source_file}")
            

            destination_file.parent.mkdir(parents=True, exist_ok=True)
                
            logger.info("Copying %s to %s", source_file,  destination_file)
            shutil.copy2(source_file, destination_file)

            copied_files.append(str(relative_path))


        if not copied_files:
            raise AirflowFailException(f"No files found to copy in source: {source_path}")
        
        logger.info("Copied %s file(s)", len(copied_files))

        return copied_files
        

    @task
    def verify_copy(
            destination: str,
            manifest: list[dict[str, int | str]]
            ) -> None:
        """
        Verify all copied files exist at the destination.
        """
        logger.info("VERIFY_COPY - VERSION 4")

        destination_path = Path(destination)

        for entry in manifest:
            relative_path = Path(str(entry["path"]))
            expected_size = entry["size"]
            expected_sha256 = entry['sha256']

            destination_file = destination_path / relative_path

            if not destination_file.exists():
                raise AirflowFailException(f"Copied file missing from destination: {destination_file}")

            if not destination_file.is_file():
                raise AirflowFailException(f"Copied path exists but is not a file: {destination_file}")
            
            actual_size = destination_file.stat().st_size

            if actual_size != expected_size:
                raise AirflowFailException(f"Copied file size does not match original")
            
            actual_sha256 = sha256_for_file(destination_file)

            if actual_sha256 != expected_sha256:
                raise AirflowFailException(f"Copied file checksum does not match original: {destination_file}")

            logger.info("Verified copied file exists, size matches, and checksum matches: %s", destination_file)

        logger.info("Verified %s copied file(s)", len(manifest))


    validated_source = validate_source()
    prepared_destination = prepare_destination()
    manifest = build_manifest(validated_source)
    copied_files = copy_manifest_files(validated_source, prepared_destination, manifest)
    verified_copy = verify_copy(prepared_destination, manifest)

    validated_source >> prepared_destination >> manifest >> copied_files >> verified_copy

copy_files() # pyright: ignore[reportUnusedExpression]
