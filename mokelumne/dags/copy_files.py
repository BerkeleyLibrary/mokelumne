"""File copy DAG to transfer files from one location to another."""

from __future__ import annotations

import logging

from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from mokelumne.util.storage import run_dir
from mokelumne.util.file_transfer import (
    build_manifest as build_file_manifest,
    copy_manifest_files as copy_files_from_manifest,
    read_manifest,
    verify_manifest as verify_file_manifest,
    write_manifest,
)

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
    """Copy files from a source directory to an empty destination directory."""

    @task
    def validate_source() -> str:
        """Checks that the source is a valid directory."""

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
        """Prepare the destination directory."""

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
    def build_manifest(source: str) -> str:
        """Build a manifest of all files under the source directory."""

        source_path = Path(source)
        manifest = build_file_manifest(source_path)

        if not manifest:
            raise AirflowFailException(f"No files found in source: {source_path}")

        logger.info("Manifest contains %s file(s)", len(manifest))

        ctx = get_current_context()
        manifest_path = run_dir(ctx["run_id"]) / "manifest.json"

        write_manifest(manifest, manifest_path)

        logger.info("Manifest written to: %s", manifest_path)

        return str(manifest_path)

    @task
    def copy_manifest_files(source: str, destination: str, manifest_path: str) -> None:
        """Copy all files in manifest to destination directory."""

        try:
            copy_files_from_manifest(
                Path(source),
                Path(destination),
                Path(manifest_path),
            )
        except Exception as ex:
            raise AirflowFailException(
                f"Manifest copy failed: {ex}"
            ) from ex

        manifest = read_manifest(Path(manifest_path))
        logger.info("Copied %s file(s)", len(manifest))

    @task
    def verify_manifest(destination: str, manifest_path: str) -> None:
        """Verify all copied files exist at the destination."""

        try:
            verify_file_manifest(Path(destination), Path(manifest_path))
        except Exception as ex:
            raise AirflowFailException(
                f"Manifest verification failed: {ex}"
            ) from ex

        manifest = read_manifest(Path(manifest_path))
        logger.info("Verified %s copied file(s)", len(manifest))

    validated_source = validate_source()
    prepared_destination = prepare_destination()
    manifest = build_manifest(validated_source)
    copied_manifest_files = copy_manifest_files(
        validated_source,
        prepared_destination,
        manifest,
    )
    verified_manifest = verify_manifest(prepared_destination, manifest)

    validated_source >> prepared_destination >> manifest >> copied_manifest_files >> verified_manifest

copy_files()  # pyright: ignore[reportUnusedExpression]
