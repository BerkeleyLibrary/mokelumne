"""File copy DAG to transfer files from one location to another."""

# pyright: reportTypedDictNotRequiredAccess=false

from __future__ import annotations

import logging
import os

from pathlib import Path

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Param, dag, chain, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from mokelumne.util.storage import run_dir
from mokelumne.util.file_transfer import (
    DESTINATION_VOLUMES,
    SOURCE_VOLUMES,
    build_file_manifest,
    build_volume_path,
    clean_destination_path,
    copy_files_from_manifest,
    load_json,
    move_to_uploaded,
    rename_temp_dir,
    save_json,
    verify_file_manifest,
)

logger = logging.getLogger(__name__)

@dag(
    description="Transfers files from one location to another",
    schedule=None,
    catchup=False,
    params={
        "source_volume": Param(
            default=SOURCE_VOLUMES[0],
            type="string",
            title="Source volume",
            description="Root source volume.",
            enum=SOURCE_VOLUMES,
        ),
        "source_subdirectory": Param(
            default="",
            type="string",
            title="Source subdirectory",
            description="Relative path within the source volume. Do not include leading '/'.",
        ),
        "destination_volume": Param(
            default=DESTINATION_VOLUMES[0],
            type="string",
            title="Destination volume",
            description="Root destination volume.",
            enum=DESTINATION_VOLUMES,
        ),
        "destination_subdirectory": Param(
            default="",
            type="string",
            title="Destination subdirectory",
            description="Relative path within the destination volume. Do not include leading '/' or the final '/incoming' to the path.",
        ),
        "exclude_regex": Param(
            default="",
            title="Filename exclusion pattern",
            description_md="""Regular expression to exclude files from the copy process. Defaults to excluding Thumbs.db and file or path name that starts with a period: `^(\.(.*)|(?i:Thumbs\.db))$`""", # pyright: ignore[reportInvalidStringEscapeSequence]
            type=["string", "null"],
            format="regex",
            section="Exclude Files",
        ),
    },
    tags=["file-transfer"],
)
def copy_files():
    """Copy files from a source directory to an empty destination directory."""

    @task
    def build_copy_paths() -> dict[str, str]:
        """Build the resolved source and destination paths."""

        ctx = get_current_context()

        try:
            source_path = build_volume_path(
                ctx["params"]["source_volume"],
                ctx["params"]["source_subdirectory"],
                "Source",
            )

            destination_path = build_volume_path(
                ctx["params"]["destination_volume"],
                ctx["params"]["destination_subdirectory"],
                "Destination",
            )

            destination_path = clean_destination_path(destination_path)
        except ValueError as ex:
            raise AirflowFailException(str(ex)) from ex

        return {
            "source": str(source_path),
            "destination": str(destination_path)
        }


    @task
    def validate_source(copy_paths: dict[str, str]) -> str:
        """Checks that the source is a valid directory."""

        source_path = Path(copy_paths["source"])

        if not source_path.exists():
            raise AirflowFailException(f"Source path does not exist: {source_path}")

        if not source_path.is_dir():
            raise AirflowFailException(f"Source is not a directory: {source_path}")

        return str(source_path)

    @task
    def validate_destination(copy_paths: dict[str, str]) -> str:
        """Checks that the destination is valid."""
        destination_path = Path(copy_paths["destination"])
        parent_path = destination_path.parent

        if not parent_path.exists():
            raise AirflowFailException(
                f"Destination parent directory does not exist: {parent_path}"
            )

        if not parent_path.is_dir():
            raise AirflowFailException(
                f"Destination parent path is not a directory: {parent_path}"
            )

        if destination_path.exists():
            if not destination_path.is_dir():
                raise AirflowFailException(f"Destination is not a directory: {destination_path}")

            if next(os.scandir(destination_path), None) is not None:
                raise AirflowFailException(f"Destination directory contains files: {destination_path}")
        
        return str(destination_path)

    @task
    def build_temp_destination(destination: str) -> str:
        """Build the run-specific temporary staging path."""

        ctx = get_current_context()
        run_id = ctx["run_id"]

        destination_path = Path(destination)
        temp_dir = destination_path.parent / ".airflow" / run_id

        return str(temp_dir)

    @task
    def build_manifest(source: str) -> str:
        """Build a manifest of all files under the source directory."""

        source_path = Path(source)
        ctx = get_current_context()
        exclude_regex = ctx["params"].get("exclude_regex")
        manifest = build_file_manifest(source_path, exclude_regex=exclude_regex)

        if not manifest["files"]:
            raise AirflowFailException(f"No files found in source: {source_path}")

        logger.info("Manifest contains %s file(s)", len(manifest["files"]))

        ctx = get_current_context()
        manifest_path = run_dir(ctx["run_id"]) / "manifest.json"

        save_json(manifest, manifest_path)

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

        manifest = load_json(Path(manifest_path))
        logger.info("Copied %s file(s)", len(manifest["files"]))

    @task
    def verify_manifest(destination: str, manifest_path: str) -> None:
        """Verify all copied files exist at the destination."""

        try:
            verification_report = verify_file_manifest(
                Path(destination),
                Path(manifest_path),
            )
        except Exception as ex:
            raise AirflowFailException(
                f"Manifest verification failed: {ex}"
            ) from ex

        ctx = get_current_context()
        run_path = run_dir(ctx["run_id"])

        verification_report_path = run_path / "verification_report.json"

        save_json(verification_report, verification_report_path)

        logger.info("Verification report written to: %s", verification_report_path)
        logger.info("Verified %s copied file(s)", len(verification_report))

    @task
    def rename_temp(destination: str) -> None:
        """rename airflow temp dir to incoming."""

        temp_dir_path = Path(destination)

        try:
            incoming_path = rename_temp_dir(
                temp_dir_path,
            )
        except Exception as ex:
            raise AirflowFailException(
                f"Renaming temporary directory {temp_dir_path} failed: {ex}"
            ) from ex

        logger.info("renamed temp directory %s to destination %s", temp_dir_path, incoming_path)

    @task.short_circuit()
    def sourcedir_is_lpsdata(source_dir: str) -> bool:
        logger.info("Checking if %s volume is an lpsdata volume. If so will move files to Uploaded directory", source_dir)
        return source_dir.lower().startswith("/srv/lpsdata")

    @task
    def move_lpsdata_to_uploaded(source_dir: str) -> None:
        """Move lpsdata files from Ready_to_Upload to Uploaded"""
        try:        
            uploaded_dir, moved_count = move_to_uploaded(source_dir)
        except Exception as ex:
            raise AirflowFailException(
                f"Moving files from #{source_dir} to uploaded directory failed: {ex}"
            )

        logger.info("Moved source_dir %s files/subdirectories to %s. %s files and/or subdirectories moved",
                     source_dir, uploaded_dir, moved_count)
        
    # Need to run this before defining confirm_copy since we need the resolved paths:
    copy_paths = build_copy_paths()

    # The user needs to confirm the file copy paths before proceeding
    confirm_copy = ApprovalOperator(
        task_id="confirm_copy",
        subject="Review the copy operation and approve it to continue.",
        body=(
            "Approve file copy from "
            f"**{copy_paths['source']}** "
            "to "
            f"**{copy_paths['destination']}**"
        ),
    )

    validated_source = validate_source(copy_paths)
    validated_destination = validate_destination(copy_paths)
    temp_destination = build_temp_destination(validated_destination)
    manifest = build_manifest(validated_source)
    copied_manifest_files = copy_manifest_files(
        validated_source,
        temp_destination,
        manifest,
    )
    verified_manifest = verify_manifest(temp_destination, manifest)
    renamed_temp_dir =  rename_temp(temp_destination)
    check_lpsdata = sourcedir_is_lpsdata(validated_source)
    moved_lpsdata_to_uploaded = move_lpsdata_to_uploaded(validated_source) 


    (
        copy_paths
        >> confirm_copy
        >> validated_source
        >> validated_destination
        >> temp_destination
        >> manifest
        >> copied_manifest_files
        >> verified_manifest
        >> renamed_temp_dir
    )
    
    # This should be last in the chain    
    chain(renamed_temp_dir, check_lpsdata, moved_lpsdata_to_uploaded)

copy_files()  # pyright: ignore[reportUnusedExpression]
