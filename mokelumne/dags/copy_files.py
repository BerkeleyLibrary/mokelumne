"""File copy DAG to transfer files from one location to another."""

# pyright: reportTypedDictNotRequiredAccess=false

from __future__ import annotations

import logging
import os

from pathlib import Path

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from mokelumne.util.storage import run_dir
from mokelumne.util.file_transfer import (
    build_file_manifest,
    copy_files_from_manifest,
    load_json,
    save_json,
    verify_file_manifest,
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
        "exclude_regex": Param(
            default="",
            title="Filename exclusion pattern",
            description_md="""Regular expression to exclude files from the copy process. Defaults to excluding Thumbs.db and file or path name that starts with a period: `^(\.(.*)|(?i:Thumbs\.db))$`""", # pyright: ignore[reportInvalidStringEscapeSequence]
            type=["string", "null"],
            format="regex",
            section="Exclude Files"
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

        source_path = Path(source)
        if not source_path.exists():
            raise AirflowFailException(f"Source directory does not exist: {source_path}")

        if not source_path.is_dir():
            raise AirflowFailException(f"Source is not a directory: {source_path}")

        return source

    @task
    def prepare_destination() -> str:
        """
        Prepare the destination directory.
        TODO: one of the requirements that we got from Lynne is that the destination 
        should be an incoming subdirectory of any directory on PA or DA, 
        e.g. /srv/pa/aerial/ucb/incoming. i don't have a good answer for how to 
        approach this, but i'm curious about how you think we should confirm that 1) 
        we're actually writing to the appropriate incoming subdirectory, and if the 
        incoming directory exists and has files in it, we should fail the job.
        """

        ctx = get_current_context()
        destination = ctx["params"]["destination"]

        destination_path = Path(destination)

        if not destination_path.exists():
            logger.info("Creating destination directory: %s", destination_path)
            destination_path.mkdir(parents=True)

        if not destination_path.is_dir():
            raise AirflowFailException(f"Destination is not a directory: {destination_path}")

        if next(os.scandir(destination_path), None) is not None:
            raise AirflowFailException(f"Destination directory contains files: {destination_path}")

        return destination

    @task
    def build_manifest(source: str) -> str:
        """Build a manifest of all files under the source directory."""

        source_path = Path(source)
        ctx = get_current_context()
        exclude_regex = ctx["params"].get("exclude_regex")
        manifest = build_file_manifest(source_path, exclude_regex=exclude_regex)

        if not manifest:
            raise AirflowFailException(f"No files found in source: {source_path}")

        logger.info("Manifest contains %s file(s)", len(manifest))

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
        logger.info("Copied %s file(s)", len(manifest))

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

    """The user needs to confirm the file copy paths before proceeding"""
    confirm_copy = ApprovalOperator(
        task_id="confirm_copy",
        subject="Review the copy operation and approve it to continue.",
        body="Approve file copy from **{{ params.source }}** to **{{ params.destination }}**",
    )
    validated_source = validate_source()
    prepared_destination = prepare_destination()
    manifest = build_manifest(validated_source)
    copied_manifest_files = copy_manifest_files(
        validated_source,
        prepared_destination,
        manifest,
    )
    verified_manifest = verify_manifest(prepared_destination, manifest)

    (
        confirm_copy
        >> validated_source
        >> prepared_destination
        >> manifest
        >> copied_manifest_files
        >> verified_manifest
    )

copy_files()  # pyright: ignore[reportUnusedExpression]
