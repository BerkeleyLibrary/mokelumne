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
    clean_destination_path,
    copy_files_from_manifest,
    load_json,
    save_json,
    verify_file_manifest,
)

logger = logging.getLogger(__name__)

SOURCE_VOLUMES = [
    "/srv/da",
    "/srv/pa",
    "/srv/lpsdata4",
]

DESTINATION_VOLUMES = [
    "/srv/pa",
    "/srv/da",
]


def build_volume_path(
    volume: str,
    subdirectory: str,
    label: str,
) -> Path:
    """Build a path from a volume and relative subdirectory."""

    if subdirectory == "":
        raise AirflowFailException(
            f"{label} subdirectory is required"
        )

    subdirectory_path = Path(subdirectory)

    if subdirectory_path.is_absolute():
        raise AirflowFailException(
            f"{label} subdirectory must be relative: {subdirectory}"
        )

    return Path(volume) / subdirectory_path


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
            description="Relative path within the destination volume. Do not include leading '/'.",
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
    def validate_source() -> str:
        """Checks that the source is a valid directory."""

        ctx = get_current_context()
        source_volume = ctx["params"]["source_volume"]
        source_subdirectory = ctx["params"]["source_subdirectory"]

        source_path = build_volume_path(
            source_volume,
            source_subdirectory,
            "Source",
        )

        if not source_path.exists():
            raise AirflowFailException(f"Source path does not exist: {source_path}")

        if not source_path.is_dir():
            raise AirflowFailException(f"Source is not a directory: {source_path}")

        return str(source_path)

    @task
    def prepare_destination() -> str:
        """Prepare the destination directory."""

        ctx = get_current_context()
        destination_volume = ctx["params"]["destination_volume"]
        destination_subdirectory = ctx["params"]["destination_subdirectory"]

        destination_path = build_volume_path(
            destination_volume,
            destination_subdirectory,
            "Destination",
        )
        destination_path = clean_destination_path(destination_path)

        if not destination_path.exists():
            logger.info("Creating destination directory: %s", destination_path)
            destination_path.mkdir(parents=True)

        if not destination_path.is_dir():
            raise AirflowFailException(f"Destination is not a directory: {destination_path}")

        if next(os.scandir(destination_path), None) is not None:
            raise AirflowFailException(f"Destination directory contains files: {destination_path}")

        return str(destination_path)

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

    """The user needs to confirm the file copy paths before proceeding"""
    confirm_copy = ApprovalOperator(
        task_id="confirm_copy",
        subject="Review the copy operation and approve it to continue.",
        body=(
            "Approve file copy from "
            "**{{ params.source_volume }}/{{ params.source_subdirectory }}** "
            "to "
            "**{{ params.destination_volume }}/{{ params.destination_subdirectory }}**"
        ),
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
        validated_source
        >> prepared_destination
        >> manifest
        >> confirm_copy
        >> copied_manifest_files
        >> verified_manifest
    )

copy_files()  # pyright: ignore[reportUnusedExpression]
