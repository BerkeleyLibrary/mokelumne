"""Utilities for validating and preparing files for PDF creation."""

import shutil
from pathlib import Path


IMAGE_EXTENSIONS = {".tif", ".tiff", ".jpg", ".jpeg"}

DocumentWorkItem = dict[str, str]


def validate_source_path(source_path: Path) -> None:
    """Validate the source directory containing document subdirectories."""

    if not source_path.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_path}")

    if not source_path.is_dir():
        raise ValueError(f"Source path is not a directory: {source_path}")


def validate_destination_path(destination_path: Path) -> None:
    """Validate that the destination path exists and is a directory."""

    if not destination_path.exists():
        raise FileNotFoundError(f"Destination directory does not exist: {destination_path}")

    if not destination_path.is_dir():
        raise ValueError(f"Destination path is not a directory: {destination_path}")


def validate_source_structure(source_path: Path) -> None:
    """Validate the source directory structure for PDF creation."""

    source_entries = list(source_path.iterdir())

    root_images = [
        path
        for path in source_entries
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    ]

    # Thou shalt not have images in the root path!
    if root_images:
        raise ValueError(f"Source directory contains TIFF/JPEG files: {source_path}")

    document_dirs = [
        path
        for path in source_entries
        if path.is_dir()
    ]

    # Thou shalt not have nested subdirectories!!
    for document_dir in document_dirs:
        nested_image_dirs = _directories_with_images(document_dir)

        if nested_image_dirs:
            raise ValueError(f"Document directory contains nested TIFF/JPEG directories: {document_dir}")

    valid_document_dirs = _directories_with_images(source_path)

    # Thou shalt contain at least one subdirectory with TIFFs or JPEGs!!!
    if not valid_document_dirs:
        raise ValueError(f"No TIFF/JPEG document directories found: {source_path}")


def discover_documents(source_path: Path) -> list[DocumentWorkItem]:
    """Build work items for document subdirectories in the source directory."""

    # TODO: Consider natural sorting if document directory names require it.
    document_dirs = sorted(_directories_with_images(source_path))

    work_items = [
        {
            "source": str(document_dir),
            "output": f"{document_dir.name}.pdf",
        }
        for document_dir in document_dirs
    ]

    return work_items


def _directories_with_images(source_path: Path) -> list[Path]:
    """Return immediate subdirectories containing TIFF/JPEG images."""

    return [
        path
        for path in source_path.iterdir()
        if (
            path.is_dir()
            and any(
                child.is_file()
                and child.suffix.lower() in IMAGE_EXTENSIONS
                for child in path.iterdir()
            )
        )
    ]


def output_exists(destination_path: Path, output_filename: str) -> bool:
    """Return whether the expected output PDF already exists."""

    output_path = destination_path / output_filename

    if output_path.is_file():
        return True

    if not output_path.exists():
        return False

    raise FileExistsError(
        f"Output path exists and is not a regular file: {output_path}"
    )


def prepare_workspace(run_path: Path, document_name: str) -> Path:
    """Create and return a clean workspace for a document."""

    workspace_path = run_path / document_name

    if workspace_path.exists():
        shutil.rmtree(workspace_path)

    workspace_path.mkdir(parents=True)

    return workspace_path
