"""Utilities for validating and preparing files for PDF creation."""

import re
import shutil
from pathlib import Path

import pyvips  # type: ignore[import-untyped]

from mokelumne.providers.alma.hooks.alma import AlmaHook
from mokelumne.util import marc

DEFAULT_OCR_LANGUAGES = "eng+spa+fra+ita+deu"
IMAGE_EXTENSIONS = {".tif", ".tiff", ".jpg", ".jpeg"}
MAX_DOCUMENT_IMAGES = 99_999_999
MM_PER_INCH = 25.4

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
            raise ValueError(
                f"Document directory contains nested TIFF/JPEG directories: {document_dir}"
            )

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


def _source_images(source_path: Path) -> list[Path]:
    """Return TIFF/JPEG source images in page order."""

    return sorted(
        path
        for path in source_path.iterdir()
        if (
            path.is_file()
            and not path.name.startswith(".")
            and path.suffix.lower() in IMAGE_EXTENSIONS
        )
    )


def _prepare_image(source_path: Path, output_path: Path, max_resolution: int) -> None:
    """Normalize a source image to TIFF for OCR."""

    image = pyvips.Image.new_from_file(
        str(source_path),
        access="sequential",
    )

    resolution = image.xres * MM_PER_INCH
    target_xres = image.xres
    target_yres = image.yres

    if resolution > max_resolution:
        scale = max_resolution / resolution
        image = image.resize(scale)
        target_xres = max_resolution / MM_PER_INCH
        target_yres = max_resolution / MM_PER_INCH

    image.tiffsave(
        str(output_path),
        compression="lzw",
        xres=target_xres,
        yres=target_yres,
    )


def prepare_images(source_path: Path, workspace_path: Path, max_resolution: int) -> Path:
    """Prepare source images for OCR and return the Tesseract file list path."""

    source_images = _source_images(source_path)

    if len(source_images) > MAX_DOCUMENT_IMAGES:
        raise ValueError(
            f"Document contains {len(source_images):,} images "
            f"(maximum allowed: {MAX_DOCUMENT_IMAGES:,})"
        )

    prepared_images = []

    for sequence, source_image in enumerate(source_images, start=1):
        output_path = workspace_path / f"{sequence:08}.tif"

        _prepare_image(source_image, output_path, max_resolution)

        prepared_images.append(output_path)

    file_list_path = workspace_path / "filelist.txt"

    file_list_path.write_text(
        "".join(f"{path}\n" for path in prepared_images),
        encoding="utf-8",
    )

    return file_list_path


def extract_mms_id(document_name: str) -> str | None:
    """Return an Alma MMS ID from a document name when present."""

    mms_id_match = r'9\d{17}'
    match = re.match(mms_id_match, document_name)

    if match:
        return match.group(0)

    return None


def determine_language(
    requested_language: str,
    document_name: str,
) -> str:
    """Determine the Tesseract language codes for a document."""
    if requested_language:
        return requested_language

    mms_id = extract_mms_id(document_name)

    if not mms_id:
        return DEFAULT_OCR_LANGUAGES

    record_xml = AlmaHook().get_record_by_mms_id(mms_id)
    language = marc.derive_tesseract_codes_from_marc(record_xml)

    if not language:
        return DEFAULT_OCR_LANGUAGES

    return language
