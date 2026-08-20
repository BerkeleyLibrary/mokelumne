"""PyTest cases for the mokelumne.util.pdf_utils module."""

from pathlib import Path

import pytest

from mokelumne.util import pdf_utils


class TestPDFUtils:
    """Test for PDF utils module"""

    def test_validate_source_path_accepts_directory(self, tmp_path: Path):
        """Accept an existing source directory."""
        pdf_utils.validate_source_path(tmp_path)


    def test_validate_source_path_rejects_missing_directory(self, tmp_path: Path):
        """Reject a source directory that does not exist."""
        source_path = tmp_path / "missing"

        with pytest.raises(
            FileNotFoundError,
            match="Source directory does not exist",
        ):
            pdf_utils.validate_source_path(source_path)


    def test_validate_source_path_rejects_file(self, tmp_path: Path):
        """Reject a source path that is a file."""
        source_path = tmp_path / "source.txt"
        source_path.write_text("not a directory", encoding="utf-8")

        with pytest.raises(
            ValueError,
            match="Source path is not a directory",
        ):
            pdf_utils.validate_source_path(source_path)

    def test_validate_source_structure_root(self, tmp_path: Path):
        """Reject a source path that contains images in the root."""
        root_image = tmp_path / "page001.tif"
        root_image.write_text("test image", encoding="utf-8")

        with pytest.raises(
            ValueError,
            match="Source directory contains TIFF/JPEG files",
        ):
            pdf_utils.validate_source_structure(tmp_path)


    def test_validate_source_structure_nested(self, tmp_path: Path):
        """Reject a source path that has nested subdirectories."""
        document_dir = tmp_path / "document_1"
        document_dir.mkdir()

        nested_dir = document_dir / "nested"
        nested_dir.mkdir()

        nested_image = nested_dir / "page001.jpg"
        nested_image.write_text("test image", encoding="utf-8")

        with pytest.raises(
            ValueError,
            match="Document directory contains nested TIFF/JPEG directories",
        ):
            pdf_utils.validate_source_structure(tmp_path)


    def test_validate_source_structure_no_images(self, tmp_path: Path):
        """Reject a source path that contains no images in any subdirectories."""
        document_dir = tmp_path / "document_1"
        document_dir.mkdir()

        non_image = document_dir / "notes.txt"
        non_image.write_text("not an image", encoding="utf-8")

        with pytest.raises(
            ValueError,
            match="No TIFF/JPEG document directories found",
        ):
            pdf_utils.validate_source_structure(tmp_path)


    def test_validate_source_structure_accepts_valid_structure(self, tmp_path: Path):
        """Accept source subdirectories that contain TIFF/JPEG images."""
        document_dir = tmp_path / "document_1"
        document_dir.mkdir()

        image = document_dir / "page001.tif"
        image.write_text("test image", encoding="utf-8")

        pdf_utils.validate_source_structure(tmp_path)


    def test_validate_destination_path_accepts_directory(self, tmp_path: Path):
        """Accept an existing destination directory."""
        pdf_utils.validate_destination_path(tmp_path)


    def test_validate_destination_path_rejects_missing_directory(self, tmp_path: Path):
        """Reject a destination directory that does not exist."""
        destination_path = tmp_path / "missing"

        with pytest.raises(
            FileNotFoundError,
            match="Destination directory does not exist",
        ):
            pdf_utils.validate_destination_path(destination_path)


    def test_validate_destination_path_rejects_file(self, tmp_path: Path):
        """Reject a destination path that is a file."""
        destination_path = tmp_path / "destination.txt"
        destination_path.write_text("not a directory", encoding="utf-8")

        with pytest.raises(
            ValueError,
            match="Destination path is not a directory",
        ):
            pdf_utils.validate_destination_path(destination_path)
