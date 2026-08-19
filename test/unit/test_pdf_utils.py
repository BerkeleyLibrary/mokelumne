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