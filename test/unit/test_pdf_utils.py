"""PyTest cases for the mokelumne.util.pdf_utils module."""

from pathlib import Path

import pytest
import pyvips

from mokelumne.util import pdf_utils

MM_PER_INCH = 25.4


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

    def test_discover_documents_builds_work_items(self, tmp_path: Path):
        """Build work items for document directories containing images."""
        pdf_1 = tmp_path / "pdf_1"
        pdf_1.mkdir()
        (pdf_1 / "001.tif").write_text("image", encoding="utf-8")

        pdf_2 = tmp_path / "pdf_2"
        pdf_2.mkdir()
        (pdf_2 / "001.jpg").write_text("image", encoding="utf-8")

        work_items = pdf_utils.discover_documents(tmp_path)

        assert work_items == [
            {
                "source": str(pdf_1),
                "output": "pdf_1.pdf",
            },
            {
                "source": str(pdf_2),
                "output": "pdf_2.pdf",
            },
        ]

    def test_discover_documents_ignores_directories_without_images(self, tmp_path: Path):
        """Ignore subdirectories that do not contain TIFF/JPEG images."""
        document_dir = tmp_path / "document"
        document_dir.mkdir()
        (document_dir / "001.tif").write_text("image", encoding="utf-8")

        notes_dir = tmp_path / "notes"
        notes_dir.mkdir()
        (notes_dir / "README.txt").write_text("notes", encoding="utf-8")

        work_items = pdf_utils.discover_documents(tmp_path)

        assert work_items == [
            {
                "source": str(document_dir),
                "output": "document.pdf",
            }
        ]

    def test_discover_documents_accepts_uppercase_image_extensions(self, tmp_path: Path):
        """Accept TIFF/JPEG extensions regardless of case."""
        document_dir = tmp_path / "document"
        document_dir.mkdir()
        (document_dir / "001.TIF").write_text("image", encoding="utf-8")

        work_items = pdf_utils.discover_documents(tmp_path)

        assert work_items == [
            {
                "source": str(document_dir),
                "output": "document.pdf",
            }
        ]

    def test_output_exists_returns_true_when_pdf_exists(self, tmp_path: Path):
        """Return true when the expected output PDF exists."""
        output_file = tmp_path / "document.pdf"
        output_file.write_text("pdf", encoding="utf-8")

        assert pdf_utils.output_exists(tmp_path, "document.pdf")

    def test_output_exists_returns_false_when_pdf_does_not_exist(self, tmp_path: Path):
        """Return false when the expected output PDF does not exist."""
        assert not pdf_utils.output_exists(tmp_path, "document.pdf")

    def test_output_exists_raises_when_output_is_directory(self, tmp_path: Path):
        """Raise when the expected output path exists but is not a regular file."""
        output_dir = tmp_path / "document.pdf"
        output_dir.mkdir()

        with pytest.raises(
            FileExistsError,
            match="Output path exists and is not a regular file",
        ):
            pdf_utils.output_exists(tmp_path, "document.pdf")

    def test_prepare_workspace_creates_directory(self, tmp_path: Path):
        """Create the document workspace when it does not exist."""
        workspace_path = pdf_utils.prepare_workspace(tmp_path, "document")

        assert workspace_path.is_dir()

    def test_prepare_workspace_returns_expected_path(self, tmp_path: Path):
        """Return the expected document workspace path."""
        workspace_path = pdf_utils.prepare_workspace(tmp_path, "document")

        assert workspace_path == tmp_path / "document"

    def test_prepare_workspace_replaces_existing_directory(self, tmp_path: Path):
        """Replace an existing workspace with a clean directory."""
        existing_workspace = tmp_path / "document"
        existing_workspace.mkdir()

        stale_file = existing_workspace / "stale.txt"
        stale_file.write_text("old data", encoding="utf-8")

        workspace_path = pdf_utils.prepare_workspace(tmp_path, "document")

        assert workspace_path.is_dir()
        assert not stale_file.exists()

    def test_source_images_returns_supported_images_in_lexical_order(self, tmp_path: Path):
        """Return TIFF/JPEG images sorted lexically by filename."""

        filenames = [
            "002.tif",
            ".hidden.tif",
            "001.jpg",
            "010.jpeg",
            "003.TIFF",
            "notes.txt",
        ]

        for filename in filenames:
            (tmp_path / filename).touch()

        images = pdf_utils._source_images(tmp_path)

        assert [image.name for image in images] == [
            "001.jpg",
            "002.tif",
            "003.TIFF",
            "010.jpeg",
        ]

    def test_prepare_image_does_not_resize_low_resolution_image(self, tmp_path: Path):
        """Do not resize images at or below the maximum resolution."""

        source_path = tmp_path / "source.tif"
        output_path = tmp_path / "output.tif"

        image = pyvips.Image.black(2000, 3000)
        image.tiffsave(
            str(source_path),
            compression="lzw",
            xres=150 / 25.4,
            yres=150 / 25.4,
        )

        pdf_utils._prepare_image(source_path, output_path, 200)

        output = pyvips.Image.new_from_file(str(output_path))

        assert output.width == 2000
        assert output.height == 3000
        assert output.xres * MM_PER_INCH == pytest.approx(150)

    def test_prepare_image_converts_jpeg_to_tiff(self, tmp_path: Path):
        """Convert JPEG source images to TIFF."""

        source_path = tmp_path / "source.jpg"
        output_path = tmp_path / "output.tif"

        image = pyvips.Image.black(1000, 1500)
        image.jpegsave(str(source_path))

        pdf_utils._prepare_image(source_path, output_path, 200)

        output = pyvips.Image.new_from_file(str(output_path))

        assert output.width == 1000
        assert output.height == 1500
        assert output.get("vips-loader") == "tiffload"

    def test_prepare_images_creates_ordered_tiffs_and_file_list(self, tmp_path: Path):
        """Prepare images in page order and create the Tesseract file list."""

        source_path = tmp_path / "source"
        workspace_path = tmp_path / "workspace"

        source_path.mkdir()
        workspace_path.mkdir()

        pyvips.Image.black(100, 100).jpegsave(
            str(source_path / "002.jpg")
        )
        pyvips.Image.black(100, 100).jpegsave(
            str(source_path / "001.jpg")
        )

        file_list_path = pdf_utils.prepare_images(
            source_path,
            workspace_path,
            200,
        )

        assert file_list_path == workspace_path / "filelist.txt"

        assert (workspace_path / "00000001.tif").is_file()
        assert (workspace_path / "00000002.tif").is_file()

        assert file_list_path.read_text(encoding="utf-8").splitlines() == [
            str(workspace_path / "00000001.tif"),
            str(workspace_path / "00000002.tif"),
        ]

    def test_prepare_image_resizes_high_resolution_image(self, tmp_path: Path):
        """Downsample images whose resolution exceeds the maximum."""

        source_path = tmp_path / "source.tif"
        output_path = tmp_path / "output.tif"

        image = pyvips.Image.black(4000, 6000)
        image.tiffsave(
            str(source_path),
            compression="lzw",
            xres=400 / 25.4,
            yres=400 / 25.4,
        )

        pdf_utils._prepare_image(source_path, output_path, 200)

        output = pyvips.Image.new_from_file(str(output_path))

        assert output.width == 2000
        assert output.height == 3000
        assert output.xres * MM_PER_INCH == pytest.approx(200)

    def test_prepare_images_rejects_too_many_images(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Reject documents with more images than the allowed maximum."""

        source_path = tmp_path / "source"
        workspace_path = tmp_path / "workspace"

        source_path.mkdir()
        workspace_path.mkdir()

        pyvips.Image.black(100, 100).jpegsave(
            str(source_path / "001.jpg")
        )
        pyvips.Image.black(100, 100).jpegsave(
            str(source_path / "002.jpg")
        )

        monkeypatch.setattr(pdf_utils, "MAX_DOCUMENT_IMAGES", 1)

        with pytest.raises(
            ValueError,
            match=r"Document contains 2 images \(maximum allowed: 1\)",
        ):
            pdf_utils.prepare_images(source_path, workspace_path, 200)

    def test_prepare_image_uses_configured_max_resolution(self, tmp_path: Path):
        """Downsample images to the configured maximum resolution."""

        source_path = tmp_path / "source.tif"
        output_path = tmp_path / "output.tif"

        image = pyvips.Image.black(4000, 6000)
        image.tiffsave(
            str(source_path),
            compression="lzw",
            xres=400 / MM_PER_INCH,
            yres=400 / MM_PER_INCH,
        )

        pdf_utils._prepare_image(source_path, output_path, 300)

        output = pyvips.Image.new_from_file(str(output_path))

        assert output.width == 3000
        assert output.height == 4500
        assert output.xres * MM_PER_INCH == pytest.approx(300)
