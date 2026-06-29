"""PyTest cases for the mokelumne.util.file_transfer module."""

from pathlib import Path

import pytest

from mokelumne.util import file_transfer


class TestFileTransfer:
    """Tests for the Mokelumne file transfer module."""

    def test_sha256_for_file(self, tmp_path: Path):
        """Ensure that sha256_for_file returns the expected checksum."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello", encoding="utf-8")

        result = file_transfer.sha256_for_file(test_file)

        assert result == (
            "2cf24dba5fb0a30e26e83b2ac5b9e"
            "29e1b161e5c1fa7425e73043362938b9824"
        )

    def test_save_and_load_json(self, tmp_path: Path):
        """Ensure that JSON data can be saved and loaded."""
        data = [
            {
                "path": "test.txt",
                "size": 5,
                "sha256": "fake-checksum",
            }
        ]

        json_file_path = tmp_path / "data.json"

        file_transfer.save_json(data, json_file_path)
        result = file_transfer.load_json(json_file_path)

        assert result == data

    def test_build_manifest_empty_directory(self, tmp_path: Path):
        """Ensure that build_manifest returns an empty list for an empty directory."""
        result = file_transfer.build_file_manifest(tmp_path)

        assert result == []

    def test_build_manifest_includes_nested_files(self, tmp_path: Path):
        """Ensure that build_manifest includes files recursively."""
        file_one = tmp_path / "file_one.txt"
        file_one.write_text("hello", encoding="utf-8")

        subdir = tmp_path / "subdir"
        subdir.mkdir()

        file_two = subdir / "file_two.txt"
        file_two.write_text("goodbye", encoding="utf-8")

        result = file_transfer.build_file_manifest(tmp_path)

        paths = {entry["path"] for entry in result}

        assert paths == {
            "file_one.txt",
            "subdir/file_two.txt",
        }
        assert len(result) == 2

    def test_verify_manifest_success(self, tmp_path: Path):
        """Ensure verify_manifest succeeds for valid files."""

        test_file = tmp_path / "test.txt"
        test_file.write_text("hello", encoding="utf-8")

        manifest = file_transfer.build_file_manifest(tmp_path)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        result = file_transfer.verify_file_manifest(tmp_path, manifest_path)

        assert result == [
            {
                "path": "test.txt",
                "status": "verified",
                "size": 5,
                "sha256": file_transfer.sha256_for_file(test_file),
            }
        ]

    def test_verify_manifest_missing_file(self, tmp_path: Path):
        """Ensure verify_manifest fails when a file is missing."""

        test_file = tmp_path / "test.txt"
        test_file.write_text("hello", encoding="utf-8")

        manifest = file_transfer.build_file_manifest(tmp_path)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        test_file.unlink()

        with pytest.raises(FileNotFoundError):
            file_transfer.verify_file_manifest(tmp_path, manifest_path)

    def test_copy_manifest_files(self, tmp_path: Path):
        """Ensure copy_manifest_files copies files from source to destination."""
        source = tmp_path / "source"
        source.mkdir()

        destination = tmp_path / "destination"
        destination.mkdir()

        source_file = source / "test.txt"
        source_file.write_text("hello", encoding="utf-8")

        manifest = file_transfer.build_file_manifest(source)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        file_transfer.copy_files_from_manifest(source, destination, manifest_path)

        destination_file = destination / "test.txt"

        assert destination_file.exists()
        assert destination_file.read_text(encoding="utf-8") == "hello"

    def test_copy_manifest_files_missing_source_file(self, tmp_path: Path):
        """Ensure copy_manifest_files fails if a manifest file is missing from source."""
        source = tmp_path / "source"
        source.mkdir()

        destination = tmp_path / "destination"
        destination.mkdir()

        source_file = source / "test.txt"
        source_file.write_text("hello", encoding="utf-8")

        manifest = file_transfer.build_file_manifest(source)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        source_file.unlink()

        with pytest.raises(FileNotFoundError):
            file_transfer.copy_files_from_manifest(source, destination, manifest_path)
