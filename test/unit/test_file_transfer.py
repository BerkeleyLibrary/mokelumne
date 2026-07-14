"""PyTest cases for the mokelumne.util.file_transfer module."""

from pathlib import Path

import pytest

from mokelumne.util import file_transfer

@pytest.fixture(scope="session")
def manifest_source_path(tmpdir_factory):
    tmp_path = Path(tmpdir_factory.mktemp("manifest_source"))
    visible_file = tmp_path / "visible.tif"
    visible_file.write_text("hello", encoding="utf-8")

    hidden_file = tmp_path / ".hidden.txt"
    hidden_file.write_text("secret", encoding="utf-8")

    hidden_dir = tmp_path / ".hidden"
    hidden_dir.mkdir()
    hidden_dir_file = hidden_dir / "nested.txt"
    hidden_dir_file.write_text("nested", encoding="utf-8")

    ds_store = tmp_path / ".DS_Store"
    ds_store.write_text("folder metadata", encoding="utf-8")

    thumbs_db = tmp_path / "Thumbs.db"
    thumbs_db.write_text("thumbnail images db", encoding="utf-8")

    return tmp_path

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

    def test_build_volume_path(self):
        """Ensure build_volume_path joins a volume and relative subdirectory."""

        result = file_transfer.build_volume_path(
            "/srv/pa",
            "aerial/ucb",
            "Source",
        )

        assert result == Path("/srv/pa/aerial/ucb")

    def test_build_volume_path_requires_subdirectory(self):
        """Ensure build_volume_path requires a subdirectory."""

        with pytest.raises(ValueError, match="Source subdirectory is required"):
            file_transfer.build_volume_path(
                "/srv/lpsdata4",
                "",
                "Source",
            )

    def test_build_volume_path_rejects_absolute_subdirectory(self):
        """Ensure build_volume_path rejects absolute subdirectories."""

        with pytest.raises(ValueError, match="Source subdirectory must be relative"):
            file_transfer.build_volume_path(
                "tmp/srv/pa",
                "/aerial/ucb",
                "Source",
            )

    def test_build_manifest_empty_directory(self, tmp_path: Path):
        """Ensure that build_manifest returns an empty list for an empty directory."""
        result = file_transfer.build_file_manifest(tmp_path)

        assert result == {
            "source_root": str(tmp_path),
            "files": [],
        }

    def test_build_manifest_includes_nested_files(self, tmp_path: Path):
        """Ensure that build_manifest includes files recursively."""
        file_one = tmp_path / "file_one.txt"
        file_one.write_text("hello", encoding="utf-8")

        subdir = tmp_path / "subdir"
        subdir.mkdir()

        file_two = subdir / "file_two.txt"
        file_two.write_text("goodbye", encoding="utf-8")

        result = file_transfer.build_file_manifest(tmp_path)

        paths = {entry["path"] for entry in result["files"]}

        assert paths == {
            "file_one.txt",
            "subdir/file_two.txt",
        }
        assert len(result["files"]) == 2

    @pytest.mark.parametrize(
        "pattern,expected",
        [
            pytest.param(
                None, {"visible.tif"}, id="with_default_regex"
            ),
            pytest.param(
                r"^(visible\.tif|\.(.*))$", {"Thumbs.db"}, id="with_custom_regex"
            )
        ]
    )
    def test_build_manifest_excludes_hidden_and_excluded_files(self, manifest_source_path, pattern, expected):
        """Ensure that build_manifest skips hidden and system junk files."""
        result = file_transfer.build_file_manifest(manifest_source_path, exclude_regex=pattern)

        paths = {entry["path"] for entry in result["files"]}

        assert paths == expected
        assert len(result["files"]) == len(expected)

    @pytest.mark.parametrize(
        ("input_path", "expected_path"),
        [
            (
                Path("/srv/pa/aerial/ucb"),
                Path("/srv/pa/aerial/ucb/incoming"),
            ),
            (
                Path("/srv/pa/aerial/ucb/"),
                Path("/srv/pa/aerial/ucb/incoming"),
            ),
            (
                Path("/srv/pa/aerial/ucb/incoming"),
                Path("/srv/pa/aerial/ucb/incoming"),
            ),
            (
                Path("/srv/pa/aerial/ucb/incoming/"),
                Path("/srv/pa/aerial/ucb/incoming"),
            ),
        ],
    )
    def test_clean_destination_path(self, input_path: Path, expected_path: Path):
        """Ensure clean_destination_path normalizes the incoming directory suffix."""

        assert file_transfer.clean_destination_path(input_path) == expected_path

    def test_rename_temp_dir_moves_directory_to_incoming(self, tmp_path: Path):
        """Ensure rename_temp_dir moves the temp directory to incoming."""

        temp_dir = tmp_path / ".airflow" / "run-123"
        temp_dir.mkdir(parents=True)

        # temp_file = temp_dir / "manifest.json"
        temp_file = temp_dir / "some_file.tif"
        temp_file.write_text("{}", encoding="utf-8")

        file_transfer.rename_temp_dir(temp_dir)

        incoming_dir = tmp_path / "incoming"

        assert not temp_dir.exists()
        assert incoming_dir.exists()
        assert (incoming_dir / temp_file.name).exists()

    def test_move_to_uploaded_uses_existing_uploaded_directory(self, tmp_path: Path):
        """Ensure move_to_uploaded reuses an existing *_uploaded directory."""

        source_dir = tmp_path / "source"
        source_dir.mkdir()

        uploaded_dir = tmp_path / "03_uploaded"
        uploaded_dir.mkdir()

        source_file = source_dir / "some_file.tif"
        source_file.write_text("some contents", encoding="utf-8")

        result_path, moved_count = file_transfer.move_to_uploaded(str(source_dir))

        assert result_path == str(uploaded_dir)
        assert moved_count == 1
        assert not source_file.exists()
        assert (uploaded_dir / source_file.name).exists()

    @pytest.mark.parametrize(
        "existing_dir_name, expected_uploaded_name",
        [
            ("03_Files_to_Review", "04_Uploaded"),  # Case 1: Digit prefix exists -> Increment
            ("No_Digits_Here", "Uploaded"),           # Case 2: No digit prefix -> Use default fallback
        ]
    )

    def test_move_to_uploaded_creates_new_uploaded_directory(
        self, tmp_path: Path, existing_dir_name: str, expected_uploaded_name: str
    ):
        """Ensure move_to_uploaded creates the correct uploaded directory based on context."""
        parent_dir = tmp_path / "review"
        source_dir = parent_dir / "source"
        source_dir.mkdir(parents=True)
        
        # Dynamically create the environment based on the parameter
        review_dir = parent_dir / existing_dir_name
        review_dir.mkdir()
        
        # Set up a dummy file to trace the movement
        source_file = source_dir / "some_file.tif"
        source_file.write_text("some contents", encoding="utf-8")
        
        # Execute the function under test
        result_path, moved_count = file_transfer.move_to_uploaded(str(source_dir))
        
        # Verify outcomes using the parameterized target name
        if existing_dir_name == "No_Digits_Here":
            expected_uploaded_dir = source_dir / expected_uploaded_name
        else:
            expected_uploaded_dir = parent_dir / expected_uploaded_name

        assert result_path == str(expected_uploaded_dir)
        assert moved_count == 1
        assert not source_file.exists()
        assert (expected_uploaded_dir / source_file.name).exists()

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

    def test_copy_manifest_files_skips_matching_destination_file(
        self,
        tmp_path: Path,
        monkeypatch,
    ):
        """Ensure matching destination files are not overwritten."""

        source = tmp_path / "source"
        source.mkdir()

        destination = tmp_path / "destination"
        destination.mkdir()

        source_file = source / "test.txt"
        source_file.write_text("hello", encoding="utf-8")

        destination_file = destination / "test.txt"
        destination_file.write_text("hello", encoding="utf-8")

        manifest = file_transfer.build_file_manifest(source)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        def fail_if_called(*args, **kwargs):
            raise AssertionError("shutil.copy2 should not be called")

        monkeypatch.setattr(
            file_transfer.shutil,
            "copy2",
            fail_if_called,
        )

        file_transfer.copy_files_from_manifest(
            source,
            destination,
            manifest_path,
        )

        assert destination_file.read_text(encoding="utf-8") == "hello"

    def test_copy_manifest_files_replaces_mismatched_destination_file(
        self,
        tmp_path: Path,
    ):
        """Ensure mismatched destination files are copied again."""

        source = tmp_path / "source"
        source.mkdir()

        destination = tmp_path / "destination"
        destination.mkdir()

        source_file = source / "test.txt"
        source_file.write_text("correct contents", encoding="utf-8")

        destination_file = destination / "test.txt"
        destination_file.write_text("partial", encoding="utf-8")

        manifest = file_transfer.build_file_manifest(source)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        file_transfer.copy_files_from_manifest(
            source,
            destination,
            manifest_path,
        )

        assert destination_file.read_text(
            encoding="utf-8"
        ) == "correct contents"

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

    def test_copy_manifest_files_resumes_after_partial_failure(
        self,
        tmp_path: Path,
        monkeypatch,
    ):
        """Ensure a rerun skips completed files and copies the remaining files."""

        source = tmp_path / "source"
        source.mkdir()

        destination = tmp_path / "destination"
        destination.mkdir()

        for filename in ["one.txt", "two.txt", "three.txt"]:
            (source / filename).write_text(filename, encoding="utf-8")

        manifest = file_transfer.build_file_manifest(source)

        manifest_path = tmp_path / "manifest.json"
        file_transfer.save_json(manifest, manifest_path)

        real_copy2 = file_transfer.shutil.copy2
        copy_count = 0

        def fail_on_second_copy(source_file, destination_file):
            nonlocal copy_count
            copy_count += 1

            if copy_count == 2:
                raise OSError("Simulated network failure")

            return real_copy2(source_file, destination_file)

        monkeypatch.setattr(
            file_transfer.shutil,
            "copy2",
            fail_on_second_copy,
        )

        with pytest.raises(OSError, match="Simulated network failure"):
            file_transfer.copy_files_from_manifest(
                source,
                destination,
                manifest_path,
            )

        copied_after_failure = {
            path.name for path in destination.iterdir()
        }

        assert len(copied_after_failure) == 1

        copied_on_resume = []

        def track_resume_copy(source_file, destination_file):
            copied_on_resume.append(Path(source_file).name)
            return real_copy2(source_file, destination_file)

        monkeypatch.setattr(
            file_transfer.shutil,
            "copy2",
            track_resume_copy,
        )

        file_transfer.copy_files_from_manifest(
            source,
            destination,
            manifest_path,
        )

        assert {path.name for path in destination.iterdir()} == {
            "one.txt",
            "two.txt",
            "three.txt",
        }

        assert copied_after_failure.isdisjoint(copied_on_resume)
        assert len(copied_on_resume) == 2
