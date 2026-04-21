"""PyTest cases for the mokelumne.util.storage module."""

from pathlib import Path
from unittest.mock import Mock
import pytest

from mokelumne.util import storage


@pytest.fixture
def test_dl_dir(monkeypatch, tmp_path: Path) -> Path:
    """Create a test directory and set `MOKELUMNE_TIND_DOWNLOAD` to it."""
    test_path = tmp_path
    monkeypatch.setenv("MOKELUMNE_TIND_DOWNLOAD", str(test_path))
    return test_path


@pytest.fixture
def test_pub_dir(monkeypatch, tmp_path: Path) -> Path:
    """Create a public directory and set `MOKELUMNE_PUBLIC_STORAGE` to it."""
    pub_path = tmp_path
    monkeypatch.setenv("MOKELUMNE_PUBLIC_STORAGE", str(pub_path))
    return pub_path


class TestStorage:
    """Tests for the Mokelumne storage module."""

    def test_storage_dir_uses_env(self, test_dl_dir):
        """Ensure that `storage_dir` respects the `MOKELUMNE_TIND_DOWNLOAD` variable."""
        result = storage.storage_dir()
        assert result == test_dl_dir

    def test_storage_dir_default(self, monkeypatch):
        """Ensure that `storage_dir` has a reasonable default."""
        monkeypatch.delenv("MOKELUMNE_TIND_DOWNLOAD", raising=False)
        result = storage.storage_dir()
        assert result == Path('/opt/airflow/download')

    def test_run_dir(self, test_dl_dir):
        """Ensure that `run_dir` is a subdir of `storage_dir`."""
        test_id = "TestID"
        result = storage.run_dir(test_id)
        assert result == (test_dl_dir / test_id)

    def test_run_dir_creates(self, test_dl_dir, monkeypatch):
        """Ensure that `run_dir` creates the directory if it doesn't exist."""
        test_id = "TestID"
        with monkeypatch.context() as m:
            mkdir_mock = Mock(return_value=True)
            m.setattr(Path, 'mkdir', mkdir_mock)
            result = storage.run_dir(test_id)
            assert mkdir_mock.called
        assert result == (test_dl_dir / test_id)

    def test_record_dir(self, test_dl_dir):
        """Ensure that `record_dir` is a subdir of `run_dir`."""
        test_id = "TestID"
        run = storage.run_dir(test_id)
        result = storage.record_dir(test_id, "123456")
        assert result.parts[:len(run.parts)] == run.parts

    def test_record_dir_sharding(self, test_dl_dir):
        """Ensure that `record_dir` is sharded correctly."""
        result = storage.record_dir("TestRun", "123456")
        assert result.parts[-2:] == ("12", "123456")

    def test_public_dir(self, test_pub_dir):
        """Ensure that `public_dir` respects the `MOKELUMNE_PUBLIC_STORAGE` variable."""
        result = storage.public_dir()
        assert result == test_pub_dir

    def test_public_dir_default(self, monkeypatch):
        """Ensure that `public_dir` has a reasonable default."""
        monkeypatch.delenv("MOKELUMNE_PUBLIC_STORAGE", raising=False)
        result = storage.public_dir()
        assert result == Path('/opt/airflow/public')

    def test_public_path_to_url(self, monkeypatch, test_pub_dir):
        """Ensure that we can turn a public path into a URL."""
        test_url = "https://test.example/"
        monkeypatch.setenv("MOKELUMNE_PUBLIC_URL", test_url)
        test_path = test_pub_dir / "some_run" / "some_asset.file"
        result = storage.public_path_to_url(test_path)
        assert result == f"{test_url}some_run/some_asset.file"
