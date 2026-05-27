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
def test_dl_dir_arg(monkeypatch, tmp_path: Path) -> Path:
    test_path = tmp_path / "with_argument"
    monkeypatch.delenv("MOKELUMNE_TIND_DOWNLOAD", raising=False)
    return test_path

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

    def test_storage_dir_with_argument(self, monkeypatch, test_dl_dir_arg):
        """Ensure that `storage_dir` gets set with a base_dir argument."""
        monkeypatch.delenv("MOKELUMNE_TIND_DOWNLOAD", raising=False)
        result = storage.storage_dir(base_dir=str(test_dl_dir_arg))
        assert result == test_dl_dir_arg

    def test_run_dir(self, test_dl_dir):
        """Ensure that `run_dir` is a subdir of `storage_dir`."""
        test_id = "TestID"
        result = storage.run_dir(test_id)
        assert result == (test_dl_dir / test_id)

    def test_run_dir_with_argument(self, test_dl_dir_arg):
        """Ensure that `run_dir` is a subdir of `storage_dir`."""
        test_id = "TestID"
        result = storage.run_dir(test_id, base_dir=str(test_dl_dir_arg))
        assert result == (test_dl_dir_arg / test_id)

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
        """Ensure that `record_dir` is a subdir of `storage_dir`."""
        test_id = "TestID"
        base_dir = storage.storage_dir()
        result = storage.record_dir("123456")
        assert result.parts[:len(base_dir.parts)] == base_dir.parts

    def test_record_dir_sharding(self, test_dl_dir):
        """Ensure that `record_dir` is sharded correctly."""
        result = storage.record_dir("123456")
        assert result.parts[-2:] == ("12", "123456")

