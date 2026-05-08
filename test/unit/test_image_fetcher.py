"""PyTest cases for the mokelumne.util.image_fetcher module."""

from pathlib import Path
from typing import Any
from unittest.mock import Mock, call

from mokelumne.util.image_fetcher import ImageFetcher, base64_size


FIXTURE_PATH: Path = Path(__file__).parent.parent / 'fixtures'
"""The directory that contains our fixtures."""


class TestSizeCallables:
    """Tests for the built-in size callables."""
    def test_base64_size(self):
        """Ensure that base64_size returns the base64 inflated size."""
        assert base64_size(4.5) == 6


class MockTindHook:
    """An object that pretends to be a TindHook object."""
    def __init__(self):
        """It is assumed subclasses will override these attributes."""
        self._files = [{'path': str(FIXTURE_PATH / 'test1.jpg'), 'mime': 'image/jpeg',
                        'size': 349076, 'width': 1000, 'height': 704}]

    def get_file_metadata(self, _record_id: str) -> list[dict[str, Any]]:
        """Retrieve file metadata for our mocked file(s)."""
        metadata = []
        for file in self._files:
            metadata.append({'mime': file['mime'], 'size': file['size'], 'width': file['width'],
                             'height': file['height']})
        return metadata

    def download_image_file(self, _record_id: str, _run_id: str) -> str:
        """Pretend to download an image for a given TIND record."""
        return self._files[0]['path']

    def download_image_from_record_sized(self, _record_id: str, _run_id: str, _width: int, _height: int) -> str:
        """Pretend to download an image for a given TIND record at a specific resolution."""
        return self._files[0]['path']


class MockTindHookWithMultipleFiles(MockTindHook):
    """An object that pretends to be a TindHook object, with a record with multiple files."""
    def __init__(self):
        super().__init__()
        self._files = [{'path': str(FIXTURE_PATH / 'test1.jpg'), 'mime': 'image/jpeg',
                        'size': 349076, 'width': 1000, 'height': 704},
                       {'path': str(FIXTURE_PATH / 'test2.jpg'), 'mime': 'image/jpeg',
                        'size': 860528, 'width': 1200, 'height': 647}]


class MockTindHookThatScales(MockTindHook):
    """An object that pretends to be a TindHook object, with a large image that scales."""
    def __init__(self):
        super().__init__()
        self._files = [{'path': str(FIXTURE_PATH / 'test3.jpg'), 'mime': 'image/jpeg',
                        'size': 1977086, 'width': 4032, 'height': 3024}]

    def download_image_file(self, _record_id: str, _run_id: str) -> str:
        """Pretend to download an image for a given TIND record."""
        return self._files[0]['path']

    def download_image_from_record_sized(self, _record_id: str, _run_id: str, _width: int, _height: int) -> str:
        """Give a smaller version of the test image."""
        return str(FIXTURE_PATH / 'test3_scaled.jpg')


def fetch_factory(tind_mock: MockTindHook, **kwargs) -> ImageFetcher:
    """Create an ImageFetcher with a mocked TIND fetcher client.

    This method is factored out so that we don't duplicate the pyright pragma in each test.
    """
    fetcher = ImageFetcher(tind_mock, **kwargs)  # pyright: ignore[reportArgumentType]
    return fetcher


class TestImageFetcher:
    """Tests for the ImageFetcher class."""
    def test_get_metadata(self):
        """Test the `get_metadata_for_record` method returns the correct result."""
        tind = MockTindHook()
        fetcher = fetch_factory(tind)
        assert fetcher.get_metadata_for_record('12345') == tind.get_file_metadata('12345')

    def test_get_metadata_cache(self):
        """Ensure that `get_metadata_for_record` caches its result."""
        tind = MockTindHook()
        md = tind.get_file_metadata('12345')
        tind.get_file_metadata = Mock(return_value=md)
        fetcher = fetch_factory(tind)
        results = [fetcher.get_metadata_for_record('12345') for _i in range(10)]
        assert len(results) == 10
        tind.get_file_metadata.assert_called_once()

    def test_fetch_one_image(self):
        """Test a simple `fetch_one_image_for_record` with no constraints."""
        fetcher = fetch_factory(MockTindHook())
        fetcher.fetch_one_image_for_record('12345', 'test_run')

    def test_fetch_one_image_bounds(self):
        """Test calling `fetch_one_image_for_record` with an out-of-bounds index."""
        fetcher = fetch_factory(MockTindHook())
        assert fetcher.fetch_one_image_for_record('12345', 'test_run', 1337) is None

    def test_fetch_multiple_images(self):
        """Test a simple `fetch_images_for_record` call."""
        fetcher = fetch_factory(MockTindHookWithMultipleFiles())
        result = fetcher.fetch_images_for_record('12345', 'test_run')
        assert len(result) == 2

    def test_fetch_all_images_single(self):
        """Test a simple `fetch_images_for_record` call with a single-file record."""
        fetcher = fetch_factory(MockTindHook())
        result = fetcher.fetch_images_for_record('12345', 'test_run')
        assert len(result) == 1

    def test_fetch_scale_unneeded(self):
        """Test `fetch_one_image_for_record` with constraints satisfied by the source image."""
        tind = MockTindHook()
        my_tind = Mock(wraps=tind)
        fetcher = fetch_factory(my_tind, max_h=2000, max_w=2000, max_size=1048576)
        fetcher.fetch_one_image_for_record('12345', 'test_run')
        my_tind.download_image_file.assert_called_once()
        my_tind.download_image_from_record_sized.assert_not_called()

    def test_fetch_scale_needed(self):
        """Test `fetch_one_image_for_record` with constraints that require a resize."""
        tind = MockTindHookThatScales()
        my_tind = Mock(wraps=tind)
        fetcher = fetch_factory(my_tind, max_h=2000, max_w=2000, max_size=1048576)
        fetcher.fetch_one_image_for_record('12345', 'test_run')
        my_tind.download_image_file.assert_not_called()
        my_tind.download_image_from_record_sized.assert_called_once()

    def test_fetch_scale_oversized(self):
        """Test `fetch_one_image_for_record` with constraints the initial resize can't meet."""
        def transform(size: int) -> int:
            """identity transformation"""
            return size

        tind = MockTindHookThatScales()
        my_tind = Mock(wraps=tind)
        my_transform = Mock(wraps=transform)
        fetcher = fetch_factory(my_tind, max_h=2000, max_w=2000, max_size=262144,
                                size_transform=my_transform)
        fetcher.fetch_one_image_for_record('12345', 'test_run')
        my_transform.assert_called_with(481229)
        my_tind.download_image_file.assert_not_called()
        my_tind.download_image_from_record_sized.assert_has_calls([
            call('12345', 'test_run', 534, 400), call('12345', 'test_run', 264, 197)
        ])
