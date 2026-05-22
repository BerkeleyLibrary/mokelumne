"""PyTest cases for the mokelumne.util.image_describer module."""

from pathlib import Path
from contextlib import contextmanager
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
from mokelumne.util import image_describer


FIXTURE_PATH: Path = Path(__file__).parent.parent / "fixtures"
"""The directory that contains our fixtures."""


TEST_PROMPT: str = "Test prompt."
"""The prompt used for our tests."""


NORMAL_RECORD_FIXTURE: dict[str, str] = {
    "Record ID": "123456", "Image Path": str(FIXTURE_PATH / "test1.jpg"), "035__a": "(TEST)test1"
}
"""A fixture representing a 'normal' record."""


LARGE_RECORD_FIXTURE: dict[str, str] = {
    "Record ID": "101101", "Image Path": str(FIXTURE_PATH / "test4.jpg"), "035__a": "(TEST)test4"
}
"""A fixture representing a record that has an image too large for Bedrock."""


MIME_FAIL_RECORD_FIXTURE: dict[str, str] = {
    "Record ID": "234567", "Image Path": str(Path(__file__)), "035__a": "(TEST)test5"
}
"""A fixture representing a record that will not pass the MIME test."""


class MockModel:
    """An object that can behave like an LLM model for the sake of ImageDescriber."""
    def __init__(self):
        self.describe_image = Mock()
        self.model_id = "test-model"


class MockLangfuseObservation:
    """An object that can behave like a Langfuse observation context."""

    def __init__(self):
        self.update = Mock()


class MockLangfuseClient:
    """An object that can behave like a Langfuse client."""

    def __init__(self):
        self.observation = MockLangfuseObservation()

    @contextmanager
    def start_as_current_observation(self, **kwargs):
        del kwargs
        yield self.observation


class MockError(ClientError):
    """A mocked error to test error handling paths."""
    def __init__(self, message: str = "An error has occurred."):
        self.response = {
            "message": message,
            "Error": {
                "Code": "12345",
                "Message": message
            }
        }


class TestImageDescriber:
    """Tests for the ImageDescriber class."""
    def test_describe_record(self):
        """Test describing a record."""
        desc = "An image of a regal building with the text 'The University Library' inscribed."
        model = MockModel()
        model.describe_image.return_value = desc
        with patch.object(
                image_describer.langfuse,
                "get_langfuse_client",
                return_value=MockLangfuseClient(),
        ):
            describer = image_describer.ImageDescriber(model, TEST_PROMPT)
        result = describer.describe(NORMAL_RECORD_FIXTURE.copy())
        model.describe_image.assert_called_once()
        assert result["Description"] == desc

    def test_client_error(self):
        """Test case where Invoke raises a ClientError."""
        err = "Bedrock is temporarily unavailable."
        model = MockModel()
        model.describe_image.side_effect = MockError(err)
        with patch.object(
                image_describer.langfuse,
                "get_langfuse_client",
                return_value=MockLangfuseClient(),
        ):
            describer = image_describer.ImageDescriber(model, TEST_PROMPT)
        result = describer.describe(NORMAL_RECORD_FIXTURE.copy())
        assert "failure" in result["Status"]
        assert err in result["Status description"]

    def test_size_error(self):
        """Test case where the record's image is too large."""
        model = MockModel()
        with patch.object(
                image_describer.langfuse,
                "get_langfuse_client",
                return_value=MockLangfuseClient(),
        ):
            describer = image_describer.ImageDescriber(model, TEST_PROMPT)
        result = describer.describe(LARGE_RECORD_FIXTURE.copy())
        model.describe_image.assert_not_called()
        assert "failure" in result["Status"]
