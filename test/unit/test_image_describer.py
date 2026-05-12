"""PyTest cases for the mokelumne.util.image_describer module."""

from pathlib import Path
from unittest.mock import Mock

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
        self.invoke = Mock()


class MockResult:
    """An object that can behave like a result from an LLM model invocation."""
    def __init__(self, content):
        self.content = content


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
        result = MockResult(desc)
        model.invoke.return_value = result
        describer = image_describer.ImageDescriber(model, TEST_PROMPT)
        result = describer.describe(NORMAL_RECORD_FIXTURE)
        model.invoke.assert_called_once()
        assert result["Description"] == desc

    def test_client_error(self):
        """Test case where Invoke raises a ClientError."""
        err = "Bedrock is temporarily unavailable."
        model = MockModel()
        model.invoke.side_effect = MockError(err)
        describer = image_describer.ImageDescriber(model, TEST_PROMPT)
        result = describer.describe(NORMAL_RECORD_FIXTURE)
        assert "failure" in result["Status"]
        assert err in result["Status description"]

    def test_size_error(self):
        """Test case where the record's image is too large."""
        model = MockModel()
        describer = image_describer.ImageDescriber(model, TEST_PROMPT)
        result = describer.describe(LARGE_RECORD_FIXTURE)
        model.invoke.assert_not_called()
        assert "failure" in result["Status"]
