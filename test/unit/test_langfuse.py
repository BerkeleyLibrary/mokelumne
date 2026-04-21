"""PyTest cases for the mokelumne.util.langfuse module."""

from unittest.mock import Mock
import pytest

from mokelumne.util import langfuse


DEFAULT_TEST_PROMPT: str = "A test prompt."
"""The prompt used if none is specified to the mocked Langfuse object."""


DEFAULT_TEST_VERSION: int = 67
"""The version used if none is specified to the mocked Langfuse object."""


class MockResult:
    """An object for return by our mock Langfuse."""
    def __init__(self, prompt, version):
        """Create a mocked Langfuse prompt object."""
        self.prompt = prompt
        self.version = version


class MockLangfuse:
    """A mock Langfuse class for testing Langfuse integration."""
    def __init__(self, **kwargs):
        if kwargs.get('release'):
            self.release = kwargs['release']
        if kwargs.get('environment'):
            self.environment = kwargs['environment']

        prompt = kwargs.get('prompt', DEFAULT_TEST_PROMPT)
        version = kwargs.get('version', DEFAULT_TEST_VERSION)
        mocked_result = MockResult(prompt=prompt, version=version)

        self.get_prompt = Mock(return_value=mocked_result)


class TestLangfuse:
    """Tests for the Mokelumne Langfuse integration utility class."""

    def test_simple_prompt(self, monkeypatch):
        """Test a simple call of the `get_prompt` method."""
        with monkeypatch.context() as m:
            m.setattr(langfuse, 'Langfuse', MockLangfuse)
            result = langfuse.get_prompt('test', 'production')
        assert result.prompt == DEFAULT_TEST_PROMPT
        assert result.version == DEFAULT_TEST_VERSION

    def test_label_is_used(self, monkeypatch):
        """Ensure that passing a label to `get_prompt` works as expected."""
        lf_object = MockLangfuse()
        mock = Mock(return_value=lf_object)

        name = 'test_prompt'
        label = 'staging'

        with monkeypatch.context() as m:
            m.setattr(langfuse, 'Langfuse', mock)
            langfuse.get_prompt(name, label)

        lf_object.get_prompt.assert_called_with(name, label=label)

    def test_version_is_used(self, monkeypatch):
        """Ensure that passing a version to `get_prompt` works as expected."""
        lf_object = MockLangfuse()
        mock = Mock(return_value=lf_object)

        name = 'test_prompt'
        version = 67

        with monkeypatch.context() as m:
            m.setattr(langfuse, 'Langfuse', mock)
            langfuse.get_prompt(name, version)

        lf_object.get_prompt.assert_called_with(name, version=version)
