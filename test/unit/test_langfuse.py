"""PyTest cases for the mokelumne.util.langfuse module."""

from unittest.mock import Mock
import pytest
from mokelumne.util import langfuse

DEFAULT_TEST_PROMPT: str = "A test prompt."
"""The prompt used if none is specified to the mocked Langfuse object."""

DEFAULT_TEST_VERSION: int = 67
"""The version used if none is specified to the mocked Langfuse object."""

FAKE_HOST: str = "https://langfuse.example.com"
FAKE_PUBLIC_KEY: str = "pk-test-123"
FAKE_SECRET_KEY: str = "sk-test-456"
FAKE_CONN_SETTINGS: tuple[str, str, str] = (FAKE_HOST, FAKE_PUBLIC_KEY, FAKE_SECRET_KEY)


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


@pytest.fixture
def mock_conn_settings(monkeypatch):
    """Patch _get_langfuse_connection_settings to avoid hitting Airflow."""
    monkeypatch.setattr(
        langfuse, '_get_langfuse_connection_settings', lambda: FAKE_CONN_SETTINGS
    )


class MockConnection:
    """Mock Airflow connection object."""
    def __init__(self, host, extras=None):
        self.host = host
        self._extra_dejson = extras or {}

    @property
    def extra_dejson(self):
        return self._extra_dejson


class TestHost:
    """Tests for _host."""

    def test_preserves_https_scheme(self):
        """Ensure https:// scheme is preserved."""
        conn = MockConnection(host="https://example.com")
        assert langfuse._host(conn) == "https://example.com"

    def test_adds_https_to_plain_host(self):
        """Ensure https:// is prepended to plain host."""
        conn = MockConnection(host="example.com")
        assert langfuse._host(conn) == "https://example.com"

    def test_returns_none_host(self):
        """Ensure None host is returned as-is."""
        conn = MockConnection(host=None)
        assert langfuse._host(conn) is None


class TestGetLangfuseConnectionSettings:
    """Tests for _get_langfuse_connection_settings."""

    def test_returns_correct_settings(self, monkeypatch):
        """Ensure connection settings are correctly extracted and returned."""
        mock_conn = MockConnection(
            host="https://langfuse.example.com",
            extras={
                'extra': '{"public_key": "pk-123", "secret_key": "sk-456"}'
            }
        )
        monkeypatch.setattr(
            langfuse.BaseHook, 'get_connection', Mock(return_value=mock_conn)
        )

        host, public_key, secret_key = langfuse._get_langfuse_connection_settings()

        assert host == "https://langfuse.example.com"
        assert public_key == "pk-123"
        assert secret_key == "sk-456"

    def test_raises_on_missing_public_key(self, monkeypatch):
        """Ensure ValueError is raised when public_key is missing."""
        mock_conn = MockConnection(
            host="https://langfuse.example.com",
            extras={
                'extra': '{"secret_key": "sk-456"}'
            }
        )
        monkeypatch.setattr(
            langfuse.BaseHook, 'get_connection', Mock(return_value=mock_conn)
        )

        with pytest.raises(ValueError, match="Missing public_key"):
            langfuse._get_langfuse_connection_settings()


@pytest.mark.usefixtures("mock_conn_settings")
class TestGetLangfuseClient:
    """Tests for get_langfuse_client."""

    def test_returns_langfuse_instance(self, monkeypatch):
        """Ensure get_langfuse_client constructs a Langfuse with the connection settings."""
        mock_langfuse_cls = Mock(return_value=Mock())
        monkeypatch.setattr(langfuse, 'Langfuse', mock_langfuse_cls)

        client = langfuse.get_langfuse_client()

        mock_langfuse_cls.assert_called_once()
        call_kwargs = mock_langfuse_cls.call_args.kwargs
        assert call_kwargs['host'] == FAKE_HOST
        assert call_kwargs['public_key'] == FAKE_PUBLIC_KEY
        assert call_kwargs['secret_key'] == FAKE_SECRET_KEY
        assert client is mock_langfuse_cls.return_value

    def test_passes_release_and_environment(self, monkeypatch):
        """Ensure release and environment are forwarded to the Langfuse constructor."""
        mock_langfuse_cls = Mock(return_value=Mock())
        monkeypatch.setattr(langfuse, 'Langfuse', mock_langfuse_cls)
        monkeypatch.setenv('DEPLOYMENT_ID', 'staging')

        langfuse.get_langfuse_client()

        call_kwargs = mock_langfuse_cls.call_args.kwargs
        assert call_kwargs['environment'] == 'staging'
        assert 'release' in call_kwargs


@pytest.mark.usefixtures("mock_conn_settings")
class TestGetPrompt:
    """Tests for get_prompt."""

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
