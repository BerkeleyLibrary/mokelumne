"""PyTest cases for the mokelumne.util.aws_connection module."""

from unittest.mock import Mock
import pytest
from mokelumne.util import aws_connection

class TestAwsConnection:
    """Tests for AWS connection setting extraction helpers."""

    def test_get_aws_connection_settings(self, monkeypatch):
        """Map aws_default connection values onto Bedrock client kwargs."""
        mock_conn = Mock(
            login="test-access-key",
            password="test-secret-key",
            extra_dejson={
                "region_name": "us-west-1",
                "endpoint_url": "https://bedrock-runtime.us-west-1.amazonaws.com"
            },
        )

        monkeypatch.setattr(
            aws_connection.BaseHook,
            "get_connection",
            Mock(return_value=mock_conn),
        )

        settings = aws_connection.get_aws_connection_settings("aws_default")

        assert settings["aws_access_key_id"] == "test-access-key"
        assert settings["aws_secret_access_key"] == "test-secret-key"
        assert settings["region_name"] == "us-west-1"
        assert settings["endpoint_url"] == "https://bedrock-runtime.us-west-1.amazonaws.com"

    def test_get_aws_connection_settings_no_connection(self, monkeypatch):
        """Raise when aws_default is absent."""
        monkeypatch.setattr(
            aws_connection.BaseHook,
            "get_connection",
            Mock(side_effect=RuntimeError("missing aws connection")),
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to resolve AWS connection aws_default: missing aws connection",
        ):
            aws_connection.get_aws_connection_settings("aws_default")
