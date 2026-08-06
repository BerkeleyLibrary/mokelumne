"""Tests for CAS-specific JOSE registry customization."""

import time

import pytest

from authlib.integrations.base_client import OpenIDMixin
from joserfc import jwt
from joserfc.errors import InvalidHeaderValueError, UnsupportedHeaderError
from joserfc.jwk import OctKey
from joserfc.jws import JWSRegistry

from mokelumne.oidc.jose_registry import register_cas_client_id_header


class StubOpenIDClient(OpenIDMixin):
    """Minimal Authlib OIDC client for exercising ID-token parsing."""

    client_id = "mokelumne"

    def __init__(self, key):
        self.key = key

    def load_server_metadata(self):
        """Return the metadata Authlib uses for ID-token validation."""
        return {
            "issuer": "https://cas.example.edu",
            "id_token_signing_alg_values_supported": ["HS256"],
        }

    def fetch_jwk_set(self, force=False):
        """Return the test signing key as a JSON Web Key Set."""
        return {"keys": [self.key.as_dict()]}


def test_registers_client_id_for_new_and_default_registries():
    """CAS's client_id header is accepted on both joserfc decode paths."""
    register_cas_client_id_header()
    key = OctKey.import_key("test-secret-with-at-least-112-bits")
    token = jwt.encode(
        {"alg": "HS256", "client_id": "mokelumne"},
        {"sub": "test-user"},
        key,
    )

    assert jwt.decode(token, key).claims["sub"] == "test-user"
    assert jwt.decode(token, key, algorithms=["HS256"]).claims["sub"] == "test-user"


def test_authlib_accepts_cas_client_id_header():
    """Authlib's OIDC parser accepts a CAS ID token with client_id."""
    register_cas_client_id_header()
    key = OctKey.import_key("test-secret-with-at-least-112-bits")
    now = int(time.time())
    token = jwt.encode(
        {"alg": "HS256", "client_id": "mokelumne"},
        {
            "iss": "https://cas.example.edu",
            "sub": "test-user",
            "aud": "mokelumne",
            "exp": now + 300,
            "iat": now,
            "nonce": "test-nonce",
        },
        key,
    )

    userinfo = StubOpenIDClient(key).parse_id_token(
        {"id_token": token},
        nonce="test-nonce",
    )

    assert userinfo["sub"] == "test-user"


def test_client_id_header_must_be_a_string():
    """The workaround validates the non-standard header's value."""
    register_cas_client_id_header()

    with pytest.raises(InvalidHeaderValueError):
        JWSRegistry().check_header({"alg": "HS256", "client_id": 123})


def test_other_unregistered_headers_remain_rejected():
    """The workaround does not disable joserfc's strict header checking."""
    register_cas_client_id_header()

    with pytest.raises(UnsupportedHeaderError):
        JWSRegistry().check_header({"alg": "HS256", "unexpected": "value"})
