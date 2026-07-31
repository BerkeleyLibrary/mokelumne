"""JOSE registry customizations for CAS-issued ID tokens."""

from joserfc.jws import JWSRegistry, default_registry
from joserfc.registry import HeaderParameter


def register_cas_client_id_header() -> None:
    """Allow CAS's optional string ``client_id`` JWS header."""
    client_id_header = HeaderParameter("OAuth 2.0 Client Identifier", "str")

    # Authlib constructs its JWS registry internally and does not expose
    # joserfc's registry argument. Update the defaults used for both registries
    # constructed with an algorithm allowlist and the cached default registry.
    JWSRegistry.default_header_registry["client_id"] = client_id_header
    default_registry.header_registry["client_id"] = client_id_header
