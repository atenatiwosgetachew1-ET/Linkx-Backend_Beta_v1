"""JWKS (JSON Web Key Set) client for verifying external JWTs."""

import os
import time
import json
from typing import Optional, Dict, Any
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
import requests
from flask import current_app

from .parent_jwt import ParentJwtError, _validate_parent_auth_url


class JWKSClient:
    """
    Fetches and caches JWKS (public keys) from a remote endpoint.
    Used to verify JWTs signed by external providers (e.g., Parent project).
    
    Features:
    - Automatic key caching with TTL
    - Refresh on demand or when expired
    - Conversion of JWKS keys to cryptography objects
    """

    def __init__(self, jwks_url: str, cache_ttl_seconds: int = 3600):
        self.jwks_url = jwks_url
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache = None
        self._cache_time = 0

    def _fetch_keys(self) -> Dict[str, Any]:
        """Fetch JWKS from remote endpoint."""
        try:
            _validate_parent_auth_url(self.jwks_url)
            response = requests.get(
                self.jwks_url,
                headers={"Accept": "application/json", "User-Agent": "linkx-api-parent-jwks/1.0"},
                timeout=5,
            )
            response.raise_for_status()
            return response.json()
        except ParentJwtError:
            current_app.logger.warning("Rejected unsafe JWKS URL configuration")
            raise
        except Exception as e:
            current_app.logger.error(f"Failed to fetch JWKS from {self.jwks_url}: {e}")
            raise

    def _get_cached_keys(self) -> Dict[str, Any]:
        """Get keys from cache if still valid, otherwise refresh."""
        now = time.time()
        if self._cache is None or (now - self._cache_time) > self.cache_ttl_seconds:
            self._cache = self._fetch_keys()
            self._cache_time = now
        return self._cache

    def get_key(self, kid: Optional[str] = None):
        """
        Get the public key for the given key ID (kid).
        
        Args:
            kid: Key ID from JWT header. If None, returns first key.
        
        Returns:
            cryptography public key object (for ES256: EllipticCurvePublicKey)
        
        Raises:
            ValueError: If key not found or invalid format
        """
        jwks = self._get_cached_keys()
        keys = jwks.get("keys", [])
        
        if not keys:
            raise ValueError("No keys found in JWKS response")
        
        # If kid specified, find matching key
        if kid:
            for key_data in keys:
                if key_data.get("kid") == kid:
                    return self._convert_jwk_to_public_key(key_data)
            raise ValueError(f"Key with kid '{kid}' not found in JWKS")
        
        # Otherwise return first key
        return self._convert_jwk_to_public_key(keys[0])

    @staticmethod
    def _convert_jwk_to_public_key(jwk: Dict[str, Any]):
        """
        Convert JWK (JSON Web Key) to cryptography public key object.
        
        Supports:
        - EC (Elliptic Curve) keys, specifically P-256 for ES256
        - RSA keys (for future use)
        """
        kty = jwk.get("kty")
        
        if kty == "EC":
            return JWKSClient._ec_jwk_to_public_key(jwk)
        elif kty == "RSA":
            return JWKSClient._rsa_jwk_to_public_key(jwk)
        else:
            raise ValueError(f"Unsupported key type: {kty}")

    @staticmethod
    def _ec_jwk_to_public_key(jwk: Dict[str, Any]):
        """Convert EC JWK to cryptography EllipticCurvePublicKey."""
        from cryptography.hazmat.backends import default_backend
        
        crv = jwk.get("crv")
        x_b64 = jwk.get("x")
        y_b64 = jwk.get("y")
        
        if not all([crv, x_b64, y_b64]):
            raise ValueError("Missing required EC key components")
        
        # Decode base64url (with padding)
        import base64
        padding = "=" * (-len(x_b64) % 4)
        x_bytes = base64.urlsafe_b64decode(x_b64 + padding)
        y_bytes = base64.urlsafe_b64decode(y_b64 + padding)
        
        # Map curve name to cryptography curve
        curve_map = {
            "P-256": ec.SECP256R1(),
            "P-384": ec.SECP384R1(),
            "P-521": ec.SECP521R1(),
        }
        
        curve = curve_map.get(crv)
        if not curve:
            raise ValueError(f"Unsupported EC curve: {crv}")
        
        # Create public numbers and key
        x = int.from_bytes(x_bytes, byteorder="big")
        y = int.from_bytes(y_bytes, byteorder="big")
        
        public_numbers = ec.EllipticCurvePublicNumbers(x, y, curve)
        return public_numbers.public_key(default_backend())

    @staticmethod
    def _rsa_jwk_to_public_key(jwk: Dict[str, Any]):
        """Convert RSA JWK to cryptography RSAPublicKey (for future use)."""
        from cryptography.hazmat.backends import default_backend
        
        e_b64 = jwk.get("e")
        n_b64 = jwk.get("n")
        
        if not all([e_b64, n_b64]):
            raise ValueError("Missing required RSA key components")
        
        import base64
        padding_e = "=" * (-len(e_b64) % 4)
        padding_n = "=" * (-len(n_b64) % 4)
        e_bytes = base64.urlsafe_b64decode(e_b64 + padding_e)
        n_bytes = base64.urlsafe_b64decode(n_b64 + padding_n)
        
        e = int.from_bytes(e_bytes, byteorder="big")
        n = int.from_bytes(n_bytes, byteorder="big")
        
        public_numbers = __import__("cryptography.hazmat.primitives.asymmetric.rsa", fromlist=["RSAPublicNumbers"]).RSAPublicNumbers(e, n)
        return public_numbers.public_key(default_backend())


# Global Parent project JWKS client (lazy-loaded on first use)
_parent_jwks_client = None
_parent_jwks_client_url = None


def get_parent_jwks_client() -> Optional[JWKSClient]:
    """Get or create the Parent project JWKS client."""
    global _parent_jwks_client, _parent_jwks_client_url

    parent_jwks_url = (
        os.getenv("LINKX_PARENT_JWKS_URL")
        or os.getenv("LINKX_PARENT_JWT_JWKS_URL")
    )
    if not parent_jwks_url:
        return None

    cache_ttl = int(
        os.getenv("LINKX_PARENT_JWKS_CACHE_SECONDS")
        or "3600"
    )
    if _parent_jwks_client is None or _parent_jwks_client_url != parent_jwks_url:
        _parent_jwks_client = JWKSClient(parent_jwks_url, cache_ttl_seconds=cache_ttl)
        _parent_jwks_client_url = parent_jwks_url

    return _parent_jwks_client
