"""
Test script for CTMS JWT token verification integration.

This creates mock CTMS tokens and verifies they can be validated,
testing the ES256 signature verification chain.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

# Add parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.backends import default_backend
import jwt
import base64


def generate_p256_keypair():
    """Generate an EC P-256 keypair for testing."""
    private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
    public_key = private_key.public_key()
    return private_key, public_key


def create_mock_ctms_token(private_key, sub="a1b2c3d4-e5f6-7890-abcd-ef1234567890"):
    """Create a mock CTMS ES256 JWT token."""
    now = int(time.time())
    payload = {
        "sub": sub,
        "role": "SUPER_ADMIN",
        "entity_id": None,
        "permissions": [
            "UserRead", "UserCreate", "ReportSubmit",
            "LinkAnalysisRead", "LinkAnalysisManage"
        ],
        "assignable_roles": ["ANALYST", "DATA_ENCODER"],
        "token_version": 0,
        "exp": now + 3600,  # Valid for 1 hour
        "iat": now,
        "token_type": "access"
    }
    
    token = jwt.encode(
        payload,
        private_key,
        algorithm="ES256",
        headers={"typ": "JWT", "alg": "ES256"}
    )
    return token, payload


def create_mock_jwks(public_key):
    """Create a mock JWKS response with the public key."""
    # Encode EC public key to JWK format
    public_numbers = public_key.public_numbers()
    
    # Convert to bytes
    x_bytes = public_numbers.x.to_bytes(32, byteorder='big')
    y_bytes = public_numbers.y.to_bytes(32, byteorder='big')
    
    # Encode as base64url
    x_b64 = base64.urlsafe_b64encode(x_bytes).rstrip(b'=').decode('utf-8')
    y_b64 = base64.urlsafe_b64encode(y_bytes).rstrip(b'=').decode('utf-8')
    
    jwks = {
        "keys": [{
            "kty": "EC",
            "crv": "P-256",
            "x": x_b64,
            "y": y_b64,
            "use": "sig",
            "alg": "ES256",
            "kid": "ctms-auth-v1"
        }]
    }
    return jwks


def test_token_verification():
    """Test the ES256 token verification flow."""
    print("\n" + "="*60)
    print("CTMS JWT Token Verification Test")
    print("="*60)
    
    # Step 1: Generate keypair
    print("\n[1] Generating P-256 keypair...")
    private_key, public_key = generate_p256_keypair()
    print("    ✓ Keypair generated")
    
    # Step 2: Create mock CTMS token
    print("\n[2] Creating mock CTMS ES256 token...")
    token, payload = create_mock_ctms_token(private_key)
    print(f"    ✓ Token created (sub={payload['sub']})")
    print(f"      Token preview: {token[:50]}...")
    
    # Step 3: Verify token signature
    print("\n[3] Verifying token signature...")
    try:
        verified_payload = jwt.decode(
            token,
            public_key,
            algorithms=["ES256"],
            options={"verify_exp": True}
        )
        print("    ✓ Signature valid")
        print(f"    ✓ Payload: {json.dumps(verified_payload, indent=2)}")
    except Exception as e:
        print(f"    ✗ Signature verification failed: {e}")
        return False
    
    # Step 4: Validate claims
    print("\n[4] Validating claims...")
    checks = [
        ("token_type", verified_payload.get("token_type") == "access"),
        ("sub exists", bool(verified_payload.get("sub"))),
        ("role exists", bool(verified_payload.get("role"))),
        ("exp in future", verified_payload.get("exp", 0) > time.time()),
    ]
    
    for check_name, result in checks:
        status = "✓" if result else "✗"
        print(f"    {status} {check_name}")
        if not result:
            return False
    
    # Step 5: Test role mapping
    print("\n[5] Testing role mapping...")
    from auth.repository import normalize_parent_role
    
    test_roles = [
        ("SUPER_ADMIN", "admin"),
        ("HIGHER_OFFICIAL", "admin"),
        ("DIRECTOR", "admin"),
        ("ANALYST", "analyst"),
        ("VIEWER", "viewer"),
        ("team_leader", "admin"),  # Legacy
    ]
    
    for input_role, expected_output in test_roles:
        output = normalize_parent_role(input_role)
        status = "✓" if output == expected_output else "✗"
        print(f"    {status} {input_role:20} → {output:10} (expected: {expected_output})")
        if output != expected_output:
            return False
    
    # Step 6: Test JWKS conversion
    print("\n[6] Testing JWKS key conversion...")
    jwks = create_mock_jwks(public_key)
    print(f"    ✓ JWKS generated with {len(jwks['keys'])} key(s)")
    
    # Convert back
    from auth.jwks_client import JWKSClient
    try:
        jwk_key = JWKSClient._ec_jwk_to_public_key(jwks['keys'][0])
        print("    ✓ JWK successfully converted to public key")
    except Exception as e:
        print(f"    ✗ JWK conversion failed: {e}")
        return False
    
    # Step 7: Test error cases
    print("\n[7] Testing error handling...")
    
    # Invalid signature
    print("    - Testing invalid signature...")
    tampered_token = token[:-10] + "0000000000"
    try:
        jwt.decode(tampered_token, public_key, algorithms=["ES256"])
        print("      ✗ Should have rejected tampered token")
        return False
    except jwt.InvalidSignatureError:
        print("      ✓ Correctly rejected tampered token")
    
    # Expired token
    print("    - Testing expired token...")
    expired_payload = payload.copy()
    expired_payload["exp"] = int(time.time()) - 3600
    expired_token = jwt.encode(
        expired_payload,
        private_key,
        algorithm="ES256"
    )
    try:
        jwt.decode(expired_token, public_key, algorithms=["ES256"], options={"verify_exp": True})
        print("      ✗ Should have rejected expired token")
        return False
    except jwt.ExpiredSignatureError:
        print("      ✓ Correctly rejected expired token")
    
    # Wrong algorithm
    print("    - Testing wrong algorithm (HS256)...")
    hs256_token = jwt.encode(payload, "secret", algorithm="HS256")
    try:
        jwt.decode(hs256_token, public_key, algorithms=["ES256"])
        print("      ✗ Should have rejected HS256 token")
        return False
    except jwt.InvalidAlgorithmError:
        print("      ✓ Correctly rejected HS256 token")
    
    print("\n" + "="*60)
    print("✓ All tests passed!")
    print("="*60 + "\n")
    return True


if __name__ == "__main__":
    import sys
    success = test_token_verification()
    sys.exit(0 if success else 1)
