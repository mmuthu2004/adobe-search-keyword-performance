"""
pii_masker.py
=============
SHA-256 hashing for PII columns (ip, user_agent).
Values are hashed before any logging. Never written to output.
"""
import hashlib


def mask(value: str) -> str:
    """Return SHA-256 hex digest of the input string."""
    if not value:
        return "EMPTY"
    return hashlib.sha256(value.strip().encode("utf-8")).hexdigest()[:16] + "..."
