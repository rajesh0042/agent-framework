# Copyright (c) Microsoft. All rights reserved.

"""Constant-time HMAC-SHA256 validation for inbound GitHub webhook deliveries.

GitHub signs each webhook payload with the repository/app webhook secret and
sends the result in the `X-Hub-Signature-256` header as `sha256=<hex digest>`.
Validation must use a constant-time comparison so a timing side-channel
cannot be used to recover the signature (and therefore forge deliveries).
"""

from __future__ import annotations

import hashlib
import hmac

__all__ = ["verify_github_signature"]

_SIGNATURE_PREFIX = "sha256="


def verify_github_signature(*, secret: str, payload: bytes, signature_header: str | None) -> bool:
    """Validate a GitHub webhook delivery's `X-Hub-Signature-256` header.

    Args:
        secret: The configured webhook secret.
        payload: The raw (undecoded) request body GitHub signed.
        signature_header: The value of the `X-Hub-Signature-256` request header.

    Returns:
        `True` if the signature matches, `False` otherwise (including when the
        header is missing or malformed).
    """
    if not signature_header or not signature_header.startswith(_SIGNATURE_PREFIX):
        return False
    expected_digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    expected_header = _SIGNATURE_PREFIX + expected_digest
    return hmac.compare_digest(expected_header, signature_header)
