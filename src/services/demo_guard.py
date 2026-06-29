"""Demo-mode upload allowlist (#82).

The public portfolio demo is anonymous and shared, so visitors must not be able
to upload arbitrary files from their filesystem — that would expose the parser
to untrusted PDFs, invite strangers to drop real bank-statement PII into a shared
sandbox, and uncap LLM cost. Instead the frontend bundles a small set of
synthetic sample statements and POSTs their bytes to the normal upload endpoints;
this guard (active only when ``DEMO_MODE`` is set) rejects anything whose sha256
isn't in the committed allowlist.

This is the ONE ``DEMO_MODE``-gated backend control (#82). The home server never
sets ``DEMO_MODE``, so the guard is inert there and the deployments stay
byte-for-byte identical otherwise.

The allowlisted samples are the committed *synthetic* fixtures (no PII), so the
hashes are safe to commit. They must stay byte-identical to the copies the
frontend bundles — identical bytes, identical hash.
"""

import hashlib
import os

from fastapi import HTTPException, status

from src.logging_config import get_logger

logger = get_logger(__name__)

# sha256 of tests/parsers/fixtures/{amex,tdbank,schwab}_sample.csv — the
# synthetic statements the demo frontend offers as "Try this sample →".
DEMO_SAMPLE_SHA256 = frozenset({
    "ac69766db216eb65f5a11f4ba25c48f82aee26f195534ec80bb42c10dc3a8fdf",  # amex_sample.csv
    "fb84c896b0ad0bc1aaae9d8304f76a0e8cddf1f019c9929ba40c295c4a4e4c76",  # tdbank_sample.csv
    "a56f271c0c2919cd12092c4e9d07dddcce361caf667be238cc9b55b7b35619a4",  # schwab_sample.csv
})


def demo_mode_enabled() -> bool:
    return os.getenv("DEMO_MODE", "").strip().lower() in ("1", "true", "yes", "on")


def enforce_demo_upload_allowlist(contents: bytes) -> None:
    """In demo mode, 403 any upload whose bytes aren't a known sample.

    No-op when ``DEMO_MODE`` is unset (i.e. on the home server). Call with the
    raw file bytes already read by the endpoint.
    """
    if not demo_mode_enabled():
        return
    digest = hashlib.sha256(contents).hexdigest()
    if digest not in DEMO_SAMPLE_SHA256:
        logger.warning("demo upload rejected (sha256=%s not in allowlist)", digest)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="In demo mode, only the provided sample statements can be uploaded.",
        )
