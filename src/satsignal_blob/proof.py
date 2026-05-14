"""Build the ``<object>.proof.json`` sidecar.

The .proof.json is a human-readable summary that lives next to the
original object + its .mbnt sibling. Schema is locked at
``satsignal-blob-proof-v1``; the golden-shape test pins it so a
change forces a major bump.

The full evidence bundle lives in the sibling ``.mbnt`` — verifiable
offline against any BSV explorer. proof.json is the "what was this?"
index; .mbnt is the "prove it" payload.
"""
from __future__ import annotations

import json
import time
from typing import Optional


SCHEMA = "satsignal-blob-proof-v1"
VERIFIER_URL = "https://satsignal.cloud/verify"


def build_proof_record(
    *,
    object_uri: str,
    object_name: str,
    sha256_hex: str,
    file_size: int,
    bundle_id: str,
    txid: Optional[str],
    receipt_url: str,
    bundle_url: Optional[str],
    matter_slug: str,
    sidecar_mbnt_name: str,
    duplicate: bool,
    dry_run: bool,
    anchored_at_utc: Optional[str] = None,
) -> dict:
    """Return the dict that becomes ``<object>.proof.json``."""
    if anchored_at_utc is None:
        anchored_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    record = {
        "schema": SCHEMA,
        "source_uri": object_uri,
        "file": object_name,
        "size": int(file_size),
        "sha256": sha256_hex.lower(),
        "bundle_id": bundle_id,
        "txid": txid,
        "receipt_url": receipt_url,
        "bundle_url": bundle_url,
        "matter_slug": matter_slug,
        "anchored_at_utc": anchored_at_utc,
        "duplicate": bool(duplicate),
        "dry_run": bool(dry_run),
        "sidecar_mbnt": sidecar_mbnt_name,
        "verifier_url": VERIFIER_URL,
    }
    return record


def serialize_proof(record: dict) -> bytes:
    """Stable encoding for sidecar writes — pretty JSON + trailing
    newline so the file is grep-friendly and diff-clean."""
    return (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")
