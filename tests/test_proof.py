import json

from satsignal_blob.proof import SCHEMA, build_proof_record, serialize_proof


def test_proof_record_shape_v1():
    rec = build_proof_record(
        object_uri="s3://bucket/path/contract.pdf",
        object_name="contract.pdf",
        sha256_hex="ab" * 32,
        file_size=12345,
        bundle_id="deadbeef",
        txid="0" * 64,
        receipt_url="https://app.satsignal.cloud/w/o/m/test/r/deadbeef",
        bundle_url="https://app.satsignal.cloud/bundle/deadbeef.mbnt",
        matter_slug="test",
        sidecar_mbnt_name="contract.pdf.mbnt",
        duplicate=False,
        dry_run=False,
        anchored_at_utc="2026-05-14T18:00:00Z",
    )
    assert rec["schema"] == SCHEMA
    # The v1 wire contract: lock the key set so a silent shape change
    # surfaces in CI rather than as a broken downstream consumer.
    expected_keys = {
        "schema", "source_uri", "file", "size", "sha256",
        "bundle_id", "txid", "receipt_url", "bundle_url",
        "matter_slug", "anchored_at_utc",
        "duplicate", "dry_run", "sidecar_mbnt", "verifier_url",
    }
    assert set(rec.keys()) == expected_keys
    assert rec["sha256"] == "ab" * 32
    assert rec["sidecar_mbnt"] == "contract.pdf.mbnt"


def test_proof_serialization_is_pretty_and_stable():
    rec = build_proof_record(
        object_uri="file:///tmp/x.txt",
        object_name="x.txt",
        sha256_hex="cd" * 32,
        file_size=4,
        bundle_id="b",
        txid=None,
        receipt_url="r",
        bundle_url=None,
        matter_slug="m",
        sidecar_mbnt_name="x.txt.mbnt",
        duplicate=True,
        dry_run=False,
        anchored_at_utc="2026-05-14T18:00:00Z",
    )
    blob = serialize_proof(rec)
    assert blob.endswith(b"\n")
    # Two-space indent + sorted keys -> stable byte output for git diff.
    parsed = json.loads(blob)
    assert parsed == rec
    text = blob.decode("utf-8")
    assert text.startswith("{\n  \"anchored_at_utc\":")
