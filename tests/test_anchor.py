"""End-to-end tests for anchor_object + anchor_prefix.

Uses fsspec's MemoryFileSystem so we never touch the network and
never burn chain fees. The mock_api fixture replaces the HTTP
transport with a canned-response function.
"""
from __future__ import annotations

import hashlib
import json

from satsignal_blob import (
    MBNT_SUFFIX,
    PROOF_SUFFIX,
    anchor_object,
    anchor_prefix,
)
from satsignal_blob._api import APIError, SatsignalApi
from tests.conftest import make_canned_transport


def _write(memfs, path, payload):
    memfs.pipe(path, payload)


def test_anchor_object_writes_both_sidecars(memfs, mock_api):
    payload = b"hello-world-contract"
    _write(memfs, "/bkt/contract.pdf", payload)

    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="test-matter",
        api=mock_api,
        fs=memfs,
        path="/bkt/contract.pdf",
    )

    assert outcome.status == "anchored"
    assert outcome.sha256_hex == hashlib.sha256(payload).hexdigest()
    assert outcome.size == len(payload)
    assert outcome.bundle_id == "deadbeefcafe1234"

    # Sidecars present.
    assert memfs.exists("/bkt/contract.pdf.mbnt")
    assert memfs.exists("/bkt/contract.pdf.proof.json")

    # .mbnt is the canned bundle bytes.
    with memfs.open("/bkt/contract.pdf.mbnt", "rb") as f:
        assert f.read() == b"FAKE-MBNT-BYTES"

    # .proof.json conforms to the v1 schema.
    with memfs.open("/bkt/contract.pdf.proof.json", "rb") as f:
        proof = json.loads(f.read())
    assert proof["schema"] == "satsignal-blob-proof-v1"
    assert proof["sha256"] == hashlib.sha256(payload).hexdigest()
    assert proof["bundle_id"] == "deadbeefcafe1234"
    assert proof["sidecar_mbnt"] == "contract.pdf.mbnt"


def test_anchor_object_skips_when_mbnt_exists(memfs, mock_api):
    _write(memfs, "/bkt/contract.pdf", b"abc")
    _write(memfs, "/bkt/contract.pdf.mbnt", b"existing")

    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        fs=memfs,
        path="/bkt/contract.pdf",
    )

    assert outcome.status == "skipped_existing"
    # The existing sidecar is left untouched.
    with memfs.open("/bkt/contract.pdf.mbnt", "rb") as f:
        assert f.read() == b"existing"
    # No API call happened.
    assert mock_api._captured == []  # type: ignore[attr-defined]


def test_anchor_object_force_reanchors(memfs, mock_api):
    _write(memfs, "/bkt/contract.pdf", b"abc")
    _write(memfs, "/bkt/contract.pdf.mbnt", b"old-bundle")

    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        fs=memfs,
        path="/bkt/contract.pdf",
        force=True,
    )

    assert outcome.status == "anchored"
    # .mbnt overwritten with the fresh canned bytes.
    with memfs.open("/bkt/contract.pdf.mbnt", "rb") as f:
        assert f.read() == b"FAKE-MBNT-BYTES"
    # force_new=true in the request body.
    assert mock_api._captured[0]["body"]["force_new"] is True  # type: ignore[attr-defined]


def test_anchor_object_skips_sidecar_path(memfs, mock_api):
    _write(memfs, "/bkt/contract.pdf.mbnt", b"already-an-mbnt")

    outcome = anchor_object(
        "memory:///bkt/contract.pdf.mbnt",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        fs=memfs,
        path="/bkt/contract.pdf.mbnt",
    )

    assert outcome.status == "skipped_sidecar"
    assert mock_api._captured == []  # type: ignore[attr-defined]


def test_anchor_object_dry_run_no_api_no_sidecar(memfs, mock_api):
    _write(memfs, "/bkt/contract.pdf", b"xyz")

    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        fs=memfs,
        path="/bkt/contract.pdf",
        dry_run=True,
    )

    assert outcome.status == "anchored"
    assert outcome.sha256_hex == hashlib.sha256(b"xyz").hexdigest()
    assert outcome.bundle_id is None
    assert not memfs.exists("/bkt/contract.pdf.mbnt")
    assert not memfs.exists("/bkt/contract.pdf.proof.json")
    assert mock_api._captured == []  # type: ignore[attr-defined]


def test_anchor_object_api_failure_fails_open(memfs):
    transport = make_canned_transport(
        anchor_response={"error": {"code": "rate_limited",
                                   "message": "slow down"}},
        anchor_status=429,
    )
    api = SatsignalApi(
        api_base="https://app.satsignal.cloud",
        api_key="sk_test", transport=transport,
    )

    _write(memfs, "/bkt/contract.pdf", b"abc")

    # Retry wrapper will exhaust on a 429 (transient), so 3 attempts +
    # final raise — but we want the caller to receive failed status,
    # not bubble. anchor_object catches APIError and returns failed.
    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=api,
        fs=memfs,
        path="/bkt/contract.pdf",
    )

    assert outcome.status == "failed"
    assert "api_failed: 429" in (outcome.error or "")
    assert not memfs.exists("/bkt/contract.pdf.mbnt")


def test_anchor_object_no_bundle_writes_proof_only(memfs, mock_api):
    _write(memfs, "/bkt/contract.pdf", b"abc")

    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        fs=memfs,
        path="/bkt/contract.pdf",
        write_bundle=False,
    )

    assert outcome.status == "anchored"
    assert not memfs.exists("/bkt/contract.pdf.mbnt")
    assert memfs.exists("/bkt/contract.pdf.proof.json")


def test_anchor_object_handles_duplicate_response(memfs):
    transport = make_canned_transport(
        anchor_response={
            "bundle_id": "preexisting1234",
            "txid": "0" * 64,
            "mode": "standard",
            "category": "output",
            "dry_run": False,
            "matter_slug": "m",
            "receipt_url": "https://app/m/r/preexisting1234",
            "bundle_url": "https://app/bundle/preexisting1234.mbnt",
            "duplicate": True,
        },
    )
    api = SatsignalApi(
        api_base="https://app.satsignal.cloud",
        api_key="sk_test", transport=transport,
    )
    _write(memfs, "/bkt/contract.pdf", b"abc")

    outcome = anchor_object(
        "memory:///bkt/contract.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=api,
        fs=memfs,
        path="/bkt/contract.pdf",
    )

    assert outcome.status == "duplicate"
    proof = json.loads(memfs.cat("/bkt/contract.pdf.proof.json"))
    assert proof["duplicate"] is True


# ---- anchor_prefix --------------------------------------------------


def test_anchor_prefix_walks_recursively(memfs, mock_api):
    _write(memfs, "/bkt/a.pdf", b"a")
    _write(memfs, "/bkt/sub/b.pdf", b"b")
    _write(memfs, "/bkt/sub/c.txt", b"c")

    outcomes = list(anchor_prefix(
        "memory:///bkt/",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
    ))

    statuses = [(o.object_name, o.status) for o in outcomes]
    assert ("a.pdf", "anchored") in statuses
    assert ("b.pdf", "anchored") in statuses
    assert ("c.txt", "anchored") in statuses
    # 3 anchored + 6 sidecars (.mbnt + .proof.json each) created during
    # the walk, but the walk reads from the original snapshot — fsspec's
    # find() captures the listing at call time, so sidecars from earlier
    # iterations won't appear in the snapshot we iterate. 3 outcomes.
    assert len(outcomes) == 3


def test_anchor_prefix_include_filters(memfs, mock_api):
    _write(memfs, "/bkt/a.pdf", b"a")
    _write(memfs, "/bkt/b.txt", b"b")
    _write(memfs, "/bkt/c.pdf", b"c")

    outcomes = list(anchor_prefix(
        "memory:///bkt/",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        include=["*.pdf"],
    ))
    anchored = {o.object_name for o in outcomes if o.status == "anchored"}
    assert anchored == {"a.pdf", "c.pdf"}


def test_anchor_prefix_exclude_filters(memfs, mock_api):
    _write(memfs, "/bkt/a.pdf", b"a")
    _write(memfs, "/bkt/b.txt", b"b")
    _write(memfs, "/bkt/c.pdf", b"c")

    outcomes = list(anchor_prefix(
        "memory:///bkt/",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        exclude=["*.txt"],
    ))
    anchored = {o.object_name for o in outcomes if o.status == "anchored"}
    assert anchored == {"a.pdf", "c.pdf"}


def test_anchor_prefix_skips_sidecars_in_walk(memfs, mock_api):
    _write(memfs, "/bkt/a.pdf", b"a")
    _write(memfs, "/bkt/a.pdf.mbnt", b"old-mbnt")
    _write(memfs, "/bkt/a.pdf.proof.json", b"{}")

    outcomes = list(anchor_prefix(
        "memory:///bkt/",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
    ))

    by_name = {o.object_name: o.status for o in outcomes}
    # Sidecar paths surface as skipped_sidecar.
    assert by_name["a.pdf.mbnt"] == "skipped_sidecar"
    assert by_name["a.pdf.proof.json"] == "skipped_sidecar"
    # The original is already sidecar-d, so default dedup skips it.
    assert by_name["a.pdf"] == "skipped_existing"


def test_anchor_prefix_max_files(memfs, mock_api):
    for i in range(5):
        _write(memfs, f"/bkt/f{i}.pdf", b"x")

    outcomes = list(anchor_prefix(
        "memory:///bkt/",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
        max_files=2,
    ))

    anchored = [o for o in outcomes if o.status == "anchored"]
    assert len(anchored) == 2


def test_anchor_prefix_single_file_uri(memfs, mock_api):
    _write(memfs, "/bkt/only.pdf", b"only")

    outcomes = list(anchor_prefix(
        "memory:///bkt/only.pdf",
        api_key="sk_test",
        matter_slug="m",
        api=mock_api,
    ))

    assert len(outcomes) == 1
    assert outcomes[0].status == "anchored"
    assert outcomes[0].object_name == "only.pdf"
