"""End-to-end CLI tests: monkeypatch ``anchor_prefix`` to feed canned
outcomes through the formatter and exit-code path."""
from __future__ import annotations

import json

import pytest

from satsignal_blob import BlobAnchorOutcome
from satsignal_blob import cli as cli_mod


def _run(capsys, argv, *, outcomes):
    def fake_prefix(*args, **kwargs):
        return iter(outcomes)
    # Monkeypatch the imported reference inside cli, not the module.
    orig = cli_mod.anchor_prefix
    cli_mod.anchor_prefix = fake_prefix
    try:
        rc = cli_mod.main(argv)
    finally:
        cli_mod.anchor_prefix = orig
    captured = capsys.readouterr()
    return rc, captured


def test_cli_text_summary_exit_0_when_no_failures(capsys, monkeypatch):
    monkeypatch.setenv("SATSIGNAL_API_KEY", "sk_test")
    outcomes = [
        BlobAnchorOutcome(
            object_uri="memory:///bkt/a.pdf", object_name="a.pdf",
            status="anchored", sha256_hex="ab" * 32, size=10,
            bundle_id="b1", txid="cd" * 32,
            receipt_url="r1",
        ),
        BlobAnchorOutcome(
            object_uri="memory:///bkt/b.pdf", object_name="b.pdf",
            status="skipped_existing",
        ),
    ]
    rc, cap = _run(
        capsys,
        ["anchor", "memory:///bkt/", "--matter", "m"],
        outcomes=outcomes,
    )
    assert rc == 0
    assert "[OK]" in cap.out
    assert "[SKIP]" in cap.out
    assert "anchored=1" in cap.err
    assert "skipped_existing=1" in cap.err


def test_cli_json_mode_emits_jsonl(capsys, monkeypatch):
    monkeypatch.setenv("SATSIGNAL_API_KEY", "sk_test")
    outcomes = [
        BlobAnchorOutcome(
            object_uri="memory:///bkt/a.pdf", object_name="a.pdf",
            status="anchored", sha256_hex="ab" * 32, size=10,
            bundle_id="b1", txid="cd" * 32,
            receipt_url="r1",
        ),
    ]
    rc, cap = _run(
        capsys,
        ["anchor", "memory:///bkt/", "--matter", "m", "--json"],
        outcomes=outcomes,
    )
    assert rc == 0
    line = cap.out.strip().splitlines()[0]
    parsed = json.loads(line)
    assert parsed["status"] == "anchored"
    assert parsed["bundle_id"] == "b1"


def test_cli_exit_1_when_any_failure(capsys, monkeypatch):
    monkeypatch.setenv("SATSIGNAL_API_KEY", "sk_test")
    outcomes = [
        BlobAnchorOutcome(
            object_uri="memory:///bkt/a.pdf", object_name="a.pdf",
            status="failed", error="api_failed: 500 oops",
        ),
    ]
    rc, _ = _run(
        capsys,
        ["anchor", "memory:///bkt/", "--matter", "m"],
        outcomes=outcomes,
    )
    assert rc == 1


def test_cli_missing_api_key_exits_2(capsys, monkeypatch):
    monkeypatch.delenv("SATSIGNAL_API_KEY", raising=False)
    # Force stdin to look like a TTY so the CLI's stdin fallback doesn't fire.
    import sys as _sys
    monkeypatch.setattr(_sys.stdin, "isatty", lambda: True)
    with pytest.raises(SystemExit) as exc:
        cli_mod.main(["anchor", "memory:///bkt/", "--matter", "m"])
    assert exc.value.code == 2
