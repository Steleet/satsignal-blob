"""Shared fixtures for satsignal-blob tests.

The ``memfs`` fixture returns a clean fsspec MemoryFileSystem for each
test. The ``mock_api`` fixture returns a SatsignalApi whose transport
is replaced with a canned-response function so tests never call out.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Optional

import fsspec
import pytest

from satsignal_blob._api import SatsignalApi


@pytest.fixture
def memfs():
    fs = fsspec.filesystem("memory")
    # MemoryFileSystem is a class-level cache — wipe between tests.
    fs.store.clear()
    fs.pseudo_dirs.clear()
    yield fs
    fs.store.clear()
    fs.pseudo_dirs.clear()


def make_canned_transport(
    *,
    anchor_response: Optional[dict] = None,
    bundle_bytes: bytes = b"FAKE-MBNT-BYTES",
    anchor_status: int = 200,
    bundle_status: int = 200,
    capture: Optional[list] = None,
) -> Callable[..., Any]:
    """Build a transport function that mimics the Satsignal API."""

    if anchor_response is None:
        anchor_response = {
            "bundle_id": "deadbeefcafe1234",
            "txid": "0" * 64,
            "mode": "standard",
            "category": "output",
            "dry_run": False,
            "matter_slug": "test-matter",
            "receipt_url":
                "https://app.satsignal.cloud/w/o_TEST/m/test-matter/r/deadbeefcafe1234",
            "bundle_url":
                "https://app.satsignal.cloud/bundle/deadbeefcafe1234.mbnt",
        }

    def transport(method, url, headers, body, timeout):
        if capture is not None:
            capture.append({
                "method": method, "url": url, "headers": dict(headers),
                "body": json.loads(body.decode("utf-8")) if body else None,
            })
        if method == "POST" and url.endswith("/api/v1/anchors"):
            return anchor_status, json.dumps(anchor_response).encode("utf-8")
        if method == "GET" and "/bundle/" in url:
            return bundle_status, bundle_bytes
        return 404, b'{"error":{"code":"not_found","message":"unexpected URL"}}'

    return transport


@pytest.fixture
def mock_api():
    capture: list = []
    transport = make_canned_transport(capture=capture)
    api = SatsignalApi(
        api_base="https://app.satsignal.cloud",
        api_key="sk_test",
        transport=transport,
    )
    api._captured = capture  # type: ignore[attr-defined]
    return api
