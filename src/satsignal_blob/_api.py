"""Stdlib HTTP client for ``POST /api/v1/anchors`` (standard mode only).

No third-party HTTP dep. ``transport=`` lets tests inject canned
responses so CI does not burn chain fees.

v0.1 ships **standard mode** only: one chain spend per object.
Manifest mode (one chain spend, N objects under a shared Merkle root)
is the obvious v0.2 follow-up for users who walk thousand-file
prefixes. v0.1 keeps the one-anchor-one-sidecar mental model.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


DEFAULT_API_BASE = "https://app.satsignal.cloud"

MAX_LABEL_LEN = 256


class APIError(RuntimeError):
    """Non-2xx response from /api/v1/anchors."""

    def __init__(self, status: int, code: str, message: str,
                 *, body: Optional[dict] = None):
        super().__init__(f"satsignal API {status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.body = body or {}


@dataclass
class AnchorResult:
    bundle_id: str
    txid: Optional[str]
    mode: str
    matter_slug: str
    receipt_url: str
    bundle_url: Optional[str]
    duplicate: bool
    session_id: Optional[str] = None
    raw: dict = field(default_factory=dict)


TransportFn = Callable[[str, str, dict, bytes, float], "tuple[int, bytes]"]


def _urllib_transport(
    method: str, url: str, headers: dict, body: bytes, timeout: float,
) -> "tuple[int, bytes]":
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, (e.read() or b"")


def _parse_api_error(status: int, body_bytes: bytes) -> APIError:
    text = body_bytes.decode("utf-8", errors="replace")
    try:
        body = json.loads(text)
    except (ValueError, json.JSONDecodeError):
        return APIError(status, "non_json_response",
                        text[:200] or f"HTTP {status}")
    err = body.get("error") if isinstance(body, dict) else None
    if isinstance(err, dict):
        return APIError(
            status,
            str(err.get("code") or "unknown_error"),
            str(err.get("message") or ""),
            body=body,
        )
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return APIError(status, body["error"], "", body=body)
    return APIError(status, "unknown_error", str(body)[:200], body=body)


def _safe_label(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) > MAX_LABEL_LEN:
        s = s[:MAX_LABEL_LEN]
    return s


class SatsignalApi:
    """Synchronous HTTP client. One instance per CLI run / Lambda call."""

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        timeout: float = 30.0,
        transport: Optional[TransportFn] = None,
        user_agent: str = "satsignal-blob/0.1.0",
    ):
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._transport: TransportFn = transport or _urllib_transport
        self._user_agent = user_agent

    def anchor_standard(
        self,
        *,
        matter_slug: str,
        sha256_hex: str,
        file_size: Optional[int] = None,
        label: Optional[str] = None,
        filename: Optional[str] = None,
        session_id: Optional[str] = None,
        force_new: bool = False,
    ) -> AnchorResult:
        body: dict[str, Any] = {
            "matter_slug": matter_slug,
            "sha256_hex": sha256_hex.lower().strip(),
        }
        if file_size is not None:
            body["file_size"] = int(file_size)
        label = _safe_label(label)
        if label:
            body["label"] = label
        filename = _safe_label(filename)
        if filename:
            body["filename"] = filename
        if session_id:
            body["session_id"] = session_id
        if force_new:
            body["force_new"] = True

        url = f"{self.api_base}/api/v1/anchors"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._user_agent,
        }
        raw_body = json.dumps(body, separators=(",", ":")).encode("utf-8")
        status, resp_bytes = self._transport(
            "POST", url, headers, raw_body, self.timeout,
        )
        if status >= 400:
            raise _parse_api_error(status, resp_bytes)
        try:
            data = json.loads(resp_bytes.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise APIError(status, "bad_response",
                           f"non-JSON 2xx body: {e}")
        return AnchorResult(
            bundle_id=str(data.get("bundle_id") or ""),
            txid=data.get("txid"),
            mode=str(data.get("mode") or "standard"),
            matter_slug=str(data.get("matter_slug") or matter_slug),
            receipt_url=str(data.get("receipt_url") or ""),
            bundle_url=data.get("bundle_url"),
            duplicate=bool(data.get("duplicate", False)),
            session_id=data.get("session_id") or session_id,
            raw=data,
        )

    def fetch_bundle(self, bundle_url: str) -> bytes:
        """GET the .mbnt bytes from ``bundle_url`` with bearer auth."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": self._user_agent,
        }
        status, resp_bytes = self._transport(
            "GET", bundle_url, headers, b"", self.timeout,
        )
        if status >= 400:
            raise _parse_api_error(status, resp_bytes)
        return resp_bytes


_TRANSIENT_STATUSES = frozenset({0, 408, 429, 500, 502, 503, 504})


def _is_transient(exc: APIError) -> bool:
    return exc.status in _TRANSIENT_STATUSES


def call_with_retry(
    fn: Callable[[], Any],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except APIError as e:
            last_exc = e
            if not _is_transient(e) or i == attempts - 1:
                raise
            sleep(base_delay * (4 ** i))
    raise last_exc  # type: ignore[misc]
