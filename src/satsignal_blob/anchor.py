"""Walk an object store, anchor each object, write sidecars.

Public surface:

- :class:`BlobAnchorOutcome` — per-object result (anchored / skipped /
  failed).
- :func:`anchor_object` — one object: hash, anchor, write
  ``.mbnt`` + ``.proof.json`` sidecars next to the original.
- :func:`anchor_prefix` — generator over a (recursive) prefix.
  Yields one ``BlobAnchorOutcome`` per object the caller can stream
  to a CLI or pipeline.

Sidecar suffixes ``.mbnt`` and ``.proof.json`` are skipped during
walks so the second run doesn't anchor the first run's output.

Default behavior is **dedup-on-existing-mbnt**: if a ``.mbnt`` sidecar
is already present next to the object, we skip and report
``status="skipped_existing"``. Pass ``force=True`` to re-anchor.
"""
from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, List, Optional

import fsspec  # type: ignore[import-untyped]

from ._api import (
    APIError,
    AnchorResult,
    DEFAULT_API_BASE,
    SatsignalApi,
    call_with_retry,
)
from .hashing import hash_object
from .proof import build_proof_record, serialize_proof

log = logging.getLogger("satsignal_blob")

MBNT_SUFFIX = ".mbnt"
PROOF_SUFFIX = ".proof.json"
SIDECAR_SUFFIXES = (MBNT_SUFFIX, PROOF_SUFFIX)


@dataclass
class BlobAnchorOutcome:
    """Per-object result. ``status`` is one of:

    - ``"anchored"`` — new anchor; sidecars written.
    - ``"duplicate"`` — server returned an existing receipt for this
      sha (default dedup); sidecars rewritten to match.
    - ``"skipped_existing"`` — local ``.mbnt`` sidecar already
      present and ``force=False``.
    - ``"skipped_sidecar"`` — path itself ends in ``.mbnt`` /
      ``.proof.json`` — never re-anchor sidecars.
    - ``"failed"`` — hashing or API error; ``error`` carries the
      message. Walks do not abort on a per-object failure.
    """

    object_uri: str
    object_name: str
    status: str
    sha256_hex: Optional[str] = None
    size: Optional[int] = None
    bundle_id: Optional[str] = None
    txid: Optional[str] = None
    receipt_url: Optional[str] = None
    proof: Optional[dict] = None
    error: Optional[str] = None
    raw: dict = field(default_factory=dict)


def _is_sidecar_path(path: str) -> bool:
    return path.endswith(SIDECAR_SUFFIXES)


def _fs_for(uri: str, storage_options: Optional[dict] = None):
    """Resolve ``uri`` to ``(fs, path)`` via fsspec.

    Local paths without a scheme map to ``LocalFileSystem``. Other
    schemes (``s3://``, ``gs://``, ``az://``, ``file://``) require the
    matching optional dep — fsspec raises a clear ImportError if not
    installed.
    """
    return fsspec.url_to_fs(uri, **(storage_options or {}))


def _basename(path: str) -> str:
    # fsspec paths always use forward slash; not os.path.
    return path.rsplit("/", 1)[-1] if "/" in path else path


def _uri_join(base_uri: str, relative_path: str) -> str:
    """Rebuild an fsspec URI keeping the scheme + netloc but
    swapping the path. ``base_uri="s3://bucket/old/key"``,
    ``relative_path="new/key"`` -> ``"s3://bucket/new/key"``.
    """
    if "://" in base_uri:
        scheme, rest = base_uri.split("://", 1)
        netloc = rest.split("/", 1)[0]
        return f"{scheme}://{netloc}/{relative_path.lstrip('/')}"
    # Local path: just return the path itself.
    return relative_path


def _sibling_path(path: str, suffix: str) -> str:
    return path + suffix


def anchor_object(
    object_uri: str,
    *,
    api_key: str,
    matter_slug: str,
    api_base: str = DEFAULT_API_BASE,
    label: Optional[str] = None,
    force: bool = False,
    write_bundle: bool = True,
    dry_run: bool = False,
    storage_options: Optional[dict] = None,
    api: Optional[SatsignalApi] = None,
    fs: Any = None,
    path: Optional[str] = None,
    session_id: Optional[str] = None,
) -> BlobAnchorOutcome:
    """Anchor a single object.

    Pass ``object_uri`` to resolve via fsspec, OR pass an explicit
    ``fs`` + ``path`` pair (used internally by ``anchor_prefix`` to
    reuse the same filesystem handle across the walk).

    ``dry_run=True`` computes the sha + size but does NOT call the
    Satsignal API and does NOT write sidecars. Returns an
    ``anchored``-status outcome with ``bundle_id=None`` so callers
    can preview what would be anchored.
    """
    if fs is None or path is None:
        fs, path = _fs_for(object_uri, storage_options)

    name = _basename(path)
    if _is_sidecar_path(path):
        return BlobAnchorOutcome(
            object_uri=object_uri, object_name=name,
            status="skipped_sidecar",
        )

    mbnt_path = _sibling_path(path, MBNT_SUFFIX)
    proof_path = _sibling_path(path, PROOF_SUFFIX)
    mbnt_uri = _sibling_path(object_uri, MBNT_SUFFIX)
    proof_uri = _sibling_path(object_uri, PROOF_SUFFIX)

    if not force and fs.exists(mbnt_path):
        return BlobAnchorOutcome(
            object_uri=object_uri, object_name=name,
            status="skipped_existing",
        )

    try:
        sha256_hex, size = hash_object(fs, path)
    except Exception as e:  # noqa: BLE001 — surface any I/O error per-object
        return BlobAnchorOutcome(
            object_uri=object_uri, object_name=name,
            status="failed", error=f"hash_failed: {e}",
        )

    if dry_run:
        return BlobAnchorOutcome(
            object_uri=object_uri, object_name=name,
            status="anchored",
            sha256_hex=sha256_hex, size=size,
        )

    client = api or SatsignalApi(api_base=api_base, api_key=api_key)
    try:
        result: AnchorResult = call_with_retry(
            lambda: client.anchor_standard(
                matter_slug=matter_slug,
                sha256_hex=sha256_hex,
                file_size=size,
                label=label,
                filename=name,
                session_id=session_id,
                force_new=force,
            )
        )
    except APIError as e:
        return BlobAnchorOutcome(
            object_uri=object_uri, object_name=name,
            status="failed",
            sha256_hex=sha256_hex, size=size,
            error=f"api_failed: {e.status} {e.code} {e.message}",
        )

    proof_record = build_proof_record(
        object_uri=object_uri,
        object_name=name,
        sha256_hex=sha256_hex,
        file_size=size,
        bundle_id=result.bundle_id,
        txid=result.txid,
        receipt_url=result.receipt_url,
        bundle_url=result.bundle_url,
        matter_slug=result.matter_slug,
        sidecar_mbnt_name=name + MBNT_SUFFIX,
        duplicate=result.duplicate,
        dry_run=False,
    )

    bundle_bytes: Optional[bytes] = None
    if write_bundle and result.bundle_url:
        try:
            bundle_bytes = call_with_retry(
                lambda: client.fetch_bundle(result.bundle_url)  # type: ignore[arg-type]
            )
        except APIError as e:
            log.warning(
                "satsignal-blob: bundle fetch failed for %s "
                "(receipt is still good, .mbnt sidecar skipped): %s",
                object_uri, e,
            )

    try:
        if bundle_bytes is not None:
            with fs.open(mbnt_path, "wb") as f:
                f.write(bundle_bytes)
        with fs.open(proof_path, "wb") as f:
            f.write(serialize_proof(proof_record))
    except Exception as e:  # noqa: BLE001
        return BlobAnchorOutcome(
            object_uri=object_uri, object_name=name,
            status="failed",
            sha256_hex=sha256_hex, size=size,
            bundle_id=result.bundle_id, txid=result.txid,
            receipt_url=result.receipt_url, proof=proof_record,
            error=f"sidecar_write_failed: {e}",
            raw=result.raw,
        )

    return BlobAnchorOutcome(
        object_uri=object_uri, object_name=name,
        status="duplicate" if result.duplicate else "anchored",
        sha256_hex=sha256_hex, size=size,
        bundle_id=result.bundle_id, txid=result.txid,
        receipt_url=result.receipt_url, proof=proof_record,
        raw=result.raw,
    )


def _matches(name: str, patterns: Optional[Iterable[str]]) -> bool:
    if not patterns:
        return False
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)


def _walk_prefix(
    fs, root_path: str, *, recursive: bool,
) -> List[str]:
    if fs.isfile(root_path):
        return [root_path]
    if recursive:
        # find() returns files only with withdirs=False (the default).
        return sorted(fs.find(root_path))
    # Non-recursive: only direct children.
    entries = fs.ls(root_path, detail=True)
    return sorted(e["name"] for e in entries if e.get("type") == "file")


def anchor_prefix(
    prefix_uri: str,
    *,
    api_key: str,
    matter_slug: str,
    api_base: str = DEFAULT_API_BASE,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    max_files: Optional[int] = None,
    recursive: bool = True,
    force: bool = False,
    write_bundle: bool = True,
    dry_run: bool = False,
    storage_options: Optional[dict] = None,
    api: Optional[SatsignalApi] = None,
    label: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Iterator[BlobAnchorOutcome]:
    """Yield one :class:`BlobAnchorOutcome` per object under ``prefix_uri``.

    Filters apply in order: sidecar-suffix skip (hardcoded), then
    ``include`` (fnmatch on basename, any match passes), then
    ``exclude`` (fnmatch on basename, any match drops), then
    ``max_files`` (cap on anchored attempts — sidecar-skipped paths
    do NOT count against the cap).
    """
    fs, root_path = _fs_for(prefix_uri, storage_options)
    paths = _walk_prefix(fs, root_path, recursive=recursive)
    client = api or SatsignalApi(api_base=api_base, api_key=api_key)

    anchored_attempts = 0
    for path in paths:
        name = _basename(path)
        # Always emit a skip outcome for sidecars so the CLI can
        # report what it ignored.
        if _is_sidecar_path(path):
            yield BlobAnchorOutcome(
                object_uri=_uri_join(prefix_uri, path),
                object_name=name,
                status="skipped_sidecar",
            )
            continue
        if include and not _matches(name, include):
            continue
        if exclude and _matches(name, exclude):
            continue
        if max_files is not None and anchored_attempts >= max_files:
            return
        anchored_attempts += 1
        yield anchor_object(
            _uri_join(prefix_uri, path),
            api_key=api_key,
            matter_slug=matter_slug,
            api_base=api_base,
            label=label,
            force=force,
            write_bundle=write_bundle,
            dry_run=dry_run,
            api=client,
            fs=fs,
            path=path,
            session_id=session_id,
        )
