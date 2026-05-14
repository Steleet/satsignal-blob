"""``satsignal-blob`` CLI.

::

    satsignal-blob anchor s3://bucket/prefix/ \\
        --matter blob-anchors \\
        --include '*.pdf' \\
        --max-files 50

The API key is read from ``--api-key``, env ``SATSIGNAL_API_KEY``, or
stdin. Per-object status streams to stdout (one line per object); a
final summary line goes to stderr so output is pipe-friendly.

Exit code: 0 if every attempted object succeeded; 1 if any failed.
A run with zero matching objects exits 0 (empty prefix is fine).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List, Optional

from . import __version__
from ._api import DEFAULT_API_BASE
from .anchor import BlobAnchorOutcome, anchor_prefix


def _read_api_key(arg: Optional[str], *, dry_run: bool = False) -> str:
    if arg:
        return arg
    env = os.environ.get("SATSIGNAL_API_KEY")
    if env:
        return env
    if not sys.stdin.isatty():
        data = sys.stdin.read().strip()
        if data:
            return data
    # --dry-run does no network call (anchor.py:167 short-circuits
    # before any HTTP). Don't force an env var that has no consumer in
    # this code path. Use a deterministic placeholder so downstream
    # code that interpolates the value (e.g. log lines) gets a clear
    # marker rather than empty string. Caught by the 2026-05-14
    # satsignal stress test agent 4 — dry-run should be offline-clean.
    if dry_run:
        return "dry-run-no-api-key"
    print(
        "satsignal-blob: missing API key. "
        "Pass --api-key, set SATSIGNAL_API_KEY, or pipe via stdin.",
        file=sys.stderr,
    )
    sys.exit(2)


def _format_outcome(o: BlobAnchorOutcome, *, as_json: bool) -> str:
    if as_json:
        payload = {
            "status": o.status,
            "object": o.object_uri,
            "sha256": o.sha256_hex,
            "size": o.size,
            "bundle_id": o.bundle_id,
            "txid": o.txid,
            "receipt_url": o.receipt_url,
            "error": o.error,
        }
        return json.dumps(payload, separators=(",", ":"))
    if o.status == "anchored":
        return (
            f"[OK]   {o.object_uri}  "
            f"sha={o.sha256_hex[:12] if o.sha256_hex else '-'} "
            f"bundle={o.bundle_id} txid={(o.txid or '')[:12]}"
        )
    if o.status == "duplicate":
        return (
            f"[DUP]  {o.object_uri}  "
            f"sha={o.sha256_hex[:12] if o.sha256_hex else '-'} "
            f"bundle={o.bundle_id} (server returned existing receipt)"
        )
    if o.status == "skipped_existing":
        return f"[SKIP] {o.object_uri}  (.mbnt sidecar exists; --force to re-anchor)"
    if o.status == "skipped_sidecar":
        return f"[SKIP] {o.object_uri}  (sidecar suffix)"
    if o.status == "failed":
        return f"[FAIL] {o.object_uri}  {o.error}"
    return f"[?]    {o.object_uri}  status={o.status}"


def _cmd_anchor(args: argparse.Namespace) -> int:
    api_key = _read_api_key(args.api_key, dry_run=args.dry_run)
    counts = {"anchored": 0, "duplicate": 0,
              "skipped_existing": 0, "skipped_sidecar": 0,
              "failed": 0}

    include = args.include or None
    exclude = args.exclude or None

    for outcome in anchor_prefix(
        args.uri,
        api_key=api_key,
        matter_slug=args.matter,
        api_base=args.api_base,
        include=include,
        exclude=exclude,
        max_files=args.max_files,
        recursive=not args.no_recursive,
        force=args.force,
        write_bundle=not args.no_bundle,
        dry_run=args.dry_run,
        label=args.label,
        session_id=args.session_id,
    ):
        print(_format_outcome(outcome, as_json=args.json), flush=True)
        counts[outcome.status] = counts.get(outcome.status, 0) + 1

    summary = (
        f"satsignal-blob: anchored={counts['anchored']} "
        f"duplicate={counts['duplicate']} "
        f"skipped_existing={counts['skipped_existing']} "
        f"skipped_sidecar={counts['skipped_sidecar']} "
        f"failed={counts['failed']}"
    )
    print(summary, file=sys.stderr)
    return 1 if counts["failed"] else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="satsignal-blob",
        description=(
            "Anchor objects in S3 / R2 / GCS / Azure Blob to BSV. "
            "Files stay in your bucket; sidecar .mbnt + .proof.json "
            "land next to them."
        ),
    )
    p.add_argument("--version", action="version",
                   version=f"satsignal-blob {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    pa = sub.add_parser("anchor", help="anchor a prefix or single object")
    pa.add_argument(
        "uri",
        help="fsspec URI: s3://bucket/prefix/, gs://..., az://..., "
             "or a local path. Trailing slash means 'walk the prefix'; "
             "no slash means 'single object'.",
    )
    pa.add_argument("--matter", required=True,
                    help="Satsignal matter slug. Created via "
                         "POST /api/v1/matters first if it doesn't exist.")
    pa.add_argument("--api-key", default=None,
                    help="Satsignal API key (or set SATSIGNAL_API_KEY).")
    pa.add_argument("--api-base", default=DEFAULT_API_BASE,
                    help=f"API base URL (default: {DEFAULT_API_BASE}).")
    pa.add_argument("--include", action="append",
                    help="fnmatch pattern on basename (any-match passes). "
                         "Repeatable.")
    pa.add_argument("--exclude", action="append",
                    help="fnmatch pattern on basename (any-match drops). "
                         "Repeatable.")
    pa.add_argument("--max-files", type=int, default=None,
                    help="Cap the number of anchored attempts. "
                         "Sidecar-skipped paths don't count.")
    pa.add_argument("--no-recursive", action="store_true",
                    help="Only anchor direct children of the prefix.")
    pa.add_argument("--force", action="store_true",
                    help="Re-anchor even if a .mbnt sidecar exists. "
                         "Sets force_new=true server-side.")
    pa.add_argument("--no-bundle", action="store_true",
                    help="Skip fetching .mbnt; only write .proof.json. "
                         "Faster but loses offline-verify property.")
    pa.add_argument("--dry-run", action="store_true",
                    help="Hash + plan only; do not call the API or "
                         "write sidecars.")
    pa.add_argument("--label", default=None,
                    help="Optional label attached to every anchor (max 256).")
    pa.add_argument("--session-id", default=None,
                    help="Off-chain correlation key; appears on receipt "
                         "and groups anchors in /w/<slug>/anchors.")
    pa.add_argument("--json", action="store_true",
                    help="Emit one JSON object per line instead of text.")
    pa.set_defaults(func=_cmd_anchor)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
