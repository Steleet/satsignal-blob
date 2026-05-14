# satsignal-blob

Anchor objects in **S3 / R2 / GCS / Azure Blob** to BSV via
[Satsignal](https://satsignal.cloud). Files stay in your bucket;
portable proof sidecars (`.mbnt` + `.proof.json`) land next to them.

> **Keep your files where they are. Satsignal gives them independent
> proof.**

```bash
pip install satsignal-blob[s3]    # or [gcs], [azure], [all]
```

## What it does

For each object under a bucket prefix, satsignal-blob streams the
bytes through sha256, posts the digest (not the file) to Satsignal's
`POST /api/v1/anchors`, and writes two sidecar objects next to the
original:

| sibling | what it is | who uses it |
| --- | --- | --- |
| `contract.pdf.mbnt` | full evidence bundle (zip) — txid, canonical doc, miner acceptance, BSV chain anchor | anyone who needs to verify the receipt offline against a public BSV explorer |
| `contract.pdf.proof.json` | one-page summary — sha256, txid, receipt URL, bundle URL, timestamp | humans, dashboards, grep / jq pipelines |

The original `contract.pdf` is untouched. The file bytes never leave
your bucket — Satsignal only sees the sha256.

## CLI quickstart

```bash
export SATSIGNAL_API_KEY=sk_...

# Anchor every PDF under a prefix, cap at 50 attempts.
satsignal-blob anchor s3://my-bucket/contracts/2026/ \
    --matter contracts-2026 \
    --include '*.pdf' \
    --max-files 50

# Dry-run first to preview hashes + plan without firing any anchors:
satsignal-blob anchor s3://my-bucket/contracts/2026/ \
    --matter contracts-2026 --include '*.pdf' --dry-run
```

A successful run prints one line per object plus a summary on stderr:

```
[OK]   s3://my-bucket/contracts/2026/a.pdf  sha=ab1234567890 bundle=87f9c2b4… txid=e7c4f1…
[OK]   s3://my-bucket/contracts/2026/b.pdf  sha=cd5678901234 bundle=2a91b3e5… txid=e7c4f1…
[SKIP] s3://my-bucket/contracts/2026/c.pdf  (.mbnt sidecar exists; --force to re-anchor)
satsignal-blob: anchored=2 duplicate=0 skipped_existing=1 skipped_sidecar=0 failed=0
```

Pass `--json` to emit one JSON object per line — pipe through `jq` /
`fluentd` / anything line-oriented.

## Supported backends

`satsignal-blob` resolves URIs through [fsspec](https://filesystem-spec.readthedocs.io).
Install the optional dep that matches your storage:

| storage | URI scheme | install extra | backend |
| --- | --- | --- | --- |
| AWS S3 / MinIO / Wasabi / B2 / Cloudflare R2 | `s3://` | `pip install satsignal-blob[s3]` | `s3fs` |
| Google Cloud Storage | `gs://` | `pip install satsignal-blob[gcs]` | `gcsfs` |
| Azure Blob Storage | `az://` | `pip install satsignal-blob[azure]` | `adlfs` |
| Local filesystem | `/path/` or `file:///` | (none) | built-in |

Cloudflare R2 is S3-compatible — use `s3://` and configure the
`AWS_ENDPOINT_URL_S3` env var to point at your R2 endpoint.

## Library API

```python
from satsignal_blob import anchor_prefix

for outcome in anchor_prefix(
    "s3://my-bucket/contracts/2026/",
    api_key="sk_...",
    matter_slug="contracts-2026",
    include=["*.pdf"],
):
    if outcome.status == "anchored":
        print(outcome.receipt_url)
```

Or a single object:

```python
from satsignal_blob import anchor_object

result = anchor_object(
    "s3://my-bucket/contracts/2026/contract.pdf",
    api_key="sk_...",
    matter_slug="contracts-2026",
)
print(result.txid, result.receipt_url)
```

## Event-driven (S3 → Lambda)

For real-time anchoring on every PUT, wire an S3 `ObjectCreated:*`
event to a Lambda that calls `anchor_object`. Copy
[`examples/lambda_handler.py`](examples/lambda_handler.py) into a
Python 3.11 Lambda, set three env vars, and you're done:

```python
import os, urllib.parse
from satsignal_blob import anchor_object

API_KEY = os.environ["SATSIGNAL_API_KEY"]
WORKSPACE_SLUG = os.environ["SATSIGNAL_WORKSPACE_SLUG"]
MATTER_SLUG = os.environ["SATSIGNAL_MATTER_SLUG"]


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        if key.endswith((".mbnt", ".proof.json")):
            continue  # don't anchor sidecars
        outcome = anchor_object(
            f"s3://{bucket}/{key}",
            api_key=API_KEY,
            matter_slug=MATTER_SLUG,
        )
        results.append({"key": key, "status": outcome.status,
                        "bundle_id": outcome.bundle_id})
    return {"results": results}
```

Same pattern works for Cloud Functions (GCS finalize) and Azure
Functions (blob trigger) — just swap the scheme and the event shape.

## What this proves (and what it doesn't)

A Satsignal anchor over `sha256(file)` proves:

- The **operator of this Satsignal workspace knew this sha256 by the
  block time of the on-chain anchor.** Anyone with the file can later
  recompute the sha and verify against the chain entry — without
  trusting Satsignal or your storage provider.

It does **not** prove:

- That the file existed *before* the anchor. The chain only records
  when the sha entered the ledger.
- The identity of whoever uploaded it. The receipt is "this sha was
  anchored by this workspace at this time" — not "this person did
  it."

For workflows that need before-the-anchor existence (e.g. press-
embargo proofs), pair `satsignal-blob` with a commit-reveal flow
via [`satsignal-cli`](https://github.com/Steleet/satsignal-cli) or
the `/commit_reveal.py` helper.

## Threat model

- **sha-only**. File bytes never leave your process. The Satsignal
  API only sees the digest + size + optional label + filename.
- **Untrusted attribute fields.** `label` and `filename` are clipped
  server-side at 256 chars and validated for control substrings.
  Still — don't put secrets in either.
- **Fail-open per object.** One file's API failure does not abort the
  walk. The exit code is `1` if any file failed; the per-file error
  is in the JSON output.
- **No bytes on chain.** Satsignal never anchors file contents — only
  the digest. This is by design and is not a config option.

## Related packages

- [`satsignal-cli`](https://github.com/Steleet/satsignal-cli) —
  general-purpose anchor + verify CLI; can verify any `.mbnt` file
  written by satsignal-blob.
- [`satsignal-mcp`](https://github.com/Steleet/satsignal-mcp) — MCP
  server exposing the same 5 primitives to MCP-aware agents.
- [`satsignal-otel`](https://github.com/Steleet/satsignal-otel) —
  anchor OpenTelemetry spans (failed evals, release gates).
- [`satsignal-action`](https://github.com/Steleet/satsignal-action) —
  GitHub Action: anchor build artifacts on every release.

## License

MIT
