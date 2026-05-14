"""AWS Lambda handler for S3 ObjectCreated:* events.

Wire this Lambda to your bucket via an S3 event notification with
filter ``suffix != .mbnt`` and ``suffix != .proof.json`` (defense in
depth — the handler also skips sidecars itself).

Required env vars:
- ``SATSIGNAL_API_KEY``       — bearer token (use AWS Secrets Manager)
- ``SATSIGNAL_MATTER_SLUG``   — pre-created Satsignal matter
- ``SATSIGNAL_API_BASE``      — optional; defaults to https://app.satsignal.cloud

IAM: the Lambda's execution role needs ``s3:GetObject``,
``s3:PutObject`` (for the sidecars), and outbound HTTPS.

Layer: package ``satsignal-blob[s3]`` into a Lambda layer or include
it in your deployment zip. Python 3.11 runtime recommended.
"""
from __future__ import annotations

import os
import urllib.parse

from satsignal_blob import anchor_object
from satsignal_blob._api import DEFAULT_API_BASE


API_KEY = os.environ["SATSIGNAL_API_KEY"]
MATTER_SLUG = os.environ["SATSIGNAL_MATTER_SLUG"]
API_BASE = os.environ.get("SATSIGNAL_API_BASE", DEFAULT_API_BASE)


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        if key.endswith((".mbnt", ".proof.json")):
            continue
        outcome = anchor_object(
            f"s3://{bucket}/{key}",
            api_key=API_KEY,
            matter_slug=MATTER_SLUG,
            api_base=API_BASE,
        )
        results.append({
            "key": key,
            "status": outcome.status,
            "bundle_id": outcome.bundle_id,
            "txid": outcome.txid,
            "error": outcome.error,
        })
    return {"results": results}
