"""satsignal-blob — anchor objects in S3 / R2 / GCS / Azure Blob to BSV."""

from ._api import (
    APIError,
    AnchorResult,
    DEFAULT_API_BASE,
    SatsignalApi,
)
from .anchor import (
    BlobAnchorOutcome,
    MBNT_SUFFIX,
    PROOF_SUFFIX,
    SIDECAR_SUFFIXES,
    anchor_object,
    anchor_prefix,
)
from .proof import SCHEMA as PROOF_SCHEMA

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "anchor_object",
    "anchor_prefix",
    "BlobAnchorOutcome",
    "SatsignalApi",
    "AnchorResult",
    "APIError",
    "DEFAULT_API_BASE",
    "MBNT_SUFFIX",
    "PROOF_SUFFIX",
    "SIDECAR_SUFFIXES",
    "PROOF_SCHEMA",
]
