"""Streaming sha256 over fsspec-opened objects.

Reads in 1 MiB chunks so multi-GB objects don't load into memory.
``hash_object(fs, path)`` returns ``(sha256_hex, size)``. The fsspec
``AbstractFileSystem`` is the dependency injection seam — tests use
``MemoryFileSystem``; production uses s3fs / gcsfs / adlfs / local.
"""
from __future__ import annotations

import hashlib
from typing import Tuple

CHUNK_SIZE = 1 << 20  # 1 MiB


def hash_object(fs, path: str, *, chunk_size: int = CHUNK_SIZE) -> Tuple[str, int]:
    """Stream-hash ``path`` on ``fs``. Returns (sha256_hex, byte_count).

    Raises FileNotFoundError if the path doesn't exist on the filesystem.
    """
    h = hashlib.sha256()
    size = 0
    with fs.open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size
