from __future__ import annotations

import hashlib
from pathlib import Path
from typing import BinaryIO

IDENTITY_PREFIX = "nyc-yellow:v1"


def file_sha256(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    with Path(path).open("rb") as source:
        return stream_sha256(source, chunk_size)


def stream_sha256(source: BinaryIO, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: source.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def event_id(source_sha256: str, zero_based_logical_row_number: int) -> str:
    if len(source_sha256) != 64 or any(c not in "0123456789abcdefABCDEF" for c in source_sha256):
        raise ValueError("source_sha256 must contain exactly 64 hexadecimal characters")
    if zero_based_logical_row_number < 0:
        raise ValueError("logical row number must be non-negative")
    canonical = f"{IDENTITY_PREFIX}:{source_sha256.lower()}:{zero_based_logical_row_number}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

