# goes_cloud/storage.py
"""
Minimal S3 + local cache utilities for GOES ABI L2 products.

- Anonymous read of public NOAA GOES buckets via s3fs
- S3 key builder for ABI-L2 products (ACMF, ACHA/ACHA2KM)
- Local cache that mirrors S3 layout (atomic writes)

References:
- GOES on AWS bucket + layout: registry.opendata.aws/noaa-goes  [see cloudfrac notes]  # noqa: E501
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import os
import tempfile

import fsspec  # requires s3fs installed

# ---------- Config ----------

_DEFAULT_BUCKET_BY_SAT = {
    "goes16": "noaa-goes16",
    "goes17": "noaa-goes17",  # legacy (now GOES-18 operational)
    "goes18": "noaa-goes18",
    "goes19": "noaa-goes19",  # GOES-East since 2024/2025
}

# Allow simple product aliases for height:
# - "ACHAF" "ACHT" is temperature (not height).
_PRODUCT_ALIAS = {
    "ACHA2KM": "ACHAF",   # prefer ACHAF since ACHAF2KM isn't present
    "ACHTF": "ACHAF",     # common confusion: we want height, not temperature
}

def _normalize_s3_url(url: str) -> str:
    """Return a canonical 's3://bucket/key' for any s3-like string."""
    if url.startswith("s3://"):
        return url
    # e.g., "noaa-goes19/ABI-L2-ACMF/..." → "s3://noaa-goes19/ABI-L2-ACMF/..."
    return "s3://" + url.lstrip("/")


def _split_s3_url(url: str) -> tuple[str, str]:
    """
    Split 's3://bucket/key...' into (bucket, key). Accepts also 'bucket/key...'.
    """
    u = url
    if u.startswith("s3://"):
        u = u[len("s3://"):]
    # now u = "bucket/key..."
    parts = u.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Not a valid s3 URL (missing key part): {url}")
    return parts[0], parts[1]

# ---------- Public API ----------

def cache_root() -> Path:
    """
    Return the on-disk cache root.

    Environment variable GOES_CACHE_DIR overrides the default (~/.cache/goes_cloud).
    """
    root = os.environ.get(
        "GOES_CACHE_DIR", os.path.join(os.path.expanduser("~"), ".cache", "goes_cloud")
    )
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def s3_key(product: str, t, *, satellite: str = "goes16", sector: str = "F") -> str:
    """
    Build the ABI-L2 S3 key (no bucket prefix) for a given product/time.

    Layout: ABI-L2-<PRODUCT>/<YYYY>/<DDD>/<HH>/...nc
    - PRODUCT examples:
        * ACMF  : Cloud/Clear-Sky Mask (binary)  [~10 min cadence FD]
        * ACHA  : Cloud Top Height
        * ACHA2KM: Cloud Top Height (2-km grid)
    - sector: keep "F" (Full Disk) for this minimal design.

    Notes:
    - We do NOT list S3; we derive expected file prefix. Callers will attempt download.
    """
    import pandas as pd

    prod = _PRODUCT_ALIAS.get(product.upper(), product.upper())
    ts = pd.to_datetime(t, utc=True)
    yyyy = ts.strftime("%Y")
    doy = ts.strftime("%j")  # day of year
    hh = ts.strftime("%H")

    # We don't know the full filename (it includes start/end/creation timestamps).
    # Callers will combine with bucket and attempt to download via a prefix match.
    # We return the directory prefix; ensure_cached() will resolve the object.
    return f"ABI-L2-{prod}/{yyyy}/{doy}/{hh}/"


def local_path_for(key: str, bucket: Optional[str] = None) -> Path:
    """
    Mirror 'bucket/key' under cache_root. 'bucket' should NOT include the URL scheme.
    """
    base = cache_root() / (bucket or "")
    return base / key


def _get_fs():
    # Anonymous S3 access; public buckets do not require credentials.
    return fsspec.filesystem("s3", anon=True)

def _resolve_latest_nc(prefix_url: str) -> str:
    """
    Given an s3://bucket/.../ prefix, find a single .nc object.
    """
    fs = _get_fs()
    if not prefix_url.endswith("/"):
        prefix_url += "/"
    # glob may return entries without 's3://' scheme — normalize afterwards
    files = fs.glob(prefix_url + "*.nc")
    if not files:
        raise FileNotFoundError(f"No .nc under {prefix_url}")
    latest = sorted(files)[-1]
    return _normalize_s3_url(latest)


def ensure_cached(key_prefix: str, *, satellite: str | None = None) -> Path:
    """
    Ensure the NetCDF file for the given ABI-L2 key prefix is cached locally.

    Parameters
    ----------
    key_prefix : str
        Directory-like S3 key from s3_key(...), e.g. "ABI-L2-ACMF/2025/245/21/"
    satellite : str | None
        Which satellite bucket to use (goes16/goes18/goes19). Default "goes16".

    Returns
    -------
    Path
        Local file path to the cached .nc file.

    Raises
    ------
    FileNotFoundError
        If no .nc exists under the prefix in the bucket.
    """
    sat = (satellite or "goes19").lower()
    bucket_name = _DEFAULT_BUCKET_BY_SAT.get(sat)
    if bucket_name is None:
        raise ValueError(f"Unknown satellite '{satellite}'")

    # Build canonical S3 prefix
    prefix_url = f"s3://{bucket_name}/{key_prefix.lstrip('/')}"

    # Resolve the concrete .nc object (normalized to 's3://bucket/key')
    s3_nc_url = _resolve_latest_nc(prefix_url)

    # Compute local destination path
    bkt, rel_key = _split_s3_url(s3_nc_url)
    dest = local_path_for(rel_key, bucket=bkt)
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        return dest

    # Atomic write: stream S3 → tmpfile → move
    fs = _get_fs()
    with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False) as tmp:
        with fs.open(s3_nc_url, "rb") as fsrc:
            for chunk in iter(lambda: fsrc.read(1024 * 1024), b""):
                tmp.write(chunk)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)

    tmp_path.replace(dest)
    return dest
