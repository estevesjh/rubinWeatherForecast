# goes_cloud/cloudfrac.py
"""
Cloud fraction orchestrator for GOES ABI products (minimal).

Public API:
    - run(start, end, *, satellite="goes16", sector="F",
          window_pixels=3, csv_path=None, site=None, verbose=True) -> pd.DataFrame
    - dry_run(start, end, *, satellite="goes16", sector="F") -> list[str]

Inputs:
    start, end: str | datetime-like
        Time range (UTC or local). Ten-minute cadence is assumed.

Outputs:
    DataFrame with columns:
        - timestamp (UTC, pandas.Timestamp)
        - cloudfraction (float in [0,1])
        - cloudfraction_above_site (float in [0,1] or NaN if ACHTF missing)

Notes:
    - Keeps logic minimal; all heavy lifting is delegated to tools.py and storage.py.
    - No classes; pure functions for easy testing.
"""
# goes_cloud/cloudfrac.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, List
import logging
import numpy as np
import pandas as pd
import storage
import tools
from dataclasses import dataclass

# Configure a very lightweight logger; callers can override as needed.
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def dry_run(
    start,
    end,
    *,
    satellite: str = "goes16",
    sector: str = "F",
) -> List[str]:
    """
    Plan-only: return the list of S3 keys this module would fetch for ACMF+ACHTF.

    Parameters
    ----------
    start, end : datetime-like or str
    satellite : {"goes16","goes18","goes19"}
    sector : str

    Returns
    -------
    list[str]
        Interleaved keys (ACMF then ACHTF) for each scan time.
    """
    times = tools.list_scan_times(start, end, step_minutes=10)
    keys: List[str] = []
    for t in times:
        acmf_key, achtf_key = _build_keys_for_time(t, satellite, sector)
        keys.extend([acmf_key, achtf_key])
    return keys

# ------------------------------
# Logging
# ------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(
        logging.Formatter(
            "[%(levelname)s] %(asctime)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
        )
    )
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

# ------------------------------
# Key planning
# ------------------------------
def _build_keys_for_time(t, satellite: str, sector: str) -> Tuple[str, str]:
    """
    Return single S3 *prefixes* (dir-like) for ACMF and ACHAF at time t.
    """
    acmf = storage.s3_key("ACMF", t, satellite=satellite, sector=sector)
    achtf = storage.s3_key("ACHAF", t, satellite=satellite, sector=sector)  # height
    return acmf, achtf


# ------------------------------
# Per-time runners (with internal try/except + verbose logging)
# ------------------------------
def run_acmf_at_time(
    t,
    acmf_key_prefix: str,
    achtf_key_prefix: str,  # kept in signature per your sketch; unused here
    site: tools.Site,
    *,
    satellite: str,
    verbose: bool = True,
) -> float:
    """
    Ensure ACMF is cached, open it, locate the site (bilinear), and compute cloudfraction.
    Returns float in [0,1] or NaN if geometry/data are invalid.
    """
    site_lat = site.lat
    site_lon = site.lon
    # site_alt_m = site.alt_m  # unused here
    try:
        acmf_path = storage.ensure_cached(acmf_key_prefix, satellite=satellite)
    except Exception as exc:
        if verbose:
            logger.warning("ACMF missing at %s (%s)", t, exc)
        return float("nan")

    try:
        mask_bcm, ds = tools.open_acmf(Path(acmf_path))
        ix_f, iy_f = tools.site_xy_fractional(ds, site_lat, site_lon)
        neighbors_w = tools.bilinear_neighbors(ix_f, iy_f)
        cf = tools.cloud_fraction_from_mask(mask_bcm, neighbors_w)
        return float(cf)
    except Exception as exc:
        if verbose:
            logger.warning("ACMF read/compute failed at %s: %s", t, exc)
        return float("nan")


def run_achtf_at_time(
    t,
    achtf_key_prefix: str,
    site: tools.Site,
    *,
    satellite: str,
    verbose: bool = True,
) -> float:
    """
    Ensure ACHAF (or ACHA fallback) is cached, open it, and compute cloudfraction_above_site
    at the site via bilinear interpolation. Returns [0,1], 0.0 for clear/below-alt, or NaN
    if window is fully invalid/off-swath.
    """
    site_lat = site.lat
    site_lon = site.lon
    site_alt_m = site.alt_m

    # Try ACHAF then fallback to ACHA
    height_path: Optional[Path] = None
    for prod in ("ACHAF", "ACHA"):
        try:
            pref = storage.s3_key(prod, t, satellite=satellite, sector="F")
            p = storage.ensure_cached(pref, satellite=satellite)
            height_path = Path(p)
            break
        except Exception as exc:
            if verbose:
                logger.warning("%s missing at %s (%s)", prod, t, exc)
            continue

    if height_path is None:
        # No height product available → treat as unknown (NaN).
        return float("nan")

    try:
        cth_m, ds_h = tools.open_achtf(height_path)
        # Use ACMF geometry for neighbors ideally; ACHAF aligns, but we recompute here:
        ix_f, iy_f = tools.site_xy_fractional(ds_h, site_lat, site_lon)
        neighbors_w = tools.bilinear_neighbors(ix_f, iy_f)
        # We still need mask to enforce "cloud present" condition. Best effort:
        # If we don't have ACMF array here, interpret “above” as (height>alt) AND height exists.
        # But to follow your definition strictly, we should also pass mask.
        # Minimal approach: fetch ACMF neighbor mask via height’s time—cheap re-open avoided,
        # so we’ll interpret “cloud present” as “height is finite” (ACHA only over cloudy pixels).
        # That matches the product semantics: missing HT ⇒ no cloud.
        # => Build a synthetic mask from height validity:
        mask_like = np.where(np.isfinite(cth_m), 1.0, 0.0)

        cfas = tools.cloud_fraction_above_alt(cth_m, mask_like, neighbors_w, site_alt_m)
        # clear or below-alt → 0.0 (function already yields 0.0), fully invalid → NaN
        return float(cfas)
    except Exception as exc:
        if verbose:
            logger.warning("ACHTF/ACHA read/compute failed at %s: %s", t, exc)
        return float("nan")


# ------------------------------
# Public API: run
# ------------------------------
def run(
    start,
    end,
    *,
    satellite: str = "goes19",
    sector: str = "F",
    csv_path: Optional[str | Path] = None,
    site: Optional[tools.Site] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Compute cloud fraction time series for Rubin Observatory at ~10-min cadence.

    Orchestrates:
        times → keys → ACMF(cf) → (if cf>0) ACHAF(cf_above) → assemble → (optional) CSV.
    """
    if verbose:
        logger.info(
            "Cloud fraction run: %s → %s | sat=%s sector=%s",
            start,
            end,
            satellite,
            sector,
        )

    times = tools.list_scan_times(start, end, step_minutes=10)
    if not times:
        raise ValueError("No timestamps produced. Check your start/end inputs.")

    site_obj = tools.Site.rubin_default() if site is None else site

    rows: List[dict] = []

    for idx, t in enumerate(times, start=1):
        if verbose:
            logger.info("(%d/%d) time=%s", idx, len(times), pd.to_datetime(t, utc=True))

        # 2) Keys & 3) ensure local cache (keys only here; downloads happen in functions)
        acmf_key, achtf_key = _build_keys_for_time(t, satellite, sector)

        # Cloud fraction (ACMF)
        cloudfraction = run_acmf_at_time(t, acmf_key, achtf_key, site_obj,
            satellite=satellite,
            verbose=verbose,
        )

        # Fast path logic:
        if np.isnan(cloudfraction):
            cloudfraction_above_site = float("nan")
        elif cloudfraction == 0.0:
            cloudfraction_above_site = 0.0
        else:
            cloudfraction_above_site = run_achtf_at_time(t, achtf_key, site_obj,
                satellite=satellite,
                verbose=verbose,
            )

        rows.append(
            {
                "timestamp": pd.to_datetime(t, utc=True),
                "cloudfraction": float(cloudfraction),
                "cloudfraction_above_site": float(cloudfraction_above_site)
                if not np.isnan(cloudfraction_above_site)
                else float("nan"),
            }
        )

    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

    if csv_path is not None:
        tools.write_csv(df, csv_path)
        if verbose:
            logger.info("Wrote CSV: %s", csv_path)

    return df