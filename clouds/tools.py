# goes_cloud/tools.py
"""
Minimal utilities for GOES cloud fraction sampling at the Rubin Observatory.

- Time helpers: 10-min UTC schedule
- Readers: ACMF (binary cloud mask), ACHAF/ACHA (cloud-top height)
- Geo: GOES fixed-grid CRS; fractional pixel lookup; bilinear neighbors
- Compute: weighted (bilinear) fractions at the site
- I/O: CSV writer

Design notes:
- We interpolate at the exact site location using bilinear weights over the 2×2
  pixel neighborhood, rather than averaging an N×N block.
- GOES ABI L2 x/y coordinates are often in *radians* (scan angles). We project
  lon/lat to GEOS coordinates (meters) and convert to angles using the dataset's
  perspective point height H to match the x/y axis units; this avoids unit
  mismatches between projection meters and file coordinates.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Sequence, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from pyproj import CRS, Transformer
from dataclasses import dataclass

# ---------------------------------------------------------------------
# Site constants (Rubin Observatory, Cerro Pachón)
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class Site:
    lat: float
    lon: float
    alt_m: float = 2660.0
    name: str = "Rubin"

    @staticmethod
    def rubin_default() -> "Site":
        return Site(lat=-30.2407, lon=-70.7366, alt_m=2660.0, name="Rubin")

# ---------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------


def list_scan_times(start, end, step_minutes: int = 10) -> List[pd.Timestamp]:
    """
    Return timestamps snapped to `step_minutes` boundaries in UTC.
    Inclusive of both start and end boundaries after snapping.

    We do a simple floor on `start` and generate a fixed-frequency range.
    """
    t0 = pd.to_datetime(start, utc=True)
    t1 = pd.to_datetime(end, utc=True)
    if t1 < t0:
        t0, t1 = t1, t0

    step = pd.Timedelta(minutes=step_minutes)
    t0 = t0.floor(f"{step_minutes}min")
    times = pd.date_range(start=t0, end=t1, freq=step, tz="UTC")
    return list(times)


# ---------------------------------------------------------------------
# NetCDF readers
# ---------------------------------------------------------------------


def _open_nc(path: Path) -> xr.Dataset:
    # Rely on xarray to apply scale/offset and masks.
    return xr.open_dataset(path, mask_and_scale=True, decode_times=True)


def open_acmf(path: Path) -> tuple[np.ndarray, xr.Dataset]:
    """
    Open ACMF (Cloud/Clear-Sky Mask) and return (binary_cloud_mask, dataset).

    Returns
    -------
    binary_cloud_mask : np.ndarray (float64)
        Nonzero = cloudy, 0 = clear, NaN = missing.
    dataset : xr.Dataset
        The dataset for metadata, coords, and projection attrs.

    Notes
    -----
    Public ACMF typically provides a Binary Cloud Mask (BCM).
    We normalize common encodings: {0,1}, {0,255}, or nonzero/zero bitfields.
    """
    ds = _open_nc(path)

    var_candidates = ["BCM", "Binary_Cloud_Mask", "Cloud_Mask", "cloud_mask", "mask"]
    var_name = next((v for v in var_candidates if v in ds.data_vars), None)
    if var_name is None:
        raise KeyError(f"No expected cloud mask variable found in {path.name}")

    da = ds[var_name]
    arr = da.values  # keep NaNs
    bcm = np.full(arr.shape, np.nan, dtype="float64")

    finite = np.isfinite(arr)
    uniq = np.unique(arr[finite])

    if set(uniq.tolist()).issubset({0, 1}):
        bcm[finite] = arr[finite]
    elif set(uniq.tolist()).issubset({0, 255}):
        bcm[finite] = (arr[finite] == 255).astype("float64")
    else:
        # Fallback: any nonzero value means "cloudy"
        bcm[finite] = (arr[finite] != 0).astype("float64")

    return bcm, ds


def open_achtf(path: Path) -> tuple[np.ndarray, xr.Dataset]:
    """
    Open Cloud-Top Height (ACHAF/ACHA) and return (cth_m, dataset).

    Returns
    -------
    cth_m : np.ndarray (float64)
        Cloud-top heights in meters; <=0 or missing → NaN.
    dataset : xr.Dataset
        The dataset for metadata, coords, and projection attrs.
    """
    ds = _open_nc(path)

    # Common variable names seen in ACHAF/ACHA
    candidates = [
        "HT",  # ACHAF
        "Cloud_Top_Height",
        "cloud_top_height",
        "cth",
        "CLOUD_TOP_HEIGHT",
        "mean_cloud_top_height",
        "maximum_cloud_top_height",
    ]
    var_name = next((v for v in candidates if v in ds.data_vars), None)
    if var_name is None:
        # Heuristic search
        for v in ds.data_vars:
            if "height" in v.lower():
                var_name = v
                break
    if var_name is None:
        raise KeyError(f"No cloud-top height variable found in {path.name}")

    da = ds[var_name]
    cth = da.astype("float64").values  # apply mask/scale

    units = str(da.attrs.get("units", "")).lower()
    if "km" in units and "m" not in units:
        cth = cth * 1000.0

    # Non-physical / fill values → NaN
    cth = np.where(cth > 0, cth, np.nan)
    return cth, ds


# ---------------------------------------------------------------------
# GOES fixed-grid: CRS + site fractional pixel + bilinear weights
# ---------------------------------------------------------------------


def _crs_from_meta(ds: xr.Dataset) -> tuple[CRS, float]:
    """
    Build a pyproj CRS for the GOES GEOS projection and return (CRS, H).

    H = perspective_point_height (meters).
    """
    # Locate the projection attrs
    if "goes_imager_projection" in ds:
        g = ds["goes_imager_projection"].attrs
    else:
        # Find any var that references the mapping via 'grid_mapping'
        gvar = None
        for v in ds.data_vars:
            if ds[v].attrs.get("grid_mapping", "") == "goes_imager_projection":
                gvar = v
                break
        if gvar is None or "goes_imager_projection" not in ds:
            raise KeyError("goes_imager_projection not found in dataset.")
        g = ds["goes_imager_projection"].attrs

    h = float(g["perspective_point_height"])
    lon0 = float(g["longitude_of_projection_origin"])
    sweep = g.get("sweep_angle_axis", "x")
    a = float(g.get("semi_major_axis", 6378137.0))
    b = float(g.get("semi_minor_axis", 6356752.31414))

    proj4 = (
        f"+proj=geos +lon_0={lon0} +h={h} +a={a} +b={b} "
        f"+sweep={sweep} +units=m +no_defs"
    )
    return CRS.from_proj4(proj4), h


def site_xy_fractional(ds: xr.Dataset, lat: float, lon: float) -> tuple[float, float]:
    """
    Return the site's fractional pixel coordinates (ix_f, iy_f) in image index space.

    Handles the common case where dataset coords ('x','y') are scan angles in radians:
    - We project lon/lat → (X,Y) meters (GEOS).
    - Convert to angles: x_ang = atan(X/H), y_ang = atan(Y/H) using H from metadata.
    - Then bracket against ds['x'] and ds['y'] (which may be ascending or descending).
    """
    if ("x" not in ds.coords) or ("y" not in ds.coords):
        raise KeyError("Dataset lacks 'x' and/or 'y' coordinates.")

    x_coords = np.asarray(ds["x"].values)
    y_coords = np.asarray(ds["y"].values)

    crs_geos, H = _crs_from_meta(ds)
    wgs84 = CRS.from_epsg(4326)
    fwd = Transformer.from_crs(wgs84, crs_geos, always_xy=True)
    X_m, Y_m = fwd.transform(lon, lat)

    # Convert meters → scan angles (radians) if coords look like radians
    x_units = str(ds["x"].attrs.get("units", "")).lower()
    y_units = str(ds["y"].attrs.get("units", "")).lower()
    if "rad" in x_units or "radian" in x_units:
        Xq = np.arctan2(X_m, H)
    else:
        Xq = X_m
    if "rad" in y_units or "radian" in y_units:
        Yq = np.arctan2(Y_m, H)
    else:
        Yq = Y_m

    def _bracket(axis_vals: np.ndarray, value: float) -> tuple[int, int, float]:
        ascending = axis_vals[-1] > axis_vals[0]
        if ascending:
            i1 = int(np.searchsorted(axis_vals, value, side="left"))
        else:
            # search on reversed, then map back
            i1 = len(axis_vals) - int(np.searchsorted(axis_vals[::-1], value, side="right"))
        i0 = i1 - 1
        # clamp to interior so we can form a 2×2
        i0 = max(0, min(i0, len(axis_vals) - 2))
        i1 = i0 + 1
        v0, v1 = axis_vals[i0], axis_vals[i1]
        t = 0.0 if v1 == v0 else (value - v0) / (v1 - v0)
        return i0, i1, float(t)

    i0, i1, tx = _bracket(x_coords, Xq)
    j0, j1, ty = _bracket(y_coords, Yq)

    ix_f = i0 + tx
    iy_f = j0 + ty
    return ix_f, iy_f


def bilinear_neighbors(ix_f: float, iy_f: float) -> list[tuple[int, int, float]]:
    """
    Return 2×2 neighbor indices with bilinear weights at fractional (ix_f, iy_f).

    Output format: [(i0,j0,w00), (i1,j0,w10), (i0,j1,w01), (i1,j1,w11)],
    where weights sum to 1.0 (before any validity filtering).
    """
    i0 = int(np.floor(ix_f))
    j0 = int(np.floor(iy_f))
    tx = ix_f - i0
    ty = iy_f - j0
    w00 = (1.0 - tx) * (1.0 - ty)
    w10 = tx * (1.0 - ty)
    w01 = (1.0 - tx) * ty
    w11 = tx * ty
    return [(i0, j0, w00), (i0 + 1, j0, w10), (i0, j0 + 1, w01), (i0 + 1, j0 + 1, w11)]


# ---------------------------------------------------------------------
# Computations (bilinear / weighted at site)
# ---------------------------------------------------------------------


def _in_bounds(shape: tuple[int, int], i: int, j: int) -> bool:
    H, W = shape
    return 0 <= j < H and 0 <= i < W


def cloud_fraction_from_mask(
    mask_bcm: np.ndarray, weighted_neighbors: list[tuple[int, int, float]]
) -> float:
    """
    Weighted mean of cloud presence at the site (bilinear interpolation).

    - mask_bcm: float array; nonzero=cloudy, 0=clear, NaN=missing.
    - weighted_neighbors: [(i,j,w), ...] where weights sum to 1 (pre-filter).
    - Missing/out-of-bounds neighbors are ignored and weights are renormalized.
    """
    num = 0.0
    den = 0.0
    H, W = mask_bcm.shape[:2]

    for i, j, w in weighted_neighbors:
        if w <= 0 or not _in_bounds((H, W), i, j):
            continue
        m = mask_bcm[j, i]
        if np.isfinite(m):
            num += (1.0 if m != 0 else 0.0) * w
            den += w

    if den == 0.0:
        return float("nan")
    return float(num / den)


def cloud_fraction_above_alt(
    cth_m: np.ndarray,
    mask_bcm: np.ndarray,
    weighted_neighbors: list[tuple[int, int, float]],
    site_alt_m: float,
) -> float:
    """
    Weighted fraction of pixels (2×2) where (cloud present) AND (height > site_alt_m).

    Behavior:
    - Clear skies or clouds all below `site_alt_m` → 0.0
    - If no valid neighbors (off-swath/missing) → NaN
    - Missing values (either mask or height) are skipped; weights renormalize.
    """
    num = 0.0
    den = 0.0
    Hb, Wb = mask_bcm.shape[:2]
    Hh, Wh = cth_m.shape[:2]

    for i, j, w in weighted_neighbors:
        if w <= 0:
            continue
        if not (_in_bounds((Hb, Wb), i, j) and _in_bounds((Hh, Wh), i, j)):
            continue

        m = mask_bcm[j, i]
        h = cth_m[j, i]
        if not (np.isfinite(m) and np.isfinite(h)):
            # skip this neighbor entirely so weights renormalize
            continue

        den += w
        if (m != 0) and (h > site_alt_m):
            num += w

    if den == 0.0:
        return float("nan")
    # clear or below-alt clouds naturally yield 0.0 here
    return float(num / den)


# ---------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------


def write_csv(df: pd.DataFrame, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)