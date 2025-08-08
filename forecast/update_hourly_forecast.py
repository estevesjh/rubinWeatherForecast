#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timedelta
import pytz
import shutil

from efd_temp_query import EFDTemperatureQuery  # adjust to your import path

# ── Configuration ─────────────────────────────────────────────────────────────
DATA_DIR      = Path("/sdf/data/rubin/user/esteves/forecast")
METADATA_FILE = DATA_DIR / "metadata.json"
FREQ          = "15min"           # Sample frequency for EFD query
VERBOSE       = True
# ──────────────────────────────────────────────────────────────────────────────

tz_utc = pytz.UTC
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = DATA_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 1) Load last run timestamp (or bootstrap 7 days ago)
if METADATA_FILE.exists():
    meta = json.loads(METADATA_FILE.read_text())
    last_run = datetime.fromisoformat(meta["last_timestamp"])
    if last_run.tzinfo is None:
        last_run = tz_utc.localize(last_run)
else:
    last_run = datetime.now(tz_utc) - timedelta(hours=1)

# 2) Compute next end_date (floor to hour)
now = datetime.now(tz_utc)
end_date = now.replace(minute=0, second=0, microsecond=0)
start_date = last_run

if end_date <= start_date:
    if VERBOSE: print("No new hour to fetch; exiting.")
    exit(0)

if VERBOSE:
    print(f"Fetching data from {start_date.isoformat()} to {end_date.isoformat()}")

# 4) Query EFD
# Clear cache at local midnight (first run of the day)
tz_local = pytz.timezone("America/Santiago")
local_end = end_date.astimezone(tz_local)
if local_end.hour == 0:
    # Remove all files in the cache directory
    for f in CACHE_DIR.iterdir():
        if f.is_file():
            f.unlink()

output_file = CACHE_DIR / f"rolling_window_{end_date.strftime('%Y%m%dT%H%M')}.csv"
query = EFDTemperatureQuery(
    start_date=start_date,
    end_date=end_date,
    freq=FREQ,
    verbose=VERBOSE
)
df = query.to_csv(output_file)

# 6) Persist metadata for next run
meta = {"last_timestamp": end_date.isoformat()}
METADATA_FILE.write_text(json.dumps(meta))
if VERBOSE:
    print(f"Updated metadata.last_run → {meta['last_timestamp']}")