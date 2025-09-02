# smoketest.py
"""
Minimal smoke test for goes_cloud package.
Fetches ~2–3 GOES scans and prints the resulting DataFrame.
"""

from datetime import datetime, timedelta
import cloudfrac

# Pick a recent UTC day/time (GOES has ~10 min cadence).
# Adjust this to "today minus ~2h" for fresh files.
end = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
start = end - timedelta(minutes=60)

print(f"Smoke test window: {start} → {end} (UTC)")

df = cloudfrac.run(
    start,
    end,
    satellite="goes19",   # GOES-East (Chile has good view)
    sector="F",
    csv_path="smoke_out.csv",
    verbose=True,
)

print("\n--- DataFrame head ---")
print(df.head())

print("\nSaved CSV: smoke_out.csv")
print(f"Total rows: {len(df)}")