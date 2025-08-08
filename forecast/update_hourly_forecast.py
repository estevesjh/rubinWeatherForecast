#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime, timedelta
import pytz

from efd_temp_query import EFDTemperatureQuery
from helper import DataFileHandler

# -- Get Chilean local midnight for today
tz_chile = pytz.timezone("America/Santiago")
now_chile = datetime.now(tz_chile)
today_midnight = now_chile.replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow_midnight = today_midnight + timedelta(days=1)

# -- Use DataFileHandler to get the daily cache file path
handler = DataFileHandler()
output_file = handler.get_daily_cache_path(today_midnight)

print(f"Querying EFD from {today_midnight} to {tomorrow_midnight} (Chile local)")
query = EFDTemperatureQuery(
    start_date=today_midnight.astimezone(pytz.UTC),
    end_date=tomorrow_midnight.astimezone(pytz.UTC),
    freq="15min",
    verbose=True
)
df = query.to_csv(output_file)
print(f"✅ Wrote daily cache file: {output_file}")