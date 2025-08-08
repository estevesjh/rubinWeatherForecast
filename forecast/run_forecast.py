import argparse
from datetime import datetime
import pandas as pd
import pytz

from prophetModel import ProphetTwilightValidator
from helper import DataFileHandler

def banner(msg):
    print("\n" + "=" * 60)
    print(f" {msg} ".center(58, "="))
    print("=" * 60 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Run forecast pipeline for a rolling window ending at the specified date.")
    parser.add_argument(
        "--now",
        type=str,
        default=None,
        help="Current date/time for rolling window end (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM, Chilean time). Defaults to now.",
    )
    args = parser.parse_args()

    tz_chile = pytz.timezone("America/Santiago")
    if args.now:
        print(f"[INFO] Using supplied --now: {args.now}")
        now = pd.Timestamp(args.now).tz_localize(tz_chile)
    else:
        now = datetime.now(tz_chile)
        now = pd.Timestamp(now)
        print(f"[INFO] Using Chile local time now: {now}")

    banner("Rolling Window Assembly")
    handler = DataFileHandler()
    out_path = handler.get_latest_path()

    try:
        rolling_df = handler.build_rolling_window_df(now)
    except Exception as e:
        print(f"❌ Error building rolling window: {e}")
        exit(1)
    print(f"[INFO] Rolling window shape: {rolling_df.shape}")

    banner("Running Prophet Forecast")
    # count the nan values in rolling_df
    nan_count = rolling_df['mean'].isna().sum().sum()
    print(f"[INFO] Rolling window contains {nan_count} NaN values before forecasting.")
    
    validator = ProphetTwilightValidator(rolling_df)
    merged = validator.evaluate_latest_window()
    if merged is None or merged.empty:
        print("❌ No forecast was produced.")
        exit(1)

    validator.to_csv(merged, out_path)
    print(f"✅ Forecast CSV written to: {out_path}")

if __name__ == "__main__":
    main()