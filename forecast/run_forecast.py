import pandas as pd
from pathlib import Path
from prophetModel import ProphetTwilightValidator
import pytz

def build_forecast_csv(merged: pd.DataFrame, out_path: Path | str) -> None:
    """
    Convert *merged* DataFrame into the eight-column CSV that the
    Highcharts front-end consumes and write it to *out_path*.

    merged must contain at least:
      • ds              – timestamp (tz-aware or naive)
      • min, mean, max  – observed stats
      • yhat_lower,
        yhat,
        yhat_upper      – Prophet output
      • is_evening_twilight (bool) – sunset flag
    """
    # 1. robust column renaming to canonical website schema -------------
    rename_map = {
        "ds": "timestamp",
        "min": "tmin",
        "mean": "tmean",
        "max": "tmax",
        "yhat_lower": "tpmin",
        "yhat": "tprophet",
        "yhat_upper": "tpmax",
        "is_evening_twilight": "sunset",
    }
    merged = merged.rename(columns=rename_map)

    # 2. keep only what the site needs, coerce dtypes -------------------
    df = merged[[
        "timestamp", "tmin", "tmean", "tmax",
        "sunset",    "tpmin", "tprophet", "tpmax"
    ]].copy()

    # - timestamps as ISO-8601 local-time strings (with UTC offset)
    if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = (
            pd.to_datetime(df["timestamp"])
              .dt.tz_localize("America/Santiago", nonexistent="shift_forward", ambiguous="NaT")
              .dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        )

    chile_tz = pytz.timezone("America/Santiago")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if df["timestamp"].dt.tz is None:
        # Assume it is UTC, localize and convert
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert(chile_tz)
    else:
        df["timestamp"] = df["timestamp"].dt.tz_convert(chile_tz)
    # Format with offset (add colon in offset)
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df["timestamp"] = df["timestamp"].str.replace(r'(\d{2})(\d{2})$', r'\1:\2', regex=True)

    # ensure sunset is “true/false” lowercase (so JS .toLowerCase() works)
    df["sunset"] = df["sunset"].astype(bool).map({True: "true", False: "false"})

    # 3. round temps to 2 decimals (match tooltip format) ---------------
    for col in ["tmin", "tmean", "tmax", "tpmin", "tprophet", "tpmax"]:
        df[col] = df[col].astype(float).round(2)

    # 4. write ----------------------------------------------------------
    df.to_csv(out_path, index=False)
    print(f"✅ wrote {len(df):,} rows → {out_path}")

# ----------------------------------------------------------------------
# Example workflow: integrate with ProphetTwilightValidator
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import pandas as pd
    from helper import DataFileHandler
    from datetime import datetime, timezone

    handler = DataFileHandler(window_days=7)
    now = datetime.now(timezone.utc)
    today_iso = pd.Timestamp.now(tz="America/Santiago").strftime("%Y-%m-%d")

    try:
        rolling_df = handler.build_rolling_window_df(now)
    except Exception as e:
        print(f"❌ Error building rolling window: {e}")
        exit(1)
    
    rolling_df['y'] = rolling_df['mean']
    rolling_df['ds'] = rolling_df.index.tz_convert("America/Santiago")
    # ds column should not have timezone info for Prophet
    rolling_df['ds'] = rolling_df['ds'].dt.tz_localize(None)

    # Write rolling window to latest temp_forecast_latest.csv
    out_path = handler.get_latest_path()
    print(f"Building forecast using {len(rolling_df)} rows from rolling window...")
    
    # Fit Prophet model and merge with observed data
    validator = ProphetTwilightValidator(rolling_df)
    merged = validator.evaluate_latest_window()
    build_forecast_csv(merged, out_path)