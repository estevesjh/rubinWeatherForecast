# Standard Library Imports
from datetime import timedelta, datetime
import numpy as np
import pytz

# Third-Party Imports
from astropy.time import Time
import pandas as pd

# Local Imports
from helper import TwilightTimes

class EFDTemperatureQuery:
    """
    Query and aggregate Rubin Weather temperature data.

    Attributes:
        start_date (datetime): Start datetime for the query window (tz-aware).
        end_date (datetime): End datetime for the query window (tz-aware).
        freq (str): Resampling frequency for the output (default: '15min').
        verbose (bool): If True, print query info and stats.
        columns (list): Columns to fetch from EFD (default: temperature vars).
    Methods:
        _query_efd(): Query EFD and return temperature DataFrame without event flags.
        set_twilight_flags(df): Add twilight event flags to DataFrame.
        fetch(): Fetch and return temperature DataFrame with twilight flags.
        to_csv(filename, df=None): Write DataFrame to CSV file.
    """

    def __init__(self, start_date: datetime, end_date: datetime, freq: str = "15min",
                 buffer: str = None, verbose: bool = True, columns=None):
        """
        Initialize with start_date and end_date (tz-aware), and resampling period.
        """
        if columns is None:
            columns = ["temperatureItem0", "salIndex", "location"]
        self.start_date = start_date
        self.end_date = end_date
        self.freq = freq
        # buffer time to pad the query window (default: same as period)
        if buffer is None:
            buffer = freq
        self.buffer = pd.to_timedelta(buffer)
        self.verbose = verbose
        self.columns = columns

    def _query_efd(self) -> pd.DataFrame:
        """
        Retrieve and aggregate temperature data for the date range.
        """
        from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient
        client = makeEfdClient()
        df_outside = getEfdData(
            client=client,
            topic="lsst.sal.ESS.temperature",
            columns=self.columns,
            begin=Time(self.start_date - self.buffer),
            end=Time(self.end_date + self.buffer),
        )
        
        mask = df_outside.salIndex == 301
        df_outside = df_outside[mask]
        df_outside = df_outside.drop(columns=["salIndex"])
        df_outside = df_outside.rename(columns={"temperatureItem0": "temperature"})

        df_outside = df_outside.rolling(self.freq).agg(
            {"temperature": ["min", "mean", "max"]}
        )
        df_outside.columns = df_outside.columns.droplevel(0)
        df_outside = df_outside.resample(self.freq).mean()
        
        # check timezone awareness
        if df_outside.index.tz is None:
            df_outside.index = df_outside.index.tz_localize('UTC')
        if df_outside.index.tz != 'UTC':
            df_outside.index = df_outside.index.tz_convert('UTC')

        # Align to regular time grid using asfreq (skip reindex)
        df_outside = df_outside.asfreq(self.freq)

        # Extend to rounded end_time for full coverage
        rounded_end = self.end_date.replace(minute=0, second=0, microsecond=0, tzinfo=df_outside.index.tz)
        full_index = pd.date_range(start=df_outside.index.min(), end=rounded_end, freq=self.freq)
        df_outside = df_outside.reindex(full_index)

        if self.verbose:
            print(f"Start time: {self.start_date.isoformat()}")
            print(f"End time: {self.end_date.isoformat()}")

        return df_outside

    def set_twilight_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add boolean columns to indicate twilight/sunrise/sunset events
        within the specified window around each event time, across the
        entire date range.
        """
        # check if df.index is tz aware, if not, localize to UTC
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')

        # Collect twilight events for each local day in the window
        tz_santiago = pytz.timezone("America/Santiago")
        start_local = self.start_date.astimezone(tz_santiago).date()
        end_local = self.end_date.astimezone(tz_santiago).date()

        # Initialize lists for each event type
        event_times = {
            'is_sunset': [],
            'is_sunrise': [],
            'is_evening_twilight': [],
            'is_morning_twilight': [],
        }

        # Gather UTC event times across all days in the range
        # for single_day in pd.date_range(start=start_local, end=end_local, freq='D', tz=tz_santiago):
        for single_day in pd.date_range(start=start_local, end=end_local, freq='D'):
            day_str = single_day.strftime('%Y-%m-%d')
            tw = TwilightTimes.from_day(day_str)
            event_times['is_sunset'].append(tw.sunset_utc)
            event_times['is_sunrise'].append(tw.sunrise_utc)
            event_times['is_evening_twilight'].append(tw.evening_twilight_utc)
            event_times['is_morning_twilight'].append(tw.morning_twilight_utc)

        # Determine window in minutes from period
        period_td = pd.to_timedelta(self.freq)
        window_minutes = int(period_td.total_seconds() // 60 * 2)

        # Flag events for each column
        for col, times in event_times.items():
            df[col] = flag_events(df, times, window_minutes)
        return df

    def fetch(self) -> pd.DataFrame:
        """
        Fetch and return the temperature DataFrame with twilight flags.
        """
        df = self._query_efd()
        print(f"Fetched {len(df)} rows from EFD."
              f" Temperature data from {df.index.min().isoformat()} to {df.index.max().isoformat()}.")
        df = self.set_twilight_flags(df)
        return df

    def to_csv(self, filename, df=None):
        if df is None:
            df = self.fetch()
        df_reset = df.reset_index().rename(columns={"index": "timestamp"})
        
        # Ensure ISO formatting with UTC offset
        df_reset["timestamp"] = df_reset["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        df_reset.to_csv(filename, index=False)
        print(f"Data written to file: {filename}")

def flag_events(df: pd.DataFrame, event_times: list, window_minutes: int) -> np.ndarray:
    """
    Create a boolean array indicating if each timestamp in df.index
    falls within window_minutes of any event time in event_times.
    """
    flags = pd.Series(False, index=df.index)
    for t_event in event_times:
        # Find the closest timestamp in df within tolerance
        diffs = np.abs(df.index - t_event)
        within_window = diffs <= pd.Timedelta(minutes=window_minutes)
        if within_window.any():
            closest_idx = diffs[within_window].argmin()
            closest_idx = df.index[within_window][closest_idx]
            flags[closest_idx] = True     
    return flags

if __name__ == "__main__":
    # Example usage
    # Replace with desired start and end datetimes (tz-aware)
    from datetime import datetime
    import pytz
    tz = pytz.UTC
    now = datetime.utcnow().replace(tzinfo=tz)
    # rounded to the nearest hour
    end_date = now.replace(minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(hours=6)

    datapath = "/sdf/data/rubin/user/esteves/forecast"
    print(f"Querying temperature data from {start_date.isoformat()} to {end_date.isoformat()}")
    query = EFDTemperatureQuery(start_date=start_date, end_date=end_date)
    # query.to_csv(f"{datapath}/temp_window_{end_date.isoformat()}.csv")
    query.to_csv(f"temp_window_{end_date.isoformat()}.csv")
    print("Done.")
