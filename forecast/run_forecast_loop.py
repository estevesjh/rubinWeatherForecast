import subprocess
import time
from datetime import datetime, timedelta

commands = [
    "python update_hourly_forecast.py",
    "python run_forecast.py",
    "python send_data_to_api.py"
]

def log_banner(msg):
    print("\n" + "=" * 60)
    print(f" {msg} ".center(58, "="))
    print("=" * 60)

def log_step(msg):
    print(f"\n--- {msg} ---\n")

def run_once():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_banner(f"Forecast pipeline started at {now}")
    for cmd in commands:
        log_step(f"Running: {cmd}")
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            print(f"\n❌ [ERROR] Command failed: {cmd} (exit {ret})\n")
            break

def sleep_until_next_hour():
    now = datetime.now()
    # Find next :05 after the hour (as before)
    next_hour = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
    seconds = (next_hour - now).total_seconds()
    m, s = divmod(int(seconds), 60)
    print(f"\n😴 Sleeping {m} min {s} sec until next run at {next_hour.strftime('%Y-%m-%d %H:%M:%S')}\n")
    if seconds > 0:
        time.sleep(seconds)

if __name__ == "__main__":
    while True:
        run_once()
        sleep_until_next_hour()