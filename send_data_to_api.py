import requests

csv_path = "temp_forecast_20250805.csv"
url = "https://rubin-weather-forecast.jesteves.workers.dev/api/update"

with open(csv_path, "r", encoding="utf-8") as f:
    csv_data = f.read()

headers = {"Content-Type": "text/csv"}
response = requests.post(url, data=csv_data, headers=headers)

if response.status_code != 200:
    raise RuntimeError(f"Upload failed: {response.status_code} {response.text}")

print("CSV uploaded successfully.")