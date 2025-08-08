#!/usr/bin/env bash
set -e  # exit on any error

export HOME=/sdf/home/e/esteves  # or your real home, if not default

cd "${HOME}/sitcom-analysis/rubinWeatherForecast/forecast"

# Log the time for each run
echo "------ $(date) ------" >> ${HOME}/forecast_bot.log

# Run update + forecast + upload, logging errors to forecast_bot.log
python update_hourly_forecast.py   >> ${HOME}/forecast_bot.log 2>&1
python run_forecast.py             >> ${HOME}/forecast_bot.log 2>&1
python send_data_to_api.py         >> ${HOME}/forecast_bot.log 2>&1