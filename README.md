# Sensor data manipulation

## Charge session profiling
Given two sets of data, sensor and session data, merge them together to obtain a charge session profile, along some basic aggregate data per profile.

## Simple anomaly detection
Given a DataFrame of session profiles, return a log of cases where the variable value had a bigger than 10% difference to the previous value.

## Local run
To run locally, you need the data source files available and a working spark installation. It can be run with a command similar to this:
`spark-submit --master "local[4]" --class wb.sensors.SessionBriefingApp ./target/scala-2.12/wb_2.12-0.1.jar ~/Documents/wb/Charger_logs/charger_log_session ~/Documents/wb/Charger_logs/charger_log_status`