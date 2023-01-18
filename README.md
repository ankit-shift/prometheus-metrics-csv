# prometheus-metrics-csv
A simple python multiprocess script to download metrics to individual metric csv files for long time frames using PrometheusAPI

## To Run

python3 -W ignore ./metricsDownloader.py  https://prometheus_host:9090 2022-10-01T09:30:55Z 2023-01-17T09:32:13Z metrics1{label=\"value\"},metrics2{label=\"value\"}