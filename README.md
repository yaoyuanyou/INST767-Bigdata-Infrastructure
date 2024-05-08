# GCP DATA PIPELINE
This project fetches the latest data for 4 tech companies, and 4 pharmaceutical and 4 Financial companies using Yahoo Polygon API, Tiingo API, and Alpha Vantage API in batch format storing the raw data to Google storage. Google Schedular which is scheduled to run daily, updates the data in cloud storage. ETL transformations are performed using the DataProc PySpark cluster and the processed data is then loaded into BigQuery. Finally, visualize the data using Looker.

# Techonologies Used
* Google Cloud Platform
* Alpha Vantage API
* Polygon API
* Tiingo API
* Pyspark
* DataProc
* Bigquery
* SQL

# Architecture


