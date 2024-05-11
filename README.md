# GCP DATA PIPELINE
This project fetches the latest data for 4 tech companies, and 4 pharmaceutical and 4 Financial companies using Yahoo Polygon API, Tiingo API, and Alpha Vantage API in batch format storing the raw data to Google storage. Google Schedular which is scheduled to run daily, updates the data in cloud storage. ETL transformations are performed using the DataProc PySpark cluster and the processed data is then loaded into BigQuery. Google workflow was used to automate the Transformation and loading part, scheduled to run daily. Finally, sample queries were written in Bigquery to answer some proposed business questions related to the loaded stock data

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

<img width="933" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/532ae1f2-49d2-4fc8-b3f7-485ebc5c9830">



