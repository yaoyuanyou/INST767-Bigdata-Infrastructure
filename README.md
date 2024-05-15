# GCP DATA PIPELINE
This project fetches the latest data for 4 tech companies, and 4 pharmaceutical and 4 Financial companies using Yahoo Polygon API, Tiingo API, and Alpha Vantage API in batch format storing the raw data to Google storage. Google Schedular which is scheduled to run daily at 1 am from Tuesday to Saturday ( since the market is closed on the weekend), updates the data in cloud storage. ETL transformations are performed using the DataProc PySpark cluster and the processed data is then loaded into BigQuery. Google workflow was used to automate the Transformation and loading part, scheduled to run daily at 3 am to process the new data stored in storage and appending it to BigQuery. Finally, sample queries were written in Bigquery to answer some proposed business questions related to the loaded stock data


Team memeber: Surbhi Garg(sgarg31@umd.edu), Nuwan Chamara Hewabethmage(nhewabet@umd.edu), Hengmeng Wang(hwang17@umd.edu), Yogesh Jitendra Boricha(yogi@umd.edu), Yuanyou Yao(yyao93@umd.edu), Abdulrahman Mohamed Abdullahi(aabdul24@umd.edu)

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



## Ingestion
Data is being extracted from 3 APIs namely Alphavantage API, Polygon API, Tiingo API. Alphavantage API is being used to extract daily stock
prices for financial companies ( limited to 4 for the project), Tiingo is used to extract daily stock prices for Healthcare companies, and Polygon API is used to extract daily stock prices for Tech companies. Cloud Functions are used to perform the extraction. The extracted data is then stored in JSON format in Google Cloud Storage. The whole process is scheduled to run on a daily basis using a cloud scheduler

<img width="1345" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/3aa51947-cdd1-4111-b907-17de179580ca">

<img width="1439" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/3371787d-3fd4-4841-924e-448757bcc094">

<img width="738" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/54ea6389-b298-4840-9776-c1246ee7fe27">

<img width="1459" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/042153e5-5a4e-4b31-b26b-b16a204d5e1d">


<img width="1455" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/44709746-1498-4ce3-b3a6-cae1f323ea71">

## Transformation

The data stored in Google Storage is then transformed into the right format using Dataproc and Spark jobs. After transformation, the final result is stored in Google BigQuery. The transformation process is scheduled to run daily using Google workflow, transforming the daily ingested data and appending the result into the big query

<img width="1175" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/a58f1bed-22c4-4f53-8fe1-06c0f3c89895">

<img width="1438" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/883a2aa4-b7c5-470c-a3a2-1e5f75a291bd">

<img width="1454" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/a3eb9c83-ad84-4ba8-ac5b-d05704383cd3">

<img width="1406" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/5f6a6c27-67be-48dc-b891-2d9dd7552cb3">


## Storage
The transformed data is then stored in BigQuery. The data is stored in three tables: Tech, Finance, and Healthcare
<img width="1012" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/853f38ba-8dfd-4ce5-b0ad-31742bceac07">

## Analysis
For analysis, a seperate analysis for each sector was conducted and then 4 sample business questions were formulated comparing all the sectors and the data was queried using SQL in BigQuery to get the desired data.

Questions:
Ques.1 Which stock in each sector had the highest and lowest trading volume last month?

<img width="867" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/b8b3ae20-95d2-4a9c-89b4-4a37493b5752">

Ques.2 what are  max price fluctuations (high - low) among given stocks in each sector daily basis?

<img width="861" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/44c2692a-cfa3-477f-b673-00f9753dc547">

Ques.3 What are the top stock from each sector based on the past 30 days ?

<img width="749" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/bdfcbc4d-d1b2-4fbe-8978-c2c1656fc611">

Ques.4 What are the trends in closing prices for the top stock from each sector over past 30 days?

<img width="846" alt="image" src="https://github.com/garg1998/INST-767-Bigdata-project/assets/48328700/5595ae92-7bb4-41e5-bc7b-e9e0ed1f6d26">


*Visualizations using Looker were also created for further analysis*


<img width="540" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/5c98b4de-22b7-4c5a-af7f-048c2c76ba0f">

<img width="482" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/c7426eb7-d442-42d7-aa44-179ec8a35f3c">

<img width="652" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/d7d81f22-a069-4009-a0ef-a0b401df5721">

<img width="590" alt="image" src="https://github.com/garg1998/INST-767/assets/48328700/a91da3b7-0e7c-4631-a921-f241fea0df2c">















