import json
import requests
import os
import pandas as pd
import datetime
from alpha_vantage.timeseries import TimeSeries
from google.cloud import storage


def hello_world(request):

    # cridentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']='api_key.json'
    storage_client = storage.Client()

    # bucket name
    bucket_name = 'alphavantagestorage'

    # flag
    present=0

    # check if bucket exists or not 
    if not storage_client.lookup_bucket(bucket_name):
        bucket = storage_client.create_bucket(bucket_name)
        print(f'Bucket {bucket_name} created.')
        present=1
    else:
        print('bucket already present')

        
    

    # fetch data

    key="api_key"


    # creating datatframe
    ticker=['JPM','BAC','HSBC','WFC'] #finance data
    bucket_name = 'alphavantagestorage'
    final = pd.DataFrame()
    for tck in ticker:
        url=f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={tck}&outputsize=200&apikey={key}'
        response = requests.get(url).json()
        df=pd.DataFrame(response['Time Series (Daily)'])
        df=df.T
        df=df.reset_index()
        df=df.rename(columns={'index': 'Date', '1. open': 'Open', '2. high': 'High', '3. low': 'Low', '4. close': 'Close', '5. volume':'Volume'})
        df['ticker']=tck
        final= pd.concat([final,df])
    
    if present==0:
        latest = (pd.to_datetime('today')).normalize()
        final=final[final['Date']==latest]

    # set the filename
    today=pd.to_datetime('today')
    filename = 'stock_data'+str(today)+'.json'
    

    data=final.to_json(orient='records')

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(data)

    return 'data uploaded!'