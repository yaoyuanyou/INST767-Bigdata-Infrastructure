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

    key="yourkey"


    # creating datatframe
    ticker=['JPM','BAC','HSBC','WFC']
    bucket_name = 'alphavantagestorage'
    final = pd.DataFrame()
    for tck in ticker:
        url=f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={tck}&outputsize=200&apikey={key}'
        response = requests.get(url).json()
        df=pd.DataFrame(response['Time Series (Daily)'])
        df=df.T
        df=df.reset_index()
        df['ticker']=tck

        if present==0:
            final= pd.concat([final,df.head(1)])
        else:
            final= pd.concat([final,df])
           

    # set the filename
    today=pd.to_datetime('today').date()
    filename = 'stock_data'+str(today)+'.json'
    

    data=final.to_json(orient='records')

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(data)

    return 'data uploaded!'