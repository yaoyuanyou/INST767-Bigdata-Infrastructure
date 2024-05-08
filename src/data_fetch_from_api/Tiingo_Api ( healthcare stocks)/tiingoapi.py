import json
import requests
import os
import pandas as pd
import datetime
from tiingo import TiingoClient
from google.cloud import storage

def hello_world(request):

  # cridentials
  os.environ['GOOGLE_APPLICATION_CREDENTIALS']='api_key.json'
  storage_client = storage.Client()

  # bucket name
  bucket_name = 'tiingodatastorage'

  # flag
  present=0

  # check if bucket exists or not 
  if not storage_client.lookup_bucket(bucket_name):
    bucket = storage_client.create_bucket(bucket_name)
    print(f'Bucket {bucket_name} created.')
    present=1
  else:
    print('bucket already present')
  
  # To reuse the same HTTP Session across API calls (and have better performance), include a session key.
  config={}
  config['session'] = True
  config['api_key'] = 'yourkey'

  # Initialize
  client = TiingoClient(config)

  ticker=['LLY','MRK','ABT','ABBV']
  today=pd.to_datetime('today').date()

  # final dataframe
  final=pd.DataFrame()

  for tck in ticker:
    data = client.get_ticker_price(tck,fmt='json',startDate='2024-04-01',endDate=today,frequency='daily')
    df=pd.DataFrame(data)
    df['ticker']=tck
    if present==1:
      final=pd.concat([final,df])
    else:
      final=pd.concat([final,df.iloc[[-1]]])

  
  # set the filename
    
  filename = 'stock_data'+str(today)+'.json'

  data=final.to_json(orient='records')

  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(filename)
  blob.upload_from_string(data)

  return 'data uploaded!'



   
