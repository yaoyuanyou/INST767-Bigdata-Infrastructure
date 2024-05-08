import json
import requests
import os
import pandas as pd
import datetime
from datetime import date
from google.cloud import storage

def hello_world(request):


  # cridentials
  os.environ['GOOGLE_APPLICATION_CREDENTIALS']='api_key.json'
  storage_client = storage.Client()
  key='yourkey'

  # bucket name
  bucket_name = 'polygonapistorage'
  
  # flag
  present=0

  # check if bucket exists or not 
  if not storage_client.lookup_bucket(bucket_name):
    bucket = storage_client.create_bucket(bucket_name)
    print(f'Bucket {bucket_name} created.')
    present=1
  else:
    print('bucket already present')
  
  ticker=['AAPL','MSFT','GOOG','TSLA']
  today=pd.to_datetime('today').date()
  final=pd.DataFrame()

  today=str(date.today())

  for tck in ticker:
    url= f'https://api.polygon.io/v2/aggs/ticker/{tck}/range/1/day/2024-01-01/{today}?adjusted=true&sort=asc&apiKey={key}'
    r = requests.get(url)
    text = r.text
    company = json.loads(text)
    print(company)

    values = []

    if r.status_code == 200:
      for i in company['results']:
          values.append(i)
    else:
      print('Error', r.status_code)

    df = pd.DataFrame(values)
    df['ticker']=tck
    if present==1:
      final=pd.concat([final,df])
    else:
      final=pd.concat([final,df.iloc[[-1]]])
  
  print(final.head())
  

  # set the filename
    
  filename = 'stock_data'+today+'.json'

  data=final.to_json(orient='records')

  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(filename)
  blob.upload_from_string(data)

  return 'data uploaded!'
