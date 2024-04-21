# Imports the Google Cloud client library
import os
import pandas as pd
from google.cloud import storage
from alpha_vantage.timeseries import TimeSeries
from google.cloud import storage


# setting credientials 
path=r'/Users/surbhigarg/Documents/bigdata/key/groovy-marker-420622-546ca4f96c0b.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=path



storage_client = storage.Client()


# create new bucket
bucket_name = 'alpha_vantage_data'
#bucket = storage_client.create_bucket(bucket_name)
#print(f'Bucket {bucket_name} created.')

# Fetch data
key='71UVUX0UB17L43BN'
fd=TimeSeries(key,output_format='pandas')
data=fd.get_monthly('AAPL')
print(data[0])

# Convert DataFrame to CSV file
csv_file = 'data.csv'
data[0].to_csv(csv_file, index=False)


# Upload file to GCS bucket
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(csv_file)
blob.upload_from_filename(csv_file)






    
    
