# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import bigquery
from pyspark.sql.types import *
from google.cloud.exceptions import NotFound
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
                    .appName('tiingo_api') \
                    .getOrCreate()

# Intialize below
bucket = "tiingodatastorage"
project_id = "adroit-archive-422123-h0"
dataset_id = "stockdata"
table_id='healthcare'

# json file name
today=pd.to_datetime('today').date()
file_name='stock_data'+str(today)+'.json'
file=f'gs://tiingodatastorage/{file_name}'

# Read the JSON file into a DataFrame
df = spark.read.json(file)

# Print the schema of the DataFrame
print("Schema of the DataFrame:")
df.printSchema()

df.show()


# transform the dataset

df=df[['high','low','open','close','volume','date','ticker']] # considering only these columns

#Rename
new=[ "High",'Low',"Open",'Close','Volume','Date']
for c,n in zip(df.columns,new):
    print(c)
    print(n)
    df=df.withColumnRenamed(c,n)


#df=df.withColumn('Date', F.to_utc_timestamp(F.from_unixtime(F.col("Date")/1000,'yyyy-MM-dd'),'EST')) # changing the format of date
df=df.withColumn('Date', to_date(col('Date')))

df_new = df.withColumn('average', (col('Close') + col('High')) / 2.0) # add a column doing average 
df_clean = df_new.withColumn('average', col('average').cast(DoubleType()))
df_clean.show()

# Initialize 
client = bigquery.Client()

# check if dataset exists

try:
    client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except NotFound:
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(project_id, dataset.dataset_id))
    
# Write the DataFrame to BigQuery with autodetected schema
df_clean.write.format('bigquery') \
    .option('table', f'{project_id}.{dataset_id}.{table_id}') \
    .option('temporaryGcsBucket', bucket) \
    .mode('append') \
    .save()

# Stop the Spark session
spark.stop()




