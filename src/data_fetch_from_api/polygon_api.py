import requests
import json
from pprint import pprint
import pandas as pd

import os
from google.cloud import storage
from alpha_vantage.timeseries import TimeSeries
from google.cloud import storage



ticker = 'AAPL'
apikey = 'https://api.polygon.io/v2/aggs/ticker/' + ticker + '/range/1/day/2024-01-01/2024-05-01?adjusted=true&sort=asc&apiKey=vZdNEcWKBFiGyThp_FCYj_9TTjQOXP4U'


r = requests.get(apikey)
text = r.text
company = json.loads(text)

values = []

if r.status_code == 200:
    for i in company['results']:
        values.append(i)
else:
    print('Error', r.status_code)

df = pd.DataFrame(values)
df.rename(columns={'v':'Trading volume', 'vw':'Volume Weighted Avg_Pri', 'o':'Open Pri', 'c':'Close Pri', 'h':'Highest Pri', 'l':'Lowest Pri',
                   't':'Unix Msec timestamp', 'n':'Num of Transactions'}, inplace=True)


df.to_csv('polygon_data.csv')
