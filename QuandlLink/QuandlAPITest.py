'''
Created on Jul 8, 2015

@author: shaunz
'''

from QuandlAPI import QuandlAPI
from influxdb.influxdb08 import DataFrameClient
from QuandlTicker import Quandl_ticker_list
from pandas import DataFrame

quandl = QuandlAPI()
df = DataFrameClient('localhost', 8086, 'root', 'root')
if({'name':'Quandl'} not in df.get_list_database()):
    df.create_database('Quandl')

df.switch_database('Quandl')

for series in df.get_list_series():
    df.delete_series(series)

for item in Quandl_ticker_list:
    results = quandl.get_series(item[1])
    results = results.replace(to_replace='NaN',value='.')
    data = DataFrame({'value': results})
    print item
    df.write_points({item[0]:data})
