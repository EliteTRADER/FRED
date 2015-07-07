'''
Created on Jul 2, 2015

@author: shaunz
'''
from influxdb.influxdb08 import DataFrameClient
from pandas import DataFrame
from FredAPI import FredLink
from FredLink.TickerList import ticker_list

df = DataFrameClient('localhost', 8086, 'root', 'root', 'FRED')
fred = FredLink()

for series in df.get_list_series():
    df.delete_series(series)

for item in ticker_list:
    results = fred.get_series(item[1])
    results = results.replace(to_replace='NaN',value='.')
    data = DataFrame({'value': results})
    print data
    df.write_points({item[0]:data})