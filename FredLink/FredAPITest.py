'''
Created on Jul 2, 2015

@author: shaunz
'''
from influxdb.influxdb08 import DataFrameClient
from pandas import DataFrame
from FredAPI import FredLink
from FredLink.FredTicker import Fred_ticker_list
import timeit

fred = FredLink()
df = DataFrameClient('localhost', 8086, 'root', 'root')
if({'name':'FRED'} not in df.get_list_database()):
    df.create_database('FRED')

df.switch_database('FRED')

for series in df.get_list_series():
    df.delete_series(series)

start = timeit.default_timer()
for item in Fred_ticker_list:
    results = fred.get_series(item[1])
    results = results.replace(to_replace='NaN',value='.')
    data = DataFrame({'value': results})
    print item
    df.write_points({item[0]:data})
print 'total time in seconds: %.2f' % (timeit.default_timer() - start)