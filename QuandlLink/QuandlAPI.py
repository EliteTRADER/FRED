'''
Created on Jul 7, 2015

@author: shaunz
'''
from urllib2 import urlopen, HTTPError
import json
from pandas import to_datetime, DataFrame, Series
from pandas.tools.merge import concat

class QuandlAPI(object):
    
    def __init__(self):
        self.Root = "https://www.quandl.com/api/v1/"
        
    def get_series(self, data, column=None, column_label=None):
        '''
        Get Quandl series
        ------
        column: list
            the list of columns to get from Quandl
        ------
        column_label: list
            the corresponding labels for each columns retrieved from Quandl
        ------
        '''
        if(len(column) != 0 and len(column)==len(column_label)):
            all_data = []
            for i, item in enumerate(column):
                URL = "%sdatasets/%s.json?column=%d&auth_token=otf6VxzVxjm5ZGLztqbG" % (self.Root, data, item)
                try:
                    response = urlopen(URL)
                    results = json.loads(response.read())
                    points = {}
                    for point in results['data']:
                        date = to_datetime(point[0], format='%Y-%m-%d')
                        if hasattr(date, 'to_datetime'):
                            date = date.to_datetime()
                            points[date]=point[1]
                    data_label = column_label[i]
                    points = DataFrame({data_label:Series(points)})
                    all_data.append(points)
                except HTTPError as exc:
                    print exc.read()
                    message = json.loads(exc.read())
                    raise ValueError("For %s, %s" % (data, message['error']))
            
        points = concat(all_data, axis=1, join='outer')
        return points