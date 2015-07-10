'''
Created on Jul 7, 2015

@author: shaunz
'''
from urllib2 import urlopen, HTTPError
import json
from pandas import to_datetime, DataFrame, Series

class QuandlAPI(object):
    
    def __init__(self):
        self.Root = "https://www.quandl.com/api/v1/"
        
    def get(self, data):
        URL = "%sdatasets/%s.json" % (self.Root, data)
        try:
            response = urlopen(URL)
            results = json.loads(response.read())
            
            points = {}
            for point in results['data']:
                date = to_datetime(point[0], format='%Y-%m-%d')
                if hasattr(date, 'to_datetime'):
                    date = date.to_datetime()
                    points[date]=point[1]
            
            points = DataFrame({'value':Series(points)})
        except HTTPError as exc:
            message = json.loads(exc.read())
            raise ValueError("For %s, %s" % (data, message['error']))
            
        return points