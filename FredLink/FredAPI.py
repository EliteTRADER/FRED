'''
Created on Jul 2, 2015

@author: shaunz
'''
from urllib2 import urlopen, HTTPError
import xml.etree.ElementTree as ET
from urllib import urlencode

class FredLink(object):
    
    earliest_realtime_start = '1776-07-04'
    latest_realtime_end = '9999-12-31'
    nan_char = '.'
    max_results_per_request = 1000
    
    def __init__(self, registered_key='d301ce2c048a0a18e4d57587918c6a56'):
        self.api_key = registered_key
    
    def __fetch_data(self, url):
        """
        helper function for fetching data given a request URL
        """
        try:
            response = urlopen(url)
            root = ET.fromstring(response.read())
        except HTTPError as exc:
            root = ET.fromstring(exc.read())
            raise ValueError(root.get('message'))
        return root

    def _parse(self, date_str, format='%Y-%m-%d'):
        """
        helper function for parsing FRED date string into datetime
        """
        from pandas import to_datetime
        rv = to_datetime(date_str, format=format)
        if hasattr(rv, 'to_datetime'):
            rv = rv.to_datetime()
        return rv

    def get_series_info(self, series_id):
        """
        Get information about a series such as its title, frequency, observation start/end dates, units, notes, etc.
        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'
        Returns
        -------
        info : Series
            a pandas Series containing information about the Fred series
        """
        url = "http://api.stlouisfed.org/fred/series?series_id=%s&api_key=%s" % (series_id, self.api_key)
        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No info exists for series id: ' + series_id)
        from pandas import Series
        info = Series(root.getchildren()[0].attrib)
        return info

    def get_series(self, series_id, observation_start=None, observation_end=None, **kwargs):
        """
        Get data for a Fred series id. This fetches the latest known data, and is equivalent to get_series_latest_release()
        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'
        observation_start : datetime or datetime-like str such as '7/1/2014', optional
            earliest observation date
        observation_end : datetime or datetime-like str such as '7/1/2014', optional
            latest observation date
        kwargs : additional parameters
            Any additional parameters supported by FRED. You can see http://api.stlouisfed.org/docs/fred/series_observations.html for the full list
        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s" % (series_id, self.api_key)
        from pandas import to_datetime, Series

        if observation_start is not None:
            observation_start = to_datetime(observation_start, errors='raise')
            url += '&observation_start=' + observation_start.strftime('%Y-%m-%d')
        if observation_end is not None:
            observation_end = to_datetime(observation_end, errors='raise')
            url += '&observation_end=' + observation_end.strftime('%Y-%m-%d')

        if kwargs is not None:
            url += '&' + urlencode(kwargs)

        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No data exists for series id: ' + series_id)
        data = {}
        for child in root.getchildren():
            val = child.get('value')
            if val == self.nan_char:
                val = float('NaN')
            else:
                val = float(val)
            data[self._parse(child.get('date'))] = val
        return Series(data)