#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 19:11:58 2018
Modified on 02/22/2019 for version 0.1.0
Modified on 02/04/2021 to simplify the logic and make better use of Pandas methods

@author: Bob Hentz

-------------------------------------------------------------------------------
  Name:        NasaData.py
  Purpose:     Retrieve Site specific information from Nasa Power Site using https request
               protocol
               
            Sample SinglePoint Data Request: 
            https://asdc-arcgis.larc.nasa.gov/cgi-bin/power/v1beta/DataAccess.py?
            request=execute&identifier=SinglePoint&parameters=T2M,PS,
            ALLSKY_SFC_SW_DWN&
            startDate=20160301&endDate=20160331&
            userCommunity=SSE&tempAverage=DAILY&
            outputList=JSON,ASCII&lat=36&lon=45&user=anonymous

  Copyright:   (c) Bob Hentz 2018
  License:     GNU General Public License, version 3 (GPL-3.0)

               This program is distributed WITHOUT ANY WARRANTY;
              without even the implied warranty of MERCHANTABILITY
              or FITNESS FOR A PARTICULAR PURPOSE.
 -------------------------------------------------------------------------------
"""

import datetime as dt
import json
import pandas as pd
import requests
import urllib

from collections import namedtuple



""" See https://power.larc.nasa.gov/docs/services/api/ for POWER API"""
NASA_host = "power.larc.nasa.gov"


def buildUrl(host:str, path:str, parameters:dict, fragment:str = None) -> str:
    query_string = urllib.parse.urlencode(parameters)
    components=['https', host, path, query_string, fragment]
    return urllib.parse.urlunsplit(components)


def getLocationData(dtin):
    """ Retrieves the NASA Location data from the request response
        Returns tuple of form (Lon, Lat, Elev)  """
    response = json.loads(dtin)
    try:
        coords=response['geometry']['coordinates']
        print(f"found location elevation, z={coords[2]}")
        # NASA uses -999 as NaN
        if (coords[2] and coords[2] != -999):
            return tuple(coords)
        else:
            return (None, None, None)
    except KeyError:
        print("Failed to parse elevation response")
        return (None, None, None)


def getSiteElevation(lat, lon):
    """ Request local elevation in meters (datum unspecified, but probably
    elevation above Mean Sea Level) from NASA POWER API version 2. Following
    a NASA example, the request queries parameter "T2M" (Temperature 2 meters
    above ground level, then parses only the [geometry] portion of the the
    response.
    """
    apipath = '/api/temporal/daily/point'
    query_parameters = dict()
    query_parameters["parameters"]='T2M'
    # use fixed historical date to avoid missing current data
    query_parameters["start"]="20230101"
    query_parameters["end"]="20230101"
    query_parameters["community"]='sb'
    query_parameters["format"]='json'
    query_parameters["latitude"]=lat
    query_parameters["longitude"]=lon
    query_parameters["user"]='solarpv_simulator'
    url = buildUrl(NASA_host, apipath, query_parameters)
    # Request NASA Data from API
    print(f"Trying to get data from NASA at {url}")
    try:
        data = requests.get(url).text
        return getLocationData(data)
    except requests.exceptions.ConnectionError:
        return (None, None, None)
    
def formulateRequest(lat, lon, selectparms= None):
    """ Formulate a request from NASA API for 10 years of atmospheric data 
        required to prepare daily statistical data used in Solar Insolation
        calculations """
    stdparms = [('T10M','Temperature @ 10m (c)'),
                ('T10M_MAX', 'Max Daily Temperature (c)'),
                ('T10M_MIN', 'Min Daily Temperature (c)'),
                ('WS10M','Surface Wind Speed (m/s)'),
                ('WS10M_MAX','Max Daily Wind Speed (m/s)'),
                ('WS10M_MIN','Min Daily Wind Speed (m/s)')
               ]
    now = dt.date.today()
    baseyear = now.year-1
    startdate='{0}0101'.format(baseyear-9)
    enddate ='{0}1231'.format(baseyear)
    #  build request parameters
    parms = []
    for itm in stdparms:
        if selectparms == None or itm[0] in selectparms:
            parms.append(itm[0])
    # to csv
    reqparms = ",".join(str(param) for param in parms)

    apipath = '/api/temporal/daily/point'
    query_parameters = dict()
    query_parameters["parameters"]=reqparms
    query_parameters["start"]=startdate
    query_parameters["end"]=enddate
    query_parameters["community"]='sb'
    query_parameters["format"]='json'
    query_parameters["latitude"]=lat
    query_parameters["longitude"]=lon
    query_parameters["user"]='anonymous'
    url = buildUrl(NASA_host, apipath, query_parameters)
    return (url, reqparms.split(','))


def LoadNasaData(lat, lon, show= False, selectparms= None): 
    """ Execute a request from NASA API for 10 years of atmospheric data 
        required to prepare daily statistical data used in Solar Insolation
        calculations """
    cmd = formulateRequest(-0.2739, 36.3765, selectparms)
    print(f"cmd is {json.dumps(cmd)}")
    jdi = requests.get(cmd[0]).json()
    cols = cmd[1]
    df = pd.json_normalize(jdi['properties']['parameter'][cols[0]]).T
    df.index = pd.to_datetime(df.index)
    df.rename(columns={0: cols[0]}, inplace= True)
    for c in cols[1:]:
        dfc = pd.json_normalize(jdi['properties']['parameter'][c]).T
        dfc.index = pd.to_datetime(df.index)
        dfc.rename(columns={0: c}, inplace= True)
        df = df.join(dfc)
    df['DayofYear'] = df.index.dayofyear
    df = df[df['DayofYear'] != 366]  #drop a day for leap years
    atmo_dict = dict()
    dg = df.groupby('DayofYear')
    for col in cols:
        dp = pd.DataFrame(dg[col].min())
        dp.rename(columns={col: 'Min'}, inplace= True)
        atmo_dict[col] = dp
        dp = pd.DataFrame(dg[col].max())
        dp.rename(columns={col: 'Max'}, inplace= True)
        atmo_dict[col] = atmo_dict[col].join(dp)
        dp = pd.DataFrame(dg[col].mean())
        dp.rename(columns={col: 'S-Mean'}, inplace= True)
        atmo_dict[col] = atmo_dict[col].join(dp)
        dp = pd.DataFrame(dg[col].std())
        dp.rename(columns={col: 'STDV'}, inplace= True)
        atmo_dict[col] = atmo_dict[col].join(dp)       
    for key, value in atmo_dict.items():
        print(f"key = {key}")
        print(value.to_string())
    return atmo_dict


def main():

#    find_parms = ['ALLSKY_SFC_SW_DWN', 'PS']
    d_dict = LoadNasaData(-0.2739, 36.3765, show = False) 
    tav = d_dict['T10M']['S-Mean'].values
    tmx = d_dict['T10M_MAX']['S-Mean'].values
    tmn = d_dict['T10M_MIN']['S-Mean'].values
    wav = d_dict['WS10M']['S-Mean'].values
    wmx = d_dict['WS10M_MAX']['S-Mean'].values
    wmn = d_dict['WS10M_MIN']['S-Mean'].values
    
    for i in range(10):
        st = 'Day: {0}\tAvg Temp: {1:.2f}\tMax Temp: {2:.2f}\tMin Temp: {3:.2f}\n'.format(i, tav[i], tmx[i], tmn[i])
        sw = '\tAvg WS: {0:.2f}\tMax WS: {1:.2f}\tMin WSp: {2:.2f}'.format(wav[i], wmx[i], wmn[i])
        st += sw
        print(st)


if __name__ == '__main__':
    main()    
