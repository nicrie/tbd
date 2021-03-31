#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# Imports
# =============================================================================
#part| #%%
import sys

import datetime
import numpy as np
import pandas as pd
import xarray as xr
import regionmask
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import io
import imageio
import glob
from datetime import timedelta
def normalize(x):
    return (x - x.min()) / (x.max() - x.min())

# Population
# -----------------------------------------------------------------------------
  #section| #%%
print('Population ... ', flush=True, end='')
#pop1 = pd.read_excel ('data/pop/fr/popfr.xls', sheet_name='2020')
#pop2 = pd.read_excel ('data/pop/fr/popfr.xls', sheet_name='2021')
# pop1.columns = ['depnb', 'depname', 'pop']
# pop2.columns = ['depnb', 'depname', 'pop']

# Population Index
# Min-Max-normalized values of the log10 transformation
# pop1['idx']    = normalize(np.log10(pop1['pop']))
# pop2['idx']    = normalize(np.log10(pop2['pop']))
pop         = pd.read_csv('./data/pop/fr/pop.csv', usecols=[0,1,2,3,4,5,6,42])
pop.columns = ['reg', 'dep', 'com', 'article', 'com_nom', 'lon', 'lat', 'total']
popDEP          = pop.copy().groupby('dep').median()
popDEP['total'] = pop.groupby('dep').sum()['total']

# Population Index
# Min-Max-normalized values of the log10 transformation
pop['idx']    = normalize(np.log10(pop['total']))
popDEP['idx'] = normalize(np.log10(popDEP['total']))
popDEP.reset_index(inplace = True)
print(popDEP)
print('OK', flush=True)


#endsection

# Covid
# -----------------------------------------------------------------------------
  #section| #%%
print('Covid ... ', flush=True, end='')
filePath = 'data/'
fileName = 'Covid_data_history.csv'
covid = pd.read_csv(filePath + fileName, sep=',').dropna()
print(covid)
covid['numero'] = covid['numero'].replace({'2A':'201','2B':'202'}).astype(int)

# take 1-week moving average and take today's values
#covid = covid.groupby('dep').rolling(window=7).mean()
#covid = covid.groupby(level=0).tail(1).reset_index(drop=True)

popSubset = popDEP[['lon','lat','dep','idx']].drop_duplicates(subset=['dep'])
covid['lon'] = [popSubset[popSubset['dep']==int(depNum)].lon.values.squeeze() for depNum in covid['numero']]
covid['lat'] = [popSubset[popSubset['dep']==int(depNum)].lat.values.squeeze() for depNum in covid['numero']]
covid['popidx'] = [popSubset[popSubset['dep']==int(depNum)].idx.values.squeeze() for depNum in covid['numero']]
# remove French oversea departments
covid = covid[:-5]

# extrapolate covid cases from deprtement to commune level
#covidExtraToCom = pop.copy()
#covidExtraToCom['hosp'] = [covid[covid['dep'] == depNum].hosp.values.squeeze() for depNum in covidExtraToCom['dep']]
#covidExtraToCom['idx']  = covidExtraToCom['hosp'].where(covidExtraToCom.hosp>200,(covidExtraToCom.hosp/200)).where(covidExtraToCom.hosp<=200,1)
print(covid)
print('OK', flush=True)
  #endsection

# PM2.5
# -----------------------------------------------------------------------------
  #section| #%%
print('PM2.5 ... ', flush=True, end='')
#pm25 = pm25.sortby('longitude')
#pm25 = pm25.sel(longitude=slice(-10,10),latitude=slice(55,40))
#pm25Norm = pm25.where(pm25>10,0).where(pm25<20,1).where(pm25<10, (pm25/10 - 1))
#pm25 = pm25Norm.where(pm25Norm<1,0)
covid["pm25"]=0
dflist =[]
for index, row in covid.iterrows():
    print(row["date"])
    filename = "pm25-"+row["date"]+".nc"
    pm25 = xr.open_dataset("data/pm25/" + filename)
    print(pm25)
    pm25 = pm25.drop('level').squeeze()
    pm25 = pm25.sel(time = timedelta(days = 0, hours = 0, minutes = 0))
    #pm25 = pm25.to_array()
    pm25.coords['longitude'] = (pm25.coords['longitude'] + 180) % 360 - 180
    pm25df = pm25.to_dataframe()
    pm25df = pm25df.reset_index()
    print(pm25df)
    for index2,row2 in pm25df.iterrows():
        if ((np.round(row2["longitude"],2),np.round(row2["latitude"],2)==(np.round(row["lon"],2),np.round(row["lat"],2)))):
            row["pm25"]=row2["pm2p5_conc"]
            break
print(covid)
covid.to_csv("Enriched_Covid_history_data.csv")




# for filename in glob.glob('data/pm25/*.nc'):
#     datefromfile = filename.replace("pm25-","").replace(".nc","").replace('data/pm25/',"")
#     print(datefromfile)
#     pm25 = xr.open_dataset(filename)
#     print(pm25)
#     pm25 = pm25.drop('level').squeeze()
#     pm25 = pm25.drop('date').squeeze()
#     pm25 = pm25.to_array()
#     pm25.coords['longitude'] = (pm25.coords['longitude'] + 180) % 360 - 180
#     pm25df = pm25.to_dataframe(name = "pm25")
#     pm25df["date"]=datefromfile
#     print(pm25df)
#     dflist.append(pm25df)
# counter = 0
# for df in dflist:
#     counter += 1
#     if counter == 1:
#         df1 = df
#     else:
#         frames = [df1, df]
#         df1 = pd.concat(frames)

# df1.to_csv("pm25history.csv")

    
print('OK')
  #endsection

#endpart


# =============================================================================
# Interpolation
# =============================================================================
# We have to interpolate PM2.5 data from the gridded field
# to the irregular grid points of regions/departments/communities
# You may set a filter (takeEvery) which reduces the number of regions taken
# into account if compuational cost is to high
#part| #%%
takeEvery = 1
#lons, lats = pop.lon[::takeEvery], pop.lat[::takeEvery]
#xrLons = xr.DataArray(lons, dims='com')
#xrLats = xr.DataArray(lats, dims='com')
#pm25Interpolated = pm25.interp(longitude=xrLons, latitude=xrLats)
#endpart
