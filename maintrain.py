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

def normalize(x):
    return (x - x.min()) / (x.max() - x.min())

# Population
# -----------------------------------------------------------------------------
  #section| #%%
print('Population ... ', flush=True, end='')
pop1 = pd.read_excel ('data/pop/fr/popfr.xls', sheet_name='2020')
pop2 = pd.read_excel ('data/pop/fr/popfr.xls', sheet_name='2021')
pop1.columns = ['depnb', 'depname', 'pop']
pop2.columns = ['depnb', 'depname', 'pop']

# Population Index
# Min-Max-normalized values of the log10 transformation
pop1['idx']    = normalize(np.log10(pop1['pop']))
pop2['idx']    = normalize(np.log10(pop2['pop']))
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

#popSubset = pop[['lon','lat','dep']].drop_duplicates(subset=['dep'])
#covid['lon'] = [popSubset[popSubset['dep']==int(depNum)].lon.values.squeeze() for depNum in covid['dep']]
#covid['lat'] = [popSubset[popSubset['dep']==int(depNum)].lat.values.squeeze() for depNum in covid['dep']]
# remove French oversea departments
#covid = covid[:-5]

# extrapolate covid cases from deprtement to commune level
#covidExtraToCom = pop.copy()
#covidExtraToCom['hosp'] = [covid[covid['dep'] == depNum].hosp.values.squeeze() for depNum in covidExtraToCom['dep']]
#covidExtraToCom['idx']  = covidExtraToCom['hosp'].where(covidExtraToCom.hosp>200,(covidExtraToCom.hosp/200)).where(covidExtraToCom.hosp<=200,1)
print('OK', flush=True)
  #endsection

# PM2.5
# -----------------------------------------------------------------------------
  #section| #%%
print('PM2.5 ... ', flush=True, end='')
filePath = ''
fileName = 'pm25_history.nc'
pm25 = xr.open_dataset(filePath + fileName)
pm25 = pm25.to_array()
pm25.coords['longitude'] = (pm25.coords['longitude'] + 180) % 360 - 180
pm25 = pm25.sortby('longitude')
pm25 = pm25.sel(longitude=slice(-10,10),latitude=slice(55,40))
#pm25Norm = pm25.where(pm25>10,0).where(pm25<20,1).where(pm25<10, (pm25/10 - 1))
#pm25 = pm25Norm.where(pm25Norm<1,0)
print(pm25)
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
