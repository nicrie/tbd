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

# =============================================================================
# Functions
# =============================================================================

def max_normalize(x):
    return (x - x.min()) / (x.max() - x.min())

# =============================================================================
# Merge all data
# =============================================================================


# Population
# -----------------------------------------------------------------------------
  #section| #%%
print('Population ... ', flush=True, end='')

pop         = pd.read_csv('./data/pop/fr/pop.csv', usecols=[0,1,2,3,4,5,6,42])
pop.columns = ['reg', 'dep', 'com', 'article', 'com_nom', 'lon', 'lat', 'total']
pop = pop.reset_index().drop(columns='index')
popDEPgroupby = pop.groupby("dep")
popDEP = pop.copy().groupby('dep').median()
popDEP['total'] = pop.sort_values("dep").groupby('dep').sum()['total']
df2 = pd.DataFrame(popDEPgroupby[['total']].max().sort_values('total', ascending = False)).reset_index()
df2["lon"]=0
df2["lat"]=0
lonlatlist =[]

for total in df2["total"]:
  (dep,lon,lat,tot) = (pop[pop["total"]==total].reset_index().loc[0,['dep']][0], pop[pop["total"]==total].reset_index().loc[0,['lon']][0],pop[pop["total"]==total].reset_index().loc[0,['lat']][0],total)
  lonlatlist.append((dep,lon,lat,tot))
lonlatlistdf = pd.DataFrame(lonlatlist)
lonlatlistdf.columns = ["dep","lon","lat","total"]
lonlatlistdf.sort_values("dep", inplace = True)
lonlatlistdf["total"]= popDEP['total']
popDEP = lonlatlistdf
# Population Index
# Min-Max-normalized values of the log10 transformation
popDEP['idx'] = max_normalize(np.log10(popDEP['total']))
popDEP.reset_index(inplace = True)

del lonlatlistdf
del lonlatlist
del df2
print('OK', flush=True)


#endsection

# Covid
# -----------------------------------------------------------------------------
  #section| #%%
print('Covid ... ', flush=True, end='')
filePath = 'data/'
fileName = 'Covid_data_history.csv'
covid = pd.read_csv(filePath + fileName, sep=',').dropna()
covid['date'] = pd.to_datetime(covid['date'])
# rename departments of la Corse to assure integer
covid['numero'] = covid['numero'].replace({'2A':'201','2B':'202'}).astype(int)
covid['numero'] = covid['numero'].astype(int)

# remove oversea departments
covid = covid[covid['numero']<203]

# take 1-week moving average and take today's values
#covid = covid.groupby('dep').rolling(window=7).mean()
#covid = covid.groupby(level=0).tail(1).reset_index(drop=True)

# add lon/lat + population index to covid dataframe
popSubset = popDEP[['lon','lat','dep','idx']].drop_duplicates(subset=['dep'])
covid = covid.merge(popSubset, how='inner', left_on='numero', right_on='dep')

# extrapolate covid cases from deprtement to commune level
#covidExtraToCom = pop.copy()
#covidExtraToCom['hosp'] = [covid[covid['dep'] == depNum].hosp.values.squeeze() for depNum in covidExtraToCom['dep']]
#covidExtraToCom['idx']  = covidExtraToCom['hosp'].where(covidExtraToCom.hosp>200,(covidExtraToCom.hosp/200)).where(covidExtraToCom.hosp<=200,1)
print('OK', flush=True)
  #endsection

# PM2.5
# -----------------------------------------------------------------------------
  #section| #%%

start_date, end_date = ['2020-01-01','2021-04-01']
dates = pd.date_range(start_date, end_date, freq='h')[:-1]

print('PM2.5 ... ', flush=True, end='')
filePath = './data/train/cams/reanalysis/'
cams = xr.open_mfdataset(
    './data/train/cams/reanalysis/*',
    combine='nested', concat_dim='time',
    parallel=True)
# n_time_steps = cams.coords['time'].size
# dates = dates[:-24]
cams = cams.drop('level').squeeze()
cams = cams.assign_coords(time=dates)
cams = cams.assign_coords(longitude=(((cams['longitude'] + 180) % 360) - 180))
cams = cams.sel(longitude=slice(-10,10),latitude=slice(55,40))
cams = cams.sortby('longitude')

# CAMS is hourly ==> take daily means
cams = cams.resample({'time':'D'}).mean()
# there seems to be a pretty annoying issue with dask.array
# somehow I cannot manage to convert the dask.array to
# a standard xarray.DataArray; unfortunately, xarray.interp()
# seem not yet to work with dask.array; Therefore, as a workaround, I recreate
# a DataArray from scratch to assure that is a standard DataArray and no
# dask.array
# another minor issue here is that this workaround is only possible for each
# variable individually; really annoying....
pm25 = xr.DataArray(
    cams.pm2p5_conc.values,
    dims=['time','latitude','longitude'],
    coords = {
        'time':dates.to_period('d').unique(),
        'latitude':cams.coords['latitude'].values,
        'longitude':cams.coords['longitude'].values
    }
)
no2 = xr.DataArray(
    cams.no2_conc.values,
    dims=['time','latitude','longitude'],
    coords = {
        'time':dates.to_period('d').unique(),
        'latitude':cams.coords['latitude'].values,
        'longitude':cams.coords['longitude'].values
    }
)
# recreate Dataset (without dask)
cams = xr.Dataset({'pm25': pm25, 'no2':no2})
# interpolate CAMS data to lon/lat of departments
lons = xr.DataArray(
    popDEP['lon'],
    dims='dep',
    coords={'dep':popDEP['dep']},
    name='lon')
lats = xr.DataArray(
    popDEP['lat'],
    dims='dep',
    coords={'dep':popDEP['dep']},
    name='lat')

cams = cams.interp(longitude=lons, latitude=lats)
cams = cams.to_dataframe().reset_index('dep')
cams.index = cams.index.to_timestamp()
cams = cams.reset_index()
cams.columns
covid = covid.rename(columns = {'date':'time'})
covid = covid.merge(cams, how='inner', on=['time','dep'])

covid.to_csv("Enriched_Covid_history_data.csv")





print('OK')
  #endsection

#endpart

#endpart
