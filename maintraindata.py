#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# Imports #%%
# =============================================================================
#part| #%%
import sys
import datetime
import numpy as np
import pandas as pd
import xarray as xr
from datetime import timedelta


# =============================================================================
# Functions #%%
# =============================================================================

def max_normalize(x):
    return (x - x.min()) / (x.max() - x.min())

# =============================================================================
# Merge data #%%
# =============================================================================

# Population
# -----------------------------------------------------------------------------
print('Population ... ', flush=True, end='')
population  = pd.read_csv('./data/pop/fr/population_2020.csv')

# Population Index
# Min-Max-normalized values of the log10 transformation
population['idx'] = max_normalize(np.log10(population['total']))
population.reset_index(inplace = True, drop=True)
print('OK', flush=True)


# Covid #%%
# -----------------------------------------------------------------------------
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
# covid = covid.groupby('dep').rolling(window=7).mean()
# covid = covid.groupby(level=0).tail(1).reset_index(drop=True)

# add lon/lat + population index to covid dataframe
covid = covid.merge(population, how='inner', left_on='numero', right_on='dep_num')
print('OK', flush=True)

# CAMS #%%
# -----------------------------------------------------------------------------
start_date, end_date = ['2020-01-01','2021-04-01']
dates = pd.date_range(start_date, end_date, freq='h')[:-1]

print('CAMS ... ', flush=True, end='')
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
    population['lon'],
    dims='dep_num',
    coords={'dep_num':population['dep_num']},
    name='lon')
lats = xr.DataArray(
    population['lat'],
    dims='dep_num',
    coords={'dep_num':population['dep_num']},
    name='lat')

cams = cams.interp(longitude=lons, latitude=lats)
cams = cams.to_dataframe().reset_index('dep_num')
cams.index = cams.index.to_timestamp()
cams = cams.reset_index()
cams.columns
covid = covid.rename(columns = {'date':'time'})
covid = covid.merge(cams, how='inner', on=['time','dep_num'])

df = covid
df["time"]=pd.to_datetime(df["time"])
print (df)
print (df.columns)
avgpm25 = []
avgno2 = []
for i in df.index:
    date0 = df.loc[i,"time"]
    depnum = df.loc[i,"numero"]
    date1 = date0 -pd.Timedelta("1 days")
    date2 = date0 -pd.Timedelta("2 days")
    date3 = date0 -pd.Timedelta("3 days")
    date4 = date0 -pd.Timedelta("4 days")
    date5 = date0 -pd.Timedelta("5 days")
    date6 = date0 -pd.Timedelta("6 days")
    day0data = df.loc[i]
    day1data = df[(df["time"]== date1) & (df["numero"]==depnum)].reset_index()
    day2data = df[(df["time"]== date2) & (df["numero"]==depnum)].reset_index()
    day3data = df[(df["time"]== date3) & (df["numero"]==depnum)].reset_index()
    day4data = df[(df["time"]== date4) & (df["numero"]==depnum)].reset_index()
    day5data = df[(df["time"]== date5) & (df["numero"]==depnum)].reset_index()
    day6data = df[(df["time"]== date6) & (df["numero"]==depnum)].reset_index()

    avgPM25 = ((day0data["pm25"] + day1data["pm25"]\
                               + day1data["pm25"]\
                               + day2data["pm25"]\
                               + day3data["pm25"]\
                               + day4data["pm25"]\
                               + day5data["pm25"])/7)
    avgNO2 = ((day0data["no2"] + day1data["no2"]\
                               + day1data["no2"]\
                               + day2data["no2"]\
                               + day3data["no2"]\
                               + day4data["no2"]\
                               + day5data["no2"])/7)
    if list(avgPM25)==[]: 
        avgpm25.append("NaN") 
    else:
        avgpm25.append(list(avgPM25)[0])
    
    if list(avgNO2)==[]: 
        avgno2.append("NaN") 
    else:
        avgno2.append(list(avgPM25)[0])


avgpm25df = pd.DataFrame(avgpm25)
avgpm25df.columns=["pm257davg"]

avgno2df = pd.DataFrame(avgno2)
avgno2df.columns=["no27davg"]

df["pm257davg"]=avgpm25df["pm257davg"]
df["no27davg"]=avgno2df["no27davg"]

print(df)
df.to_csv("Enriched_Covid_history_data.csv", index = False)


print('OK')
