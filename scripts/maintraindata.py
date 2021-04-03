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
print(cams)
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
co = xr.DataArray(
    cams.co_conc.values,
    dims=['time','latitude','longitude'],
    coords = {
        'time':dates.to_period('d').unique(),
        'latitude':cams.coords['latitude'].values,
        'longitude':cams.coords['longitude'].values
    }
)
o3 = xr.DataArray(
    cams.o3_conc.values,
    dims=['time','latitude','longitude'],
    coords = {
        'time':dates.to_period('d').unique(),
        'latitude':cams.coords['latitude'].values,
        'longitude':cams.coords['longitude'].values
    }
)
pm10 = xr.DataArray(
    cams.pm10_conc.values,
    dims=['time','latitude','longitude'],
    coords = {
        'time':dates.to_period('d').unique(),
        'latitude':cams.coords['latitude'].values,
        'longitude':cams.coords['longitude'].values
    }
)
# recreate Dataset (without dask)
cams = xr.Dataset({'pm25': pm25, 'no2': no2, 'o3': o3, 'co':co, 'pm10':pm10})
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

# Compute the engineered features: 7 trailing day averages of gas' concentrations
df = covid
df["time"]=pd.to_datetime(df["time"])
print (df)
print (df.columns)
avgpm25 = []
avgno2 = []
avgo3 = []
avgco = []
avgpm10 = []
hospi = []
for i in df.index:
    date0 = df.loc[i,"time"]
    depnum = df.loc[i,"numero"]
    date1 = date0 -pd.Timedelta("1 days")
    date2 = date0 -pd.Timedelta("2 days")
    date3 = date0 -pd.Timedelta("3 days")
    date4 = date0 -pd.Timedelta("4 days")
    date5 = date0 -pd.Timedelta("5 days")
    date6 = date0 -pd.Timedelta("6 days")
    dayminus1tothospi  = df[(df["time"]== date1) & (df["numero"]==depnum)].reset_index()["hospi"]

    if list(dayminus1tothospi)==[]: 
        hospi.append("NaN") 
    else:
        hospi.append(list(dayminus1tothospi)[0])

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
    avgO3 = ((day0data["o3"] + day1data["o3"]\
                               + day1data["o3"]\
                               + day2data["o3"]\
                               + day3data["o3"]\
                               + day4data["o3"]\
                               + day5data["o3"])/7)
    avgCO = ((day0data["co"] + day1data["co"]\
                               + day1data["co"]\
                               + day2data["co"]\
                               + day3data["co"]\
                               + day4data["co"]\
                               + day5data["co"])/7)
    avgPM10 = ((day0data["pm10"] + day1data["pm10"]\
                               + day1data["pm10"]\
                               + day2data["pm10"]\
                               + day3data["pm10"]\
                               + day4data["pm10"]\
                               + day5data["pm10"])/7)
    if list(avgPM25)==[]: 
        avgpm25.append("NaN") 
    else:
        avgpm25.append(list(avgPM25)[0])
    
    if list(avgNO2)==[]: 
        avgno2.append("NaN") 
    else:
        avgno2.append(list(avgNO2)[0])
    
    if list(avgO3)==[]: 
        avgo3.append("NaN") 
    else:
        avgo3.append(list(avgO3)[0])
    
    if list(avgPM10)==[]: 
        avgpm10.append("NaN") 
    else:
        avgpm10.append(list(avgPM10)[0])
    
    if list(avgCO)==[]: 
        avgco.append("NaN") 
    else:
        avgco.append(list(avgCO)[0])



tothospi = pd.DataFrame(hospi)
tothospi.columns =["hospiprevday"]

avgpm25df = pd.DataFrame(avgpm25)
avgpm25df.columns=["pm257davg"]

avgno2df = pd.DataFrame(avgno2)
avgno2df.columns=["no27davg"]

avgo3df = pd.DataFrame(avgo3)
avgo3df.columns=["o37davg"]

avgpm10df = pd.DataFrame(avgpm10)
avgpm10df.columns=["pm107davg"]

avgcodf = pd.DataFrame(avgco)
avgcodf.columns=["co7davg"]

df["hospiprevday"]=tothospi["hospiprevday"]
df["pm257davg"]=avgpm25df["pm257davg"]
df["no27davg"]=avgno2df["no27davg"]
df["o37davg"]=avgo3df["o37davg"]
df["pm107davg"]=avgpm10df["pm107davg"]
df["co7davg"]=avgcodf["co7davg"]
print(df)

df.to_csv("Enriched_Covid_history_data.csv", index = False)

# Get Mobility indices historcial data and merge it by time & region with the rest of the data
#  and export it to the Enriched_Covid_history_data.csv 
df = pd.read_csv("mouvement-range-FRA-final.csv", sep = ';')
df2 = pd.read_csv("Enriched_Covid_history_data.csv", sep = ",")
df2 = df2.dropna()

df3 = pd.read_csv("regions_departements.csv", sep = ";")

mdlist = []

df.reset_index(inplace=  True)
df2.reset_index(inplace = True)
df3.reset_index(inplace = True)
df.drop(columns = ["index"],inplace = True)
df2.drop(columns = ["index"],inplace = True)
df3.drop(columns = ["index"],inplace = True)
df["ds"]=pd.to_datetime(df["ds"])
df2["time"]=pd.to_datetime(df2["time"])
print(df)
print(df2)
print(df3)
df3['depnum'] = df3['depnum'].replace({'2A':'201','2B':'202'}).astype(int)
df2 = df2.merge(df3,left_on = "numero", right_on = "depnum")
df2 = df2.merge(df, left_on = ["time","Region"], right_on = ["ds","polygon_name"])
df2.to_csv("Enriched_Covid_history_data.csv", index = False)
print(df2)

print('OK')
