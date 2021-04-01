#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# Imports
# =============================================================================
import pandas as pd
import re

# =============================================================================
# Functions
# =============================================================================
def parse_dsm(coord):
    deg, min, sec, dir = re.split('[°\'"]', coord)
    dd = float(deg) + (float(min)/60) + (float(sec)/60/60)
    if (dir == 'W') | (dir == 'S'):
        dd *= -1
    return dd
# =============================================================================
# Data
# =============================================================================
population  = pd.read_csv('./departements-francais.csv', sep=';')
population.columns = ['dep_num', 'name', 'region', 'capital', 'area', 'total', 'density']
population['dep_num'] = population['dep_num'].replace({'2A':'201','2B':'202'}).astype(int)
population = population.sort_values('dep_num')
population = population[:-5]


dep_centre = pd.read_excel(
    './Centre_departement.xlsx',
    engine='openpyxl', header=1, usecols=[0,1,2,3,4])
dep_centre.columns = ['dep_num','name','area', 'lon', 'lat']
dep_centre['dep_num'] = dep_centre['dep_num'].replace({'2A':'201','2B':'202'}).astype(int)
dep_centre = dep_centre.sort_values('dep_num')
dep_centre['lon'] = dep_centre['lon'].apply(lambda x: parse_dsm(x))
dep_centre['lat'] = dep_centre['lat'].apply(lambda x: parse_dsm(x))
dep_centre = dep_centre.merge(population, on=['dep_num'], how='outer')
dep_centre = dep_centre.drop(columns=['name_x', 'area_x', 'region'])
dep_centre.columns = ['dep_num','lon','lat','name','captial','area','total','density']

dep_centre.to_csv('./population_2020.csv', index=False)
