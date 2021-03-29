#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import datetime
import cdsapi
import yaml
import numpy as np

work_dir = os.path.dirname(os.path.abspath(__file__))
save_to = os.path.join(work_dir, 'data/pm25/')
if not os.path.exists(save_to):
    os.makedirs(save_to)

print('Download data from cams ...', flush=True)
dir_cams_api = '/home/nrieger/.copernicus/atmosphere/.cdsapirc'

current_date = datetime.datetime.today().strftime('%Y-%m-%d')
file_name = 'pm25-{:}.nc'.format(current_date)

with open(dir_cams_api, 'r') as f:
        credentials = yaml.safe_load(f)

c = cdsapi.Client(url=credentials['url'], key=credentials['key'])
c.retrieve(
    'cams-europe-air-quality-forecasts',
    {
        'model': 'ensemble',
        'date': current_date,
        'format': 'netcdf',
        'variable': 'particulate_matter_2.5um',
        'level': '0',
        'type': 'forecast',
        'time': '00:00',
        'leadtime_hour': list(range(0,97,1)),
    },
    save_to + file_name)

print('OK', flush=True)
