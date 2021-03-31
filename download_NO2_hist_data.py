import os
import datetime
import cdsapi
import yaml
import numpy as np
import pandas as pd
from tqdm import tqdm

work_dir = os.path.dirname(os.path.abspath(__file__))
save_to = os.path.join(work_dir, 'data/NO2/')
if not os.path.exists(save_to):
    os.makedirs(save_to)

print('Download data from cams ...', flush=True)
dir_cams_api = '/home/ludo915/.cdsapirc'
df = pd.read_csv("calendar.csv", sep = ";")
for date in tqdm(df["Dates"]):
    print(date)
    referencedate = datetime.datetime.strptime(date, '%Y-%m-%d')
    referencedatestring = referencedate.strftime("%Y-%m-%d")
    file_name = 'NO2-{:}.nc'.format(referencedatestring)

    with open(dir_cams_api, 'r') as f:
            credentials = yaml.safe_load(f)

    c = cdsapi.Client(url=credentials['url'], key=credentials['key'])
    import cdsapi

    c = cdsapi.Client()

    c.retrieve(
        'cams-europe-air-quality-forecasts',
        {
            'variable': 'nitrogen_dioxide',
            'model': 'ensemble',
            'level': '0',
            'date': referencedatestring,
            'type': 'forecast',
            'time': '00:00',
            'leadtime_hour': list(range(0,96,4)),
            'area': [
                51.75, -5.83, 41.67,
                11.03,
            ],
            'format': 'netcdf',
        },
        save_to + file_name)

print('OK', flush=True)
