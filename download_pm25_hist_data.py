import os
import datetime
import cdsapi
import yaml
import numpy as np
import pandas as pd
from tqdm import tqdm

work_dir = os.path.dirname(os.path.abspath(__file__))
save_to = os.path.join(work_dir, 'data/pm25/')
if not os.path.exists(save_to):
    os.makedirs(save_to)

# get personal directory of cdsapi
try:
    with open('.cdsapirc_dir', 'r') as file:
        dir_cams_api = file.readline().rstrip()
except OSError:
    raise OSError("cdsapirc file cannot be found. Set the directory in .cdsapirc_dir")

print('Download data from cams ...', flush=True)
df = pd.read_csv("calendar.csv", sep = ";")
for date in tqdm(df["Dates"]):
    referencedate = datetime.datetime.strptime(date, '%Y-%m-%d')
    referencedatestring = referencedate.strftime("%Y-%m-%d")
    file_name = 'pm25-{:}.nc'.format(referencedatestring)

    with open(dir_cams_api, 'r') as f:
            credentials = yaml.safe_load(f)

    c = cdsapi.Client(url=credentials['url'], key=credentials['key'])
    c.retrieve(
        'cams-europe-air-quality-forecasts',
        {
            'model': 'ensemble',
            'date': referencedatestring,
            'area': [51.75, -5.83, 41.67,11.03,],
            'format': 'netcdf',
            'variable': 'particulate_matter_2.5um',
            'level': '0',
            'type': 'forecast',
            'time': '00:00',
            'leadtime_hour': list(range(0,96,4)),
        },
        save_to + file_name)

    print('OK', flush=True)
