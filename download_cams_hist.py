import os
import datetime
import cdsapi
import yaml
import numpy as np
import pandas as pd
from tqdm import tqdm


work_dir = os.path.dirname(os.path.abspath(__file__))
save_to = os.path.join(work_dir, 'data/cams/')
if not os.path.exists(save_to):
    os.makedirs(save_to)

# get personal directory of cdsapi
try:
    with open('.cdsapirc_dir', 'r') as file:
        dir_cams_api = file.readline().rstrip()
except FileNotFoundError:
    raise FileNotFoundError("""cdsapirc file cannot be found. Write the
        directory of your personal .cdsapirc file in a local file called
        `.cdsapirc_dir` and place it in this git directory.""")

# Download CAMS
# -----------------------------------------------------------------------------
print('Download data from CAMS ...', flush=True)

with open(dir_cams_api, 'r') as f:
    credentials = yaml.safe_load(f)

dates = pd.date_range(
    start="2020-01-01",
    end='2021-03-31'
    ).strftime("%Y-%m-%d").tolist()

variables = [
    'carbon_monoxide',
    'nitrogen_dioxide',
    'ozone',
    'particulate_matter_2.5um',
    'particulate_matter_10um',
]

for date in tqdm(dates):
    file_name = 'cams-{:}.nc'.format(date)
    output = os.path.join(save_to,file_name)
    if not os.path.exists(output):
        c = cdsapi.Client(url=credentials['url'], key=credentials['key'])
        c.retrieve(
            'cams-europe-air-quality-forecasts',
            {
                'model': 'ensemble',
                'date': date,
                'area': [51.75, -5.83, 41.67,11.03,],
                'format': 'netcdf',
                'variable': variables,
                'level': '0',
                'type': 'forecast',
                'time': '00:00',
                'leadtime_hour': list(range(0,96,4)),
            },
            output
        )

print('Download finished.', flush=True)
