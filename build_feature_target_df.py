import datetime
import numpy as np
import pandas as pd
import xarray as xr

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