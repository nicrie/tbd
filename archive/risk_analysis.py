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


grayDark = '#e1e1e1'
grayLight = '#404040'

sns.set(
	context 	= 'paper',
	style 		= 'dark',
	palette 	= 'muted',
	color_codes = True,
    font_scale  = 2.0,
	font 		= 'sans-serif',
	rc={
		'axes.edgecolor'	: grayDark
		,'text.color' 		: grayDark
		,'axes.labelcolor' 	: grayDark
		,'xtick.color' 		: grayDark
		,'ytick.color' 		: grayDark
        ,'figure.facecolor' : grayLight
        ,'axes.facecolor'   : grayLight
        ,'savefig.facecolor': grayLight
		#,'figure.subplot.left' 		: 0.1 # 0.125
		#,'figure.subplot.right' 	: 0.95 # 0.9
		#,'figure.subplot.top' 		: 0.90 # 0.88
		#,'figure.subplot.bottom' 	: 0.11 # 0.11
		#,'figure.subplot.wspace' 	: 0.2 # 0.2
		#,'figure.subplot.hspace' 	: 0.2 # 0.2
		#,'text.usetex':True
		#,'text.latex.preamble':[
		#	r'\usepackage{cmbright}', 	# set font; for Helvetica use r'\usepackage{helvet}'
		#	r'\usepackage{relsize}',
		#	r'\usepackage{upgreek}',
		#	r'\usepackage{amsmath}'
		#	r'\usepackage{siunitx}',
		#	r'\usepackage{physics}',
		#	r'\usepackage{wasysym}', 	# permil symbol in mathmode: \permil
		#	r'\usepackage{textcomp}', 	# permil symbol: \textperthousand
		#	r'\usepackage{mathtools}',
		#	r'\setlength\parindent{0pt}'
		#	]
		}
)

#endpart

# =============================================================================
# Functions
# =============================================================================
#part|
def normalize(x):
    return (x - x.min()) / (x.max() - x.min())

def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()
#endpart

# =============================================================================
# Data
# =============================================================================
print('Data pre-processing:', flush=True)
#part|
countryBorders = cfeature.NaturalEarthFeature(
    category    = 'cultural',
    name        = 'ne_admin_0_countries',
    scale       = '10m',
    facecolor   = 'none'
)

countryEU = regionmask.defined_regions.natural_earth.countries_50

currentDate = datetime.datetime.today().strftime('%Y-%m-%d')

# Population
# -----------------------------------------------------------------------------
  #section| #%%
print('Population ... ', flush=True, end='')
pop         = pd.read_csv('./data/pop/fr/pop.csv', usecols=[0,1,2,3,4,5,6,42])
pop.columns = ['reg', 'dep', 'com', 'article', 'com_nom', 'lon', 'lat', 'total']
popDEP          = pop.copy().groupby('dep').median()
popDEP['total'] = pop.groupby('dep').sum()['total']

# Population Index
# Min-Max-normalized values of the log10 transformation
pop['idx']    = normalize(np.log10(pop['total']))
popDEP['idx'] = normalize(np.log10(popDEP['total']))
print('OK', flush=True)

  #endsection

# Covid
# -----------------------------------------------------------------------------
  #section| #%%
print('Covid ... ', flush=True, end='')
filePath = './data/covid/fr/'
fileName = 'covid-{:}.csv'.format(currentDate)
covid = pd.read_csv(filePath + fileName, sep=';').dropna()
covid = covid[covid.sexe==0].drop(labels='sexe',axis=1)
covid['dep'] = covid['dep'].replace({'2A':'201','2B':'202'}).astype(int)

# take 1-week moving average and take today's values
covid = covid.groupby('dep').rolling(window=7).mean()
covid = covid.groupby(level=0).tail(1).reset_index(drop=True)

popSubset = pop[['lon','lat','dep']].drop_duplicates(subset=['dep'])
covid['lon'] = [popSubset[popSubset['dep']==int(depNum)].lon.values.squeeze() for depNum in covid['dep']]
covid['lat'] = [popSubset[popSubset['dep']==int(depNum)].lat.values.squeeze() for depNum in covid['dep']]
# remove French oversea departments
covid = covid[:-5]

# extrapolate covid cases from deprtement to commune level
covidExtraToCom = pop.copy()
covidExtraToCom['hosp'] = [covid[covid['dep'] == depNum].hosp.values.squeeze() for depNum in covidExtraToCom['dep']]
covidExtraToCom['idx']  = covidExtraToCom['hosp'].where(covidExtraToCom.hosp>200,(covidExtraToCom.hosp/200)).where(covidExtraToCom.hosp<=200,1)
print('OK', flush=True)
  #endsection

# PM2.5
# -----------------------------------------------------------------------------
  #section| #%%
print('PM2.5 ... ', flush=True, end='')
filePath = './data/pm25/'
fileName = 'pm25-{:}.nc'.format(currentDate)
pm25 = xr.open_dataset(filePath + fileName)
pm25 = pm25.pm2p5_conc.drop('level').squeeze()
pm25.coords['longitude'] = (pm25.coords['longitude'] + 180) % 360 - 180
pm25 = pm25.sortby('longitude')
pm25 = pm25.sel(longitude=slice(-10,10),latitude=slice(55,40))
pm25Norm = pm25.where(pm25>10,0).where(pm25<20,1).where(pm25<10, (pm25/10 - 1))
pm25 = pm25Norm.where(pm25Norm<1,0)
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
lons, lats = pop.lon[::takeEvery], pop.lat[::takeEvery]
xrLons = xr.DataArray(lons, dims='com')
xrLats = xr.DataArray(lats, dims='com')
pm25Interpolated = pm25.interp(longitude=xrLons, latitude=xrLats)
#endpart


# =============================================================================
# Risk Assessment
# =============================================================================
#part| #%%
riskMaps = []

for lead in progressbar(range(97), 'Compute risk: ', 60):
    risk = pm25Interpolated[lead] * pop['idx'][::takeEvery] * covidExtraToCom['idx'][::takeEvery]
    risk = risk**(1/3)
    risk = np.vstack((lons,lats,risk)).T
    risk = pd.DataFrame(risk, columns = ['lon', 'lat', 'idx'])

    riskMaps.append(risk)

#endpart



# =============================================================================
# Create Figures
# =============================================================================
#part| #%%
markersize = .1
images = []

for lead in progressbar(range(97), 'Create figures: ', 60):

    fig = plt.figure(figsize=(8,8))
    gs = fig.add_gridspec(1, 1)
    ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
    axes = [ax1]
    ax1.background_patch.set_fill(False)
    for a in axes:
        a.add_geometries(countryEU['F'].polygon, ccrs.PlateCarree(),
        edgecolor=grayDark, lw=2, facecolor=grayDark, alpha=0.6, zorder=0)
        a.set_extent([-5,10,41,52])
        a.set_aspect('auto')
        a.outline_patch.set_linewidth(0.)
        pass

    cax = ax1.scatter(riskMaps[lead].lon,riskMaps[lead].lat,c=riskMaps[lead].idx,
    cmap='RdYlGn_r', s=markersize*5, vmin=0, vmax=.8, zorder=4)
    cbar = fig.colorbar(cax, orientation='horizontal', pad=0, aspect=50,
    fraction=.01, extend='max', drawedges=False, ticks=[0, .8])
    cbar.ax.set_xticklabels(['low', 'high'])
    cbar.ax.xaxis.set_ticks_position('top')
    cbar.ax.xaxis.set_label_position('top')

    ax1.text(0,.0,'Data \nCAMS \ndata.gouv.fr', transform=ax1.transAxes,fontdict={'size':12})

    currentDateWD = datetime.datetime.strptime(currentDate, '%Y-%m-%d').strftime('%a, %d %b %Y')
    ax1.set_title('Risk of severe Covid-19 cases due to PM2.5\n{:}\n+ {:02} h'.format(currentDateWD,lead),
    loc='left', pad=-60)

    fig.subplots_adjust(bottom=.01, left=.01, right=.99, top=.99)

    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', dpi=70)
    buffer.seek(0)
    images.append(imageio.imread(buffer))
    buffer.close()
    plt.close()

#endpart

print('Create gif ...', flush=True, end='')
gifPath = './forecast/fr/'
gifName = 'covid-risk-fc-{:}.gif'.format(currentDate)
kargs = { 'duration': .2 }
imageio.mimwrite(gifPath + gifName, images, 'GIF', **kargs)
print('OK')
print('Finished.')
