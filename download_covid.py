#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import datetime
import requests

work_dir = os.path.dirname(os.path.abspath(__file__))
save_to = os.path.join(work_dir, 'data/covid/fr/')
if not os.path.exists(save_to):
    os.makedirs(save_to)

print('Download data from data.gouv.fr ...', flush=True, end='')
current_date = datetime.datetime.today().strftime('%Y-%m-%d')
file_name = 'covid-{:}.csv'.format(current_date)
url = "https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7"
r = requests.get(url, allow_redirects=True)

open(save_to + file_name, 'wb').write(r.content)

print('OK', flush=True)
