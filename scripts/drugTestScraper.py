''' drugTestScraper.py '''
import io
import ssl
# import json
import requests
import numpy as np
import pandas as pd
import urllib.request

# bug fix for SSL self-signed cert error
ssl._create_default_https_context = ssl._create_unverified_context

### VARS ###
url = './data/source/drugcheckingbc-sept2022.csv'


### FUNCTIONS ###

def fetchFile(url):
     # fetch the url
    req = urllib.request.Request(url)
    remote_file = urllib.request.urlopen(req).read()
    memory_file = io.BytesIO(remote_file)

    return memory_file

def init(url):
    df = pd.read_csv(url)

    # we're only interested in results from benzo/fenty test strips
    df.loc[df['Benzo Test Strip'] == 'Positive', 'results'] = 'Benzodiazepines'
    df.loc[df['Fentanyl Test Strip'] == 'Positive', 'results'] = 'Fentanyl'
    df.loc[(df['Benzo Test Strip'] == 'Positive') & (df['Fentanyl Test Strip'] == 'Positive'), 'results'] = 'Both'
    df.loc[(df['Benzo Test Strip'] != 'Positive') & (df['Fentanyl Test Strip'] != 'Positive'), 'results'] = 'Neither'

    # group the results & create a 'count' column
    df_grp = pd.DataFrame({'count': df.groupby(['Date', 'results']).size()}).reset_index()
    
    # pivot wider for output
    df_wide = pd.pivot(df_grp, index='Date', columns='results', values='count')

    # NaN should be zero
    df_wide.fillna(value = 0, inplace=True)
    print(df_wide)
    
    # write to file
    df_wide.to_csv('./data/drug-test-results.csv')
    

    # memory_file = fetchFile(url)

    # print(memory_file)

init(url)