from tableauscraper import TableauScraper as TS
import pandas as pd
# import numpy as np

# NOTES
# rate is per 100k population

# VARS
df_all = pd.DataFrame()
haList = ['All BC', 'Interior', 'Fraser', 'Vancouver Coastal', 'Vancouver Island', 'Northern']
url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/IllicitOverdoseDeathsIndicator'

# GET TO WORK!
ts = TS()
ts.loads(url)
# workbook = ts.getWorkbook()
# ws = ts.getWorksheet('BCCS Deaths Rate')
ws = ts.getWorksheet('BCCS Deaths Sex-Age')


# get filter columns & values
filters = ws.getFilters()
# print(filters)

for ha in haList:
    # set filter value
    wb = ws.setFilter('HA Name1', ha, filterDelta=True)

    # show new data for the worksheet
    filtered = wb.getWorksheet('BCCS Deaths Sex-Age')
    
    # create pandas dataframe
    df = pd.DataFrame(data = filtered.data)

    # print(ha)
    # print(df)
    
    # drop alias columns & add ha
    df = df.drop(df.columns[[1,4]], axis=1)
    df = df.assign(health_authority = ha)

    # text cleanup
    df['health_authority'] = df['health_authority'].str.replace('All BC', 'All B.C.')
    df['health_authority'] = df['health_authority'].str.replace('Vancouver Island', 'Island')
    df['health_authority'] = df['health_authority'].str.replace('Vancouver Coastal', 'VCH')
    
    # merge datasets
    df_all = pd.concat([df_all, df])

# rename columns
df_all = df_all.rename(columns={'BCCS Date (C Months)-value':'date','AGG(Rate)-value':'rate', 'Breakdown by-alias':'gender'})

# pivot wider by sex & make colheds reader friendly
df2 = df_all.pivot(index=['date','health_authority'],columns='gender', values='rate')
df2 = df2.rename(columns={'Male':'Men','Female':'Women'})

# write csv file
df2.to_csv('./data/deaths-by-sex.csv')


# print('DONE!')