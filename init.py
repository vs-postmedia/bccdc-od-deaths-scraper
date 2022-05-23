from tableauscraper import TableauScraper as TS
import pandas as pd
# import numpy as np

# NOTES
# rate is per 100k population

# VARS
ha_sex = pd.DataFrame()
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
    # print(ha)
    # set filter value
    wb = ws.setFilter('HA Name1', ha)

    # show new data for the worksheet
    filtered = wb.getWorksheet('BCCS Deaths Sex-Age')
    # print(type(filtered.data))
    df = pd.DataFrame(data = filtered.data)
    # drop alias columns & add ha
    df = df.drop(df.columns[[1,4]], axis=1)
    df = df.assign(health_authority = ha)
    
    # save dataframe to file
    # print(df)
    ha_sex = ha_sex.append(df, ignore_index=True)

# rename columns
ha_sex = ha_sex.rename(columns={'BCCS Date (C Months)-value':'date','AGG(Rate)-value':'rate', 'Breakdown by-alias':'gender'})

# pivot wider by sex
df2 = ha_sex.pivot(index=['date','health_authority'],columns='gender', values='rate')
df2 = df2.rename(columns={'Male':'Men','Female':'Women'})

# write csv file
df2.to_csv('./data/deaths-by-sex.csv')


# print('DONE!')``