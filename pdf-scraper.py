import pandas as pd
from tabula import read_pdf
# from geopandas import gpd

# from shapely.geometry import shape
# import json
# from glom import glom


# VARS
current_year = '2021'
file_path = './data/pdf/illicit-drug.pdf'
lha_geo_path = './data/source/lha-2018-epsg4326_7pct.json'
lha_csv_path = './data/deaths-by-lha.csv'
lha_json_path = './data/deaths-by-lha.json'
city_deaths_path = './data/deaths-by-city.csv'
monthly_deaths_path = './data/monthly-deaths.csv'
# why this agent? who knows...
user_agent_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
file_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'

# FUNCTIONS
def scrapeMonthlyDeaths(input_file, csv_output):
    df = read_pdf(input_file, output_format="dataframe", pages='4', stream=True, area=[86,52.5,355,589], user_agent=user_agent_string)

    # drop "total" & "average" rows
    df = df[0].iloc[:-2]
    # drop "subtotal" row
    df.drop(index = df[df['Month'] == 'Subtotal'].index, inplace=True)

    # pivot longer
    df = df.melt(id_vars='Month', value_name='Deaths')
    # make a date column
    df['Date'] = df['Month'].str.cat(df['variable'].values, sep='. ')
    # cleanup dates
    df['Date'] = df['Date'].str.replace('May.', 'May')
    df['Date'] = df['Date'].str.replace('Jun.', 'June')
    df['Date'] = df['Date'].str.replace('Jul.', 'July')

    # drop unused columns & row
    df.drop(['Month', 'variable'], axis=1, inplace=True)
    df.drop(index = df[df['Deaths'] == '-'].index, inplace=True)
    
    # reorder columns
    df = df[['Date', 'Deaths']]
    
    # print(df)
    # write to file
    df.to_csv(csv_output, index=False)



def scrapeCityDeaths(input_file, output_file):
    # read city deaths table from PDF
    df = read_pdf(input_file, output_format="dataframe", pages='11', stream=True, area=[130,52.5,424,588], user_agent=user_agent_string)

    # drop "total" & "other" rows
    df = df[0].iloc[:-2]
    print(df)

    # write csv file
    df.to_csv(output_file, index=False)


def scrapeLHA(input_file, json_output, csv_output):
    # admin boundaries for LHAs
    lha_df = gpd.read_file(lha_geo_path)

    # read LHA tables from PDF
    dfx = read_pdf(input_file, output_format="json", pages="19", stream=True, area=[180,31,715,572])
    
    with open('./data/test.json', 'w') as f:
        json.dump(dfx, f)

    
    # convert json into dataframe
    df1 = pd.json_normalize(dfx, record_path=['data'][0])
    print(df1[0][17])

    df1.to_csv('./data/test.csv')

    

    

    df1 = read_pdf(input_file, output_format="dataframe", pages='19', pandas_options={'header': None, 'names':['LHA_NAME','2016','2017','2018','2019','2020','2021','Deaths this year']}, stream=True, area=[180,31,715,572])
    df2 = read_pdf(input_file, output_format="dataframe", pages='20', pandas_options={'header': None, 'names':['LHA_NAME','2016','2017','2018','2019','2020','2021','Deaths this year']}, stream=True, area=[180,31,400,572])
    
    # drop non-data rows
    df1 = df1[0].iloc[:-16]
    df2 = df2[0]

    df1.to_csv('./data/test.csv')
 
    # text cleanup
    df1['LHA_NAME'] = df1['LHA_NAME'].str.replace('Maple Ridge/Pitt', 'Maple Ridge/Pitt Meadows')
    df2['LHA_NAME'] = df2['LHA_NAME'].str.replace('West Vancouver/ Bowen', 'West Vancouver & Bowen Island')

    df1.drop(index = df1[df1['LHA_NAME'] == 'Meadows'].index, inplace=True)
    df2.drop(index = df2[df2['LHA_NAME'] == 'Island'].index, inplace=True)

    # concat results from all 3 df pages
    df = pd.concat([df1, df2], axis=0)

    lha = ['Gulf']
    print(df1[df1['LHA_NAME'].isin(lha)])

    # merge with LHA boundaries
    df_current = df[['LHA_NAME', current_year, 'Deaths this year']]
    df_geo = lha_df.merge(df_current, on='LHA_NAME', how='left')
    df_geo.fillna('', inplace=True)

    # write geojson file
    df_geo.to_file(json_output, driver='GeoJSON', drop_id=True)

    # rename columns & write CSV file
    df = df.rename(columns={'LHA_NAME':'Local Health Area'})
    df.to_csv(csv_output, index=False)



# AUTOBOTS... ROLL OUT!!!

scrapeMonthlyDeaths(file_url, monthly_deaths_path)

# scrapeLHA(file_path, lha_json_path, lha_csv_path)
# scrapeCityDeaths(file_path, city_deaths_path)
# more scrapers here...

print('DONE!!!')