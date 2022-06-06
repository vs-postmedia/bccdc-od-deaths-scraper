from tabula import convert_into, read_pdf
# from shapely.geometry import shape
from geopandas import gpd
# from tabulate import tabulate
import pandas as pd
# import json

# VARS
file_path = './data/pdf/illicit-drug.pdf'
lha_geo_path = './data/source/lha-2018-epsg4326_7pct.json'
lha_jcsv_path = './data/deaths-by-lha.csv'
lha_json_path = './data/deaths-by-lha.json'
city_deaths_path = './data/deaths-by-city.csv'
#  this URL doesn't work for some reason...
file_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'

# FUNCTIONS
def scrapeLHA(input_file, json_output, csv_output):
    # lha_json = pd.read_json(lha_path)
    lha_df = gpd.read_file(lha_geo_path)

    # read LHA table from PDF
    df1 = read_pdf(input_file, output_format="dataframe", pages='18-19', pandas_options={'header': None, 'names':['LHA_NAME','2016','2017','2018','2019','2020','2021','Deaths this year']}, stream=True, area=[180,31,725,572])
    df2 = read_pdf(input_file, output_format="dataframe", pages='20', pandas_options={'header': None, 'names':['LHA_NAME','2016','2017','2018','2019','2020','2021','Deaths this year']}, stream=True, area=[180,31,400,572])
    
    # drop non-data rows
    df1 = df1[0].iloc[:-16]
    df2 = df2[0]
 
    # text cleanup
    df1['LHA_NAME'] = df1['LHA_NAME'].str.replace('Maple Ridge/Pitt', 'Maple Ridge/Pitt Meadows')
    df2['LHA_NAME'] = df2['LHA_NAME'].str.replace('West Vancouver/Bowen', 'West Vancouver/Bowen Island')

    df1.drop(index = df1[df1['LHA_NAME'] == 'Meadows'].index, inplace=True)
    df2.drop(index = df2[df2['LHA_NAME'] == 'Island'].index, inplace=True)

    df = df1.merge(df2, on='LHA_NAME', how='left')

    # merge with LHA geojson data
    df_geo = lha_df.merge(df, on='LHA_NAME', how='left')
    df_geo.fillna('', inplace=True)

    # write geojson file
    df_geo.to_file(json_output, driver='GeoJSON', drop_id=True)

    # rename columns & write CSV file
    df = df.rename(columns={'LHA':'Local Health Area'})
    df.to_csv(csv_output, index=False)


def scrapeCityDeaths(input_file, output_file):
    # read city deaths table from PDF
    df = read_pdf(input_file, output_format="dataframe", pages='11', stream=True, area=[130,52.5,424,588])

    # print(df)
    # drop "total" & "other" rows
    df = df[0].iloc[:-2]
    print(df)

    # write csv file
    df.to_csv(output_file, index=False)


# AUTOBOTS... ROLL OUT!!!
scrapeLHA(file_path, lha_json_path, lha_jcsv_path)
# scrapeCityDeaths(file_path, city_deaths_path)
# more scrapers here...

print('DONE!!!')