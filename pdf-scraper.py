from tabula import convert_into, read_pdf
# from shapely.geometry import shape
from geopandas import gpd
# from tabulate import tabulate
import pandas as pd
# import json

# VARS
file_path = './data/pdf/illicit-drug.pdf'
lha_path = './data/source/lha-2018-epsg3857.json'
lha_data = './data/deaths-by-lha.csv'
lha_json_path = './data/lha-json-test.json'
file_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'


def scrapeLHA(input, output,):
    # lha_json = pd.read_json(lha_path)
    lha_df = gpd.read_file(lha_path)

    # print(lha_df)

    # read LHA table from PDF
    df = read_pdf(file_path, output_format="dataframe", pages='18-20', pandas_options={'header': None, 'names':['LHA_NAME','2016','2017','2018','2019','2020','2021','Deaths this year']}, multiple_tables=True, stream=True, area=[194.6,31,699,572])
    
    # drop non-data rows
    df = df[0].iloc[:-16]
    
    # merge with LHA geojson data
    df_geo = lha_df.merge(df, on='LHA_NAME', how='left')
    # print(df_geo)
    

    # write csv file
    df_geo.to_csv(lha_data, index=False)
    df_geo.to_file(lha_json_path, driver='GeoJSON', drop_id=True)


# AUTOBOTS... ROLL OUT!!!
scrapeLHA(file_url, lha_data)
# more scrapers here...

print('DONE!!!')