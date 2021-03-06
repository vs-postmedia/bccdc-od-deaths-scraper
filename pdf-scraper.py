from asyncore import file_dispatcher
from operator import index
import pandas as pd
from tabula import read_pdf

# VARS
current_year = '2021'
lha_geo_path = './data/source/lha-2018-epsg4326_7pct.json'
city_pop = './data/source/city-populations.csv'
# why this agent? who knows...
user_agent_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'


### INPUTS ### 
# file_path = './data/source/illicit-drug.pdf'
deaths_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'
# drugs_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug-type.pdf'


### OUTPUT FILES ###
lha_csv_path = './data/deaths-by-lha.csv'
lha_json_path = './data/deaths-by-lha.json'
age_deaths_path = './data/deaths-by-age.csv'
city_deaths_path = './data/deaths-by-city.csv'
monthly_deaths_path = './data/monthly-deaths.csv'
yearly_deaths_path = './data/yearly-deaths.csv'
ha_location_deaths_path = './data/ha-location-deaths.csv'


# FUNCTIONS
def scrapeDeathsTimeseries(input_file, monthly_output, yearly_output):
    df = read_pdf(input_file, output_format="dataframe", pages='4', stream=True, area=[76,52.5,355,589], user_agent=user_agent_string)
    df = df[0]

    ### YEARLY ###
    # get annual numbers & pivot longer
    df_totals = df[df['Month'] == 'Total']
    df_totals = df_totals.melt(id_vars='Month', var_name='Year', value_name='Deaths', ignore_index=True)
    
    # convert string to number
    df_totals['Deaths'].replace(',', '', inplace=True)

    # drop unused col
    df_totals.drop(['Month'], axis=1, inplace=True)
    
    # write to file
    df_totals.to_csv(yearly_output, index=False)

    

    ### MONTHLY ###
    # drop "total" & "average" rows
    df = df.iloc[:-2]

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
    df.to_csv(monthly_output, index=False)

def scrapeCityDeaths(input_file, output_file):
    # get city populations
    pop = pd.read_csv(city_pop)
    # read city deaths table from PDF
    df = read_pdf(input_file, output_format="dataframe", pages='11', stream=True, area=[130,52.5,424,588], user_agent=user_agent_string)

    # drop "total" & "other" rows/vs-postmedia/bccdc-od-deaths-scraper/raw/main/data/deaths-by-city.csv
    df = df[0].iloc[:-2]
    # rename township
    df.rename(columns = {'Township':'City'}, inplace = True)
   
    # wide to long 
    df_long = df.melt(id_vars='City', var_name='Year', value_name='Deaths', ignore_index=True)
    # add population column
    df_long = df_long.merge(pop, on='City', how='left')

    # calculate rate
    df_long['Deaths per 100,000'] = df_long['Deaths'].astype(int) / df_long['population_2021'] * 100000

    # pivot wide for small multiple
    df_wide = df_long.pivot(index='City', columns='Year', values='Deaths per 100,000')
    df_wide = df_wide.transpose()

    # we only want the latest year for the LHA map
    # df_long = df_long[df_long['Year'] == '2022']

    # write csv file
    df_wide.to_csv(output_file)
    # NOTE: HAVE TO WRITE FILE FOR DF_MAP

def scrapeAges(input_file, output_file):
    # read age by year
    df = read_pdf(input_file, output_format="dataframe", pages='8', stream=True, multiple_tables=False, user_agent=user_agent_string, area=[542.74,35.05,684.72,568.67])

    # df comes as list, we won't want that
    df = df[0]

    # the space in "Age Group" will cause trouble later on
    df.rename(columns = {'Age Group': 'Age'}, inplace=True)
    # rename
    df['Age'] = df['Age'].str.replace('<19', 'Under 19')
    
    # Sum by age
    df_long = df.melt(id_vars='Age', var_name='Year', value_name='Deaths', ignore_index=True)
    df_long['Deaths'] = df_long['Deaths'].astype(int)
    df_sum = df_long[df_long['Year'].astype(int) >= 2016 ].groupby(['Age']).sum()

    # Sort for Flourish
    df_sum = df_sum.reset_index()
    df_sum = df_sum.reindex([5,4,3,2,1,0,6])
    
    # write CSV file
    df_sum.to_csv(output_file, index=False)

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

def scrapeHaLocation(input_file, output_file):
     # read city deaths table from PDF
    df = read_pdf(input_file, output_format="dataframe", pages='5', stream=True, multiple_tables=True, user_agent=user_agent_string, area=[401,48.84,518.20,552.58])

    # clean this fugly sh*t
    # list to dataframe & drop NaN
    df = df[0].dropna()

    # rename columns
    df.rename(columns = {'Unnamed: 0':'Location', 'Interior':'Interior', 'Fraser':'Fraser', 'Unnamed: 3':'VCH', 'Unnamed: 4':'Island', 'Unnamed: 5':'Northern'}, inplace = True)

    # Clean up location names
    df['Location'].replace('Other Residence', 'Hotel, SRO, etc.', inplace = True)
    df['Location'].replace('Private Residence', 'Private home', inplace = True)
    df['Location'].replace('Other Inside', 'Indoors, non-residential', inplace = True)

    # we only want the % value (yes, I know there's a better way to do this... :P)
    df['Interior'] = df['Interior'].str.replace('%)', '', regex = False).str.replace('^.*\s[(]', '', regex = True)
    df['Fraser'] = df['Fraser'].str.replace('%)', '', regex = False).str.replace('^.*\s[(]', '', regex = True)
    df['VCH'] = df['VCH'].str.replace('%)', '', regex = False).str.replace('^.*\s[(]', '', regex = True)
    df['Island'] = df['Island'].str.replace('%)', '', regex = False).str.replace('^.*\s[(]', '', regex = True)
    df['Northern'] = df['Northern'].str.replace('%)', '', regex = False).str.replace('^.*\s[(]', '', regex = True)

    # write CSV file
    df.to_csv(output_file, index=False)


# AUTOBOTS... ROLL OUT!!!
scrapeAges(deaths_url, age_deaths_path)
scrapeCityDeaths(deaths_url, city_deaths_path)
scrapeDeathsTimeseries(deaths_url, monthly_deaths_path, yearly_deaths_path)
scrapeHaLocation(deaths_url, ha_location_deaths_path)
# LHA doesn't quite work using tableau.py
# scrapeLHA(file_path, lha_json_path, lha_csv_path)
# more scrapers here...


print('DONE!!!')