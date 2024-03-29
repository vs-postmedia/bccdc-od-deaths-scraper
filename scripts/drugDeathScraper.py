''' drugDeathScraper.py '''

import ssl
from asyncore import file_dispatcher
from cmath import nan
from operator import index
import pandas as pd
import geopandas as gpd
from tabula import read_pdf
import scripts.toplineNumbers as toplineNumbers

# VARS
current_year = '2022'
health_emergency_date = '2016-04-01'
lha_geo_path = './data/source/lha-2018-epsg4326_7pct.json'
city_pop = './data/source/city-populations.csv'
# why this agent? who knows...
user_agent_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'

# TABLE AREAS
# https://tabula-py.readthedocs.io/en/latest/faq.html#how-can-i-ignore-useless-area
lha_area = [150,32,720,568]
ages_area_v1 = [540,35.05,690,568.67]
ages_area_v2 = [560,35.05,690,568.67]

### INPUTS ###
file_path = './data/source/illicit-drug.pdf'

### OUTPUT FILES ###
lha_csv_path = './data/deaths-by-lha.csv' # LHA table
lha_json_path = './data/deaths-by-lha.json' # LHA map
age_deaths_path = './data/deaths-by-age.csv' # total deaths by age group
city_deaths_ts_path = './data/deaths-by-city.csv' # timeseries of death rates for key cities
city_deaths_latest_path = './data/deaths-by-city-latest.csv' # total deaths for current year
monthly_deaths_path = './data/monthly-deaths.csv' # monthly deaths (unused)
yearly_deaths_path = './data/yearly-deaths.csv' # annual deaths
ha_location_deaths_path = './data/ha-location-deaths.csv' # private resident, SRO, etc


### FUNCTIONS ###
# get total number of deaths by age group
def scrapeAges(input_file, output_file):
    # read age by year...
    # BUT...
    # the position of the age table shifts from report to report...
    df1 = read_pdf(input_file, output_format="dataframe", pages='8', stream=True, multiple_tables=False, user_agent=user_agent_string, area=ages_area_v1)
    df2 = read_pdf(input_file, output_format="dataframe", pages='8', stream=True, multiple_tables=False, user_agent=user_agent_string, area=ages_area_v2)
    
    if list(df1[0].columns)[0] == 'Age Group':
        # df comes as list, we don't want that
        df = df1[0]
    else:
        # df comes as list, we don't want that
        df = df2[0]

    # drop NaN rows
    df.dropna(inplace=True)
    # the space in "Age Group" will cause trouble later on
    df.rename(columns = {'Age Group': 'Age'}, inplace=True)
    df['Age'] = df['Age'].str.replace('<19', 'Under 19')
    
    # Sum by age
    df_long = df.melt(id_vars='Age', var_name='Year', value_name='Deaths', ignore_index=True)
    df_long['Deaths'] = df_long['Deaths'].astype(int)

    # fix PDF glitches
    df_long['Year'] = df_long['Year'].str.replace('2O0c1t-72 2', '2017')
    df_long['Year'] = df_long['Year'].str.replace('2N0o1v-72 2', '2017')

    # do the math
    df_sum = df_long[df_long['Year'].astype(int) >= 2016 ].groupby(['Age']).sum()

    # Sort for Flourish
    df_sum = df_sum.reset_index()
    df_sum = df_sum.reindex([5,4,3,2,1,0,6])
    
    # write CSV file
    df_sum.to_csv(output_file, index=False)

# UNUSED IN VIZ: For small-multiple timeseries for key B.C. cities
def scrapeCityDeaths(input_file, timeseries_output_file):
    # get city populations
    pop = pd.read_csv(city_pop)
    # read city deaths table from PDF
    df = read_pdf(input_file, output_format="dataframe", pages='11', stream=True, area=[130,52.5,424,588], user_agent=user_agent_string)

    # drop "total" & "other" rows/vs-postmedia/bccdc-od-deaths-scraper/raw/main/data/deaths-by-city.csv
    df = df[0].iloc[:-2]
    
    # rename township
    df.rename(columns = {'Township':'City'}, inplace = True)

    # rename Victoria  
    df.replace(to_replace = 'Greater Victoria', value = 'Victoria', inplace = True)
    
    # wide to long 
    df_long = df.melt(id_vars='City', var_name='Year', value_name='Deaths', ignore_index=True)
    # add population column
    df_long = df_long.merge(pop, on='City', how='left')

    # calculate rate
    df_long['Deaths per 1,000'] = df_long['Deaths'].astype(int) / df_long['population_2021'] * 1000

    # pivot wide for small multiple
    df_wide = df_long.pivot(index='City', columns='Year', values='Deaths per 1,000')
    df_wide = df_wide.transpose()

    # write csv files
    df_wide.to_csv(timeseries_output_file)

# deaths per month & per year (separate files)
def scrapeDeathsTimeseries(input_file, monthly_output, yearly_output):
    df = read_pdf(input_file, output_format="dataframe", pages='4', stream=True, area=[76,52.5,355,589], user_agent=user_agent_string)
    df = df[0]

    ##############
    ### YEARLY ###
    ##############
    
    # get annual numbers & pivot longer
    df_totals = df[df['Month'] == 'Total']
    df_totals = df_totals.melt(id_vars='Month', var_name='Year', value_name='Deaths', ignore_index=True)
    
    # convert string to number
    df_totals['Deaths'].replace(',', '', inplace=True)

    # drop unused col
    df_totals.drop(['Month'], axis=1, inplace=True)
    
    # write anual totals to file
    df_totals.to_csv(yearly_output, index=False)

    ###############
    ### MONTHLY ###
    ###############

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
    
    # pass monthly deaths to scrapeToplineNumers function
    scrapeToplineNumbers(df)
    
    # write to file
    df.to_csv(monthly_output, index=False)

# deaths by location (private residence, SRO, etc.)
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

# deaths per LHA: geojson for map, csv for table
def scrapeLHA(input_file, json_output, csv_output):
    # admin boundaries for LHAs
    lha_json = gpd.read_file(lha_geo_path)

    # read LHA tables from PDF
    df = read_pdf(input_file, output_format="dataframe", pages="18-20", stream=True, area=lha_area, user_agent=user_agent_string)

    # df comes as list, we don't want that
    df = df[0]
    
    # add headers to dataframe
    df.columns = ['LHA_NAME', '2017', '2018', '2019', '2020', '2021', '2022', 'deaths']

    # drop non-data rows
    df['LHA_NAME'].replace('', nan, inplace = True)
    df.dropna(subset = ['LHA_NAME'], inplace = True)
    # df.drop(index=[0,1], inplace = True)
    df.drop(df.tail(16).index, inplace = True)

    # some zeros end up as NaN, for some reason <shrug>
    df['deaths'] = df['deaths'].fillna(0)

    # text cleanup
    df['LHA_NAME'] = df['LHA_NAME'].str.replace('Maple Ridge/Pitt', 'Maple Ridge/Pitt Meadows')
    df['LHA_NAME'] = df['LHA_NAME'].str.replace('West Vancouver/ Bowen', 'West Vancouver & Bowen Island')
    df.drop(index = df[df['LHA_NAME'] == 'Meadows'].index, inplace=True)
    df.drop(index = df[df['LHA_NAME'] == 'Island'].index, inplace=True)

    # merge with LHA boundaries
    # df_current = df[['LHA_NAME', current_year, 'Deaths this year']]
    df_geo = lha_json.merge(df, on='LHA_NAME', how='left')
    df_geo.fillna('', inplace=True) 

    # write geojson file
    df_geo.to_file(json_output, driver='GeoJSON', drop_id=True)

    # prep csv output for the table
    df['Rate per 100,000'] = df.loc[:, '2022']
    df = df[['LHA_NAME', '2017', '2018', '2019', '2020', '2021', '2022', 'Rate per 100,000', 'deaths']]
    
    # write CSV file
    df = df.rename(columns={'LHA_NAME':'Local Health Area', 'deaths': 'Total deaths'})
    df.to_csv(csv_output, index=False)

def scrapeToplineNumbers(df_monthly):
    # convert date to datetime for filtering
    df_monthly['Date'] = pd.to_datetime(df_monthly['Date'])
    # filter out dates before health emergecy was declared
    df_monthly = df_monthly[df_monthly['Date'] >= health_emergency_date]

    # get the total deaths since health
    deaths_total = df_monthly['Deaths'].astype(int).sum()
    
    # pass totals to the toplineNumbers script
    toplineNumbers.init(deaths_total)


def init(deaths_url):
    # for SSL certificate error
    ssl._create_default_https_context = ssl._create_unverified_context

    # AUTOBOTS... ROLL OUT!!!
    scrapeAges(deaths_url, age_deaths_path)
    scrapeCityDeaths(deaths_url, city_deaths_ts_path)
    scrapeDeathsTimeseries(deaths_url, monthly_deaths_path, yearly_deaths_path)
    scrapeHaLocation(deaths_url, ha_location_deaths_path)
    scrapeLHA(deaths_url, lha_json_path, lha_csv_path)
    print('PDF DONE!!!')



# TEST SCRAPERS HERE....
# for SSL certificate error
# ssl._create_default_https_context = ssl._create_unverified_context
# scrapeCityDeaths(deaths_url, city_deaths_ts_path)