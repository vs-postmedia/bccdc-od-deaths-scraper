from tableauscraper import TableauScraper as TS
import pandas as pd


# NOTES
# rate is per 100k population

# VARS
paramedic_sheet = 'Paramedic Rate Attended' # 'Illegal ODs and Projected'
od_sheet = 'BCCS Deaths Sex-Age' # 'BCCS Deaths Rate'

haList = ['All BC', 'Interior', 'Fraser', 'Vancouver Coastal', 'Vancouver Island', 'Northern']

# OUTPUT FILES
paramedic_output = './data/paramedic-calls.csv'

# SOURCE DATA
od_url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/IllicitOverdoseDeathsIndicator'
paramedic_url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/ParamedicAttendedOverdoseIndicator2'

# GET TO WORK!
ts = TS()


def scrapeParamedicEvents(url, sheet, output_file):
    df_all = pd.DataFrame()
    
    ts.loads(url)
    # get the worksheet
    ws = ts.getWorksheet(sheet)

    # create pandas dataframe
    df = pd.DataFrame(data = ws.data)

    for ha in haList:
        # set filter value
        wb = ws.setFilter('HA Name1', ha, filterDelta=False)

        # get new data for the worksheet
        filtered = wb.getWorksheet(sheet)

        # create pandas dataframe
        df = pd.DataFrame(data = filtered.data)

        # merge datasets
        df_all = pd.concat([df_all, df])

    # drop alias columns & rename the rest
    df_all = df_all.loc[:,~df.columns.str.contains('alias', case=False)]
    df_all = df_all.rename(columns={'HA Name1-value':'Health authority','MONTH(BCAS-Date)-value':'Date', 'AGG(Rate)-value':'Overdose calls per 100,000 people'})

    # text cleanup & copy edits
    df_all['Date'] = df_all['Date'].str.replace(' 00:00:00', '')
    df_all['Health authority'] = df_all['Health authority'].str.replace('All BC', 'All B.C.')
    df_all['Health authority'] = df_all['Health authority'].str.replace('Vancouver Coastal', 'VCH')
    df_all['Health authority'] = df_all['Health authority'].str.replace('Vancouver Island', 'Island')
    
    # pivot wider by health authority
    df_all = df_all.pivot(index='Date',columns='Health authority', values='Overdose calls per 100,000 people')

    # write csv file
    df_all.to_csv(output_file)


    ### GET WORKBOOK NAMES ###
    workbook = ts.getWorkbook()
    for t in workbook.worksheets:
        print(f"worksheet name : {t.name}") #show worksheet name


def scrapeDeathsBySexHA(url, sheet):
    ts.loads(url)
    df_all = pd.DataFrame()
    ws = ts.getWorksheet(sheet)

    # workbook = ts.getWorkbook()
    # for t in workbook.worksheets:
    #     print(f"worksheet name : {t.name}") #show worksheet names

    # get filter columns & values
    # filters = ws.getFilters()
    # print(filters)

    for ha in haList:
        # set filter value
        wb = ws.setFilter('HA Name1', ha, filterDelta=False)

        # get new data for the worksheet
        filtered = wb.getWorksheet(sheet)
        
        # create pandas dataframe
        df = pd.DataFrame(data = filtered.data)
        
        # drop alias columns & add ha
        # df = df.drop(df.columns[[1,4]], axis=1)
        df = df.assign(health_authority = ha)

        # text cleanup & copy edits
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




### AUTOBOTS... ROLL OUT!!! ###
scrapeDeathsBySexHA(od_url, od_sheet)
scrapeParamedicEvents(paramedic_url, paramedic_sheet, paramedic_output)


print('DONE!')


### JANK!!! ###
def scrapeProjectedOverdoses(url, sheet):
    ts.loads(url)
    # get the worksheet
    ws = ts.getWorksheet(sheet)

    # create pandas dataframe
    df = pd.DataFrame(data = ws.data)

    # drop unused columns
    df.drop(['WEEK(Days)-alias', 'AGG(HA)-alias', 'Measure Values-alias'], axis=1, inplace=True)
   
    # rename headers
    df.rename(
        columns={
            'WEEK(Days)-value': 'Date',
            'Measure Names-alias': 'Health authority',
            'Measure Values-value': 'Overdoses attended'
        },
        inplace=True
    )
    
    # trim whitespace & copy edits
    df['Health authority'] = df['Health authority'].str.strip()
    df['Health authority'] = df['Health authority'].str.replace('Events', 'B.C.')
    df['Health authority'] = df['Health authority'].str.replace('Vancouver Coastal', 'VCH')

    ### CONVERT TO RATE??? ###

    # pivot wider
    df_wide = df.pivot(index='Date', columns='Health authority', values='Overdoses attended')
    
    # write csv file
    df_wide.to_csv('./data/paramedic-projected.csv')
    

    ### GET WORKBOOK NAMES ###
    workbook = ts.getWorkbook()
    for t in workbook.worksheets:
        print(f"worksheet name : {t.name}") #show worksheet name