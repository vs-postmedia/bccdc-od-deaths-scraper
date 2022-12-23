''' tableauScraper.py '''

from tableauscraper import TableauScraper as TS
import pandas as pd


# NOTES
# rate is per 100k population

# VARS
# paramedic_sheet = 'Paramedic Rate Attended' # 'Illegal ODs and Projected'
paramedic_sheet = 'Paramedic Attended (2)'
# od_sex_sheet = 'BCCS Deaths Sex-Age' # 'BCCS Deaths Rate'
# od_sex_sheet = 'Paramedic Attended Sex-Age (2)'
# od_sex_sheet = 'Illicit Drug Toxicity Deaths '
od_sex_sheet = 'BCCS Deaths Rate (2)'
od_sheet = 'Illicit Drug Toxicity Deaths '
ops_sheet = 'OPS Visits' #OPS Inhalation Visits, OPS Overdose
haList = ['All BC', 'Interior', 'Fraser', 'Vancouver Coastal', 'Vancouver Island', 'Northern']

# OUTPUT FILES
paramedic_output = './data/paramedic-calls.csv'
od_sex_output = './data/deaths-by-sex.csv'
ops_output = './data/ops-visits.csv'

# SOURCE DATA
# od_url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/IllicitOverdoseDeathsIndicator'
ops_url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/OPSSitesIndicators'
od_sex_url = 'http://public.tableau.com/views/UnregulatedDrugPoisoningEmergencyDashboard/Introduction'
paramedic_url = 'http://public.tableau.com/views/UnregulatedDrugPoisoningEmergencyDashboard/Introduction'



# GET TO WORK!
ts = TS()

### FUNCTIONS ###
def scrapeDeathsBySexHA(url, sheet, output_file):
    ts.loads(url)
    wb = ts.getWorkbook()
    df_all = pd.DataFrame()
    # print(wb.getSheets())
    ws = wb.goToSheet(sheet)

    
    for t in ws.worksheets:
        # print(t.name)
        # if t.name == sheet:
        if t.name == 'BCCS Deaths Rate (2)':
            # df = pd.DataFrame(data = t.data)
            print(t.getFilters())

            for ha in haList:
                # set filter value
                wb = t.setFilter('HA Name1', ha, filterDelta=False)
                filtered_data = wb.getWorksheet('BCCS Deaths Rate (2)')
                df = pd.DataFrame(data = filtered_data.data)
                df = df.loc[:,~df.columns.str.contains('alias', case=False)]
                # print(ha)
                print(df)

                # for f in filtered.worksheets:
                #     # print(f.name)
                #     if f.name == sheet:
                #         # drop alias columns & add ha
                #         # df = df.drop(df.columns[[1,4]], axis=1)
                #         df = df.loc[:,~df.columns.str.contains('alias', case=False)]
                #         df = df.assign(health_authority = ha)

                       
                    
                #         # merge datasets
                #         df_all = pd.concat([df_all, df])

                # # get new data for the worksheet
                # filtered = wb.getWorksheet(sheet)
                
                # # create pandas dataframe
                # df = pd.DataFrame(data = filtered.data)
                
               
                # # text cleanup & copy edits
                # df['health_authority'] = df['health_authority'].str.replace('All BC', 'All B.C.')
                # df['health_authority'] = df['health_authority'].str.replace('Vancouver Island', 'Island')
                # df['health_authority'] = df['health_authority'].str.replace('Vancouver Coastal', 'VCH')
                
                #
    
    print(df_all)

    # # rename columns
    # df_all = df_all.rename(columns={'BCCS Date (C Months)-value':'date','AGG(Rate)-value':'rate', 'Breakdown by-alias':'gender'})

    # # pivot wider by sex & make colheds reader friendly
    # df2 = df_all.pivot(index=['date','health_authority'],columns='gender', values='rate')
    # df2 = df2.rename(columns={'Male':'Men','Female':'Women'})

    # # write csv file
    # df2.to_csv(output_file)

def scrapeOpsIndicators(url, sheet, output_file):
    ts.loads(url)

    # get the worksheet
    ws = ts.getWorksheet(sheet)

    # create pandas dataframe
    df = pd.DataFrame(data = ws.data)

    # drop duplicate columns & rename the rest
    df = df.loc[:,~df.columns.str.contains('alias', case=False)]
    df = df.loc[:,~df.columns.str.contains('federated', case=False)]
    df = df.rename(columns={'HA Name1-value':'Health authority','DAY(OPS Date)-value':'Date', 'AGG(ZN(SUM([Visits])))-value':'OPS visits'})

    # text cleanup & copy edits
    df['Date'] = df['Date'].str.replace(' 00:00:00', '')
    df['Health authority'] = df['Health authority'].str.replace('All BC', 'B.C.')
    df['Health authority'] = df['Health authority'].str.replace('Vancouver Coastal', 'VCH')
    df['Health authority'] = df['Health authority'].str.replace('Vancouver Island', 'Island')

    # ### TO DO:
    # JOIN WITH OPS SITE COUNT OR POPULATION TO CREATE RATE
    # ###
    
    # pivot wider by health authority
    df = df.pivot(index='Date',columns='Health authority', values='OPS visits')

    # write csv file
    df.to_csv(output_file)

def scrapeParamedicEvents(url, sheet, output_file):
    ts.loads(url)
    wb = ts.getWorkbook()
    df_all = pd.DataFrame()
    # print(wb.getSheets())
    ws = wb.goToSheet(paramedic_sheet)

    for t in ws.worksheets:
        if t.name == 'Paramedic Rate Attended (2)':
            # print(t.data)
            # create pandas dataframe
            df = pd.DataFrame(data = t.data)

            for ha in haList:
                # set filter value
                # print(t.getFilters())
                filtered = t.setFilter('HA Name1', ha, filterDelta=False)

                # get new data for the worksheet
                for f in filtered.worksheets:
                    if f.name == 'Paramedic Rate Attended (2)':        
                        # print(f.data)
                        # create pandas dataframe
                        df = pd.DataFrame(data = f.data)

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


### AUTOBOTS... ROLL OUT!!! 
def init():
    scrapeParamedicEvents(paramedic_url, paramedic_sheet, paramedic_output)
    # scrapeDeathsBySexHA(od_sex_url, od_sheet, od_sex_output)
    # scrapeOpsIndicators(ops_url, ops_sheet, ops_output)

    print('TABLEAU DONE!')



init()