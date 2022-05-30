from tabula import convert_into, read_pdf
from tabulate import tabulate
import pandas as pd

# VARS
file_path = './data/pdf/illicit-drug.pdf'
file_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'

lha_data = './data/deaths-by-lha.csv'

def scrapeLHA(input, output,):
    # read LHA table from PDF
    df = read_pdf(file_path, output_format="dataframe", pages='18-20', pandas_options={'header': None, 'names':['LHA','2016','2017','2018','2019','2020','2021','Deaths this year']}, multiple_tables=True, stream=True, area=[194.6,31,699,572])
    
    # drop non-data rows
    df = df[0].iloc[:-16]
    print(df)

    # write csv file
    df.to_csv(lha_data, index=False)


# AUTOBOTS... ROLL OUT!!!
scrapeLHA(file_url, lha_data)
# more scrapers here...

print('DONE!!!')