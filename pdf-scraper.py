from tabula import convert_into, read_pdf
from tabulate import tabulate
import pandas as pd

# VARS
file_path = './data/pdf/illicit-drug.pdf'
file_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'

output_path = './data/deaths-by-lha.csv'

# read pdf from file
df = read_pdf(file_path, pages='18-20', output_format="json")
# convert_into(file_path, output_path, pages='18-20', output_format='csv')

# df = tabulate(df)
df = pd.json_normalize(df)

print(df.data)

# write csv file
df.data.to_csv('./data/deaths-by-lha.csv')

print('DONE!!!')