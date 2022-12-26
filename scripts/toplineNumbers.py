''' toplineNumbers.py '''

import io
import csv
import ssl
from PyPDF2 import PdfReader
import urllib.request


### VARS ###
# csv headers
csv_header = ['metric', 'value']
# output csv file path
out_file = './data/topline-numbers.csv'
# illicit-drug.pdf url
pdf_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'
# url request header
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
}


### FUNCTIONS ###
def init(deaths_total):
     # For SSL certificate error
    ssl._create_default_https_context = ssl._create_unverified_context

    memory_file = fetchPDF()

    # create pdf file object
    reader = PdfReader(memory_file)

    page = reader.pages[0]
    text = page.extract_text().split('  ')

    # print(text)

    for i in range(len(text)):
        if ('January 1, 2012 – ' in text[i]):
            # print(text[i], end = '\n\n')
            last_update = text[i].split(' – ')[1].strip().replace(' ,', ',')
            print('LAST UPDATE: ')
            print(last_update)
        

        if ('The total of ' in text[i]):
            deaths_new = text[i].replace('• The total of ', '').strip()
            print('NEW DEATHS: ')
            print(deaths_new)

        # if ('• The number of illicit drug toxicity deaths' in text[i]):
        if ('equates to about' in text[i]):
            deaths_daily = text[i].split('about ')[1].split(' deaths')[0].strip().replace('. ', '.')
            print('DAILY DEATHS: ')
            print(deaths_daily)
    

    # write list for csv rows
    csv_rows = [
        ['deaths_new', deaths_new],
        ['deaths_total', deaths_total],
        ['deaths_daily', deaths_daily],
        ['last_update', last_update]
    ]

    writeCSV(csv_rows)


def fetchPDF():
     # fetch the pdf
    req = urllib.request.Request(pdf_url, headers = headers)
    remote_file = urllib.request.urlopen(req).read()
    memory_file = io.BytesIO(remote_file)

    return memory_file

def writeCSV(csv_rows):
    with open(out_file, 'w') as csvfile:
        # create csv writer
        csvwriter = csv.writer(csvfile)

        # write header fields
        csvwriter.writerow(csv_header)

        # write data rows
        csvwriter.writerows(csv_rows)



# KICK IT OFF!!!
# init(10550)