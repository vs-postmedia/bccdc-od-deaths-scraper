# bccdc-od-deaths-scraper

B.C. Coroners’s Service stats: https://www2.gov.bc.ca/gov/content/life-events/death/coroners-service/statistical-reports

Scrapes drug overdose deaths & related data from BCCDC Tableau dashboard at: 
https://public.tableau.com/views/ODQuarterlyReportDashboard/IllicitOverdoseDeathsIndicator

the B.C. Coroners’s Service illicit drug deaths at: https://www2.gov.bc.ca/gov/content/life-events/death/coroners-service/statistical-reports


** View the tracker here: https://vancouversun.com/news/local-news/tracking-overdose-deaths-in-british-columbia **


# Data update instructions:
1. run pdf & tableau scrapers

## MANUAL (just for now, I hope!)
### Illicit drug report:
• *big_num*: Summary text, grafs 1 & 2  
    • deaths_total = deaths_total + deaths_new
• LHA map
    • manually overwrite Flourish data:
        • deaths-by-lha.json
        • deaths-by-city-latest.csv

### Illicit drug type report:
• *fentanyl_year*: deaths w/ fentanyl detected (annual) (fig.1)  
• *fentanyl_extreme*: extreme fentanyl concentrations (monthly) (fig.2)  
• **carfentanyl – currently not used (monthly) (fig.4)**  
• *drug_types_long*: expedited toxicology results (monthly) (fig.5)  

####  URL: https://docs.google.com/spreadsheets/d/1bk3cOdwsUuK7CxUdsQUcng78H3cDd-QNTqPMa2Lw790/edit#gid=440911543 


### Install instructions
Ugh... this is a serious pita...
1. brew install gdal
2. pip3 install:
    a. pandas
    b. geopandas
    c. tabula-py
    d. tableauscraper
    e. PyPDF2
3. Setup package to automate this!!! lol.


