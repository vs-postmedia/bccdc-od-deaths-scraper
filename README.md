# bccdc-od-deaths-scraper

Scrapes drug overdose deaths & related data from BCCDC Tableau dashboard at: 
• https://public.tableau.com/views/ODQuarterlyReportDashboard/IllicitOverdoseDeathsIndicator

the B.C. Coroners’s Service illicit drug deaths at: 
• https://www2.gov.bc.ca/gov/content/life-events/death/coroners-service/statistical-reports


**View the tracker here: https://vs-postmedia.github.io/od-tracker**

# Data update instructions:
1. run pdf & tableau scrapers

<!-- MANUAL (just for now, I hope!) -->
# ILLICIT DRUG REPORT:
• *big_num*: Summary text, grafs 1 & 2
    • deaths_total = deaths_total + deaths_new

# ILLICIT DRUG TYPE REPORT:
• *fentanyl_year*: deaths w/ fentanyl detected (annual) (fig.1)
• *fentanyl_extreme*: extreme fentanyl concentrations (monthly) (fig.2)
• **carfentanyl – currently not used (monthly) (fig.4)**
• *drug_types_long*: expedited toxicology results (monthly) (fig.5)

#  URL: https://docs.google.com/spreadsheets/d/1bk3cOdwsUuK7CxUdsQUcng78H3cDd-QNTqPMa2Lw790/edit#gid=440911543 

