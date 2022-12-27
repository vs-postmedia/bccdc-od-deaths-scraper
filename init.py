import scripts.drugDeathScraper as drugDeathScraper
import scripts.tableauScraper as tableauScraper


### SOURCE DATA ###

# CORONERS SERVICE
deaths_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug.pdf'
# drugs_url = 'https://www2.gov.bc.ca/assets/gov/birth-adoption-death-marriage-and-divorce/deaths/coroners-service/statistical/illicit-drug-type.pdf'

# TABLEAU
# od_url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/IllicitOverdoseDeathsIndicator'
ops_url = 'https://public.tableau.com/views/ODQuarterlyReportDashboard/OPSSitesIndicators'
od_sex_url = 'http://public.tableau.com/views/UnregulatedDrugPoisoningEmergencyDashboard/Introduction'
paramedic_url = 'http://public.tableau.com/views/UnregulatedDrugPoisoningEmergencyDashboard/Introduction'

# DRUG TESTING
# tk

drugDeathScraper.init(deaths_url) # <-- this calls toplineNumbers.py
tableauScraper.init()
