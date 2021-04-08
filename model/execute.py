from google.cloud import bigquery
from utility import *

client = bigquery.Client()

fc_date_peril = ClaimForecast('2021-04-07', 'Water')

fc_date_peril.ReportingPattern()

fc_date_peril.HistAccMthUltimateClaimCount()

fc_date_peril.RptClaimCountFromHistAccMth()

fc_date_peril.RptFreqNonLagged()
