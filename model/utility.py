from google.cloud import bigquery
import pandas as pd
import os
os.getcwd()
os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from data.claimsSqlQueries import *
##from config import *


class ClaimForecast():
    '''
    '''

    def __init__(self, fc_month, peril):
        self.fc_month = fc_month
        self.peril = peril

    def IngestEarnedExposureForecast(self):

    def ProjectAccidentMonthUltimateClaimCount(self):
        client = bigquery.Client()
        sql = reportedClaimsByAccMonth(
            self.fc_month, self.peril, numEmgergenceMonths(self.peril))
        reported_claims_by_acc_month = client.query(sql).to_dataframe()
        report_pattern = pd.read_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/data/report_pattern_input.csv')
        ultimate_claim_count_by_acc_month = pd.merge(reported_claims_by_acc_month,
            report_pattern, how='left', left_on=['accident_month_age'], right_on=['lag_month'])
        ultimate_claim_count_by_acc_month['ultimate_claim_count'] = 
            round(ultimate_claim_count_by_acc_month['claim_count']/ultimate_claim_count_by_acc_month['cumulative_percent_reported_by_lag_month'],0)
        return ultimate_claim_count_by_acc_month

    def ProjectAccidentMonthClaimCount(self):
        '''
        Use historical accident month frequency
        Check seasonality
        '''

    def ExecuteReportedClaimsForecast(self):
        new_open_claim_loss_dev_month =
        actual_new_open_claim_by_acc_month =
        ultimate_claim_count_by_acc_month =
        claim_count_by_report_month =
        projected_new_open_by_acc_month

    def ApplyErrorMargin(self):


sql = reportedClaimsByAccMonth('2021-03-25', 'Water', 12)
