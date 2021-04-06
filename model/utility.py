from google.cloud import bigquery
import pandas as pd
import os
os.getcwd()
os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from data.claimsSqlQueries import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
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

        sql = historicalReportPattern(
            self.fc_month, self.peril)
        report_pattern = client.query(sql).to_dataframe()
        print(report_pattern)
        print('Evaluate Max Emergence Month')

        report_pattern_trunc = report_pattern[report_pattern[
            'lag_month'] <= numEmergenceMonths(self.peril)]
        report_pattern_trunc['percent_report_by_lag_month_select'] = report_pattern_trunc['claim_count']/sum(
            report_pattern_trunc['claim_count'])
        report_pattern_trunc['cumulative_percent_reported_by_lag_month'] = report_pattern_trunc['percent_report_by_lag_month_select'].cumsum()
        report_pattern_trunc = report_pattern_trunc[[
            'lag_month', 'percent_report_by_lag_month_select', 'cumulative_percent_reported_by_lag_month']]

        sql = reportedClaimsByAccMonth(
            self.fc_month, self.peril, numEmergenceMonths(self.peril))
        reported_claims_by_acc_month = client.query(sql).to_dataframe()

        ultimate_claim_count_by_acc_month = pd.merge(reported_claims_by_acc_month,
                                                     report_pattern_trunc, how='left', left_on=['accident_month_age'], right_on=['lag_month'])
        ultimate_claim_count_by_acc_month['ultimate_claim_count'] =
            round(ultimate_claim_count_by_acc_month['claim_count'] /
                ultimate_claim_count_by_acc_month['cumulative_percent_reported_by_lag_month'], 0)
        return ultimate_claim_count_by_acc_month

    def ProjectAccidentMonthClaimCount(self):
        '''
        Use historical accident month frequency
        Check seasonality
        '''
        ult_claim_count = ultimate_claim_count_by_acc_month[['accident_month','ultimate_claim_count']]
        ult_claim_count['temp'] = 1
        lag_month_list = report_pattern_trunc[['lag_month', 'percent_report_by_lag_month_select']]
        lag_month_list['temp'] = 1
        projected_claim_count_acc_month = pd.merge(ult_claim_count, lag_month_list, on=['temp'])
        projected_claim_count_acc_month = projected_claim_count_acc_month.drop('temp', axis=1)
        
        projected_claim_count_acc_month['lag_month'] = projected_claim_count_acc_month['lag_month'].astype(float)
        projected_claim_count_acc_month['report_month'] = pd.to_datetime(projected_claim_count_acc_month['accident_month']) + projected_claim_count_acc_month['lag_month'].apply(lambda x: relativedelta(months=x))

    def ExecuteReportedClaimsForecast(self):
        new_open_claim_loss_dev_month =
        actual_new_open_claim_by_acc_month =
        ultimate_claim_count_by_acc_month =
        claim_count_by_report_month =
        projected_new_open_by_acc_month

    def ApplyErrorMargin(self):


sql = historicalReportPattern('2021-03-31', 'Water')
sql = reportedClaimsByAccMonth('2021-04-06', 'Water', 12)
