import pandas as pd
import numpy as np
from google.cloud import bigquery
import os
os.getcwd()
os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from data.claimsSqlQueries import *
from config import numEmergenceMonths
from datetime import datetime
from dateutil.relativedelta import relativedelta
##from config import *


class ClaimForecast():
    '''
    '''

    def __init__(self, fc_month, peril):
        self.fc_month = fc_month
        self.peril = peril
        self.client = bigquery.Client()

    def ReportingPattern(self):
        
        sql = historicalReportPattern(
            self.fc_month, self.peril)
        report_pattern = self.client.query(sql).to_dataframe()
        print(report_pattern)
        print('Evaluate Max Emergence Month')

        report_pattern_trunc = report_pattern[report_pattern[
            'lag_month'] <= numEmergenceMonths(self.peril)]
        report_pattern_trunc['percent_report_by_lag_month_select'] = report_pattern_trunc['claim_count']/sum(
            report_pattern_trunc['claim_count'])
        report_pattern_trunc['cumulative_percent_reported_by_lag_month'] = report_pattern_trunc['percent_report_by_lag_month_select'].cumsum()
        report_pattern_trunc = report_pattern_trunc[[
            'lag_month', 'percent_report_by_lag_month_select', 'cumulative_percent_reported_by_lag_month']]
        print(report_pattern_trunc)
        report_pattern_trunc.to_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/report_pattern ' + self.fc_month + '.csv')

    def HistAccMthUltimateClaimCount(self):
        sql = reportedClaimsByAccMonth(
            self.fc_month, self.peril, numEmergenceMonths(self.peril))
        reported_claims_by_acc_month = self.client.query(sql).to_dataframe()

        report_pattern_trunc = pd.read_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/report_pattern ' + self.fc_month + '.csv')
        ultimate_claim_count_by_acc_month = pd.merge(reported_claims_by_acc_month,
                                                     report_pattern_trunc, how='left', left_on=['accident_month_age'], right_on=['lag_month'])
        ultimate_claim_count_by_acc_month['ultimate_claim_count'] = round(ultimate_claim_count_by_acc_month['claim_count'] /
                ultimate_claim_count_by_acc_month['cumulative_percent_reported_by_lag_month'], 0)
        print(ultimate_claim_count_by_acc_month)
        ultimate_claim_count_by_acc_month.to_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/ultimate_claim_count_projection_by_historical_acc_month ' + self.fc_month + '.csv', index=False)

    def RptClaimCountFromHistAccMth(self):
        '''
        Use historical accident month frequency
        Check seasonality - highly volatile and pandemic has a huge impact
        '''
        ultimate_claim_count_by_acc_month = pd.read_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/ultimate_claim_count_projection_by_historical_acc_month ' + self.fc_month + '.csv')
        report_pattern_trunc = pd.read_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/report_pattern ' + self.fc_month + '.csv')
        ult_claim_count = ultimate_claim_count_by_acc_month[['accident_month','ultimate_claim_count']]
        ult_claim_count['temp'] = 1
        lag_month_list = report_pattern_trunc[['lag_month', 'percent_report_by_lag_month_select']]
        lag_month_list['temp'] = 1
        projected_claim_count_acc_month = pd.merge(ult_claim_count, lag_month_list, on=['temp'])
        projected_claim_count_acc_month = projected_claim_count_acc_month.drop('temp', axis=1)
        
        projected_claim_count_acc_month['lag_month'] = projected_claim_count_acc_month['lag_month'].astype(
            float)
        projected_claim_count_acc_month['report_month'] = pd.to_datetime(
            projected_claim_count_acc_month['accident_month']) + projected_claim_count_acc_month['lag_month'].apply(lambda x: relativedelta(months=x))
        month_day_one = datetime.strptime(
            self.fc_month, '%Y-%m-%d').date().replace(day=1)
        projected_claim_count_acc_month = projected_claim_count_acc_month[
            projected_claim_count_acc_month['report_month'].dt.date >= month_day_one]
        projected_claim_count_acc_month['claim_count_projection'] = (
            projected_claim_count_acc_month['ultimate_claim_count']*projected_claim_count_acc_month['percent_report_by_lag_month_select'] + 0.6).apply(np.floor)
        projected_claim_count_report_month = projected_claim_count_acc_month[[
            'report_month', 'claim_count_projection']].groupby(['report_month']).sum('claim_count_projection').reset_index()
        print(projected_claim_count_report_month)
        projected_claim_count_report_month.to_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/projected_claim_count_from_historical_acc_month ' + self.fc_month + '.csv', index=False)

    def RptFreqNonLagged(self):
        ##historical reported frequency on non-lagged losses (# of loss from current month)
        sql = reportedClaimWithEE(self.fc_month, self.peril)
        reported_frequency = self.client.query(sql).to_dataframe()
        reported_frequency['reported_freq_ee'] = reported_frequency['reported_claims']/reported_frequency['earned_exposure'].astype(
            float)
        reported_frequency['reported_freq_pif'] = reported_frequency['reported_claims']/reported_frequency['pif'].astype(
            float)
        reported_frequency = reported_frequency.sort_values(by=['report_month'])
        print(reported_frequency)
        
        reported_frequency_subset = reported_frequency[reported_frequency['report_month']>= datetime.strptime(
            '2019-01-01', '%Y-%m-%d').date()]
        avg_reported_freq_ee = reported_frequency_subset['reported_freq_ee'].mean()
        std_reported_freq_ee = reported_frequency_subset['reported_freq_ee'].std()
        avg_reported_freq_pif = reported_frequency_subset['reported_freq_pif'].mean()
        std_reported_freq_pif = reported_frequency_subset['reported_freq_pif'].std()
        #ingest fc from finance
        finance_fc = pd.read_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/data/finance_fc_input.csv')
        finance_fc['avg_reported_freq_ee'] = avg_reported_freq_ee
        finance_fc['std_reported_freq_ee'] = std_reported_freq_ee
        finance_fc['avg_reported_freq_pif'] = avg_reported_freq_pif
        finance_fc['std_reported_freq_pif'] = std_reported_freq_pif
        finance_fc['claim_count_projection_nonlagged_mid'] = finance_fc['pif'] * finance_fc['avg_reported_freq_pif']
        finance_fc['claim_count_projection_nonlagged_low'] = finance_fc['pif'] * (finance_fc['avg_reported_freq_pif'] - finance_fc['std_reported_freq_pif'])
        finance_fc['claim_count_projection_nonlagged_high'] = finance_fc['pif'] * (finance_fc['avg_reported_freq_pif'] + finance_fc['std_reported_freq_pif'])
        print(finance_fc)

        projected_claim_count_report_month = pd.read_csv(os.path.dirname(os.path.dirname(
                    os.path.realpath(__file__))) + f'/output/projected_claim_count_from_historical_acc_month ' + self.fc_month + '.csv')
        projected_claim_count_report_month['report_month'] = pd.to_datetime(projected_claim_count_report_month['report_month'])
        projected_nonlagged_claim_count = finance_fc[['report_month', 'claim_count_projection_nonlagged_mid', 'claim_count_projection_nonlagged_low', 'claim_count_projection_nonlagged_high']]
        projected_nonlagged_claim_count['report_month'] = pd.to_datetime(projected_nonlagged_claim_count['report_month'])
        final_freq_projection = pd.merge(projected_nonlagged_claim_count,
            projected_claim_count_report_month, how = 'left', on=['report_month'])
        final_freq_projection['claim_count_projection_low'] = final_freq_projection['claim_count_projection_nonlagged_low'] + final_freq_projection['claim_count_projection']
        final_freq_projection['claim_count_projection_mid'] = final_freq_projection['claim_count_projection_nonlagged_mid'] + final_freq_projection['claim_count_projection']
        final_freq_projection['claim_count_projection_high'] = final_freq_projection['claim_count_projection_nonlagged_high'] + final_freq_projection['claim_count_projection']
        print(final_freq_projection)
        final_freq_projection.to_csv(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))) + f'/output/final_freq_projection ' + self.fc_month + '.csv', index=False)


        

