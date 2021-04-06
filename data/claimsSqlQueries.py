'''
This sript is designed to hodl a series of function that return the text of SQL queries.
'''

def historicalReportPattern(fc_month, peril):
    return f'''
    with claim_supp as(
    SELECT mon.*
        , case when cc.cat_ind is true then 'Y'
            when cc.cat_ind is false then 'N'
            when peril = 'wind' or peril = 'hail' then 'Y'
            when cat_code is not null then 'Y'
          else 'N' end as CAT
        , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    FROM dw_prod_extracts.ext_all_claims_combined mon
        left join dbt_actuaries.cat_coding_w_loss_20210331 cc on (case when tbl_source = 'topa_tpa_claims' then trim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
    WHERE date_knowledge = date_add(date_trunc('{fc_month}', MONTH), interval -1 DAY)
        and peril_group = '{peril}'),

    agg as(
    select DATE_DIFF(date_first_notice_of_loss, recoded_loss_date, month) AS lag_month,
        sum(claim_count) as claim_count
    from claim_supp
    where CAT = 'N'
    group by 1
    order by 1 desc)

    select lag_month, claim_count, claim_count/total as percent_reported_by_lag_month
    from agg,
    (select sum(claim_count) as total from agg)
    order by 1,2
    '''

def reportedClaimsByAccMonth(fc_month, peril, max_emgergence_month):
    return f'''
    with df as(
    SELECT
    mon.claim_number
        , claim_count
        , case when cc.cat_ind is true then 'Y'
            when cc.cat_ind is false then 'N'
            when peril = 'wind' or peril = 'hail' then 'Y'
            when cat_code is not null then 'Y'
          else 'N' end as CAT
        , date_trunc(date_of_loss, MONTH) as accident_month
        , date_trunc(date_first_notice_of_loss, MONTH) as report_month
        , date_diff(date_first_notice_of_loss, date_of_loss, MONTH) as report_lag_month
        , date_diff(date_knowledge, date_of_loss, MONTH) as accident_month_age
        , string_field_1
        , date_knowledge
    FROM dw_prod_extracts.ext_all_claims_combined mon
        left join dbt_actuaries.claims_peril_mappings_202103 map on mon.peril = map.string_field_0
        left join dbt_actuaries.cat_coding_w_loss_20210331 cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
    WHERE date_knowledge = date_add(date_trunc('{fc_month}', MONTH), interval -1 DAY)
    and string_field_1 = '{peril}'
    order by date_diff(date_first_notice_of_loss, date_of_loss, DAY)  desc)
    
    select date_knowledge
        , accident_month
        , accident_month_age
        , sum(claim_count) as claim_count 
    from df where CAT = 'N' 
    and accident_month_age<={max_emgergence_month}
    group by 1,2,3 order by 3
    '''