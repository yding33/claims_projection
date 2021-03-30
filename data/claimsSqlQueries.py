'''
This sript is designed to hodl a series of function that return the text of SQL queries.
'''

def reportedClaimsByAccMonth(fc_month, peril, max_emgergence_month):
    return f'''
    with df as(
    SELECT
    claim_number
        , claim_count
        , case when peril = 'wind' or peril = 'hail' then 'Y'
        when cat_code is not null then 'Y'
        when is_CAT is true then 'Y'
        else 'N' end as CAT
        , date_trunc(date_of_loss, MONTH) as accident_month
        , date_trunc(date_first_notice_of_loss, MONTH) as report_month
        , date_diff(date_first_notice_of_loss, date_of_loss, MONTH) as report_lag_month
        , date_diff(date_knowledge, date_of_loss, MONTH) as accident_month_age
        , string_field_1
        , date_knowledge
    FROM dw_prod_extracts.ext_all_claims_combined mon
        left join dbt_actuaries.claims_mappings_202012 map on mon.peril = map.string_field_0
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