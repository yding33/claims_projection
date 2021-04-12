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
        , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
        , date_trunc(date_first_notice_of_loss, MONTH) as report_month
        , string_field_1
        , date_knowledge
    FROM dw_prod_extracts.ext_all_claims_combined mon
        left join dbt_actuaries.claims_peril_mappings_202103 map on mon.peril = map.string_field_0
        left join dbt_actuaries.cat_coding_w_loss_20210331 cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
    WHERE date_knowledge = date_add(date_trunc('{fc_month}', MONTH), interval -1 DAY)
    and string_field_1 = '{peril}')
    
    select date_knowledge
        , date_trunc(recoded_loss_date, MONTH) as accident_month
        , date_diff(date_knowledge, recoded_loss_date, MONTH) as accident_month_age
        , sum(claim_count) as claim_count 
    from df where CAT = 'N' 
    and date_diff(date_knowledge, recoded_loss_date, MONTH) <= {max_emgergence_month}
    group by 1,2,3 order by 3
    '''

def reportedClaimWithEE(fc_month, peril):
    return f'''
    with ee as
    (select 
            date_calendar_month_accounting_basis as date_accounting_start
            
            ,sum(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl_x_policy_fees
            ,sum(earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl_x_policy_fees
            
            ,sum(earned_base + earned_total_optionals + earned_policy_fee) as earned_prem_inc_ebsl_inc_pol_fees
            ,sum(written_base + written_total_optionals + written_policy_fee) as written_prem_inc_ebsl_inc_pol_fees
            
            ,sum(written_optionals_equipment_breakdown + written_optionals_service_line) as written_EBSL
            ,sum(earned_optionals_equipment_breakdown + earned_optionals_service_line) as earned_EBSL
            
            ,sum(written_exposure) as written_exposure
            ,sum(earned_exposure) as earned_exposure
            
            ,sum(written_policy_fee) as written_policy_fee
            ,sum(earned_policy_fee) as earned_policy_fee
            
            ,sum(expense_load_digital) as written_expense_load
            ,sum(expense_load_digital*earned_exposure) as earned_expense_load
            
                    
    from dw_prod_extracts.ext_policy_monthly_premiums epud
        left join dw_prod.map_expense_loads as exp ON epud.state=exp.state and epud.product=exp.product and epud.carrier = exp.carrier
        left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) dpg on epud.policy_id = dpg.policy_id

    where date_knowledge = '2021-03-31'
        and epud.carrier <> 'canopius'
        and date_calendar_month_accounting_basis is not null
    group by 1),

    inforce as(
    SELECT
        target_month
        , sum(number_of_activations + number_of_reinstatements-number_of_terminations-number_of_expirations) as pif
        FROM dw_prod_extracts.ext_policy_in_force pif
        where target_month <= date_trunc('{fc_month}', MONTH)
        group by 1),
        
    policy_if as (select 
    target_month,
    SUM(pif) OVER (ORDER BY target_month) AS pif
    from inforce
    order by 1
    ),

    claim as(
        select mon.claim_number
            , claim_count
            , case when cc.cat_ind is true then 'Y'
                when cc.cat_ind is false then 'N'
                when peril = 'wind' or peril = 'hail' then 'Y'
                when cat_code is not null then 'Y'
            else 'N' end as CAT
            , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
            , date_trunc(date_first_notice_of_loss, MONTH) as report_month
            , string_field_1
            , date_knowledge
        FROM dw_prod_extracts.ext_all_claims_combined mon
            left join dbt_actuaries.claims_peril_mappings_202103 map on mon.peril = map.string_field_0
            left join dbt_actuaries.cat_coding_w_loss_20210331 cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
        WHERE date_knowledge = date_add(date_trunc('{fc_month}', MONTH), interval -1 DAY)
        and string_field_1 = '{peril}'),

    claim_report as(
        select report_month
            , sum(claim_count) as claim_count 
        from claim where CAT = 'N' 
        and date_diff(report_month, recoded_loss_date, MONTH) = 0
        group by 1)
        
        select report_month,
        coalesce(claim_count,0) as reported_claims,
        earned_exposure,
        pif
        from ee
            left join claim_report on claim_report.report_month = ee.date_accounting_start
            left join policy_if on policy_if.target_month = ee.date_accounting_start
        where ee.date_accounting_start < date_trunc('{fc_month}', MONTH)
    '''