{{ config(materialized='incremental', unique_key=['event_date','integration_id'], incremental_strategy='merge', cluster_by=['event_date','integration_id']) }}

with events as (
    select *
    from {{ ref('fct_product_event') }}
    {% if is_incremental() %}
    where event_date >= dateadd('day', -{{ var('late_arrival_days', 7) }},
        (select coalesce(max(event_date), '1970-01-01'::date) from {{ this }}))
    {% endif %}
),
scoped as (
    select *
    from events
    where integration_id is not null
      or event_type in ('integration_launch','ehr_context_open','ehr_autofill','ehr_result_writeback')
),
agg as (
    select
        event_date,
        coalesce(integration_id, 'unknown') as integration_id,
        count_if(event_type='integration_launch') as launches,
        count_if(event_type='ehr_context_open') as context_opens,
        count_if(event_type='ehr_autofill') as autofill_events,
        count_if(event_type='ehr_result_writeback') as result_writebacks,
        sum(coalesce(autofill_field_count,0)) as autofill_fields_offered,
        sum(coalesce(autofill_confirmed_count,0)) as autofill_fields_confirmed,
        count(distinct tool_id) as unique_tools,
        count(distinct account_key) as unique_accounts,
        count(distinct user_token) as unique_users,
        count(distinct session_id) as unique_sessions,
        count_if(event_type in ('ehr_autofill','ehr_result_writeback') and confidence_band in ('low','failed')) as degraded_events,
        count_if(event_type in ('ehr_autofill','ehr_result_writeback')) as integration_operations
    from scoped
    group by 1,2
)
select
    *,
    div0(autofill_fields_confirmed, autofill_fields_offered) as autofill_confirmation_rate,
    1 - div0(degraded_events, nullif(integration_operations,0)) as success_rate,
    div0(result_writebacks, context_opens) as context_to_writeback_rate,
    current_timestamp() as model_updated_at
from agg
