{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='merge') }}

select
    e.event_id,
    e.event_ts,
    e.event_date,
    e.event_type,
    e.content_key,
    e.account_key,
    e.user_token,
    e.session_id,
    e.channel,
    e.country_code,
    try_to_number(e.payload:cme_credit_hours::string, 10, 2) as cme_credit_hours,
    e.source_system,
    e.source_version,
    e.source_updated_at,
    e.canonical_hash
from {{ ref('fct_product_event') }} e
where e.event_type in ('cme_content_view', 'cme_credit_earned')
{% if is_incremental() %}
  and e.event_date >= dateadd('day', -{{ var('late_arrival_days', 7) }},
      (select coalesce(max(event_date), '1970-01-01'::date) from {{ this }}))
{% endif %}
