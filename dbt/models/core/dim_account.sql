{{ config(materialized='incremental', unique_key='account_key', incremental_strategy='merge') }}

with events as (
    select
        account_token,
        min(event_ts) as first_seen_at,
        max(event_ts) as last_seen_at,
        max(country_code) as country_code,
        max(source_updated_at) as source_updated_at
    from {{ ref('stg_product_events') }}
    where account_token is not null
    group by 1
)
select
    abs(hash(account_token)) as account_key,
    account_token,
    'user_account' as account_type,
    null::string as organization_token,
    country_code,
    to_date(first_seen_at) as created_date,
    first_seen_at,
    last_seen_at,
    true as is_active,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from events
