{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}

with raw as (
    select
        event_id,
        lower(event_type) as event_type,
        event_ts,
        source_updated_at,
        source_system,
        coalesce(payload:source_version::number, 1) as source_version,
        coalesce(business_key, event_id) as business_key,
        payload:session_id::string as session_id,
        payload:tool_id::string as tool_id,
        payload:content_id::string as content_id,
        payload:account_token::string as account_token,
        payload:user_token::string as user_token,
        lower(payload:channel::string) as channel,
        upper(payload:country_code::string) as country_code,
        lower(payload:language_code::string) as language_code,
        payload:integration_id::string as integration_id,
        payload:query_token::string as search_query_token,
        payload:completion_id::string as completion_id,
        sha2(to_json(payload), 256) as canonical_hash,
        ingested_at
    from {{ source('raw', 'product_event') }}
    where event_id is not null
      and event_type is not null
      {% if is_incremental() %}
      and ingested_at >= dateadd('hour', -{{ var('product_event_lookback_hours', 48) }},
          (select coalesce(max(ingested_at), '1970-01-01'::timestamp_tz) from {{ this }}))
      {% endif %}
),
ranked as (
    select
        *,
        row_number() over (
            partition by event_id
            order by source_version desc, source_updated_at desc, ingested_at desc
        ) as version_rank
    from raw
),
normalized as (
    select
        event_id,
        event_type,
        event_ts,
        source_updated_at,
        source_system,
        source_version,
        business_key,
        session_id,
        tool_id,
        content_id,
        account_token,
        user_token,
        channel,
        coalesce(country_code, '{{ var("default_country_code", "ZZ") }}') as country_code,
        language_code,
        integration_id,
        search_query_token,
        completion_id,
        canonical_hash,
        ingested_at,
        current_timestamp() as normalized_at
    from ranked
    where version_rank = 1
)
select * from normalized
