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
        payload:quality_rating_id::string as quality_rating_id,
        payload:account_token::string as account_token,
        payload:user_token::string as user_token,
        payload:organization_token::string as organization_token,
        lower(payload:channel::string) as channel,
        upper(payload:country_code::string) as country_code,
        lower(payload:language_code::string) as language_code,
        payload:integration_id::string as integration_id,
        payload:query_token::string as search_query_token,
        payload:search_result_rank::number as search_result_rank,
        payload:completion_id::string as completion_id,
        payload:result_band::string as result_band,
        payload:input_count::number as input_count,
        payload:autofill_field_count::number as autofill_field_count,
        payload:autofill_confirmed_count::number as autofill_confirmed_count,
        payload:confidence_band::string as confidence_band,
        payload:recommendation_model::string as recommendation_model,
        payload:recommendation_position::number as recommendation_position,
        payload:cme_credit_hours::float as cme_credit_hours,
        payload:experiment_assignments as experiment_assignments,
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
    select *, row_number() over (
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
        quality_rating_id,
        account_token,
        user_token,
        organization_token,
        coalesce(channel, 'web') as channel,
        coalesce(country_code, '{{ var("default_country_code", "ZZ") }}') as country_code,
        language_code,
        integration_id,
        search_query_token,
        search_result_rank,
        completion_id,
        result_band,
        input_count,
        autofill_field_count,
        autofill_confirmed_count,
        confidence_band,
        recommendation_model,
        recommendation_position,
        cme_credit_hours,
        experiment_assignments,
        canonical_hash,
        ingested_at,
        current_timestamp() as normalized_at
    from ranked
    where version_rank = 1
)
select * from normalized
