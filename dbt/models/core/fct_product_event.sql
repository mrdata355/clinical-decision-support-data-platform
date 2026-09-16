{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    cluster_by=['event_date', 'event_type']
) }}

with events as (
    select *
    from {{ ref('stg_product_events') }}
    {% if is_incremental() %}
    where source_updated_at >= dateadd(
        'day',
        -{{ var('late_arrival_days', 7) }},
        (select coalesce(max(source_updated_at), '1970-01-01'::timestamp_tz) from {{ this }})
    )
    {% endif %}
),
tools as (
    select tool_id, clinical_tool_key, primary_specialty, condition_group, tool_type
    from {{ ref('dim_clinical_tool') }}
    where is_current
),
content as (
    select content_id, content_key
    from {{ ref('dim_content') }}
    where is_current
),
accounts as (
    select account_token, account_key
    from {{ ref('dim_account') }}
),
resolved as (
    select
        e.event_id,
        e.event_type,
        e.event_ts,
        to_date(e.event_ts) as event_date,
        e.business_key,
        e.session_id,
        e.tool_id,
        t.clinical_tool_key,
        t.primary_specialty,
        t.condition_group,
        t.tool_type,
        e.content_id,
        c.content_key,
        e.quality_rating_id,
        e.account_token,
        a.account_key,
        e.user_token,
        e.organization_token,
        e.channel,
        e.country_code,
        e.language_code,
        e.integration_id,
        e.search_query_token,
        e.search_result_rank,
        e.completion_id,
        e.result_band,
        e.input_count,
        e.autofill_field_count,
        e.autofill_confirmed_count,
        e.confidence_band,
        e.recommendation_model,
        e.recommendation_position,
        e.cme_credit_hours,
        e.experiment_assignments,
        e.source_system,
        e.source_version,
        e.source_updated_at,
        e.canonical_hash,
        current_timestamp() as model_updated_at
    from events e
    left join tools t on e.tool_id = t.tool_id
    left join content c on e.content_id = c.content_id
    left join accounts a on e.account_token = a.account_token
)
select * from resolved
