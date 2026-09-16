{{ config(materialized='incremental', unique_key=['event_date','model_name','model_version'], incremental_strategy='merge', cluster_by=['event_date']) }}

with events as (
    select *
    from {{ ref('fct_product_event') }}
    {% if is_incremental() %}
    where event_date >= dateadd('day', -{{ var('late_arrival_days', 7) }},
        (select coalesce(max(event_date), '1970-01-01'::date) from {{ this }}))
    {% endif %}
),
normalized as (
    select
        event_date,
        coalesce(split_part(recommendation_model, ':', 1), 'unknown') as model_name,
        coalesce(nullif(split_part(recommendation_model, ':', 2), ''), 'unknown') as model_version,
        event_type,
        session_id,
        user_token,
        tool_id,
        recommendation_position
    from events
    where event_type in ('recommendation_impression','recommendation_click','tool_start','tool_complete')
      and recommendation_model is not null
),
agg as (
    select
        event_date,
        model_name,
        model_version,
        count_if(event_type='recommendation_impression') as impressions,
        count_if(event_type='recommendation_click') as clicks,
        count_if(event_type='tool_start') as attributed_tool_starts,
        count_if(event_type='tool_complete') as attributed_tool_completions,
        count(distinct user_token) as unique_users,
        count(distinct session_id) as unique_sessions,
        count(distinct tool_id) as unique_tools,
        avg(iff(event_type='recommendation_impression', recommendation_position, null)) as avg_impression_position
    from normalized
    group by 1,2,3
)
select
    *,
    div0(clicks, impressions) as ctr,
    div0(attributed_tool_starts, clicks) as click_to_start_rate,
    div0(attributed_tool_completions, clicks) as click_to_completion_rate,
    current_timestamp() as model_updated_at
from agg
