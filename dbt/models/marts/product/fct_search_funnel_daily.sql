{{ config(materialized='incremental', unique_key=['event_date','channel','country_code'], incremental_strategy='merge', cluster_by=['event_date']) }}

with events as (
    select *
    from {{ ref('fct_product_event') }}
    {% if is_incremental() %}
    where event_date >= dateadd('day', -{{ var('late_arrival_days', 7) }},
        (select coalesce(max(event_date), '1970-01-01'::date) from {{ this }}))
    {% endif %}
),
search_sessions as (
    select
        event_date,
        channel,
        country_code,
        session_id,
        count_if(event_type='search') as searches,
        count_if(event_type='search_result_click') as search_result_clicks,
        count_if(event_type='tool_view') as downstream_tool_views,
        count_if(event_type='tool_start') as downstream_tool_starts,
        count_if(event_type='tool_complete') as downstream_tool_completions,
        count(distinct iff(event_type='search', search_query_token, null)) as unique_query_tokens,
        min(iff(event_type='search', event_ts, null)) as first_search_at,
        min(iff(event_type='tool_view', event_ts, null)) as first_tool_view_at
    from events
    where session_id is not null
    group by 1,2,3,4
),
rolled as (
    select
        event_date,
        channel,
        country_code,
        sum(searches) as searches,
        sum(search_result_clicks) as search_result_clicks,
        sum(downstream_tool_views) as downstream_tool_views,
        sum(downstream_tool_starts) as downstream_tool_starts,
        sum(downstream_tool_completions) as downstream_tool_completions,
        sum(unique_query_tokens) as unique_query_tokens,
        count_if(searches > 0) as search_sessions,
        count_if(searches > 0 and downstream_tool_views > 0) as search_to_tool_sessions,
        avg(iff(searches > 0 and first_tool_view_at is not null,
            datediff('second', first_search_at, first_tool_view_at), null)) as avg_seconds_search_to_tool
    from search_sessions
    group by 1,2,3
)
select
    *,
    div0(search_result_clicks, searches) as search_result_ctr,
    div0(search_to_tool_sessions, search_sessions) as search_to_tool_rate,
    div0(downstream_tool_starts, downstream_tool_views) as tool_start_rate,
    div0(downstream_tool_completions, downstream_tool_starts) as tool_completion_rate,
    current_timestamp() as model_updated_at
from rolled
