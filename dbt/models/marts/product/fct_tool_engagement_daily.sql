{{ config(materialized='table', cluster_by=['event_date', 'primary_specialty']) }}

with events as (
    select * from {{ ref('fct_product_event') }}
),
tools as (
    select
        clinical_tool_key,
        tool_id,
        tool_name,
        primary_specialty,
        condition_group
    from {{ ref('dim_clinical_tool') }}
    where is_current
),
base as (
    select
        e.event_date,
        e.clinical_tool_key,
        t.tool_id,
        t.tool_name,
        t.primary_specialty,
        t.condition_group,
        e.channel,
        e.country_code,
        e.user_token,
        e.account_key,
        e.session_id,
        e.event_type,
        e.event_ts
    from events e
    join tools t on e.clinical_tool_key = t.clinical_tool_key
    where e.event_type in ('tool_view','tool_start','tool_complete','favorite_add','favorite_remove')
),
daily as (
    select
        event_date,
        clinical_tool_key,
        tool_id,
        tool_name,
        primary_specialty,
        condition_group,
        channel,
        country_code,
        count_if(event_type='tool_view') as tool_views,
        count_if(event_type='tool_start') as tool_starts,
        count_if(event_type='tool_complete') as tool_completions,
        count_if(event_type='favorite_add') as favorite_adds,
        count_if(event_type='favorite_remove') as favorite_removes,
        count(distinct user_token) as unique_users,
        count(distinct account_key) as unique_accounts,
        count(distinct session_id) as sessions,
        min(event_ts) as first_event_at,
        max(event_ts) as last_event_at
    from base
    group by 1,2,3,4,5,6,7,8
),
conversion as (
    select
        *,
        div0(tool_starts, tool_views) as view_to_start_rate,
        div0(tool_completions, tool_starts) as start_to_complete_rate,
        div0(tool_completions, tool_views) as view_to_complete_rate,
        favorite_adds - favorite_removes as net_favorite_change,
        div0(tool_completions, sessions) as completions_per_session,
        div0(tool_views, unique_users) as views_per_user
    from daily
),
with_prior as (
    select
        *,
        lag(tool_views, 1) over (
            partition by clinical_tool_key, channel, country_code
            order by event_date
        ) as prior_day_views,
        lag(tool_completions, 1) over (
            partition by clinical_tool_key, channel, country_code
            order by event_date
        ) as prior_day_completions,
        avg(tool_views) over (
            partition by clinical_tool_key, channel, country_code
            order by event_date rows between 6 preceding and current row
        ) as rolling_7d_avg_views,
        avg(tool_completions) over (
            partition by clinical_tool_key, channel, country_code
            order by event_date rows between 6 preceding and current row
        ) as rolling_7d_avg_completions,
        avg(unique_users) over (
            partition by clinical_tool_key, channel, country_code
            order by event_date rows between 27 preceding and current row
        ) as rolling_28d_avg_users
    from conversion
),
final as (
    select
        event_date,
        clinical_tool_key,
        tool_id,
        tool_name,
        primary_specialty,
        condition_group,
        channel,
        country_code,
        tool_views,
        tool_starts,
        tool_completions,
        favorite_adds,
        favorite_removes,
        net_favorite_change,
        unique_users,
        unique_accounts,
        sessions,
        view_to_start_rate,
        start_to_complete_rate,
        view_to_complete_rate,
        completions_per_session,
        views_per_user,
        first_event_at,
        last_event_at,
        prior_day_views,
        prior_day_completions,
        rolling_7d_avg_views,
        rolling_7d_avg_completions,
        rolling_28d_avg_users,
        div0(tool_views - prior_day_views, nullif(prior_day_views,0)) as day_over_day_view_change,
        div0(tool_completions - prior_day_completions, nullif(prior_day_completions,0)) as day_over_day_completion_change,
        current_timestamp() as refreshed_at
    from with_prior
)
select * from final
