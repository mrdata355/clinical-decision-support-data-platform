{{ config(materialized='incremental', unique_key=['event_date','tool_id'], incremental_strategy='merge', cluster_by=['event_date']) }}

with events as (
    select *
    from {{ ref('fct_product_event') }}
    {% if is_incremental() %}
    where event_date >= dateadd('day', -{{ var('late_arrival_days', 7) }},
        (select coalesce(max(event_date), '1970-01-01'::date) from {{ this }}))
    {% endif %}
),
ratings as (
    select
        tool_id,
        max_by(overall_score, source_updated_at) as overall_score,
        max_by(scientific_soundness_score, source_updated_at) as scientific_soundness_score,
        max_by(importance_score, source_updated_at) as importance_score,
        max_by(usability_feasibility_score, source_updated_at) as usability_feasibility_score,
        max_by(fairness_equity_status, source_updated_at) as fairness_equity_status,
        max_by(external_validation_count, source_updated_at) as external_validation_count,
        max_by(guideline_alignment_status, source_updated_at) as guideline_alignment_status
    from {{ ref('fct_quality_rating') }}
    group by 1
),
agg as (
    select
        e.event_date,
        e.tool_id,
        max(e.primary_specialty) as primary_specialty,
        max(r.overall_score) as overall_score,
        max(r.scientific_soundness_score) as scientific_soundness_score,
        max(r.importance_score) as importance_score,
        max(r.usability_feasibility_score) as usability_feasibility_score,
        max(r.fairness_equity_status) as fairness_equity_status,
        max(r.external_validation_count) as external_validation_count,
        max(r.guideline_alignment_status) as guideline_alignment_status,
        count_if(e.event_type='quality_rating_view') as quality_rating_views,
        count_if(e.event_type='tool_view') as tool_views,
        count_if(e.event_type='tool_start') as tool_starts,
        count_if(e.event_type='tool_complete') as tool_completions,
        count(distinct e.user_token) as unique_users
    from events e
    left join ratings r on e.tool_id = r.tool_id
    where e.tool_id is not null
    group by 1,2
)
select
    *,
    case
      when overall_score >= 8.5 then 'reference_high'
      when overall_score >= 7.5 then 'reference_strong'
      when overall_score >= 6.0 then 'reference_moderate'
      when overall_score is null then 'unrated'
      else 'reference_review'
    end as reference_quality_band,
    div0(tool_starts, tool_views) as view_to_start_rate,
    div0(tool_completions, tool_starts) as start_to_complete_rate,
    div0(quality_rating_views, tool_views) as quality_detail_view_rate,
    current_timestamp() as model_updated_at
from agg
