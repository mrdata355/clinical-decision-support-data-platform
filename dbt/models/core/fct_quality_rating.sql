{{ config(materialized='incremental', unique_key='rating_key', incremental_strategy='merge', on_schema_change='append_new_columns') }}

with ratings as (
    select * from {{ ref('stg_quality_ratings') }}
),
tools as (
    select tool_id, clinical_tool_key
    from {{ ref('dim_clinical_tool') }}
    where is_current
)
select
    abs(hash(r.rating_id, r.rating_version)) as rating_key,
    r.rating_id,
    r.tool_id,
    t.clinical_tool_key,
    r.rating_version,
    r.overall_score,
    r.scientific_soundness_score,
    r.importance_score,
    r.usability_feasibility_score,
    r.fairness_equity_status,
    r.development_study_count,
    r.external_validation_count,
    r.guideline_alignment_status,
    r.society_guidance_refs,
    r.evidence_refs,
    r.population_notes,
    r.limitations,
    r.effective_at,
    r.retired_at,
    r.source_version,
    r.source_updated_at,
    r.canonical_hash,
    current_timestamp() as loaded_at
from ratings r
left join tools t using (tool_id)
{% if is_incremental() %}
where r.source_updated_at >= dateadd('day', -{{ var('late_arrival_days', 7) }},
    (select coalesce(max(source_updated_at), '1970-01-01'::timestamp_tz) from {{ this }}))
{% endif %}
