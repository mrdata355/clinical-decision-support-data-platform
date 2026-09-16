{{ config(materialized='incremental', unique_key='rating_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}

with source_rows as (
    select
        rating_id,
        tool_id,
        rating_version,
        overall_score,
        scientific_soundness_score,
        importance_score,
        usability_feasibility_score,
        fairness_equity_status,
        development_study_count,
        external_validation_count,
        guideline_alignment_status,
        society_guidance_refs,
        evidence_refs,
        population_notes,
        limitations,
        effective_at,
        retired_at,
        source_version,
        source_updated_at,
        payload_hash as canonical_hash,
        ingested_at
    from {{ source('raw', 'quality_rating_snapshot') }}
    {% if is_incremental() %}
      where ingested_at >= dateadd('day', -{{ var('clinical_content_lookback_days', 3) }},
          (select coalesce(max(ingested_at), '1970-01-01'::timestamp_tz) from {{ this }}))
    {% endif %}
),
ranked as (
    select *, row_number() over (
        partition by rating_id
        order by source_version desc, source_updated_at desc, ingested_at desc
    ) as version_rank
    from source_rows
)
select * exclude(version_rank)
from ranked
where version_rank = 1
  and abs(overall_score - (scientific_soundness_score + importance_score + usability_feasibility_score)) <= 0.01
