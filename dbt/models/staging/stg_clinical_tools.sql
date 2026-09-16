{{ config(materialized='incremental', unique_key='tool_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}

with source_rows as (
    select
        payload:tool_id::string as tool_id,
        payload:tool_slug::string as tool_slug,
        payload:tool_name::string as tool_name,
        payload:short_description::string as short_description,
        lower(payload:tool_type::string) as tool_type,
        payload:primary_specialty::string as primary_specialty,
        payload:specialties as specialties,
        payload:conditions as conditions,
        payload:chief_complaints as chief_complaints,
        payload:organ_systems as organ_systems,
        payload:condition_group::string as condition_group,
        payload:evidence_status::string as evidence_status,
        payload:quality_rating:overall::float as quality_overall_score,
        payload:quality_rating:scientific_soundness::float as scientific_soundness_score,
        payload:quality_rating:importance::float as importance_score,
        payload:quality_rating:usability_feasibility::float as usability_feasibility_score,
        payload:quality_rating:fairness_equity_status::string as fairness_equity_status,
        payload:quality_rating:rating_version::string as quality_rating_version,
        payload:inputs as input_contract,
        payload:result_contract as result_contract,
        payload:critical_actions as critical_actions,
        payload:limitations as limitations,
        payload:creator_ids as creator_ids,
        payload:contributor_ids as contributor_ids,
        payload:evidence_reference_ids as evidence_reference_ids,
        payload:guideline_reference_ids as guideline_reference_ids,
        coalesce(payload:cme_eligible::boolean, false) as cme_eligible,
        coalesce(payload:ehr_enabled::boolean, false) as ehr_enabled,
        coalesce(payload:suggested_calc_eligible::boolean, false) as suggested_calc_eligible,
        lower(payload:publication_status::string) as publication_status,
        try_to_timestamp_tz(payload:first_published_at::string) as first_published_at,
        try_to_timestamp_tz(payload:last_reviewed_at::string) as last_reviewed_at,
        coalesce(payload:source_version::number, 1) as source_version,
        source_updated_at,
        payload_hash as canonical_hash,
        ingested_at
    from {{ source('raw', 'clinical_tool_snapshot') }}
    where payload:tool_id is not null
    {% if is_incremental() %}
      and ingested_at >= dateadd('day', -{{ var('clinical_content_lookback_days', 3) }},
          (select coalesce(max(ingested_at), '1970-01-01'::timestamp_tz) from {{ this }}))
    {% endif %}
),
ranked as (
    select *,
        row_number() over (
            partition by tool_id
            order by source_version desc, source_updated_at desc, ingested_at desc
        ) as version_rank
    from source_rows
)
select * exclude(version_rank)
from ranked
where version_rank = 1
