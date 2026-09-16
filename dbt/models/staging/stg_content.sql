{{ config(materialized='incremental', unique_key='content_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}

with source_rows as (
    select
        payload:content_id::string as content_id,
        lower(payload:content_type::string) as content_type,
        payload:title::string as title,
        payload:summary::string as summary,
        payload:body_ref::string as body_ref,
        payload:tool_ids as tool_ids,
        payload:specialties as specialties,
        payload:condition_groups as condition_groups,
        payload:society_id::string as society_id,
        try_to_date(payload:guideline_publication_date::string) as guideline_publication_date,
        payload:evidence_reference_ids as evidence_reference_ids,
        payload:contributor_ids as contributor_ids,
        coalesce(payload:cme_eligible::boolean, false) as cme_eligible,
        payload:cme_credit_hours::float as cme_credit_hours,
        lower(payload:review_status::string) as review_status,
        try_to_timestamp_tz(payload:reviewed_at::string) as reviewed_at,
        lower(payload:publication_status::string) as publication_status,
        try_to_timestamp_tz(payload:first_published_at::string) as first_published_at,
        coalesce(payload:source_version::number, 1) as source_version,
        source_updated_at,
        payload_hash as canonical_hash,
        ingested_at
    from {{ source('raw', 'content_snapshot') }}
    where payload:content_id is not null
    {% if is_incremental() %}
      and ingested_at >= dateadd('day', -{{ var('clinical_content_lookback_days', 3) }},
          (select coalesce(max(ingested_at), '1970-01-01'::timestamp_tz) from {{ this }}))
    {% endif %}
),
ranked as (
    select *, row_number() over (
        partition by content_id
        order by source_version desc, source_updated_at desc, ingested_at desc
    ) as version_rank
    from source_rows
)
select * exclude(version_rank)
from ranked
where version_rank = 1
