{{ config(materialized='incremental', unique_key='clinical_tool_key', incremental_strategy='merge') }}

with source_rows as (
    select
        payload:tool_id::string as tool_id,
        payload:tool_slug::string as tool_slug,
        payload:tool_name::string as tool_name,
        payload:primary_specialty::string as primary_specialty,
        payload:condition_group::string as condition_group,
        payload:evidence_status::string as evidence_status,
        payload:publication_status::string as publication_status,
        try_to_timestamp_tz(payload:first_published_at::string) as first_published_at,
        try_to_timestamp_tz(payload:last_reviewed_at::string) as last_reviewed_at,
        coalesce(payload:source_version::number, 1) as source_version,
        source_updated_at,
        payload_hash as row_hash
    from {{ source('raw', 'clinical_tool_snapshot') }}
    qualify row_number() over (
        partition by payload:tool_id::string
        order by coalesce(payload:source_version::number,1) desc, source_updated_at desc
    ) = 1
),
existing as (
    select * from {{ this }}
    {% if not is_incremental() %}
    where 1=0
    {% endif %}
),
resolved as (
    select
        abs(hash(s.tool_id, s.row_hash)) as clinical_tool_key,
        s.tool_id,
        s.tool_slug,
        s.tool_name,
        s.primary_specialty,
        s.condition_group,
        s.evidence_status,
        s.publication_status,
        s.first_published_at,
        s.last_reviewed_at,
        s.source_version,
        s.source_updated_at,
        s.row_hash,
        s.source_updated_at as valid_from,
        null::timestamp_tz as valid_to,
        true as is_current,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from source_rows s
)
select * from resolved
