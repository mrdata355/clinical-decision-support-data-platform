{{ config(materialized='incremental', unique_key='content_key', incremental_strategy='merge') }}

with source_rows as (
    select
        payload:content_id::string as content_id,
        payload:content_type::string as content_type,
        payload:title::string as title,
        payload:specialty::string as specialty,
        payload:condition_group::string as condition_group,
        payload:publication_status::string as publication_status,
        coalesce(payload:source_version::number, 1) as source_version,
        source_updated_at,
        payload_hash as row_hash
    from {{ source('raw', 'content_snapshot') }}
    qualify row_number() over (
        partition by payload:content_id::string
        order by coalesce(payload:source_version::number,1) desc, source_updated_at desc
    )=1
)
select
    abs(hash(content_id, row_hash)) as content_key,
    content_id,
    content_type,
    title,
    specialty,
    condition_group,
    publication_status,
    source_version,
    source_updated_at,
    row_hash,
    source_updated_at as valid_from,
    null::timestamp_tz as valid_to,
    true as is_current,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from source_rows
