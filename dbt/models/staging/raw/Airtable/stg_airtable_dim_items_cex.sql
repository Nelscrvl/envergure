-- Staging Airtable : dim_items_cex
-- Source : 0_airtable_raw.dim_items_cex (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_items_cex') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `LIBELLE_CEX` as libelle_cex,
        `CODE` as code,
        `LIBELLE` as libelle,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
