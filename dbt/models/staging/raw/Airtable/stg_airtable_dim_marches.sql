-- Staging Airtable : dim_marches
-- Source : 0_airtable_raw.dim_marches (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_adv_4
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_marches') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `code_marche` as code_marche,
        `Marche` as marche,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
