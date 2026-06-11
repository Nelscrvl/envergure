-- Staging Airtable : ref_departements
-- Source : 0_airtable_raw.ref_departements (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_adv
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'ref_departements') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Departement` as departement,
        `BU` as bu,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
