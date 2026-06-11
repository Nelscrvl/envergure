-- Staging Airtable : dim_financeurs
-- Source : 0_airtable_raw.dim_financeurs (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_adv, dim_sous_financeurs
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_financeurs') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Initiales` as initiales,
        `Financeur` as financeur,
        `Type_financeur` as type_financeur,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
