-- Staging Airtable : dim_sous_financeurs
-- Source : 0_airtable_raw.dim_sous_financeurs (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_adv
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_sous_financeurs') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Financeur_associe`[safe_offset(0)] as financeur_associe,  -- lien forward Airtable aplati
        `Sous_financeur` as sous_financeur,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
