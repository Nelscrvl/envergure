-- Staging Airtable : dim_sites_boussole
-- Source : 0_airtable_raw.dim_sites_boussole (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_agences
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_sites_boussole') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `ACRONYME_from_dim_territoires_su`[safe_offset(0)] as acronyme_from_dim_territoires_su,  -- lien forward Airtable aplati
        `dim_territoires_su`[safe_offset(0)] as dim_territoires_su,  -- lien forward Airtable aplati
        `analytique_boussole_libelle` as analytique_boussole_libelle,
        `code_boussole` as code_boussole,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
