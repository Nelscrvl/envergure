-- Staging Airtable : dim_territoires_su
-- Source : 0_airtable_raw.dim_territoires_su (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_affectation_bu_exceptions, dim_sites_boussole, fait_loyers_affectation, ref_agences
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_territoires_su') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `LIBELLE_GRAND_TERRITOIRE` as libelle_grand_territoire,
        `CODE_SE_BU_SU` as code_se_bu_su,
        `TYPE_LIBELLE` as type_libelle,
        `ACRONYME` as acronyme,
        `ANALYTIQUE` as analytique,
        `Manual_sort` as manual_sort,
        `LIBELLE_SE_BU_SU` as libelle_se_bu_su,
        `CODE_GRAND_TERRITOIRE` as code_grand_territoire,
        `TERRITOIRE_SUPPORT` as territoire_support,
        `CODE_UNIQUE` as code_unique,
        `BRANCHE` as branche,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
