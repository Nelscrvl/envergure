-- Staging Airtable : dim_agences
-- Source : 0_airtable_raw.dim_agences (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_adv, dim_affectation_bu_exceptions, fait_loyers_affectation, loyers_saisie
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_agences') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Statut` as statut,
        `Bailleur` as bailleur,
        `cd_terr_su`[safe_offset(0)] as cd_terr_su,  -- lien forward Airtable aplati
        `id_agence` as id_agence,
        `Departement` as departement,
        `Adresse` as adresse,
        `ACRONYME`[safe_offset(0)] as acronyme,  -- lien forward Airtable aplati
        `LIBELLE_GRAND_TERRITOIRE_from_cd_terr_su`[safe_offset(0)] as libelle_grand_territoire_from_cd_terr_su,  -- lien forward Airtable aplati
        `CODE_SE_BU_SU_from_cd_terr_su`[safe_offset(0)] as code_se_bu_su_from_cd_terr_su,  -- lien forward Airtable aplati
        `Code_boussole`[safe_offset(0)] as code_boussole,  -- lien forward Airtable aplati
        `analytique_boussole_libelle`[safe_offset(0)] as analytique_boussole_libelle,  -- lien forward Airtable aplati
        `CODE_GRAND_TERRITOIRE_from_cd_terr_su`[safe_offset(0)] as code_grand_territoire_from_cd_terr_su,  -- lien forward Airtable aplati
        `Code_agence` as code_agence,
        `Type_bail` as type_bail,
        `Nom` as nom,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
