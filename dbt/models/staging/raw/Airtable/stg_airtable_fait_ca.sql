-- Staging Airtable : fait_ca
-- Source : 0_airtable_raw.fait_ca (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'fait_ca') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Codif_action_from_dim_adv`[safe_offset(0)] as codif_action_from_dim_adv,  -- lien forward Airtable aplati
        `Agence_from_dim_adv`[safe_offset(0)] as agence_from_dim_adv,  -- lien forward Airtable aplati
        `Famille_de_prestation_from_dim_adv`[safe_offset(0)] as famille_de_prestation_from_dim_adv,  -- lien forward Airtable aplati
        `CA` as ca,
        `Codif_prestation` as codif_prestation,
        `Nature_prestation_from_dim_adv`[safe_offset(0)] as nature_prestation_from_dim_adv,  -- lien forward Airtable aplati
        `Action_from_dim_adv`[safe_offset(0)] as action_from_dim_adv,  -- lien forward Airtable aplati
        `CODE_SE_BU_SU_from_dim_adv`[safe_offset(0)] as code_se_bu_su_from_dim_adv,  -- lien forward Airtable aplati
        `dim_adv`[safe_offset(0)] as dim_adv,  -- lien forward Airtable aplati
        `Mois` as mois,
        `Territoire_from_dim_adv`[safe_offset(0)] as territoire_from_dim_adv,  -- lien forward Airtable aplati
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
