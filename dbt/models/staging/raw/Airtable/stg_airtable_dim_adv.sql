-- Staging Airtable : dim_adv
-- Source : 0_airtable_raw.dim_adv (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : fait_book_raw, fait_ca
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_adv') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `FDG_Envergure` as fdg_envergure,
        `FDG_Mandataire` as fdg_mandataire,
        `Financeur`[safe_offset(0)] as financeur,  -- lien forward Airtable aplati
        `Secteur` as secteur,
        `Departement`[safe_offset(0)] as departement,  -- lien forward Airtable aplati
        `Nature_prestation` as nature_prestation,
        `Codif_lot` as codif_lot,
        `Superviseur` as superviseur,
        `CODE_SE_BU_SU`[safe_offset(0)] as code_se_bu_su,  -- lien forward Airtable aplati
        `Codif_presta_from_ADV_pour_controle` as codif_presta_from_adv_pour_controle,
        `code_boussole`[safe_offset(0)] as code_boussole,  -- lien forward Airtable aplati
        `Agence`[safe_offset(0)] as agence,  -- lien forward Airtable aplati
        `Action` as action,
        `sous_financeur`[safe_offset(0)] as sous_financeur,  -- lien forward Airtable aplati
        `codif_prestation` as codif_prestation,
        `site_boussole`[safe_offset(0)] as site_boussole,  -- lien forward Airtable aplati
        `Personne_referencee` as personne_referencee,
        `Date_de_debut` as date_de_debut,
        `id_marches` as id_marches,
        `Lot` as lot,
        `Territoire`[safe_offset(0)] as territoire,  -- lien forward Airtable aplati
        `nom_marche`[safe_offset(0)] as nom_marche,  -- lien forward Airtable aplati
        `Actif_Inactif` as actif_inactif,
        `Type` as type,
        `Sous_traitants` as sous_traitants,
        `Code_marche`[safe_offset(0)] as code_marche,  -- lien forward Airtable aplati
        `Structure` as structure,
        `Type_financeur`[safe_offset(0)] as type_financeur,  -- lien forward Airtable aplati
        `Codif_action` as codif_action,
        `N_prestation` as n_prestation,
        `Famille_de_prestation` as famille_de_prestation,
        `Initiales`[safe_offset(0)] as initiales,  -- lien forward Airtable aplati
        `Date_de_fin` as date_de_fin,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
