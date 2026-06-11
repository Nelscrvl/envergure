-- Staging Airtable : fait_book_raw
-- Source : 0_airtable_raw.fait_book_raw (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Colonnes liens inverses exclues : dim_adv
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'fait_book_raw') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `CA_a_facturer` as ca_a_facturer,
        `Commentaires` as commentaires,
        `Mai` as mai,
        `Juin` as juin,
        `Mars` as mars,
        `Lot` as lot,
        `Referent_ADV` as referent_adv,
        `Agence` as agence,
        `Type` as type,
        `Action` as action,
        `Agence_BOUSSOLE` as agence_boussole,
        `Actif_Inactif` as actif_inactif,
        `Date_de_fin` as date_de_fin,
        `Extournes_25_26` as extournes_25_26,
        `_1` as _1,
        `Codif_Lot` as codif_lot,
        `Analytique_BOUSSOLE` as analytique_boussole,
        `Total_Realise` as total_realise,
        `Avril` as avril,
        `Ecart_Activite_Vs_ADV` as ecart_activite_vs_adv,
        `Nature_prestation` as nature_prestation,
        `Extournes_2025_reelles` as extournes_2025_reelles,
        `Analytique_BU` as analytique_bu,
        `Janvier` as janvier,
        `FDG_Mandataire` as fdg_mandataire,
        `Structure` as structure,
        `Ecart_Extournes` as ecart_extournes,
        `Marche` as marche,
        `Territoire` as territoire,
        `FINANCEUR` as financeur,
        `N_presta` as n_presta,
        `TOTAL` as total,
        `Sous_financeur` as sous_financeur,
        `_10` as _10,
        `Famille_prestation` as famille_prestation,
        `CA_Facture` as ca_facture,
        `_7` as _7,
        `_8` as _8,
        `_6` as _6,
        `CODIFICATION_PRESTATION` as codification_prestation,
        `_3` as _3,
        `_5` as _5,
        `_11` as _11,
        `Secteur` as secteur,
        `Sous_traitants` as sous_traitants,
        `_4` as _4,
        `FDG_Envergure` as fdg_envergure,
        `Date_de_debut` as date_de_debut,
        `CODIFICATION_ACTION` as codification_action,
        `_2` as _2,
        `Fevrier` as fevrier,
        `_9` as _9,
        `N_session_convention` as n_session_convention,
        `Dept` as dept,
        `CA_TOTAL_ajuste` as ca_total_ajuste,
        `_12` as _12,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
