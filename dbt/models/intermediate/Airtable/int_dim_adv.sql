-- Intermediate Airtable : dim_adv résolu
-- Part du staging stg_airtable_dim_adv et résout les liens forward (ID rec...)
-- vers les libellés des tables cibles, par jointure sur airtable_id.
-- Les colonnes "lookup" (code_se_bu_su, territoire, type_financeur, nom_marche,
-- initiales) sont déjà des valeurs lisibles côté staging -> conservées telles quelles.

with adv as (
    select * from {{ ref('stg_airtable_dim_adv') }}
),

financeurs as (
    select airtable_id, financeur from {{ ref('stg_airtable_dim_financeurs') }}
),

sous_financeurs as (
    select airtable_id, sous_financeur from {{ ref('stg_airtable_dim_sous_financeurs') }}
),

marches as (
    select airtable_id, code_marche, marche from {{ ref('stg_airtable_dim_marches') }}
),

agences as (
    select airtable_id, nom, code_agence from {{ ref('stg_airtable_dim_agences') }}
),

departements as (
    select airtable_id, departement, bu from {{ ref('stg_airtable_ref_departements') }}
)

select
    -- colonnes dim_adv hors liens rec... (résolus ci-dessous)
    adv.* except (financeur, sous_financeur, code_marche, agence, departement),

    -- liens forward résolus en libellés
    financeurs.financeur            as financeur_libelle,
    sous_financeurs.sous_financeur  as sous_financeur_libelle,
    marches.code_marche             as marche_code,
    marches.marche                  as marche_libelle,
    agences.nom                     as agence_nom,
    agences.code_agence             as agence_code,
    departements.departement        as departement_num,
    departements.bu                 as departement_bu

from adv
left join financeurs       on adv.financeur      = financeurs.airtable_id
left join sous_financeurs  on adv.sous_financeur = sous_financeurs.airtable_id
left join marches          on adv.code_marche    = marches.airtable_id
left join agences          on adv.agence         = agences.airtable_id
left join departements     on adv.departement    = departements.airtable_id
