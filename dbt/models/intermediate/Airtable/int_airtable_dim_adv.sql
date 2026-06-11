-- Intermediate Airtable : dim_adv résolu
-- Part de stg_airtable_dim_adv, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_dim_adv') }}
),

j_financeur as (
    select airtable_id, financeur from {{ ref('stg_airtable_dim_financeurs') }}
),

j_departement as (
    select airtable_id, departement from {{ ref('stg_airtable_ref_departements') }}
),

j_code_boussole as (
    select airtable_id, code_boussole from {{ ref('stg_airtable_dim_sites_boussole') }}
),

j_agence as (
    select airtable_id, nom from {{ ref('stg_airtable_dim_agences') }}
),

j_sous_financeur as (
    select airtable_id, sous_financeur from {{ ref('stg_airtable_dim_sous_financeurs') }}
),

j_code_marche as (
    select airtable_id, marche from {{ ref('stg_airtable_dim_marches') }}
)

select
    base.* except (financeur, departement, code_boussole, agence, sous_financeur, code_marche),
    j_financeur.financeur as financeur,  -- résolu -> dim_financeurs
    j_departement.departement as departement,  -- résolu -> ref_departements
    j_code_boussole.code_boussole as code_boussole,  -- résolu -> dim_sites_boussole
    j_agence.nom as agence,  -- résolu -> dim_agences
    j_sous_financeur.sous_financeur as sous_financeur,  -- résolu -> dim_sous_financeurs
    j_code_marche.marche as code_marche,  -- résolu -> dim_marches
from base
left join j_financeur on base.financeur = j_financeur.airtable_id
left join j_departement on base.departement = j_departement.airtable_id
left join j_code_boussole on base.code_boussole = j_code_boussole.airtable_id
left join j_agence on base.agence = j_agence.airtable_id
left join j_sous_financeur on base.sous_financeur = j_sous_financeur.airtable_id
left join j_code_marche on base.code_marche = j_code_marche.airtable_id
