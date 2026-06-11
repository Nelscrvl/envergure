-- Intermediate Airtable : dim_agences résolu
-- Part de stg_airtable_dim_agences, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_dim_agences') }}
),

j_cd_terr_su as (
    select airtable_id, code_se_bu_su from {{ ref('stg_airtable_dim_territoires_su') }}
),

j_code_boussole as (
    select airtable_id, code_boussole from {{ ref('stg_airtable_dim_sites_boussole') }}
)

select
    base.* except (cd_terr_su, code_boussole),
    j_cd_terr_su.code_se_bu_su as cd_terr_su,  -- résolu -> dim_territoires_su
    j_code_boussole.code_boussole as code_boussole,  -- résolu -> dim_sites_boussole
from base
left join j_cd_terr_su on base.cd_terr_su = j_cd_terr_su.airtable_id
left join j_code_boussole on base.code_boussole = j_code_boussole.airtable_id
