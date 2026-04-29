{{ config(schema='2_mart') }}

{# Consolidation au format BDD EDM (long, type / annee / mois / rubrique / valeur).
   Faits   : MRT_CA, MRT_FDG, MRT_MS_PROD, MRT_SS_TRAITANT, MRT_CA_MANDAT.
   Chaque MRT porte déjà son `code_rubrique` (total + famille-spécifique en
   lignes distinctes). Cette table se contente de :
     - faire le UNION ALL des faits
     - lookup le libellé rubrique dans REF_RUBRIQUES
     - lookup la prestation via seed (famille → ACCOMPAGNEMENT/...)
     - lookup BU libellé + branche via REF_TERRITOIRES
     - lookup marché / site via les refs actions / agences
   Si un libellé bouge dans un référentiel, tout se propage automatiquement. #}

WITH

-- 1. Faits : UNION ALL des MRT (codif × bu × mois × code_rubrique × famille × valeur)
fact AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_CA') }}          WHERE valeur IS NOT NULL AND valeur != 0
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_FDG') }}         WHERE valeur IS NOT NULL AND valeur != 0
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_MS_PROD') }}     WHERE valeur IS NOT NULL AND valeur != 0
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_SS_TRAITANT') }} WHERE valeur IS NOT NULL AND valeur != 0
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_CA_MANDAT') }}   WHERE valeur IS NOT NULL AND valeur != 0
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_MB') }}          WHERE valeur IS NOT NULL AND valeur != 0
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_CSR') }}         WHERE valeur IS NOT NULL AND valeur != 0
),

-- 2. Dimension action — 1 ligne par codif (côté ENV)
dim_action_env AS (
  SELECT
    codification_action,
    marche,
    analytique_boussole
  FROM {{ source('DAF_Sources', 'ENV_REF_ACTIONS') }}
  WHERE codification_action IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codification_action ORDER BY codification_action) = 1
),

-- 3. Dimension action — 1 ligne par codif (côté ESO / BEN17)
dim_action_eso AS (
  SELECT
    Codif_action_site                              AS codification_action,
    March__                                        AS marche,
    Code_Site                                      AS analytique_boussole,
    Agence                                         AS agence_fallback
  FROM {{ source('DAF_Sources', 'BEN17_REF_MARCHE') }}
  WHERE Codif_action_site IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Codif_action_site ORDER BY Codif_action_site) = 1
),

-- 4. Dimension site (libellé via analytique_boussole)
dim_site AS (
  SELECT analytique_boussole, ANY_VALUE(analytique_boussole_libelle) AS site_libelle
  FROM {{ source('DAF_Sources', 'REF_AGENCES') }}
  WHERE analytique_boussole IS NOT NULL AND analytique_boussole != '-'
  GROUP BY analytique_boussole
),

-- 5. Dimension BU (code → libellé + branche) — REF_TERRITOIRES, source de vérité
dim_bu AS (
  SELECT
    code_bu_su                       AS code_bu,
    ANY_VALUE(libelle_bu_su)         AS bu_libelle,
    ANY_VALUE(branche)               AS branche
  FROM {{ source('DAF_Sources', 'REF_TERRITOIRES') }}
  WHERE code_bu_su IS NOT NULL
  GROUP BY code_bu_su
),

-- 6. Dimension rubrique (code → libellé via REF_RUBRIQUES)
dim_rubrique AS (
  SELECT code, ANY_VALUE(libelle_cex) AS rubrique_libelle
  FROM {{ source('DAF_Sources', 'REF_RUBRIQUES') }}
  WHERE code IS NOT NULL
  GROUP BY code
)

SELECT
  COALESCE(bu.branche, 'ENVERGURE')                                          AS BRANCHE,
  f.codification_action                                                      AS CODIFICATION_ACTION,
  COALESCE(de.marche, ds.marche)                                             AS MARCHE,
  COALESCE(fp.prestation, 'DIVERS')                                          AS PRESTATION,
  COALESCE(de.analytique_boussole, ds.analytique_boussole)                   AS ANALYTIQUE_BOUSSOLE,
  COALESCE(s.site_libelle, ds.agence_fallback)                               AS SITE,
  f.bu                                                                       AS CODE_BU,
  bu.bu_libelle                                                              AS BU_LIBELLE,
  'REALISE'                                                                  AS TYPE,
  CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING)                          AS ANNEE,
  CAST(f.mois AS STRING)                                                     AS MOIS,
  f.code_rubrique                                                            AS CODE_RUBRIQUE,
  dr.rubrique_libelle                                                        AS RUBRIQUE_LIBELLE,
  f.valeur                                                                   AS VALEUR
FROM fact f
LEFT JOIN dim_action_env                       de ON f.codification_action = de.codification_action
LEFT JOIN dim_action_eso                       ds ON f.codification_action = ds.codification_action
LEFT JOIN {{ ref('seed_famille_prestation') }} fp ON f.famille = fp.famille
LEFT JOIN dim_site                             s  ON COALESCE(de.analytique_boussole, ds.analytique_boussole) = s.analytique_boussole
LEFT JOIN dim_bu                               bu ON f.bu = bu.code_bu
LEFT JOIN dim_rubrique                         dr ON f.code_rubrique = dr.code
