{{ config(schema='2_mart') }}

{# Marge Brute = CA - MS_PROD - FDG - SOUS-TRAITANCE.
   Calcul agrégé par (codification_action × bu × mois) à partir des MRT_*.
   Émet :
     - 1 ligne TOTAL    : code_rubrique = C127 (MARGE BRUTE)
     - 1 ligne FAMILLE  : code_rubrique = C129 (A) / C215 (F) / C607 (P)
   Pas de code MB pour D → la famille DIVERS ne génère que la ligne TOTAL.
   CA_MANDAT n'entre PAS dans le calcul (métrique réservée pour suivi mandataire). #}

WITH fact_mrt AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_CA') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_MS_PROD') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_FDG') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_SS_TRAITANT') }}
),

per_codif AS (
  SELECT
    codification_action,
    bu,
    mois,
    ANY_VALUE(famille) AS famille,
    -- Totaux par métrique
    SUM(IF(code_rubrique = 'C110',                   valeur, 0)) AS ca_total,
    SUM(IF(code_rubrique = 'C119',                   valeur, 0)) AS msp_total,
    SUM(IF(code_rubrique = 'C123',                   valeur, 0)) AS fdg_total,
    SUM(IF(code_rubrique = 'C124',                   valeur, 0)) AS sst_total,
    -- Famille-spécifiques (chaque codif n'a qu'une famille → un seul est non-nul)
    SUM(IF(code_rubrique IN ('C115','C116','C601','C602'),  valeur, 0)) AS ca_famille,
    SUM(IF(code_rubrique IN ('C120','C121','C611'),         valeur, 0)) AS msp_famille,
    SUM(IF(code_rubrique IN ('C603','C604','C605'),         valeur, 0)) AS fdg_famille,
    SUM(IF(code_rubrique IN ('C125','C126','C606'),         valeur, 0)) AS sst_famille
  FROM fact_mrt
  GROUP BY codification_action, bu, mois
)

-- Ligne TOTAL (C127)
SELECT
  codification_action, bu, mois, famille,
  'C127' AS code_rubrique,
  ca_total - msp_total - fdg_total - sst_total AS valeur
FROM per_codif

UNION ALL

-- Ligne famille-spécifique (C129 / C215 / C607)
SELECT
  codification_action, bu, mois, famille,
  CASE famille
    WHEN 'A' THEN 'C129'
    WHEN 'F' THEN 'C215'
    WHEN 'P' THEN 'C607'
  END AS code_rubrique,
  ca_famille - msp_famille - fdg_famille - sst_famille AS valeur
FROM per_codif
WHERE famille IN ('A', 'F', 'P')
