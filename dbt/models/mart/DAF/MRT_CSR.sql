{{ config(schema='2_mart') }}

{# Coût des Services Rendus = MS_PROD + FDG + SOUS-TRAITANCE.
   Émet 1 ligne TOTAL par (codification_action × bu × mois) :
     - code_rubrique = C118 (COUTS DES SERVICES RENDUS)
   Pas de breakdown famille (le référentiel n'expose que le total pour CSR). #}

WITH fact_mrt AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_MS_PROD') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_FDG') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('MRT_SS_TRAITANT') }}
)

SELECT
  codification_action,
  bu,
  mois,
  ANY_VALUE(famille) AS famille,
  'C118'             AS code_rubrique,
  SUM(IF(code_rubrique IN ('C119','C123','C124'), valeur, 0)) AS valeur
FROM fact_mrt
GROUP BY codification_action, bu, mois
