{{ config(schema='2_mart') }}

{# Consolidation CA Mandataire : ENV (book_2026 type=FDG).
   Pas de pendant ESO/BEN17 aujourd'hui — la table reflète juste ENV_CA_MANDAT
   pour rester cohérente avec les autres MRT_*. Si une source ESO arrive
   demain, il suffira d'ajouter un UNION ALL et le GROUP BY garantira l'unicité. #}

WITH unioned AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ENV_CA_MANDAT') }}
),

agg AS (
  SELECT
    codification_action,
    bu,
    mois,
    ANY_VALUE(famille) AS famille,
    code_rubrique,
    SUM(valeur)        AS valeur
  FROM unioned
  GROUP BY codification_action, bu, mois, code_rubrique
)

SELECT * FROM agg
WHERE valeur IS NOT NULL AND valeur != 0