{{ config(schema='2_intermediate') }}

{# Sous-traitance BEN17 (ESO) — long format avec code_rubrique.
   Mapping rubriques : C124 (total A+F+P) + C125 (A) / C126 (F) / C606 (P).
   Règle métier : C124 (total) exclut les codifs famille D (pas de code SST D)
   → garantit C124 = C125 + C126 + C606. #}

WITH r2026 AS (
  SELECT
    codif_action                       AS codification_action,
    SAFE_CAST(mois AS INT64)           AS mois,
    {{ clean_numeric('ss_traitant') }} AS ss_traitant
  FROM {{ source('DAF_Sources', 'BEN17_R2026') }}
  WHERE codif_action IS NOT NULL
),

ref_marche AS (
  SELECT
    Codif_action_site                              AS codification_action,
    Code_BU                                        AS bu,
    COALESCE(NULLIF(UPPER(TRIM(Activit__)), ''), 'A')           AS famille
  FROM {{ source('DAF_Sources', 'BEN17_REF_MARCHE') }}
  WHERE Codif_action_site IS NOT NULL
),

agg AS (
  SELECT
    r.codification_action,
    m.bu,
    r.mois,
    m.famille,
    SUM(r.ss_traitant) AS valeur
  FROM r2026 r
  LEFT JOIN ref_marche m USING (codification_action)
  GROUP BY r.codification_action, m.bu, r.mois, m.famille
),

expanded AS (
  {{ expand_rubriques('agg', {
    'TOTAL': 'C124',
    'A':     'C125',
    'F':     'C126',
    'P':     'C606'
  }) }}
)

SELECT * FROM expanded
WHERE NOT (code_rubrique = 'C124' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
