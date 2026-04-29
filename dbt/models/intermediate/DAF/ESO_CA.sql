{{ config(schema='2_intermediate') }}

{# CA BEN17 (ESO) — long format avec code_rubrique.
   Mapping rubriques : C110 (total A+F+P uniquement) + C115 (A) / C116 (F) / C601 (P) / C602 (D).
   Règle métier : C110 (CHIFFRE D AFFAIRES) n'inclut PAS le CA Divers (C602)
   qui est traité comme une catégorie distincte. #}

WITH r2026 AS (
  SELECT
    codif_action            AS codification_action,
    SAFE_CAST(mois AS INT64) AS mois,
    {{ clean_numeric('ca') }} AS ca
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
    SUM(r.ca) AS valeur
  FROM r2026 r
  LEFT JOIN ref_marche m USING (codification_action)
  GROUP BY r.codification_action, m.bu, r.mois, m.famille
),

expanded AS (
  {{ expand_rubriques('agg', {
    'TOTAL': 'C110',
    'A':     'C115',
    'F':     'C116',
    'P':     'C601',
    'D':     'C602'
  }) }}
)

SELECT * FROM expanded
-- C110 (CA total) exclut les codifs Divers — C602 reste leur seule rubrique
WHERE NOT (code_rubrique = 'C110' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
