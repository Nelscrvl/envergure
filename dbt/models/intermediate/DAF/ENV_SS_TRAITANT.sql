{{ config(schema='2_intermediate') }}

{# Sous-traitance Envergure (ENV_SS_TRAITANCE) — long format avec code_rubrique.
   Mapping rubriques : C124 (total A+F+P) + C125 (A) / C126 (F) / C606 (P).
   Règle métier : C124 (total) exclut les codifs famille D (pas de code SST D)
   → garantit C124 = C125 + C126 + C606. #}

{%- set months_padded = [
  '01','02','03','04','05','06','07','08','09','10','11','12'
] -%}

WITH src AS (
  SELECT
    temps_action_id AS codification_action,
    {%- for mm in months_padded %}
    {{ clean_numeric('m_2026_' ~ mm) }} AS m_2026_{{ mm }}{{ ',' if not loop.last }}
    {%- endfor %}
  FROM {{ source('DAF_Sources', 'ENV_SS_TRAITANCE') }}
  WHERE temps_action_id IS NOT NULL
),

ref_actions AS (
  SELECT DISTINCT
    codification_action,
    analytique_bu                                  AS bu,
    COALESCE(NULLIF(UPPER(TRIM(famille_prestation)), ''), 'A')  AS famille
  FROM {{ source('DAF_Sources', 'ENV_REF_ACTIONS') }}
  WHERE codification_action IS NOT NULL
),

ref_marches AS (
  SELECT DISTINCT
    codification_action,
    analytique_bu AS bu
  FROM {{ source('DAF_Sources', 'ENV_REF_MARCHES') }}
  WHERE codification_action IS NOT NULL
),

unpiv AS (
  {%- for mm in months_padded %}
  SELECT codification_action, {{ loop.index }} AS mois, m_2026_{{ mm }} AS valeur FROM src
  {{ 'UNION ALL' if not loop.last }}
  {%- endfor %}
),

joined AS (
  SELECT
    u.codification_action,
    COALESCE(a.bu, m.bu)                  AS bu,
    COALESCE(a.famille, 'A')               AS famille,
    u.mois,
    u.valeur
  FROM unpiv u
  LEFT JOIN ref_actions a USING (codification_action)
  LEFT JOIN ref_marches m USING (codification_action)
),

agg AS (
  SELECT codification_action, bu, mois, famille, SUM(valeur) AS valeur
  FROM joined
  GROUP BY codification_action, bu, mois, famille
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
