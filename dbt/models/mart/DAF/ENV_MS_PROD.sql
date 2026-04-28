{{ config(schema='2_intermediate') }}

{# MS prod Envergure (CUBA_MS_PROD) — long format avec code_rubrique.
   Mapping rubriques : C119 (total A+F+P) + C120 (A) / C121 (F) / C611 (P).
   Pas de code MS Prod pour D → C119 exclut famille D pour assurer
   C119 = C120 + C121 + C611. #}

{%- set months_padded = [
  '01','02','03','04','05','06','07','08','09','10','11','12'
] -%}

WITH src AS (
  SELECT
    codis_action AS codification_action,
    {%- for mm in months_padded %}
    {{ clean_numeric('ms_2026_' ~ mm) }} AS ms_2026_{{ mm }}{{ ',' if not loop.last }}
    {%- endfor %}
  FROM {{ source('DAF_Sources', 'CUBA_MS_PROD') }}
  WHERE codis_action IS NOT NULL
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
  SELECT codification_action, {{ loop.index }} AS mois, ms_2026_{{ mm }} AS valeur FROM src
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
    'TOTAL': 'C119',
    'A':     'C120',
    'F':     'C121',
    'P':     'C611'
  }) }}
)

SELECT * FROM expanded
WHERE NOT (code_rubrique = 'C119' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
