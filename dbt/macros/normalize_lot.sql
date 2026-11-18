{#
  Ramène les lots SI-Emploi au format historique "MARCHE REGION N - CODE".

  Depuis la mise à jour Sirom du 12/10/2026, certaines instances (AKSIS surtout)
  transmettent "UES_IDF_23_16878" au lieu de "UES IDF 23 - 16878". Les modèles
  aval lisent le marché, la région et le numéro de lot à partir des espaces :
  on normalise donc une seule fois en staging. Le lot brut reste dans lot_source.

  Les lots PPAIP d'Envergure ("PPAIP_LOT 7_89") gardent leur forme : la règle
  département de int_pilotage_global s'appuie sur leurs underscores.
#}
{% macro normalize_lot(col) -%}
REGEXP_REPLACE(
  CASE
    WHEN {{ col }} IS NULL OR REGEXP_CONTAINS({{ col }}, r'(?i)PPAIP') THEN {{ col }}
    WHEN REGEXP_CONTAINS(TRIM({{ col }}), r'^[^ _]+_.+_\d{1,3}_\d{5}$')
      THEN REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM({{ col }}), r'^([^ _]+)_(.+)_(\d{1,3})_(\d{5})$', r'\1 \2 \3 - \4'),
        r'_', ' ')
    ELSE REGEXP_REPLACE({{ col }}, r'_', ' ')
  END,
  -- même marché que les lots "Agil'Cadres ..." des autres instances
  r"(?i)^AGIL'CADRES\b", "Agil'Cadres"
)
{%- endmacro %}
