{# Nettoie une colonne STRING pour la convertir en NUMERIC.
   Strip tout caractère non-numérique (sauf virgule, point, signe moins),
   convertit la virgule décimale française en point, puis SAFE_CAST.
   Renvoie NULL silencieusement si la valeur est invalide. #}
{% macro clean_numeric(col) -%}
SAFE_CAST(REPLACE(REGEXP_REPLACE({{ col }}, r'[^0-9,.\-]', ''), ',', '.') AS NUMERIC)
{%- endmacro %}
