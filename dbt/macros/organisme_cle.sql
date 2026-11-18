{#
  Clé de jointure vers seed_organisme : Sirom écrit le même organisme avec des
  casses et des espaces différents ("COB Formation" / "COB FORMATION",
  "ANVEOL 2.0 GRAND OUEST " avec espace final).
#}
{% macro organisme_cle(col) -%}
UPPER(REGEXP_REPLACE(TRIM({{ col }}), r'\s+', ' '))
{%- endmacro %}
