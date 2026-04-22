with source as (
        select * from {{ source('DAF_Sources', 'BDD_BU') }}
  ),
  renamed as (
      select
        BRANCHE,	
        "" AS CODIFICATION_ACTION,
        "" AS MARCHE,
        "" AS PRESTATION,
        "" AS ANALYTIQUE_BOUSSOLE,
        "" AS SITE,
        CODE_BU,
        BU_LIBELLE,	
        TYPE,
        ANNEE,
        MOIS,	
        RUBRIQUE_LIBELLE AS CODE_RUBRIQUE,
        CODE_RUBRIQUE AS RUBRIQUE_LIBELLE,	
        VALEUR  

      from source
  )
  select * from renamed
