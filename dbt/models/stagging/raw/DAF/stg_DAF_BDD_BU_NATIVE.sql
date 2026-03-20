with source as (
        select * from {{ source('DAF', 'BDD_BU_NATIVE') }}
  ),
  renamed as (
      select
        BRANCHE,	
        CODE_BU,
        BU_LIBELLE,	
        ANNEE,
        MOIS,	
        TYPE,	
        RUBRIQUE_LIBELLE AS CODE_RUBRIQUE,
        CODE_RUBRIQUE AS RUBRIQUE_LIBELLE,	
        VALEUR  

      from source
  )
  select * from renamed
    