{# ATTENTION — Les colonnes CODE_RUBRIQUE et RUBRIQUE_LIBELLE sont inversées dans la source
   GSheet (BDD_BU) : la colonne nommée "CODE_RUBRIQUE" contient en réalité les libellés,
   et "RUBRIQUE_LIBELLE" contient les codes (ex: "C110"). Ce staging corrige intentionnellement
   cet échange. Ne pas "corriger" ces alias sans avoir d'abord réparé la source GSheet. #}

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
        -- Inversion volontaire : dans le GSheet source, les noms de colonnes sont échangés.
        -- "RUBRIQUE_LIBELLE" contient les codes (C110, C127…), "CODE_RUBRIQUE" les libellés.
        RUBRIQUE_LIBELLE AS CODE_RUBRIQUE,
        CODE_RUBRIQUE    AS RUBRIQUE_LIBELLE,
        VALEUR

      from source
  )
  select * from renamed
