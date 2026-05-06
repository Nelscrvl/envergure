with source as (
        select * from {{ source('sie_emploi', 'pmsmp') }}
  ),
  renamed as (
      select
          
          instance,
          id_benef,
          date_debut,
          date_fin,
          nb_heures,
          siret,
          metier,
          code_ROME,
          objet,
          suite,
          resultat,
          `_extracted_at` AS extracted_at

      from source
  )
  select * from renamed
    