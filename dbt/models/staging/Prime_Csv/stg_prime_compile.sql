with source as (
        select * from {{ source('prime_raw', 'prime_compile_clean') }}
  ),
  renamed as (
      select
        Lot,
        Site,
        LC,	
        CIP AS referent,
        Date_sortie,	
        Date_d_entr__e AS date_entree,	
        Date_primes,	
        Montant_prime,	
        Motif,
        Statut,	
        Date_RGLT,	
        RGLT,	
        MOIS_DEMARRAGE,	
        ANNEE_DEMARRAGE,
        ANNE_PRIME,	
        MOIS_PRIME

      from source
  )
  select * from renamed
    