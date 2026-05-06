with source as (
        select * from {{ source('sie_emploi', 'facture') }}
  ),
  renamed as (
      select
          
          *

      from source
  )
  select * from renamed
    