with source as (
        select * from {{ source('DAF_Sources', 'BDD_EDM') }}
  ),
  renamed as (
    SELECT
        *

    from source
 
  )
  select * from renamed
