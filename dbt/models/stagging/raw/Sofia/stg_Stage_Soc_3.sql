with source as (
        select * from {{ source('sofia', 'Stage_Soc_3') }}
  ),
  renamed as (
      select
       *
      from source
  )
  select * from renamed
    