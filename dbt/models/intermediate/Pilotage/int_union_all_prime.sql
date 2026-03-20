SELECT
*
FROM {{ref("stg_prime_raw_16866_cvl")}}

UNION ALL 

SELECT 

*

FROM {{ref("stg_prime_raw_16871_guad")}}

UNION ALL 

SELECT

*

FROM {{ref("stg_prime_raw_16872_guad")}}

UNION ALL 

SELECT 

* 

FROM {{ref("stg_prime_raw_16873_guad")}}

UNION ALL 

SELECT

*

FROM {{ref("stg_prime_raw_16898_gd_est")}}

UNION ALL

SELECT 

*

FROM {{ref("stg_prime_raw_16900_idf")}}