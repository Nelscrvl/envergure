WITH new_col AS (

SELECT
*,
1 as nb_pmsmp
FROM {{ref("stg_pmsmp")}}

)

SELECT
id_benef,
SUM(nb_heures) AS total_nb_heure,
SUM(nb_pmsmp) AS nb_pmsmp
FROM new_col 
GROUP BY id_benef