SELECT
*,
"ENVERGURE" AS organisme,
CASE WHEN service_name = "LPR" THEN "LPR",
    WHEN service_name LIKE "Activ%" THEN AP3 
END AS marche,
"PAYS-DE-LA-LOIRE" AS region,
"CATALYS" AS organisme,
"Téo" AS instance,
CASE WHEN order__type = "Order" Then 1
ELSE 0
END AS Adhésion,
CASE WHEN order_end_date = "" THEN 1
ELSE 0
END AS abandon,
CASE WHEN order_end_date >= "2021-01-01" THEN 1 
ELSE 0 
END AS fin_de_prestation
FROM {{ref('stg_teo_dossier')}}