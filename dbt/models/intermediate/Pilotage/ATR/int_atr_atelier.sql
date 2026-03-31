WITH granu AS 
(
    SELECT
referent,
marche,
region, 
lot_unique AS lot,
site,
organisme,
date_deb,
date_fin,
type_atelier,
theme,
situation,
statut,
COUNT(p.id_benef) AS nb_de_present
FROM {{ref('int_atr_pilotage')}} as p
LEFT JOIN {{ref("stg_facture")}} as f
ON p.id_benef = f.id_benef
WHERE recu = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

---CA_ATR AS (

SELECT
*,
EXTRACT(MONTH FROM CAST(date_deb AS TIMESTAMP)) AS mois_num

FROM granu