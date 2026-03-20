WITH slct_column AS (
    SELECT
id_benef,
referent,
date_demarrage,
date_fin_prest,
marche,
region, 
lot_unique, 
organisme,
statut,
situation,
site,


FROM {{ref("int_pilotage")}} 
GROUP BY id_benef, referent, date_demarrage, date_fin_prest, marche, region, lot_unique, organisme, statut, situation,site

)

SELECT 
s.*,
f.type_facture,
f.date_facture,
f.montant_HT, 
f.montant_TVA,
f.num_facture,
FROM slct_column as s
LEFT JOIN {{ref("int_facture")}} as f
ON s.id_benef = f.id_benef


