SELECT 
f.*,
b.agence,
b.conseiller,
b.referent,
b.numero_commande,
date_demarrage,date_fin_prest,

FROM {{ref("stg_facture")}} f
LEFT JOIN {{ref("stg_benef")}} b
ON f.id_benef = b.id_benef