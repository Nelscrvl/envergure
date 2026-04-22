SELECT
f.*,
EXTRACT(MONTH FROM CAST(f.date_facture AS TIMESTAMP)) AS mois_num,
CAST(FORMAT_DATE('%Y%m', f.date_facture) AS INT64) AS annee_mois_sort,
p.Montant_prime,
p.Date_primes
FROM {{ref("int_formateur_Ca")}} as f
LEFT JOIN {{ref("int_pilotage_prime")}} as p
ON f.id_benef = p.id_benef
WHERE montant_HT >0