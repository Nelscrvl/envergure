WITH prep_data AS (

SELECT
  f.*,
  pg.marche,
  pg.lot_unique,
  pg.organisme,
  pg.referent,
  pg.numero_commande,
  pg.lot,
  pg.region,
  CAST(FORMAT_DATE('%Y%m', f.date_facture) AS INT64) AS annee_mois_factu

FROM {{ref('stg_facture')}} AS f
LEFT JOIN {{ref('int_pilotage_global')}} AS pg
  ON f.id_benef = pg.id_benef

),

filtered AS (
  SELECT * FROM prep_data
  WHERE type_facture = 'solde'
    AND (num_origine IS NULL OR num_origine = '')

  UNION ALL

  SELECT * FROM prep_data
  WHERE type_facture = 'avoir'
    AND num_origine IS NOT NULL
    AND num_origine != ''
    AND num_origine != num_facture
)

SELECT
  ai.*,
  p.Montant_prime,
  p.Date_primes
FROM filtered AS ai
LEFT JOIN {{ref('stg_prime_compile')}} p
  ON ai.numero_commande = p.LC
