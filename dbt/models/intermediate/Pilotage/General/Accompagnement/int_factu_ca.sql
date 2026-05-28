WITH benef_coll AS (
  SELECT
    id_presta_coll,
    ANY_VALUE(marche)          AS marche,
    ANY_VALUE(lot_unique)      AS lot_unique,
    ANY_VALUE(organisme)       AS organisme,
    ANY_VALUE(referent)        AS referent,
    ANY_VALUE(numero_commande) AS numero_commande,
    ANY_VALUE(lot)             AS lot,
    ANY_VALUE(region)          AS region,
    ANY_VALUE(site)            AS site
  FROM {{ ref('int_pilotage_global') }}
  WHERE id_presta_coll IS NOT NULL
  GROUP BY id_presta_coll
),

prep_data AS (

SELECT
  f.*,
  COALESCE(pg.marche, bc.marche)                  AS marche,
  COALESCE(pg.lot_unique, bc.lot_unique)           AS lot_unique,
  COALESCE(pg.organisme, bc.organisme)             AS organisme,
  COALESCE(pg.referent, bc.referent)               AS referent,
  COALESCE(pg.numero_commande, bc.numero_commande) AS numero_commande,
  COALESCE(pg.lot, bc.lot)                         AS lot,
  COALESCE(pg.region, bc.region)                   AS region,
  COALESCE(pg.site, bc.site)                       AS site,
  DATE_TRUNC(f.date_facture, MONTH) AS annee_mois_factu

FROM {{ref('stg_facture')}} AS f
LEFT JOIN {{ref('int_pilotage_global')}} AS pg
  ON f.id_benef = pg.id_benef
LEFT JOIN benef_coll AS bc
  ON f.id_presta_coll = bc.id_presta_coll

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
),

prime_dedup AS (
  SELECT DISTINCT LC, Montant_prime, Date_primes
  FROM {{ ref('stg_prime_compile') }}
)

SELECT
  ai.*,
  IF(ROW_NUMBER() OVER (PARTITION BY ai.numero_commande ORDER BY ai.date_facture) = 1, p.Montant_prime, NULL) AS Montant_prime,
  IF(ROW_NUMBER() OVER (PARTITION BY ai.numero_commande ORDER BY ai.date_facture) = 1, p.Date_primes,    NULL) AS Date_primes
FROM filtered AS ai
LEFT JOIN prime_dedup p
  ON ai.numero_commande = p.LC

