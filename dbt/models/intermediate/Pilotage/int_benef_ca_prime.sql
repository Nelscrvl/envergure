WITH ca AS (
    SELECT
        id_benef,
        referent,
        date_demarrage,
        date_fin_prest,
        marche,
        lot_unique, 
        organisme,
        statut,
        situation,
        site,
        MAX(date_facture) AS date_facture,
        MAX(region) AS region,
        SUM(montant_HT) AS CA_HT_total,
        SUM(montant_TVA) AS CA_TVA_total
    FROM {{ ref("int_formateur_Ca") }}
    GROUP BY id_benef, referent, date_demarrage, date_fin_prest, marche, lot_unique, organisme, statut, situation, site

),

prime AS (
    SELECT
        id_benef,
        Montant_prime,
        Date_primes
    FROM {{ ref("int_prime_clean") }}
)

SELECT
    ca.*,
    p.Montant_prime,
    p.Date_primes
FROM ca
LEFT JOIN prime AS p ON ca.id_benef = p.id_benef
