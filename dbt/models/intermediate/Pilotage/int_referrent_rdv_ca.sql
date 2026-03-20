WITH ca_par_benef AS (
    SELECT
        id_benef,
        MAX(date_facture) AS date_facture,
        MAX(region) AS region,
        SUM(montant_HT) AS CA_HT_total,
        SUM(montant_TVA) AS CA_TVA_total
    FROM {{ ref("int_formateur_Ca") }}
    GROUP BY id_benef
),

filtrage AS (
    SELECT
        f.*,
        ca.date_facture,
        ca.region,
        ca.CA_HT_total / NULLIF(COUNT(*) OVER (PARTITION BY f.id_benef), 0) AS CA_HT_par_rdv,
        ca.CA_TVA_total / NULLIF(COUNT(*) OVER (PARTITION BY f.id_benef), 0) AS CA_TVA_par_rdv,
        TIMESTAMP_DIFF(
            TIMESTAMP(date_fin),
            TIMESTAMP(date_deb),
            MINUTE
        ) AS tps_travail_prevu
    FROM {{ ref("int_rdv_formateur") }} AS f
    LEFT JOIN ca_par_benef AS ca ON f.id_benef = ca.id_benef
    WHERE f.id_benef <> 0
),

tps_travail AS (
    SELECT
        f.*,
        tps_travail_ref / 60 AS tps_travail_reel_heure,
        tps_travail_prevu / 60 AS tps_travail_prevu_heure,
        ROW_NUMBER() OVER (
            PARTITION BY f.referent, f.id_benef
            ORDER BY f.date_deb ASC
        ) AS rang_entree,
        IF(
            COALESCE(CAST(b.date_fin_prest AS STRING), "") = "",
            NULL,
            ROW_NUMBER() OVER (
                PARTITION BY f.referent, f.id_benef
                ORDER BY b.date_fin_prest ASC
            )
        ) AS rang_sortie,
        b.date_fin_prest,
        CAST(REPLACE(LEFT(date_deb, 7), '-', '') AS INT64) AS annee_mois_sort
    FROM filtrage AS f
    LEFT JOIN {{ ref("stg_benef") }} AS b ON f.id_benef = b.id_benef
),

final AS (
    SELECT
        t.*,
        CASE
            WHEN rang_entree = 1 THEN p.Montant_prime
            ELSE 0
        END AS Montant_prime,
        p.Date_primes
    FROM tps_travail AS t
    LEFT JOIN {{ ref("int_prime_clean") }} AS p ON t.id_benef = p.id_benef
)

SELECT * FROM final