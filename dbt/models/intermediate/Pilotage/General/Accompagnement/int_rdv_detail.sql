
WITH tps_travail AS (
    SELECT
        f.*,
        TIMESTAMP_DIFF(
            TIMESTAMP(date_fin),
            TIMESTAMP(date_deb),
            MINUTE
        ) AS tps_travail_prevu,
        tps_travail_ref / 60 AS tps_travail_reel_heure,
        TIMESTAMP_DIFF(
            TIMESTAMP(date_fin),
            TIMESTAMP(date_deb),
            MINUTE
        ) / 60 AS tps_travail_prevu_heure,
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
        CAST(REPLACE(LEFT(f.date_deb, 7), '-', '') AS INT64) AS annee_mois_sort
    FROM {{ ref("int_rdv_formateur") }} AS f
    LEFT JOIN {{ ref("stg_benef") }} AS b ON f.id_benef = b.id_benef
    WHERE f.id_benef <> 0
)

SELECT * FROM tps_travail