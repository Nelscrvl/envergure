WITH base AS (
    SELECT
        'Téo'                                                               AS instance,
        id                                                                  AS id_benef,
        iid                                                                 AS numero_commande,
        CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))     AS nom,
        email,
        CAST(NULL AS STRING)                                                AS ide,
        location_name                                                       AS agence,
        CONCAT(
            COALESCE(consultant_first_name, ''), ' ',
            COALESCE(consultant_last_name, '')
        )                                                                   AS conseiller,
        CONCAT(
            COALESCE(consultant_first_name, ''), ' ',
            COALESCE(consultant_last_name, '')
        )                                                                   AS referent,
        CAST(consultant_id AS STRING)                                       AS id_ref_principal,
        order_start_date                                                    AS date_demarrage,
        CAST(NULL AS DATE)                                                  AS date_fin_theo,
        order_end_date                                                      AS date_fin_prest,
        CAST(NULL AS STRING)                                                AS id_presta_coll,
        service_name                                                        AS lot,
        CASE
            WHEN service_name = 'LPR'             THEN 'LPR'
            WHEN service_name LIKE 'Activ%'       THEN 'AP3'
            ELSE NULL
        END                                                                 AS marche,
        'PAYS-DE-LA-LOIRE'                                                  AS region,
        CAST(NULL AS STRING)                                                AS lot_unique,
        service_name                                                        AS lot_sans_action,
        location_name                                                       AS site,
        CAST(location_id AS STRING)                                         AS id_site,
        'ENVERGURE'                                                         AS organisme,
        status                                                              AS statut,
        CAST(NULL AS STRING)                                                AS situation,

        -- Métriques — à affiner selon les valeurs réelles de status dans Teo
        CASE WHEN order_type = 'Order' THEN 1 ELSE 0 END                   AS activite,
        CASE
            WHEN TRIM(COALESCE(CONCAT(
                COALESCE(consultant_first_name, ''), ' ',
                COALESCE(consultant_last_name, '')
            ), '')) IN ('', '-') THEN 0
            ELSE 1
        END                                                                 AS adhesion,
        CASE WHEN order_type != 'Order' THEN 1 ELSE 0 END                  AS non_adhere,
        1                                                                   AS oriente,
        0                                                                   AS prime,
        CAST(NULL AS INT64)                                                 AS sortie_anticipee_rep_emploi,
        CAST(NULL AS INT64)                                                 AS Prestation_aboutie_rep_emploi,
        CAST(NULL AS INT64)                                                 AS Prestation_aboutie,
        CAST(NULL AS INT64)                                                 AS retour_emploi,
        CASE WHEN order_end_date IS NULL THEN 1 ELSE 0 END                 AS portefeuille,
        0                                                                   AS sortie_anticipee,
        0                                                                   AS sorties_anticipees_hors_sorties_positives,
        CASE WHEN order_type = 'Order' THEN 1 ELSE 0 END                   AS recu,
        CAST(FORMAT_DATE('%Y%m', order_start_date) AS INT64)               AS annee_mois_sort,

        -- Colonnes sans équivalent Teo
        0                                                                   AS retour_emploi_env_semap,
        CAST(NULL AS NUMERIC)                                               AS Montant_prime,
        CAST(NULL AS DATE)                                                  AS Date_primes,
        CAST(NULL AS INT64)                                                 AS nb_de_note,
        CAST(NULL AS INT64)                                                 AS note_globale,
        0                                                                   AS dont_sessions_annulee,
        CAST(NULL AS FLOAT64)                                               AS total_nb_heure,
        CAST(NULL AS INT64)                                                 AS nb_pmsmp

    FROM {{ ref('stg_teo_dossier') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY numero_commande ORDER BY date_demarrage ASC
        )                                                                   AS rnk_lc
    FROM base
)

SELECT
    *,
    CASE
        WHEN marche IN ('ATR', 'ESPR', 'VS2', 'SPR') AND rnk_lc = 1 THEN 1
        ELSE 0
    END                                                                     AS dont_nb_sessions
FROM ranked
ORDER BY date_demarrage DESC
