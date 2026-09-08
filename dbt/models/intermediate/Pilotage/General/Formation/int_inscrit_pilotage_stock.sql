WITH inscrits AS (
    SELECT
        stg_stagiaire_id,
        id_societe,
        code_analytique_parcours,
        libelle_parcours,
        id_parcours_groupe,
        type_region,
        id_convention,
        conv_id_societe,
        conv_numero_financeur,
        conv_client_nom,
        est_conventionne,
        abandon_parcours,
        nb_jours_ouvres,
        heures_realisees,
        heures_totales_prevues,
        ca_genere,
        montant_total_bdc,
        SAFE_CAST(LEFT(date_entree,                10) AS DATE) AS date_entree_d,
        SAFE_CAST(LEFT(date_sortie_previsionnelle, 10) AS DATE) AS date_fin_d
    FROM {{ ref('int_inscrit_pilotage') }}
    WHERE date_entree IS NOT NULL
      AND date_sortie_previsionnelle IS NOT NULL
      AND SAFE_CAST(LEFT(date_entree, 10) AS DATE)
          <= SAFE_CAST(LEFT(date_sortie_previsionnelle, 10) AS DATE)
    -- Dédup : 1 ligne par (stagiaire × société × parcours), élimine fan-out des jointures
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stg_stagiaire_id, id_societe, date_entree
        ORDER BY id_convention NULLS LAST
    ) = 1
)

SELECT
    i.stg_stagiaire_id,
    i.id_societe,
    i.code_analytique_parcours,
    i.libelle_parcours,
    i.id_parcours_groupe,
    i.type_region,
    i.id_convention,
    i.conv_numero_financeur,
    i.conv_client_nom,
    i.est_conventionne,
    i.abandon_parcours,
    i.nb_jours_ouvres,
    i.heures_realisees,
    i.heures_totales_prevues,
    i.ca_genere,
    i.montant_total_bdc,
    mois_actif,
    EXTRACT(YEAR  FROM mois_actif) AS annee,
    EXTRACT(MONTH FROM mois_actif) AS mois,
    -- Colonnes numériques pour SUM() direct dans Looker Studio
    1                              AS nb_en_cours,
    i.est_conventionne             AS nb_conventionnes
FROM inscrits i,
UNNEST(GENERATE_DATE_ARRAY(
    DATE_TRUNC(i.date_entree_d, MONTH),
    DATE_TRUNC(i.date_fin_d,    MONTH),
    INTERVAL 1 MONTH
)) AS mois_actif
-- 1 ligne par (stagiaire × société × mois) : si plusieurs parcours se chevauchent, on garde le conventionné
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY i.stg_stagiaire_id, i.id_societe, mois_actif
    ORDER BY i.est_conventionne DESC, i.id_convention NULLS LAST
) = 1
