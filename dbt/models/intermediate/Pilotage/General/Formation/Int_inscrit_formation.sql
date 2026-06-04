WITH union_inscrite AS (
    SELECT * FROM {{ ref("stg_inscrite_Soc_2") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_inscrite_Soc_3") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_inscrite_Soc_4") }}
),

dedup_inscrite AS (
    SELECT
        * EXCEPT(stg_adresse_internal_id, stg_stagiaire_internal_id),
        CASE WHEN fr_formateur_id IS NULL THEN 0 ELSE 1 END AS presence
    FROM union_inscrite
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY IDAction) = 1
),

union_presence AS (
    SELECT * FROM {{ ref("stg_presence_Soc_2") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_presence_Soc_3") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_presence_Soc_4") }}
),

heures_presence AS (
    SELECT
        inscription_id,
        SUM(TIME_DIFF(duree_seance,          TIME(0, 0, 0), MINUTE)) / 60.0 AS heures_realisees,
        SUM(TIME_DIFF(COALESCE(duree_absence, TIME(0, 0, 0)), TIME(0, 0, 0), MINUTE)) / 60.0 AS heures_absence
    FROM union_presence
    GROUP BY inscription_id
),

union_stage AS (
    SELECT * FROM {{ ref("stg_stage_Soc_2") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_stage_Soc_3") }}
),

heures_stage AS (
    SELECT
        stagiaire_id,
        IDAction,
        SUM(Duree) AS heures_stage
    FROM union_stage
    GROUP BY stagiaire_id, IDAction
)

SELECT
    i.*,
    COALESCE(p.heures_realisees, 0) AS heures_realisees,
    COALESCE(p.heures_absence,   0) AS heures_absence,
    COALESCE(s.heures_stage,     0) AS heures_stage
FROM dedup_inscrite i
LEFT JOIN heures_presence p ON p.inscription_id = i.ID
LEFT JOIN heures_stage    s ON s.stagiaire_id = i.stg_stagiaire_id
                            AND s.IDAction    = i.IDAction
