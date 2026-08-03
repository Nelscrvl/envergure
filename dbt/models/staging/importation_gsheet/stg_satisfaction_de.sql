SELECT
    Horodateur,
    CASE
        WHEN REGEXP_CONTAINS(TRIM(note_satisfaction), r'^\d+$')
        THEN CAST(TRIM(note_satisfaction) AS INT64)
    END                         AS note_globale,
    prenom,
    nom,
    NULLIF(TRIM(mail), '0')     AS mail,
    date_debut,
    date_fin,
    departement,
    objectifs_atteints,
    remarques_objectifs,
    points_positifs,
    points_amelioration,
    souhait_recontact,
    intervenant,
    organisateur
FROM {{ source('Importation_gsheet', 'stg_satisfaction_de') }}
WHERE Horodateur IS NOT NULL
