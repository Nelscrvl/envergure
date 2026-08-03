with source as (
    select * from {{ source('sofia', 'Tiers_Individuel_Soc_2') }}
    where Stagiaire = true
)

select
    CAST(Id_Tiers AS STRING)        as id_tiers,

    -- Examens (jusqu'à 4 certifications distinctes dans Sofia)
    NULLIF(TRIM(Examen1), '') as examen1, Date_Examen1, COALESCE(Examen1_Reussi, false) as examen1_reussi,
    NULLIF(TRIM(Examen2), '') as examen2, Date_Examen2, COALESCE(Examen2_Reussi, false) as examen2_reussi,
    NULLIF(TRIM(Examen3), '') as examen3, Date_Examen3, COALESCE(Examen3_Reussi, false) as examen3_reussi,
    NULLIF(TRIM(Examen4), '') as examen4, Date_Examen4, COALESCE(Examen4_Reussi, false) as examen4_reussi,

    -- Flag synthétique : au moins un examen renseigné et réussi
    (
        (NULLIF(TRIM(Examen1), '') IS NOT NULL AND COALESCE(Examen1_Reussi, false))
        OR (NULLIF(TRIM(Examen2), '') IS NOT NULL AND COALESCE(Examen2_Reussi, false))
        OR (NULLIF(TRIM(Examen3), '') IS NOT NULL AND COALESCE(Examen3_Reussi, false))
        OR (NULLIF(TRIM(Examen4), '') IS NOT NULL AND COALESCE(Examen4_Reussi, false))
    )                               as a_reussi_examen,

    -- Flag : au moins un examen renseigné (passé ou non)
    (
        NULLIF(TRIM(Examen1), '') IS NOT NULL
        OR NULLIF(TRIM(Examen2), '') IS NOT NULL
        OR NULLIF(TRIM(Examen3), '') IS NOT NULL
        OR NULLIF(TRIM(Examen4), '') IS NOT NULL
    )                               as a_passe_examen

from source
