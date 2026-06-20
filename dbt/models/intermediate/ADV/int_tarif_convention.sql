with source as (
    select
        id_convention,
        id_societe,
        SAFE_CAST(tt_montant_total              AS NUMERIC) as tt_montant_total,
        SAFE_CAST(tt_montant_centre             AS NUMERIC) as tt_montant_centre,
        SAFE_CAST(tt_montant_entrep             AS NUMERIC) as tt_montant_entrep,
        SAFE_CAST(tt_duree_centre               AS NUMERIC) as tt_duree_centre,
        SAFE_CAST(tt_duree_entrep               AS NUMERIC) as tt_duree_entrep,
        SAFE_CAST(tt_duree_prevue_heures        AS NUMERIC) as tt_duree_prevue_heures,
        SAFE_CAST(tt_duree_stagiaire_centre     AS NUMERIC) as tt_duree_stagiaire_centre,
        SAFE_CAST(tt_duree_stagiaire_entreprise AS NUMERIC) as tt_duree_stagiaire_entreprise,
        SAFE_CAST(tt_nombre_stagiaire_prevu     AS NUMERIC) as tt_nombre_stagiaire_prevu,
        SAFE_CAST(tt_prix_heure                 AS NUMERIC) as tt_prix_heure,
        SAFE_CAST(tt_prix_stagiaire_centre      AS NUMERIC) as tt_prix_stagiaire_centre,
        SAFE_CAST(tt_prix_stagiaire_entreprise  AS NUMERIC) as tt_prix_stagiaire_entreprise,
        SAFE_CAST(tt_montant_journee            AS NUMERIC) as tt_montant_journee,
        SAFE_CAST(tt_quantite_journee           AS NUMERIC) as tt_quantite_journee
    from {{ ref('stg_tarif_convention') }}
    where tt_montant_total  is not null
       or tt_duree_centre   is not null
       or tt_prix_heure     is not null
       or tt_montant_journee is not null
),

with_montant as (
    select
        *,
        COALESCE(
            tt_montant_total,
            tt_prix_heure * tt_duree_prevue_heures,
            tt_montant_journee
        ) as montant_calcule
    from source
)

select
    id_convention,
    id_societe,
    SUM(montant_calcule)                    as montant_total_bdc,
    SUM(tt_montant_centre)                  as montant_centre_bdc,
    SUM(tt_montant_entrep)                  as montant_entrep_bdc,
    SUM(tt_duree_centre)                    as duree_centre_bdc,
    SUM(tt_duree_entrep)                    as duree_entrep_bdc,
    SUM(tt_duree_prevue_heures)             as duree_prevue_heures_bdc,
    SUM(tt_duree_stagiaire_centre)          as duree_stagiaire_centre_bdc,
    SUM(tt_duree_stagiaire_entreprise)      as duree_stagiaire_entrep_bdc,
    SUM(tt_nombre_stagiaire_prevu)          as nb_stagiaire_prevu,
    MAX(tt_prix_stagiaire_centre)           as prix_stagiaire_centre,
    MAX(tt_prix_stagiaire_entreprise)       as prix_stagiaire_entrep
from with_montant
group by id_convention, id_societe
