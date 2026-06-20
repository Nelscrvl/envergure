with conv_soc2 as (
    select
        CAST(IDConvent AS STRING)    as id_convention,
        '2'                         as id_societe,
        Libelle                     as libelle,
        Libelle2                    as libelle2,
        Reference                   as reference,
        Numero_interne              as numero_interne,
        Numero_financeur            as numero_financeur,
        Num_Bon_Commande            as num_bon_commande,
        Type_Convention             as type_convention,
        Groupe_Ou_Individuelle      as groupe_ou_individuelle,
        Etat_Convention             as etat_convention,
        Terminee                    as terminee,
        Date_Debut                  as date_debut,
        Date_Fin                    as date_fin,
        financeur_nom               as financeur_nom,
        financeur_id                as financeur_id,
        client_nom                  as client_nom,
        client_id                   as client_id,
        LieuFormation               as lieu_formation,
        Total_HT_Conv               as total_ht_conv,
        Total_TTC_Conv              as total_ttc_conv
    from {{ ref('stg_convention_Soc_2') }}
),

conv_soc4 as (
    select
        CAST(IDConvent AS STRING)    as id_convention,
        '4'                         as id_societe,
        Libelle                     as libelle,
        Libelle2                    as libelle2,
        Reference                   as reference,
        Numero_interne              as numero_interne,
        Numero_financeur            as numero_financeur,
        Num_Bon_Commande            as num_bon_commande,
        Type_Convention             as type_convention,
        Groupe_Ou_Individuelle      as groupe_ou_individuelle,
        Etat_Convention             as etat_convention,
        Terminee                    as terminee,
        Date_Debut                  as date_debut,
        Date_Fin                    as date_fin,
        financeur_nom               as financeur_nom,
        financeur_id                as financeur_id,
        client_nom                  as client_nom,
        client_id                   as client_id,
        LieuFormation               as lieu_formation,
        Total_HT_Conv               as total_ht_conv,
        Total_TTC_Conv              as total_ttc_conv
    from {{ ref('stg_convention_Soc_4') }}
),

conventions as (
    select * from conv_soc2
    union all
    select * from conv_soc4
),

inscrits_par_convention as (
    select
        CAST(conv_id         AS STRING)                                 as id_convention,
        CAST(conv_id_societe AS STRING)                                 as id_societe,
        COUNT(DISTINCT stg_stagiaire_id)                                as nb_inscrits,
        SUM(heures_realisees)                                           as total_heures_realisees,
        SUM(heures_absence)                                             as total_heures_absence,
        SUM(heures_stage)                                               as total_heures_stage
    from {{ ref('Int_inscrit_formation') }}
    where conv_id is not null
    group by conv_id, conv_id_societe
)

select
    -- Identifiants convention
    c.id_convention,
    c.id_societe,
    c.libelle,
    c.libelle2,
    c.reference,
    c.numero_interne,
    c.numero_financeur,
    c.num_bon_commande,
    c.type_convention,
    c.groupe_ou_individuelle,
    c.etat_convention,
    c.terminee,
    c.date_debut,
    c.date_fin,
    DATE_DIFF(
        SAFE_CAST(c.date_fin   as DATE),
        SAFE_CAST(c.date_debut as DATE),
        DAY
    )                                                                   as duree_convention_jours,
    c.financeur_nom,
    c.financeur_id,
    c.client_nom,
    c.client_id,
    c.lieu_formation,
    c.total_ht_conv,
    c.total_ttc_conv,

    -- Objectifs BDC (depuis int_tarif_convention)
    t.montant_total_bdc,
    t.montant_centre_bdc,
    t.montant_entrep_bdc,
    t.duree_centre_bdc,
    t.duree_entrep_bdc,
    t.duree_prevue_heures_bdc,
    t.duree_stagiaire_centre_bdc,
    t.duree_stagiaire_entrep_bdc,
    t.nb_stagiaire_prevu,
    t.prix_stagiaire_centre,
    t.prix_stagiaire_entrep,

    -- Réalisé (depuis Int_inscrit_formation)
    i.nb_inscrits,
    i.total_heures_realisees,
    i.total_heures_absence,
    i.total_heures_stage,

    -- KPIs calculés
    SAFE_DIVIDE(i.total_heures_realisees, i.nb_inscrits)               as heures_realisees_par_inscrit,
    SAFE_DIVIDE(i.total_heures_stage,     i.nb_inscrits)               as heures_stage_par_inscrit,
    SAFE_DIVIDE(
        i.total_heures_realisees + i.total_heures_stage,
        t.duree_prevue_heures_bdc
    ) * 100                                                             as taux_consommation_bdc,
    SAFE_DIVIDE(i.total_heures_realisees, t.duree_centre_bdc) * 100    as taux_realisation_centre,
    SAFE_DIVIDE(i.total_heures_stage,     t.duree_entrep_bdc) * 100    as taux_realisation_entreprise

from conventions c
left join {{ ref('int_tarif_convention') }}     t
    on  t.id_convention = c.id_convention
    and t.id_societe    = c.id_societe
left join inscrits_par_convention               i
    on  i.id_convention = c.id_convention
    and i.id_societe    = c.id_societe
