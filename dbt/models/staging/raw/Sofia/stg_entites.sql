with source as (
        select * from {{ source('sofia', 'entites') }}
  ),
  renamed as (
      select
        ID,
        _id,
        Reference,
        Libelle,
        Libelle_Court,
        Libelle_Lieu,
        Libelle_Lieu_Site,
        Libelle_Lieu_Court,
        Code_Analytique,
        CSecteur,
        Diplome,
        Tronc_Commun,
        Inscription_En_Ligne,
        IDLieu_Site,
        Debut,
        Fin,
        Date_Debut_Prevue,
        Date_Fin_Prevue,
        _extracted_at,

        -- Pilote
        Pilote.ID           as pilote_id,
        Pilote._id          as pilote_internal_id,
        Pilote.Reference    as pilote_reference,
        Pilote.Civilite     as pilote_civilite,
        Pilote.Nom          as pilote_nom,
        Pilote.Prenom       as pilote_prenom,

        -- Referent
        Referent.ID         as referent_id,
        Referent._id        as referent_internal_id,
        Referent.Reference  as referent_reference,
        Referent.Civilite   as referent_civilite,
        Referent.Nom        as referent_nom,
        Referent.Prenom     as referent_prenom,

        -- Specialite
        Specialite.ID       as specialite_id,
        Specialite._id      as specialite_internal_id,
        Specialite.Reference as specialite_reference,
        Specialite.Libelle  as specialite_libelle,

        -- Objectif_General
        Objectif_General.ID           as objectif_general_id,
        Objectif_General._id          as objectif_general_internal_id,
        Objectif_General.Reference    as objectif_general_reference,
        Objectif_General.Libelle      as objectif_general_libelle,
        Objectif_General.Libelle_Court as objectif_general_libelle_court

      from source
  )
  select * from renamed
    