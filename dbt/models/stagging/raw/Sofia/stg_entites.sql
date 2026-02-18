with source as (
        select * from {{ source('sofia', 'entites') }}
  ),
  renamed as (
      select
       Date_Fin_Prevue, 
       Diplome, 
       Date_Debut_Prevue,
       Tronc_Commun,
       Pilote,
       Referent, 
       Libelle_Lieu_Site, 
       Specialite, 
       Reference, 
       Inscription_En_Ligne, 
       Libelle_Court, 
       Code_Analytique, 
       IDLieu_Site, 
       Fin, 
       Debut, 
       Libelle_Lieu, 
       Libelle, 
       CSecteur, 
       Objectif_General, 
       ID, 
       Libelle_Lieu_Court
      from source
  )
  select * from renamed
    