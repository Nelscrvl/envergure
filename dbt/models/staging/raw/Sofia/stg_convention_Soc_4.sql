with source as (
        select * from {{ source('sofia', 'Convention_Soc_4') }}
  ),
  renamed as (
      select
        Libelle_1ere_Regle_CA,
        Total_HT_Conv,
        cast(to_json_string(Montants_Convention) as string) as montants_convention,
        Commune,
        Observation,
        Liee,
        Archivee,

        -- Titulaire_Marche
        Titulaire_Marche.Nom        as titulaire_marche_nom,
        Titulaire_Marche.Siret      as titulaire_marche_siret,
        Titulaire_Marche._id        as titulaire_marche_id,

        CodeComptable,
        ConventionIota,
        Libelle_CPNE_Client,
        Validation_Convention,

        -- Prestataire
        Prestataire.Adresse         as prestataire_adresse,
        Prestataire.Telephone       as prestataire_telephone,
        Prestataire.Email           as prestataire_email,
        Prestataire.Nom             as prestataire_nom,
        Prestataire.Siret           as prestataire_siret,
        Prestataire._id             as prestataire_id,

        Type_Convention,
        Groupe_Ou_Individuelle,
        Numero_interne,
        Etat_Convention,
        ContactFinanceur,

        -- Financeur
        Financeur.Nom               as financeur_nom,
        Financeur.Reference         as financeur_reference,
        Financeur.ID                as financeur_id,
        Financeur._id               as financeur_internal_id,

        Commercial,
        Code_CPNE_Client,
        Libelle,

        -- OrigineProduitBPF
        OrigineProduitBPF.Origine_Du_Produit as origine_produit_bpf_libelle,
        OrigineProduitBPF.ID                 as origine_produit_bpf_id,
        OrigineProduitBPF._id                as origine_produit_bpf_internal_id,

        Date_Signee,
        Compte_Present_Compta,
        Date_Fin,
        Total_TVA_Conv,
        Libelle_Frais_1EREQUIP,
        Date_Debut,

        -- ContactClient
        ContactClient.Nom           as contact_client_nom,
        ContactClient.ID            as contact_client_id,
        ContactClient._id           as contact_client_internal_id,

        Libelle2,

        -- TVA
        TVA.TauxTVA                 as tva_taux,
        TVA.IDTVA                   as tva_id,
        TVA._id                     as tva_internal_id,

        Numero_financeur,
        Echeances_Indiv,
        LieuFormation,
        Libelle_Compte_produit,
        Terminee,
        ConventionRegion,
        _extracted_at,

        -- Representant
        Representant.Prenom         as representant_prenom,
        Representant.Nom            as representant_nom,
        Representant.Civilite       as representant_civilite,
        Representant._id            as representant_id,

        NPEC,

        -- Client
        Client.Nom                  as client_nom,
        Client.Reference            as client_reference,
        Client.ID                   as client_id,
        Client._id                  as client_internal_id,

        Num_Bon_Commande,
        Reference,
        Compte_produit,
        Numero_lot,
        Code_Secteur_OF,
        Total_TTC_Conv,
        AuxiliaireComptable,
        Montant_Frais_1EREQUIP,
        IDConvent,
        Regle_Facturation,
        _id
      from source
  )
  select * from renamed
    