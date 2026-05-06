with source as (
        select * from {{ source('prime_raw', '16872_GUAD') }}
  ),
  renamed as (
      select
          
       `Région de la commande`,
        LC,
        `Numero de commande`,
        `Type de prestation`, 
        `Numéro du lot`,
        `Intitulé du lot`, 
        `Numéro de marché`, 
        `Date de debut`, 
        `Date de fin`, 
        `SIRET Prestataire`, 
        `Raison sociale prestataire`, 
        `SIRET Titulaire`, 
        `Raison sociale titulaire`, 
        `Identifiant local`, 
        `Resultat déclaré`, 
        `Statut de la prestation`, 
        `Libelle du statut de la prestation`, 
        `Date de dernier statut`, 
        DPAE, 
        DSN, 
        `Situation DE`, 
        `Niveau d'alerte`, 
        `Statut du contrôle`, 
        `Resultat du contrôle`, 
        `Montant facturé`, 
        `Réfaction`, 
        `Certificat émis`, 
        `Date détection prime`, 
        `Date détection complément`

      from source
  )
  select * from renamed
    