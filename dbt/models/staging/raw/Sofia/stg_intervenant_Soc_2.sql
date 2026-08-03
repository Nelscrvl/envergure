with source as (
    select * from {{ source('sofia', 'Intervenant_Soc_2') }}
)

select
    ID                                                                          as intervenant_id,
    Reference                                                                   as intervenant_reference,
    Civilite,
    Nom,
    Prenom,
    Statut,
    Categorie,
    Est_Formateur,
    Email_Pro,
    Email_Perso,
    Telephone_Pro,
    Mobile_Pro,
    (
        SELECT chp.Valeur
        FROM UNNEST(Champs_Parametrables) chp
        WHERE chp.Reference = 'PARASALAL1'
        LIMIT 1
    )                                                                           as para_sal_1_raw,
    COALESCE((
        SELECT chp.Valeur
        FROM UNNEST(Champs_Parametrables) chp
        WHERE chp.Reference = 'PARASALAL1'
        LIMIT 1
    ), '0') = '1'                                                               as est_formateur_externe

from source
