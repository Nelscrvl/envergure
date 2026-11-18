with source as (

    select * from {{ source('sie_emploi', 'benef') }}
    -- Sirom transmet certains dossiers en double (lignes strictement identiques)
    qualify row_number() over (
        partition by instance, id_benef
        order by date_dern_modif desc
    ) = 1

),

organismes as (

    select * from {{ ref('seed_organisme') }}

),

marches as (

    select upper(trim(cle)) as cle, marche from {{ ref('seed_marche') }}

),

lots as (

    select *, {{ normalize_lot('lot') }} as lot_normalise from source

),

renamed as (

    select
        instance,
        id_benef,
        civilite,
        nom,
        prenom,
        ide,
        tel1,
        tel2,
        email,
        commentaire,
        agence,
        conseiller,
        telconseiller,
        mailconseiller,
        referent,
        id_ref_principal,
        numero_commande,
        date_demarrage,
        date_debut_csp,
        date_fin_csp,
        date_fin_theo,
        date_fin_prest,
        source.lot_normalise as lot,
        source.lot as lot_source,
        {{ marche_from_lot('source.lot_normalise', 'm_lot', 'm_mot') }} as marche,
        site,
        id_site,
        coalesce(o.organisme, regexp_replace(trim(source.organisme), r'\s+', ' ')) as organisme,
        source.organisme as organisme_source,
        adresse_benef,
        cp_benef,
        ville_benef,
        insee_adresse,
        statut,
        situation,
        suivi,
        date_naissance,
        nom_naissance,
        info_adh,
        date_dern_modif,
        util_dern_modif,
        id_util_dern_modif,
        date_crea,
        util_crea,
        id_util_crea,
        nir,
        groupe,
        util_exclu_factu,
        date_exclu_factu,
        jobready,
        web_100,
        cdp,
        cdp_util,
        cdp_placement,
        acc_deld,
        emargement_impossible,
        prime,
        modalite_accompagnement,
        id_presta_coll,
        _extracted_at

    from lots as source
    left join organismes as o
        on o.cle_organisme = {{ organisme_cle('source.organisme') }}
    left join marches as m_lot
        on m_lot.cle = upper(trim(source.lot_normalise))
    left join marches as m_mot
        on m_mot.cle = upper({{ premier_mot_lot('source.lot_normalise') }})

)

select * from renamed
