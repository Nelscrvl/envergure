-- Staging Airtable : fait_cuba_raw
-- Source : 0_airtable_raw.fait_cuba_raw (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'fait_cuba_raw') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `temps_tempsActif` as temps_tempsactif,
        `temps_isProductif` as temps_isproductif,
        `temps_actionNonProductive` as temps_actionnonproductive,
        `temps_dateSoumissionCoordinateur` as temps_datesoumissioncoordinateur,
        `temps_horodateur` as temps_horodateur,
        `temps_rtId` as temps_rtid,
        `temps_id` as temps_id,
        `temps_heuresValidees` as temps_heuresvalidees,
        `temps_createdBy` as temps_createdby,
        `temps_tempsAutres` as temps_tempsautres,
        `temps_territoireId` as temps_territoireid,
        `temps_statut` as temps_statut,
        `temps_intervenantMatricule` as temps_intervenantmatricule,
        `temps_coordinateurId` as temps_coordinateurid,
        `temps_heuresSoumises` as temps_heuressoumises,
        `temps_intervenantId` as temps_intervenantid,
        `temps_totalHeuresNonActives` as temps_totalheuresnonactives,
        `temps_rowIsModified` as temps_rowismodified,
        `temps_marcheId` as temps_marcheid,
        `temps_campagneId` as temps_campagneid,
        `temps_totalHeuresActives` as temps_totalheuresactives,
        `temps_tempsPreparation` as temps_tempspreparation,
        `temps_isDeleted` as temps_isdeleted,
        `temps_dateReponseRT` as temps_datereponsert,
        `temps_uniteProduction` as temps_uniteproduction,
        `temps_siteMarche` as temps_sitemarche,
        `temps_actionCodif` as temps_actioncodif,
        `temps_siteId` as temps_siteid,
        `temps_date` as temps_date,
        `temps_lastUpdate` as temps_lastupdate,
        `temps_commentaire` as temps_commentaire,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
