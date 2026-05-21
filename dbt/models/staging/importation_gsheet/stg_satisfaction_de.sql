SELECT
`Horodateur`,
`Cl___Mois_____Saisie_AUTO_Via_GF_` AS annee_mois,
`Cl___DPT_____Saisie_AUTO_Via_GF_` AS dpt,
`Cl___Action_` AS marche,
`NOM_ACTION___complet__De_colonne_AT____BM_____lorsque_fond_blanc_____copier_coller_ou_tirer___pour_mettre_en_forme_` AS nom_marche_complet,
`Pren__nom_` AS prenom,
`No__m_` AS nom,
`Date_Fin___JJ_MM_AA___Saisie_AUTO_Via_GF_` AS date_fin,
`_5___Votre_note_de_satisfaction_globale_pour_cette_action__` AS note_globale,
`Votre_Mail__si_pas_de_mail___crire__0____` AS mail
FROM {{ source('Importation_gsheet', 'stg_satisfaction_de') }}
