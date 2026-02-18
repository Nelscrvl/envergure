WITH unionall AS (
    SELECT 
*
FROM {{ref("stg_inscrite_Soc_2")}}

UNION ALL 

SELECT 
*
FROM
{{ref("stg_inscrite_Soc_3")}}

) 

SELECT
*
FROM unionall
WHERE fr_formateur_id IS NOT NULL
