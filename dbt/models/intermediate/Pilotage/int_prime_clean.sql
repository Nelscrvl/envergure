SELECT
p.*,
b.id_benef
FROM {{ref('stg_prime_compile')}} AS p
LEFT JOIN {{ref('stg_benef')}} AS b
ON p.LC = b.numero_commande