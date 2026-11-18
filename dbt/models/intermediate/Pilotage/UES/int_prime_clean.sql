SELECT
p.*,
b.id_benef,
-- le LC n'existe que dans une instance : on garde laquelle pour les jointures aval
b.instance
FROM {{ref('stg_prime_compile')}} AS p
LEFT JOIN {{ref('stg_benef')}} AS b
ON p.LC = b.numero_commande