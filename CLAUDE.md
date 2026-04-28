# Projet Envergure — pipeline dbt DAF

Pipeline dbt qui construit un EDM consolidé (CA, FdG, MS Prod, Sous-traitance,
CA Mandat, Marge Brute, Coût des Services Rendus) au format long, à partir
des sources Envergure (book_2026 GSheet, CUBA, ENV_REF_*) et BEN17 (Sud-Ouest,
BEN17_R2026, BEN17_REF_MARCHE).

## Stack

- **BigQuery** projet `dynamic-camp-465312-b6`, location EU
- **dbt-bigquery 1.11**, profil `dbt_envergure` en `method: oauth` (scope `bigquery` uniquement, plus aucune dépendance Drive)
- **Auth** : ADC user `bertrand.aubry.ext@actualgroup.com`
- Packages : `dbt-labs/dbt_utils`, `dbt-labs/codegen`

## Architecture des datasets

| Dataset | Rôle | Contenu |
|---|---|---|
| `0_sources` | Imports bruts GSheet | `BDD_BU_GSHEET*`, `BDD_EDM_GSHEET*`, `Looker_access_control` |
| `1_natives` | Tables natives BQ (snapshots des GSheet, refs) | `book_2026`, `BEN17_R2026`, `BEN17_REF_MARCHE`, `CUBA_MS_PROD`, `ENV_SS_TRAITANCE`, `ENV_REF_ACTIONS`, `ENV_REF_MARCHES`, `REF_RUBRIQUES`, `REF_TERRITOIRES`, `REF_AGENCES`, `BDD_BU_NATIVE`, `BDD_EDM_NATIVE` |
| `2_intermediate` | Briques par source (ESO_/ENV_) | `ESO_CA`, `ESO_FDG`, `ESO_MS_PROD`, `ESO_SS_TRAITANT`, `ENV_CA`, `ENV_FDG`, `ENV_CA_MANDAT`, `ENV_MS_PROD`, `ENV_SS_TRAITANT` |
| `2_mart` | Mart user-facing | `MRT_CA`, `MRT_FDG`, `MRT_MS_PROD`, `MRT_SS_TRAITANT`, `MRT_CA_MANDAT`, `MRT_MB`, `MRT_CSR`, `MRT_BDD_EDM` + vues sécurisées existantes |

Le projet utilise un override `generate_schema_name` (`macros/generate_schema_name.sql`) qui prend `+schema` tel quel — donc `+schema: 2_mart` → dataset `2_mart` (pas de préfixe environnement).

## Conventions de nommage

- **`ESO_*`** (UPPERCASE) : intermediate, source BEN17 (Sud-Ouest)
- **`ENV_*`** (UPPERCASE) : intermediate, source Envergure (book_2026, CUBA, ENV_SS_TRAITANCE)
- **`MRT_*`** (UPPERCASE) : mart consolidé user-facing
- `mrt_*` (lowercase) : modèles legacy existants (`mrt_BDD_FY`, `mrt_BDD_LY_CY`) — pas touchés
- Anciens seeds CSV `seed_*.csv` dans `dbt/seeds/`

## Schéma commun des modèles métriques (intermediate + MRT)

Format **long**, 1 ligne par `(codification_action × bu × mois × code_rubrique)` :

| codification_action | bu | mois (INT 1-12) | famille | code_rubrique | valeur |

`famille` est propagée pour permettre la jointure dans MRT_BDD_EDM (PRESTATION).
`MRT_BDD_EDM` ajoute les colonnes finales : `BRANCHE`, `MARCHE`, `PRESTATION`, `ANALYTIQUE_BOUSSOLE`, `SITE`, `CODE_BU`, `BU_LIBELLE`, `TYPE`, `ANNEE`, `MOIS` (STRING), `CODE_RUBRIQUE`, `RUBRIQUE_LIBELLE`, `VALEUR`.

## Règles métier critiques

### Famille de prestation
- Source `famille_prestation` (book) ou `Activit__` (BEN17) → normalisé en `'A'/'F'/'P'/'D'`
- **NULL ou chaîne vide → `'A'` (Accompagnement) par défaut**, via `COALESCE(NULLIF(UPPER(TRIM(...)), ''), 'A')`
- Le mapping famille → libellé PRESTATION est dans `seeds/seed_famille_prestation.csv` :
  - `A → ACCOMPAGNEMENT`, `F → FORMATION`, `P → PRIVEE`, `D → DIVERS`

### Branche (MRT_BDD_EDM)
- Lookup direct dans `REF_TERRITOIRES.code_bu_su` (= code_bu) → `branche` :
  - `BEN17`, `BEN18` → `'ENVERGURE SO'` (avec espace, pas underscore)
  - Autres BU → `'ENVERGURE'`

### Total = somme des breakdowns par famille
**Règle absolue** : pour chaque métrique, le code TOTAL doit égaler la somme des codes famille-spécifiques.

Conséquence : un total exclut les familles qui n'ont pas de code spécifique dans REF_RUBRIQUES. Implémenté dans chaque intermediate via :
```sql
WHERE NOT (code_rubrique = 'CXXX' AND famille IN ('D', ...))
```

## Mapping rubriques par métrique

Codes hardcodés dans chaque intermediate via le macro `expand_rubriques`. Libellé lookup via `REF_RUBRIQUES.libelle_cex`.

| Métrique | Total | Accomp (A) | Form (F) | Privé (P) | Divers (D) | Filtre TOTAL |
|---|---|---|---|---|---|---|
| **CA** | C110 | C115 | C116 | C601 | C602 | exclut D |
| **MS_PROD** | C119 | C120 | C121 | C611 | — | exclut D |
| **FDG** | C123 | C603 | C604 | C605 | — | exclut D |
| **SS_TRAITANT** | C124 | C125 | C126 | C606 | — | exclut D |
| **CA_MANDAT** | C625 | C628 | C629 | C630 | — | exclut D |
| **MB** (calculé) | C127 | C129 | C215 | C607 | — | (généré par MRT_MB) |
| **CSR** (calculé) | C118 | — | — | — | — | (généré par MRT_CSR) |

`MRT_MB.valeur = CA - MS_PROD - FDG - SS_TRAITANT` (CA_MANDAT exclu du calcul de MB).
`MRT_CSR.valeur = MS_PROD + FDG + SS_TRAITANT`.

## Macros utilitaires

- **`clean_numeric(col)`** (`macros/clean_numeric.sql`) : nettoie une colonne STRING (NBSP/NNBSP/U+202F, virgule décimale française, symboles parasites) et SAFE_CAST en NUMERIC. Réutilisé partout pour les casts.
- **`expand_rubriques(agg_ref, mapping)`** (`macros/expand_rubriques.sql`) : produit le SELECT final long format à partir d'une CTE agrégée et d'un dict {scope → code_rubrique}. `'TOTAL'` matche toutes les lignes ; les autres scopes (`'A'`, `'F'`, ...) ne matchent que si `famille` correspond.

## Tests dbt (`models/mart/DAF/_schema.yml`)

57 tests automatisés sur la chaîne `+MRT_BDD_EDM` :
- `unique_combination_of_columns(codif_action, bu, mois, code_rubrique)` sur tous les MRT
- `not_null` + `accepted_values` sur famille / code_rubrique / BRANCHE / PRESTATION / TYPE
- `relationships` MRT_BDD_EDM.CODE_RUBRIQUE → REF_RUBRIQUES.code

## Commandes courantes

```bash
# Build complet de la chaîne consolidée
dbt run --select +MRT_BDD_EDM

# Tests
dbt test --select +MRT_BDD_EDM

# Build + test en chaîne
dbt build --select +MRT_BDD_EDM

# Recharger les seeds (après édition CSV)
dbt seed
```

## Comment étendre

**Ajouter une nouvelle métrique** (ex: nouveau type de coût) :
1. Créer `2_intermediate/ENV_NEW.sql` (et `ESO_NEW.sql` si concerné) — pattern identique aux existants, en utilisant `clean_numeric` + `expand_rubriques`
2. Créer `MRT_NEW.sql` (UNION ENV + ESO + GROUP BY)
3. Ajouter UNION ALL dans `MRT_BDD_EDM.fact`
4. Documenter dans `_schema.yml` avec `accepted_values` sur `code_rubrique`

**Ajouter une nouvelle rubrique à une métrique existante** (cas typique : nouveau code rubrique dans REF_RUBRIQUES) :
1. Ajouter la ligne dans le `mapping` de `expand_rubriques` du modèle source
2. Mettre à jour `accepted_values` du test
3. Si la rubrique participe à MB → mettre à jour `msp_famille`/`fdg_famille`/etc. dans `MRT_MB`

**Ajouter une exception au filtre TOTAL** : éditer le `WHERE NOT (code_rubrique = 'CXXX' AND famille = 'D')` du modèle concerné.

## Sources connues à risque

- **GSheet externes** dans `0_sources` (book_2026 historique) : nécessitent scope Drive ; aujourd'hui contournés via snapshots natifs en `1_natives` (CSV upload manuel ou scheduled query)
- **Année 2026 hardcodée** dans les colonnes sources (`ms_2026_*`, `m_2026_*`, `ca_<mois>` qui sous-entend 2026). Pour 2027, il faudra adapter — cf. plan d'évolution.
