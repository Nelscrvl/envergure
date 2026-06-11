# Projet Envergure — pipeline dbt multi-domaines

Pipeline dbt sur BigQuery alimentant les dashboards Looker Studio d'Envergure.
Il couvre **plusieurs domaines métier** construits depuis des sources hétérogènes
(SI-Emploi / MySQL, Sofia / formation, primes CSV, GSheets, et la chaîne DAF/EDM
historique).

> ⚠️ **Ce fichier a été remis à jour le 2026-06-11.** L'ancienne version ne
> décrivait que la chaîne DAF. Le projet est aujourd'hui bien plus large — voir
> la table des domaines et le mapping dataset ci-dessous.

## Stack

- **BigQuery** projet `dynamic-camp-465312-b6`, location **EU**
- **dbt-bigquery 1.11**, profil `dbt_envergure`, `method: oauth` (scope `bigquery`)
- **Auth** : ADC user `bertrand.aubry.ext@actualgroup.com`
- Profil (`dbt/profiles.yml`) : `target: dev`, **`dataset: dbt_transfo`** (= schéma
  par défaut quand aucun `+schema` n'est défini)
- Override `macros/generate_schema_name.sql` : prend `+schema` **tel quel** (pas
  de préfixe d'environnement) ; sinon retombe sur `target.schema` = `dbt_transfo`
- Packages : `dbt-labs/dbt_utils`, `dbt-labs/codegen`
- ⚠️ `dbt` n'est pas dans le PATH du shell. Le binaire n'est pas installé
  globalement ; il existe un `dbt/target/manifest.json` compilé (source de vérité
  pour la résolution schema/alias des modèles).

## Sources dbt (`models/schema.yml`)

⚠️ **Le nom de la source dbt ≠ le nom du dataset BQ.** Mapping `identifier`/`schema` :

| Source dbt | Dataset BQ réel | Contenu |
|---|---|---|
| `sie_emploi` | **`mysql_data`** | SI-Emploi (miroir MySQL) : `dt_benef`, `dt_rdv`, `dt_facture`, `dt_pmsmp` |
| `DAF_Sources` | **`1_natives`** | Chaîne DAF/EDM : `BDD_*_NATIVE`, `book_2026`, `BEN17_*`, `CUBA_MS_PROD`, `ENV_REF_*`, `REF_*`, `HYPOTHESES_2026` |
| `sofia` | **`Extract_Sofia`** | Formation : `Presence/Inscrite/Convention/Stage_Soc_{2,3,4}`, `entites` |
| `prime_raw` | `prime_raw` | Primes par région : `16866_CVL`, `16871_GUAD`, …, `prime_compile_clean` |
| `Importation_gsheet` | `Importation_gsheet` | `stg_satisfaction_de` |

Datasets BQ **hors dbt** (à ne pas confondre) : `0_sources` (anciens imports
GSheet bruts), `REFERENTIELS` (dims externes), `teo_extract`, `dbt_bertrand`
(sandbox perso).

## Couches & mapping folder → dataset (résolu)

Règle physique : **le dossier d'un modèle détermine son dataset cible**, via
`dbt_project.yml` (`+schema`) ou un `config(schema=...)` inline (qui **gagne**
sur le `+schema` du dossier). Mapping réel :

| Dossier `models/` | Dataset BQ | Note |
|---|---|---|
| `staging/**` (Sofia, SI-Emploi, DAF, Prime_Csv, gsheet) | `dbt_transfo` | aucun `+schema` → défaut profil |
| `intermediate/ADV`, `CDG`, `Pilotage/{AP3,ATR,CA_global,General,UES}` | `dbt_transfo` | aucun `+schema` → défaut profil |
| `intermediate/DAF` (ESO_*, ENV_*) | `2_intermediate` | `config(schema=...)` inline |
| `intermediate/DAF` (int_BDD_*) | `dbt_transfo` | pas d'inline → défaut |
| `mart/DAF` (MRT_*) | `2_mart` | `config(schema='2_mart')` inline (écrase le `+schema: DAF`) |
| `mart/DAF` (BDD_BU_SECURE, mrt_BDD_FY, mrt_BDD_LY_CY) | `DAF` | `+schema: DAF` |
| `mart/ADV`, `mart/CDG` | `sie_emploi` | défaut `mart: +schema: sie_emploi` |
| `mart/Pilotage/AP3` | `dbt_transfo_AP3` | |
| `mart/Pilotage/ATR` | `dbt_transfo_ATR` | |
| `mart/Pilotage/CA_Global` | `dbt_transfo_ca_pilotage` | |
| `mart/Pilotage/General/{Accompagnement,Formation}` | `Pilotage` | |
| `mart/Pilotage/UES` | `dbt_transfo_sie_emploi` | |

`dbt_project.yml` : `intermediate.DAF` et `intermediate.Pilotage.General` sont en
`+materialized: table` ; `mart.*` est en `+materialized: table`. Le défaut dbt
(view) s'applique au reste de l'intermediate et au staging.

## Domaines métier

Les **marts sont volontairement minces** (`SELECT * FROM {{ ref('int_…') }}`) ;
la logique vit dans l'**intermediate**.

| Domaine | Source | Intermediate (logique) | Mart → dataset |
|---|---|---|---|
| **DAF / EDM** | `DAF_Sources` (1_natives) | `ESO_*`, `ENV_*` (2_intermediate), `int_BDD_*` | `MRT_*` → 2_mart ; legacy → DAF |
| **ADV** | SI-Emploi | `int_adv`, `int_convention_montants`, `int_facture` | `mrt_adv_facture` → sie_emploi |
| **CDG** | SI-Emploi | `int_activite`, `int_adhesion`, `int_extract_ca`, `int_inscrite`, `int_presence`, `int_rdv_lot_site` | `mrt_extract_ca` → sie_emploi |
| **Pilotage / AP3** | SI-Emploi | `int_pilotage_ap3`, `int_pmsmp` | `mrt_pilotage_ap3`, `mrt_pmsmp` → dbt_transfo_AP3 |
| **Pilotage / ATR** | SI-Emploi | `int_atr_atelier`, `int_atr_factu`, `int_atr_pilotage` | `mrt_atr_atelier`, `mrt_pilotage_atr` → dbt_transfo_ATR |
| **Pilotage / CA_global** | SI-Emploi + primes | `int_ca_global`, `int_benef_ca_prime` | `mrt_ca_pilotage` → dbt_transfo_ca_pilotage |
| **Pilotage / UES** | SI-Emploi + primes | `int_pilotage*`, `int_prime*`, `int_referent_rdv_ca`, `int_formateur_*`, `int_benef_ca_prime`, `int_union_all_prime` | `mrt_pilotage`, `mrt_referent_rdv_ca`, `mrt_prime`, `mrt_facture`, … → dbt_transfo_sie_emploi |
| **Pilotage / General — Accompagnement** | SI-Emploi | `int_commande`, `int_factu_ca`, `int_pilotage_global`, `int_rdv_detail`, `int_rdv_formateur`, `int_rdv_vs2` | `mrt_activite`, `mrt_adhesion`, `mrt_factu_ca_global`, `mrt_pilotage_global`, `mrt_rdv`, `mrt_rdv_vs2` → Pilotage |
| **Pilotage / General — Formation** | Sofia | `int_formateur_heures`, `Int_inscrit_formation`, `int_presence_formation` | `mrt_formateur_heures`, `mrt_inscrit_formation`, `mrt_presence_formation` → Pilotage |

## Conventions de nommage

- Staging : `stg_<source>_<table>` (ex. `stg_benef`, `stg_inscrite_Soc_2`)
- Intermediate : `int_<entité>` (lowercase) — **sauf** la chaîne DAF en
  `ESO_*` / `ENV_*` (UPPERCASE)
- Mart : `mrt_<entité>` (lowercase) — **sauf** la chaîne DAF en `MRT_*` (UPPERCASE)
- Seeds : `seed_*.csv` dans `dbt/seeds/`

⚠️ **Dette de nommage connue** (3 conventions de datasets coexistent) :
numérotée (`0_sources`, `1_natives`, `2_intermediate`, `2_mart`), préfixée
(`dbt_transfo*`), et nue (`DAF`, `Pilotage`, `sie_emploi`). `dbt_transfo` sert de
**fourre-tout** (staging + intermediate de tous les domaines). Une refonte de
nommage est envisagée — voir « Risques / dette ».

---

## Chaîne DAF — règles métier détaillées

> La chaîne DAF/EDM est la plus mature et documentée. Format **long**,
> 1 ligne par `(codification_action × bu × mois × code_rubrique)` :
> `codification_action | bu | mois (INT 1-12) | famille | code_rubrique | valeur`.
> `MRT_FACTS_EXTERNES` ajoute les colonnes finales (`BRANCHE`, `MARCHE`,
> `PRESTATION`, `ANALYTIQUE_BOUSSOLE`, `SITE`, `CODE_BU`, `BU_LIBELLE`, `TYPE`,
> `ANNEE`, `MOIS` STRING, `CODE_RUBRIQUE`, `RUBRIQUE_LIBELLE`, `VALEUR`).

### Famille de prestation
- Source `famille_prestation` (book) ou `Activit__` (BEN17) → normalisé `'A'/'F'/'P'/'D'`
- **NULL ou vide → `'A'` (Accompagnement)** via `COALESCE(NULLIF(UPPER(TRIM(...)), ''), 'A')`
- Mapping famille → libellé PRESTATION dans `seeds/seed_famille_prestation.csv` :
  `A → ACCOMPAGNEMENT`, `F → FORMATION`, `P → PRIVEE`, `D → DIVERS`

### Branche (MRT_FACTS_EXTERNES)
- Lookup `REF_TERRITOIRES.code_bu_su` (= code_bu) → `branche` :
  `BEN17`, `BEN18` → `'ENVERGURE SO'` (avec espace) ; autres → `'ENVERGURE'`

### Total = somme des breakdowns par famille
**Règle absolue** : le code TOTAL doit égaler la somme des codes famille-spécifiques.
Un total exclut donc les familles sans code dédié dans REF_RUBRIQUES, via :
```sql
WHERE NOT (code_rubrique = 'CXXX' AND famille IN ('D', ...))
```

### MS Prod brute — chargement RH × 1.45 (archi v6)
Les sources fournissent la MS Prod **non chargée** (`BEN17_R2026.ms_prod_non_chargee`,
`CUBA_MS_PROD.ms_2026_NN`). Les intermediates `ESO_MS_PROD` / `ENV_MS_PROD`
appliquent `SUM(...) * 1.45` (CTE `agg`). Tous les modèles aval consomment la
valeur **chargée**. Le taux vit côté reporting v6 dans
`1_natives.HYPOTHESES_2026` (code `H02B_TAUX_CHARGES_SOCIALES`, GLOBAL, 145%),
appliqué côté Sheet — aucune modif dbt requise si la DAF change le taux.

### Pas de lignes nulles ou zéro
Chaque intermediate / MRT filtre en sortie `WHERE valeur IS NOT NULL AND valeur != 0`
(pattern CTE `agg` puis `SELECT * FROM agg WHERE …` — éviter `HAVING SUM(...)`
qui déclenche le bug « aggregations of aggregations » BQ).

### Mapping rubriques par métrique (chaîne DAF)
Codes hardcodés via macro `expand_rubriques` ; libellé via `REF_RUBRIQUES.libelle_cex`.

| Métrique | Total | Accomp (A) | Form (F) | Privé (P) | Divers (D) | Filtre TOTAL |
|---|---|---|---|---|---|---|
| CA | C110 | C115 | C116 | C601 | C602 | exclut D |
| MS_PROD | C119 | C120 | C121 | C611 | — | exclut D |
| FDG | C123 | C603 | C604 | C605 | — | exclut D |
| SS_TRAITANT | C124 | C125 | C126 | C606 | — | exclut D |
| CA_MANDAT | C625 | C628 | C629 | C630 | — | exclut D |
| MB (calculé) | C127 | C129 | C215 | C607 | — | (MRT_MB) |
| CSR (calculé) | C118 | — | — | — | — | (MRT_CSR) |

`MRT_MB.valeur = CA - MS_PROD - FDG - SS_TRAITANT` (CA_MANDAT exclu).
`MRT_CSR.valeur = MS_PROD + FDG + SS_TRAITANT`.

### Note staging DAF
`stg_DAF_BDD_BU_NATIVE` corrige **intentionnellement** une inversion de colonnes
de la source GSheet (`CODE_RUBRIQUE` ↔ `RUBRIQUE_LIBELLE`). Ne pas « corriger »
ces alias sans réparer la source d'abord.

## Macros utilitaires

- **`clean_numeric(col)`** : nettoie une STRING (NBSP/NNBSP/U+202F, virgule
  décimale FR, symboles) et `SAFE_CAST` en NUMERIC.
- **`expand_rubriques(agg_ref, mapping)`** : SELECT final long format depuis une
  CTE agrégée + dict `{scope → code_rubrique}`. `'TOTAL'` matche tout ; les autres
  scopes (`'A'`, `'F'`, …) ne matchent que la `famille` correspondante.
- **`generate_schema_name`** : voir Stack.

## Tests dbt

Des `_schema.yml` existent par domaine sous `intermediate/**` et `mart/**`. La
chaîne DAF (`+MRT_FACTS_EXTERNES`) porte le plus de tests :
`unique_combination_of_columns`, `not_null`/`accepted_values` sur famille /
code_rubrique / BRANCHE / PRESTATION / TYPE, et `relationships`
MRT_FACTS_EXTERNES.CODE_RUBRIQUE → REF_RUBRIQUES.code.

## Commandes courantes

```bash
# Chaîne DAF consolidée
dbt run  --select +MRT_FACTS_EXTERNES
dbt test --select +MRT_FACTS_EXTERNES
dbt build --select +MRT_FACTS_EXTERNES

# Un domaine entier (par dossier)
dbt build --select path:models/mart/Pilotage/UES

dbt seed   # recharger les seeds après édition CSV
```

## Risques / dette

- **⚠️ Piège dbt ↔ Looker (déjà rencontré le 2026-06-11)** : changer le
  `+schema` d'un modèle fait écrire dbt dans le nouveau dataset **sans supprimer**
  l'ancienne table, que Looker Studio continue de lire **périmée, sans erreur**.
  → Après tout changement de dataset cible : dropper l'ancienne table **ou** y
  poser une vue de redirection. Des **vues de redirection** ont été créées dans
  `sie_emploi.*` et `dbt_transfo_sie_emploi.*` pour réparer ce cas (elles
  pointent vers `DAF.*`, `Pilotage.*`, `sie_emploi.mrt_adv_facture`).
- **`dbt_transfo` = fourre-tout** : tout le staging + l'intermediate de tous les
  domaines y atterrissent (défaut profil). Une convention de nommage unique reste
  à définir.
- **Année 2026 hardcodée** dans des colonnes sources (`ms_2026_*`, `m_2026_*`,
  `ca_<mois>`). À adapter pour 2027.
- **GSheets externes** (`0_sources`) : nécessitent scope Drive ; contournés via
  snapshots natifs en `1_natives`.
- Broutilles : table typo `int_referrent_rdv_ca` (orpheline) ; casse
  `CA_global` (intermediate) vs `CA_Global` (mart) ; `+schema: DAF` rendu mort
  par l'inline `2_mart`.

## Comment étendre

**Nouvelle métrique chaîne DAF** : créer `intermediate/DAF/ENV_NEW.sql`
(+ `ESO_NEW.sql`) avec `config(schema='2_intermediate')` + macros `clean_numeric`
/ `expand_rubriques` ; puis `mart/DAF/MRT_NEW.sql` (`config(schema='2_mart')`,
UNION ENV+ESO → `agg` → filtre nul/zéro) ; ajouter le UNION ALL dans
`MRT_FACTS_EXTERNES.fact` ; documenter dans les `_schema.yml`.

**Nouveau modèle dans un domaine existant** : le placer dans le bon dossier — le
dataset cible découle du dossier (voir mapping). Mart = `SELECT * FROM ref(int_…)`.

**Nouvelle source** : déclarer dans `models/schema.yml` (attention `name` source ≠
dataset BQ via `schema:`/`identifier:`), puis un `stg_*` sous `staging/raw/<Source>/`.
