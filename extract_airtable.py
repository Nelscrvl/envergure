"""
Extraction Airtable -> BigQuery (dataset 0_airtable_raw)

Charge en FULL REFRESH (WRITE_TRUNCATE) les tables de la base Airtable
"Dimensions" (hub de référentiels) dans le dataset brut `0_airtable_raw`.

Réutilise la classe BigQueryLoader de extract_sofia.py (même style que les
pipelines Sofia / SI-Emploi). 1 table Airtable = 1 table BQ.

Auth : nécessite un Personal Access Token (PAT) Airtable en lecture sur la base
(scopes `data.records:read` et, pour l'auto-découverte des tables,
`schema.bases:read`). À placer dans `.env` :
    AIRTABLE_TOKEN=patXXXXXXXXXXXXXX

Lancement autonome :
    python extract_airtable.py
ou via l'orchestration globale (main.py).
"""

import os
import re
import json
import time
import logging
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional

import requests
from google.cloud import bigquery

from extract_sofia import BigQueryLoader

logger = logging.getLogger(__name__)

AIRTABLE_API = "https://api.airtable.com/v0"

# Marqueurs d'erreur de formule (Airtable/Excel) -> neutralisés en NULL :
# ils polluent l'autodetect de type BigQuery (ex. '#VALUE!' dans une colonne numérique).
ERROR_SENTINELS = {
    "#VALUE!", "#ERROR!", "#REF!", "#DIV/0!", "#N/A", "#NAME?", "#NUM!", "#NULL!",
    "NaN", "Infinity", "-Infinity",
}


def _clean_value(value):
    """Remplace récursivement les marqueurs d'erreur de formule par None."""
    if isinstance(value, str):
        return None if value.strip() in ERROR_SENTINELS else value
    if isinstance(value, list):
        return [_clean_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _clean_value(v) for k, v in value.items()}
    return value


def _sanitize_name(name: str) -> str:
    """Nom de table/colonne compatible BigQuery : ASCII, [A-Za-z0-9_], pas de
    chiffre en tête. Ex: 'ref_départements' -> 'ref_departements',
    'N° prestation' -> 'N_prestation', '1' -> '_1'."""
    # Décompose les accents puis retire les diacritiques
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    ascii_name = re.sub(r"[^0-9A-Za-z_]+", "_", ascii_name).strip("_")
    if not ascii_name:
        ascii_name = "col"
    if ascii_name[0].isdigit():
        ascii_name = "_" + ascii_name
    return ascii_name


class AirtableExtractor:
    """Extrait les tables d'une base Airtable et les charge dans BigQuery."""

    def __init__(self, airtable_config: Dict, bq_config: Dict):
        self.token = airtable_config["token"]
        self.base_id = airtable_config["base_id"]
        # liste optionnelle de noms de tables à charger ; None = toutes
        self.only_tables: Optional[List[str]] = airtable_config.get("tables")
        self.exclude_tables = set(airtable_config.get("exclude", []))

        self.project_id = bq_config["project_id"]
        self.dataset_id = bq_config["dataset_id"]
        self.loader = BigQueryLoader(self.project_id)

        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    # ----------------------------------------------------------------- API
    def list_tables(self) -> List[Dict]:
        """Auto-découverte des tables via la meta API (scope schema.bases:read)."""
        url = f"{AIRTABLE_API}/meta/bases/{self.base_id}/tables"
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return [{"id": t["id"], "name": t["name"]} for t in resp.json()["tables"]]

    def fetch_records(self, table: str, max_retries: int = 3) -> List[Dict]:
        """Récupère tous les records d'une table (pagination 100/page)."""
        url = f"{AIRTABLE_API}/{self.base_id}/{requests.utils.quote(table)}"
        records: List[Dict] = []
        offset = None

        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset

            for attempt in range(max_retries):
                resp = self.session.get(url, params=params, timeout=60)
                if resp.status_code == 429:  # rate limit
                    time.sleep(2 ** attempt)
                    continue
                resp.raise_for_status()
                break
            else:
                raise RuntimeError(f"Rate limit persistant sur {table}")

            payload = resp.json()
            for r in payload.get("records", []):
                # Aplatit : métadonnées Airtable + champs assainis
                row = {
                    "_airtable_id": r["id"],
                    "_airtable_created_time": r.get("createdTime"),
                }
                for field_name, value in r.get("fields", {}).items():
                    row[_sanitize_name(field_name)] = _clean_value(value)
                records.append(row)

            offset = payload.get("offset")
            if not offset:
                break
            time.sleep(0.25)  # respect ~5 req/s

        return records

    def _load_all_strings(self, records: List[Dict], bq_table: str) -> bool:
        """Repli pour table sale : force un schéma 100% STRING (autodetect off)
        pour que BigQuery ne tente aucune conversion. scalaires -> str,
        listes/dicts -> JSON. Convient à une zone brute."""
        keys = []
        seen = set()
        str_records = []
        for r in records:
            nr = {}
            for k, v in r.items():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
                if v is None:
                    nr[k] = None
                elif isinstance(v, (list, dict)):
                    nr[k] = json.dumps(v, ensure_ascii=False)
                else:
                    nr[k] = str(v)
            nr["_extracted_at"] = datetime.now().isoformat()
            str_records.append(nr)
        keys.append("_extracted_at")

        schema = [bigquery.SchemaField(k, "STRING") for k in keys]
        table_ref = f"{self.project_id}.{self.dataset_id}.{bq_table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            autodetect=False,
        )
        try:
            self.loader.client.load_table_from_json(
                str_records, table_ref, job_config=job_config
            ).result()
            logger.info(f"✓ {len(str_records)} enregistrements chargés (all-STRING) dans {table_ref}")
            return True
        except Exception as e:
            logger.error(f"✗ Échec chargement all-STRING {table_ref} : {e}")
            return False

    # ----------------------------------------------------------------- run
    def run(self) -> Dict:
        start = time.time()
        stats = {"success": 0, "failed": 0, "total_records": 0, "duration": 0}

        try:
            tables = self.list_tables()
        except Exception as e:
            logger.error(f"❌ Auto-découverte des tables impossible : {e}")
            if not self.only_tables:
                stats["failed"] = 1
                return stats
            tables = [{"id": t, "name": t} for t in self.only_tables]

        for t in tables:
            name = t["name"]
            if self.only_tables and name not in self.only_tables:
                continue
            if name in self.exclude_tables:
                logger.info(f"⏭️  Table ignorée : {name}")
                continue

            bq_table = _sanitize_name(name)
            try:
                logger.info(f"📥 Airtable '{name}' -> {self.dataset_id}.{bq_table}")
                records = self.fetch_records(t["id"])
                if not records:
                    logger.warning(f"   (vide) {name}")
                    stats["success"] += 1
                    continue

                ok = self.loader.load_data(
                    records,
                    dataset_id=self.dataset_id,
                    table_id=bq_table,
                    write_disposition="WRITE_TRUNCATE",  # full refresh
                    stringify=False,                       # préserve types/arrays
                    autodetect=True,
                )
                if not ok:
                    # Repli table sale : conflit de type non récupérable par
                    # l'autodetect -> schéma 100% STRING explicite.
                    logger.warning(f"   ↻ retry {name} en schéma all-STRING")
                    ok = self._load_all_strings(records, bq_table)
                if ok:
                    stats["success"] += 1
                    stats["total_records"] += len(records)
                else:
                    stats["failed"] += 1
            except Exception as e:
                logger.error(f"❌ Échec table {name} : {e}")
                stats["failed"] += 1

        stats["duration"] = time.time() - start
        return stats


def _load_env(path=".env"):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip("\"'"))
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    _load_env()

    token = os.getenv("AIRTABLE_TOKEN")
    if not token:
        raise SystemExit("❌ AIRTABLE_TOKEN absent (ajoute-le dans .env)")

    extractor = AirtableExtractor(
        airtable_config={
            "token": token,
            "base_id": os.getenv("AIRTABLE_BASE_ID", "app0j1i7IH24AefxS"),  # base "Dimensions"
            # tables=None -> auto-découverte via la meta API (scope schema.bases:read).
            # Mettre une liste explicite pour restreindre, ou si le PAT n'a pas ce scope.
            "tables": None,
            # "exclude": ["fait_cuba_raw"],
        },
        bq_config={
            "project_id": "dynamic-camp-465312-b6",
            "dataset_id": "0_airtable_raw",
        },
    )

    result = extractor.run()
    logger.info(
        f"✅ Airtable terminé : {result['success']} OK / {result['failed']} échecs / "
        f"{result['total_records']:,} records en {result['duration']:.1f}s"
    )
    if result["failed"]:
        raise SystemExit(1)
