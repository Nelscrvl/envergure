"""
Extraction complète de l'API Teo → BigQuery dataset `teo_extract`

Auth : OAuth2 client credentials (https://dev.teoapp.fr/docs/authentication/)
Pagination : Hydra PartialCollectionView, 30 items/page max
"""

import os
import base64
import logging
import time
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urlparse

import requests
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"extraction_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lecture du .env (même pattern que main.py)
# ---------------------------------------------------------------------------
def _load_env(path: str = ".env") -> None:
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


_load_env()


# ---------------------------------------------------------------------------
# Endpoints à extraire (GET collections)
# Clé = nom de la table BQ cible, valeur = chemin relatif à TEO_BASE_URL
# Chemins issus de la doc API : https://teoapp.fr/api/docs (kebab-case, singulier)
# ---------------------------------------------------------------------------
ENDPOINTS: Dict[str, str] = {
    "agenda": "/agenda",
    "user": "/user",
    "chorus_invoice": "/chorus-invoice",
    "chorus_invoice_file": "/chorus-invoice-file",
    "chorus_configuration": "/chorus-configuration",
    "chorus_invoice_submission": "/chorus-invoice-submission",
    "communication_record": "/communication-record",
    "contact": "/contact",
    "document": "/document",
    "email": "/email",
    "email_domain": "/email-domain",
    "form_presence": "/form-presence",
    "teoform_response": "/teoform-response",
    "teoform_template": "/teoform-template",
    "gdpr_task": "/gdpr-task",
    "catalog_plan": "/catalog-plan",
    # plural explicite dans la doc
    "license_entitlement": "/license-entitlements",
    "usage_counter": "/usage-counters",
    "connector": "/connector",
    "external_link": "/external-link",
    "external_link_form_response": "/external-link-form-response",
    "form_template_mcp": "/form-template-mcp",
    # sous-ressources Prest@ppli
    "prestappli_connector": "/prestappli/connectors",
    "prestappli_commande": "/prestappli/commandes",
    "prestappli_session": "/prestappli/sessions",
    "prestappli_sync_run": "/prestappli/sync-runs",
    "process": "/process",
    "procedure_activity": "/procedure-activity",
}

# Renouvellement du token 60 s avant expiration (expires_in = 599 s)
TOKEN_REFRESH_MARGIN = 60


# ---------------------------------------------------------------------------
# Client Teo
# ---------------------------------------------------------------------------
class TeoClient:
    """Gère l'auth OAuth2 et les appels paginés à l'API Teo."""

    AUTH_URL = "https://auth.teoapp.fr/oauth2/token"

    def __init__(self, client_id: str, client_secret: str, teo_id: str, base_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.teo_id = teo_id
        self.base_url = base_url.rstrip("/")
        parsed = urlparse(self.base_url)
        self._server_root = f"{parsed.scheme}://{parsed.netloc}"
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    def _credentials_b64(self) -> str:
        raw = f"{self.client_id}:{self.client_secret}"
        return base64.b64encode(raw.encode()).decode()

    def _fetch_token(self) -> None:
        logger.info("Teo - Récupération d'un nouveau access token…")
        resp = self.session.post(
            self.AUTH_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {self._credentials_b64()}",
            },
            data={
                "grant_type": "client_credentials",
                "scope": f"target-entity:{self.teo_id}",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        expires_in = data.get("expires_in", 599)
        self._token_expires_at = time.time() + expires_in - TOKEN_REFRESH_MARGIN
        logger.info("Teo - Token obtenu, valide ~%d s", expires_in)

    def _ensure_token(self) -> str:
        if not self._access_token or time.time() >= self._token_expires_at:
            self._fetch_token()
        return self._access_token  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Requêtes
    # ------------------------------------------------------------------
    def _get(self, url: str) -> Dict[str, Any]:
        token = self._ensure_token()
        resp = self.session.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/ld+json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def discover_collections(self) -> Dict[str, str]:
        """
        Appelle le point d'entrée Hydra (GET /) pour récupérer la liste des
        collections disponibles sur cette instance.
        Retourne un dict {table_name: path}.
        """
        try:
            data = self._get(self.base_url)
        except Exception as exc:
            logger.warning("Teo - découverte automatique échouée (%s), utilisation de la liste statique", exc)
            return {}

        collections: Dict[str, str] = {}
        for key, value in data.items():
            if key.startswith("@") or not isinstance(value, str):
                continue
            # value est une URL absolue ou relative type "/api/contact"
            path = value if value.startswith("/") else "/" + value.split("/", 3)[-1]
            table = _sanitize_key(key.replace("-", "_").replace("/", "_").lstrip("_"))
            collections[table] = path

        if collections:
            logger.info("Teo - %d collections découvertes via le point d'entrée", len(collections))
        return collections

    def _abs_url(self, path: str) -> str:
        """Construit une URL absolue depuis un path serveur-relatif ou base_url-relatif."""
        if path.startswith("http"):
            return path
        # Les paths découverts commencent par /api/ → racine serveur seule
        # Les paths statiques sont relatifs à base_url (ex: /contact)
        if path.startswith("/api"):
            return self._server_root + path
        return self.base_url + path

    def paginate(self, path: str) -> Generator[Dict[str, Any], None, None]:
        """Itère sur toutes les pages d'une collection Hydra et yield chaque item."""
        url = self._abs_url(path)
        total_yielded = 0

        while url:
            logger.debug("GET %s", url)
            try:
                data = self._get(url)
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                logger.warning("Teo - %s HTTP %s — endpoint ignoré", path, status)
                return

            items: List[Dict] = data.get("hydra:member", [])
            if not items:
                break

            for item in items:
                # Retire les champs JSON-LD (@id, @type, @context) non utiles en BQ
                cleaned = {k: v for k, v in item.items() if not k.startswith("@")}
                yield cleaned

            total_yielded += len(items)
            total_items = data.get("hydra:totalItems", total_yielded)
            logger.info("Teo - %s : %d/%d items récupérés", path, total_yielded, total_items)

            # Navigation vers la page suivante (relative ou absolue)
            view = data.get("hydra:view", {})
            next_url = view.get("hydra:next")
            url = self._abs_url(next_url) if next_url else None


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------
def _ensure_dataset(bq: bigquery.Client, project_id: str, dataset_id: str) -> None:
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        bq.create_dataset(bigquery.Dataset(dataset_ref))
        logger.info("BigQuery - Dataset %s créé", dataset_id)
    except Conflict:
        logger.info("BigQuery - Dataset %s existe déjà", dataset_id)


def _flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    """Aplatit récursivement un dict/list imbriqué pour BigQuery."""
    result: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{prefix}_{k}" if prefix else k
            result.update(_flatten(v, new_key))
    elif isinstance(obj, list):
        # Listes converties en JSON string pour éviter les colonnes REPEATED dynamiques
        result[prefix] = str(obj)
    else:
        result[prefix] = obj
    return result


def _sanitize_key(key: str) -> str:
    """Remplace les caractères interdits en nom de colonne BQ."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in key)


def _prepare_rows(records: List[Dict]) -> List[Dict]:
    rows = []
    for rec in records:
        flat = _flatten(rec)
        rows.append({_sanitize_key(k): v for k, v in flat.items()})
    return rows


def _load_to_bq(
    bq: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    rows: List[Dict],
) -> None:
    if not rows:
        logger.info("BigQuery - %s : aucune donnée, table ignorée", table_name)
        return

    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    logger.info("BigQuery - %s : %d lignes chargées", table_ref, len(rows))


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("=" * 70)
    logger.info("DÉMARRAGE EXTRACTION TEO — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 70)

    # -- Credentials depuis .env (noms identiques à la doc Teo) -----------
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]
    teo_id = os.environ["TEO_ID"]
    teo_base_url = os.environ["TEO_BASE_URL"]  # ex: https://teoapp.fr/api
    gcp_project_id = os.environ.get("GCP_PROJECT_ID", "dynamic-camp-465312-b6")
    bq_dataset_id = "teo_extract"

    teo = TeoClient(client_id, client_secret, teo_id, teo_base_url)
    bq = bigquery.Client(project=gcp_project_id)

    _ensure_dataset(bq, gcp_project_id, bq_dataset_id)

    # Découverte dynamique en priorité ; fallback sur la liste statique
    discovered = teo.discover_collections()
    endpoints = discovered if discovered else ENDPOINTS
    if discovered:
        logger.info("Teo - Utilisation des endpoints découverts dynamiquement")
    else:
        logger.info("Teo - Utilisation de la liste statique (%d endpoints)", len(ENDPOINTS))

    total_tables = 0
    total_rows = 0

    for table_name, path in endpoints.items():
        logger.info("--- Extraction : %s (%s)", table_name, path)
        try:
            records = list(teo.paginate(path))
            rows = _prepare_rows(records)
            _load_to_bq(bq, gcp_project_id, bq_dataset_id, table_name, rows)
            total_tables += 1
            total_rows += len(rows)
        except Exception as exc:
            logger.error("Erreur sur %s : %s", table_name, exc)

    logger.info("=" * 70)
    logger.info(
        "EXTRACTION TERMINÉE — %d tables, %d lignes au total — %s",
        total_tables,
        total_rows,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
