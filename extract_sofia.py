import requests
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Dict, List, Optional
import time
import calendar

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'extraction_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_monthly_periods(start_date: str, end_date: str) -> List[Dict]:
    """
    Génère une liste de périodes mensuelles
    
    Args:
        start_date: Date de début (format 'YYYY-MM-DD')
        end_date: Date de fin (format 'YYYY-MM-DD')
    
    Returns:
        Liste de dictionnaires avec 'Debut' et 'Fin' pour chaque mois
    """
    periods = []
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    while current <= end:
        # Début du mois
        month_start = current.replace(day=1)
        
        # Dernier jour du mois
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = current.replace(day=last_day)
        
        # Ne pas dépasser la date de fin
        if month_end > end:
            month_end = end
        
        periods.append({
            'Debut': month_start.strftime('%Y-%m-%d'),
            'Fin': month_end.strftime('%Y-%m-%d')
        })
        
        # Passer au mois suivant
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1, day=1)
        else:
            current = current.replace(month=current.month + 1, day=1)
    
    return periods


def format_date_for_societe(date_str: str, id_societe: int) -> str:
    """
    IDSociete 1 et 2 : format normal AAAA-MM-JJ
    IDSociete 3 : format inversé AAAA-JJ-MM
    """
    if id_societe == 3:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return f"{dt.year}-{dt.day:02d}-{dt.month:02d}"
    return date_str


def build_sofia_endpoints(start_date: str, end_date: str) -> List[Dict]:
    """Construit la liste complète des endpoints Sofia à extraire."""
    monthly_periods = generate_monthly_periods(start_date, end_date)
    endpoints = []

    for period in monthly_periods:
        endpoints.append({
            "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
            "params": {
                "IDSociete": 3,
                "debut": format_date_for_societe(period['Debut'], 3),
                "fin": format_date_for_societe(period['Fin'], 3)
            },
            "table": "Presence_Soc_3",
            "stringify": True
        })

    for period in monthly_periods:
        endpoints.append({
            "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
            "params": {"IDSociete": 2, "Debut": period['Debut'], "Fin": period['Fin']},
            "table": "Presence_Soc_2",
            "stringify": True
        })

    for period in monthly_periods:
        endpoints.append({
            "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
            "params": {"IDSociete": 4, "Debut": period['Debut'], "Fin": period['Fin']},
            "table": "Presence_Soc_4",
            "stringify": True
        })

    endpoints.append({
        "endpoint": "/SofiaEDC/API/Facture/Get",
        "params": {
        "Entite": "Factures",
        "ValideOnly": "False",
        "IDSoc": 4,
        "DateDebut": "2020-01-01",
        "DateFin": "2026-12-31"},
        "table": "Facture_soc_4"
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Facture/Get",
        "params": {
        "Entite": "Factures",
        "ValideOnly": "False",
        "IDSoc": 2,
        "DateDebut": "2020-01-01",
        "DateFin": "2026-12-31"},
        "table": "Facture_soc_2"
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Entite/GetEntite",
        "params": {"IDSociete": 2, "ACTION": "ACTION"},
        "table": "entites"
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Entite/GetEntite",
        "params": {
            "IDSociete": 3,
            "ACTION": "INSCRIT",
            "DebutInscription": format_date_for_societe("2026-01-01", 3),
            "LimiteDebutInscription": format_date_for_societe(end_date, 3),
        },
        "table": "Inscrite_Soc_3",
        "stringify": True
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Entite/GetEntite",
        "params": {
            "IDSociete": 4,
            "ACTION": "INSCRIT",
            "DebutInscription": "2026-01-01",
            "LimiteDebutInscription": end_date,
        },
        "table": "Inscrite_Soc_4",
        "stringify": True
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Entite/GetEntite",
        "params": {
            "IDSociete": 2,
            "ACTION": "INSCRIT",
            "DebutInscription": "2026-01-01",
            "LimiteDebutInscription": end_date,
        },
        "table": "Inscrite_Soc_2",
        "stringify": True
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Financement/Get?",
        "params": {"Entite": "Convention","IDSoc": 2},
        "table": "Convention_Soc_2"
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/Financement/Get?",
        "params": {"Entite": "Convention", "IDSoc": 4},
        "table": "Convention_Soc_4"
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/StageEntreprise/GetStageEntrep",
        "params": {"IDSociete": 2, "Debut": "2026-01-01", "Fin": "2026-12-31"},
        "table": "Stage_Soc_2",
        "stringify": True
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/StageEntreprise/GetStageEntrep",
        "params": {"IDSociete": 3, "Debut": "2026-01-01", "Fin": "2026-12-31"},
        "table": "Stage_Soc_3",
        "stringify": True
    })
    endpoints.append({
        "endpoint": "/SofiaEDC/API/StageEntreprise/GetStageEntrep",
        "params": {"IDSociete": 4, "Debut": "2026-01-01", "Fin": "2026-12-31"},
        "table": "Stage_Soc_4",
        "stringify": True
    })

    return endpoints


class APIExtractor:
    """Classe pour gérer l'extraction des données depuis l'API"""
    
    def __init__(self, api_config: Dict, bq_config: Dict):
        self.base_url = api_config['base_url'].rstrip('/')
        self.username = api_config['username']
        self.password = api_config['password']
        self.bq_project = bq_config['project_id']
        self.bq_dataset = bq_config['dataset_id']
        self.session = requests.Session()
        self._authenticate()
    
    def _authenticate(self):
        """Authentification à l'API"""
        try:
            self.session.auth = (self.username, self.password)
            logger.info("✅ Authentification réussie")
        except Exception as e:
            logger.error(f"❌ Erreur d'authentification : {e}")
            raise
    
    def fetch_data(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> List[Dict]:
        """Récupère les données d'un endpoint avec gestion de retry"""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=180)
                response.raise_for_status()
                if not response.content:
                    logger.warning(f"⚠️ Réponse vide pour {endpoint} (params={params})")
                    return []
                data = response.json()
                logger.info(f"✓ {endpoint} : {len(data) if isinstance(data, list) else 1} enregistrements récupérés")
                return data if isinstance(data, list) else [data]

            except requests.exceptions.RequestException as e:
                logger.warning(f"Tentative {attempt + 1}/{max_retries} échouée pour {endpoint} : {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"✗ Échec définitif pour {endpoint}")
                    return []
        return []
    
    def extract_tarif_conventions(self, bq: 'BigQueryLoader') -> Dict:
        """
        Extraction incrémentale des TarifConvention.
        N'appelle l'API que pour les (IDSoc, IDConvent) pas encore chargés dans BQ.
        """
        logger.info("\n========== 📋 TARIF CONVENTIONS (incrémental) ==========")
        stats = {"success": 0, "failed": 0, "skipped": 0, "total_records": 0}

        # 1. Récupère tous les (IDSoc, IDConvent) connus dans les tables Convention
        all_conventions = bq.query(f"""
            SELECT DISTINCT IDConvent, 2 AS IDSoc
            FROM `{bq.project_id}.{self.bq_dataset}.Convention_Soc_2`
            WHERE IDConvent IS NOT NULL
            UNION ALL
            SELECT DISTINCT IDConvent, 4 AS IDSoc
            FROM `{bq.project_id}.{self.bq_dataset}.Convention_Soc_4`
            WHERE IDConvent IS NOT NULL
        """)

        if not all_conventions:
            logger.warning("⚠️  Aucune convention trouvée dans Convention_Soc_2 / Convention_Soc_4")
            return stats

        # 2. Récupère les (IDSoc, IDConvent) déjà chargés dans TarifConvention
        already_loaded = bq.query(f"""
            SELECT DISTINCT IDConvent, IDSoc
            FROM `{bq.project_id}.{self.bq_dataset}.TarifConvention`
        """)
        already_loaded_set = {(r["IDConvent"], r["IDSoc"]) for r in already_loaded}

        # 3. Calcule le delta
        to_fetch = [
            r for r in all_conventions
            if (r["IDConvent"], r["IDSoc"]) not in already_loaded_set
        ]

        logger.info(f"📊 {len(all_conventions)} conventions connues, "
                    f"{len(already_loaded_set)} déjà chargées, "
                    f"{len(to_fetch)} à fetcher")

        # 4. Appel API pour chaque nouvelle convention — collecte tout en mémoire
        batch = []

        for i, row in enumerate(to_fetch, 1):
            id_convent = row["IDConvent"]
            id_soc = row["IDSoc"]

            data = self.fetch_data(
                "/SofiaEDC/API/Financement/Get",
                params={"Entite": "TarifConvention", "IDSoc": id_soc, "IDConvent": id_convent}
            )

            time.sleep(0.3)

            if data:
                for record in data:
                    record["IDConvent"] = id_convent
                    record["IDSoc"] = id_soc
                batch.extend(data)
                stats["success"] += 1
                stats["total_records"] += len(data)
            else:
                stats["skipped"] += 1

            if i % 50 == 0:
                logger.info(f"  → {i}/{len(to_fetch)} conventions traitées")

        logger.info(f"  → {len(to_fetch)}/{len(to_fetch)} conventions traitées")

        # 5. Chargement unique WRITE_APPEND avec schéma explicite tout-STRING
        # (autodetect=False empêche BQ d'inférer DATE/TIMESTAMP depuis les chaînes de dates)
        if batch:
            ok = bq.load_data(
                data=batch,
                dataset_id=self.bq_dataset,
                table_id="TarifConvention",
                write_disposition="WRITE_APPEND",
                stringify=True,
                autodetect=False
            )
            if not ok:
                stats["failed"] += len(batch)
                stats["success"] -= len(batch)

        logger.info(f"✅ TarifConvention — succès: {stats['success']}, "
                    f"échecs: {stats['failed']}, vides: {stats['skipped']}")
        return stats

    def run(self, endpoints_config: List[Dict], credentials_path: Optional[str] = None) -> Dict:
        """
        Exécute le pipeline complet d'extraction
        
        Args:
            endpoints_config: Liste des configurations d'endpoints
            credentials_path: Chemin vers les credentials GCP (optionnel)
        
        Returns:
            Dictionnaire avec les statistiques d'extraction
        """
        logger.info("========== 🚀 DEBUT EXTRACTION SOFIA ==========")
        start_time = time.time()
        
        stats = {
            "success": 0,
            "failed": 0,
            "total_records": 0,
            "duration": 0
        }
        
        try:
            # Créer le loader BigQuery
            bq = BigQueryLoader(self.bq_project, credentials_path)

            # Phase 1 — Collecte : regroupe toutes les données par table avant chargement.
            # Permet un WRITE_TRUNCATE unique par table avec l'ensemble des données,
            # ce qui évite les conflits de schéma BQ quand différentes périodes
            # retournent des types inconsistants pour le même champ.
            table_batches: Dict[str, List] = {}
            table_stringify: Dict[str, bool] = {}

            for config in endpoints_config:
                endpoint = config["endpoint"]
                table = config["table"]
                params = config.get("params", {})
                stringify = config.get("stringify", False)

                logger.info(f"\n--- 📥 Traitement de {endpoint} ---")
                data = self.fetch_data(endpoint, params=params)

                if data:
                    table_batches.setdefault(table, []).extend(data)
                    table_stringify[table] = stringify
                else:
                    stats["failed"] += 1

            # Phase 2 — Chargement : un seul WRITE_TRUNCATE par table
            for table, data in table_batches.items():
                stringify = table_stringify.get(table, False)
                logger.info(f"\n--- 💾 Chargement {table} ({len(data)} enregistrements) ---")
                success = bq.load_data(
                    data=data,
                    dataset_id=self.bq_dataset,
                    table_id=table,
                    write_disposition="WRITE_TRUNCATE",
                    stringify=stringify
                )
                if success:
                    stats["success"] += 1
                    stats["total_records"] += len(data)
                else:
                    stats["failed"] += 1
            
            # Extraction incrémentale des TarifConvention
            tarif_stats = self.extract_tarif_conventions(bq)
            stats["success"] += tarif_stats["success"]
            stats["failed"] += tarif_stats["failed"]
            stats["total_records"] += tarif_stats["total_records"]

            stats["duration"] = round(time.time() - start_time)

            logger.info(f"\n========== ✅ EXTRACTION SOFIA TERMINEE ==========")

            logger.info(f"⏱️  Durée : {stats['duration']:.2f}s")
            logger.info(f"✅ Succès : {stats['success']}/{len(endpoints_config)}")
            logger.info(f"❌ Échecs : {stats['failed']}")
            logger.info(f"📊 Total : {stats['total_records']} enregistrements")

            return stats
            
        except Exception as e:
            stats["duration"] = round(time.time() - start_time)
            logger.error(f"❌ Erreur critique : {e}", exc_info=True)
            raise



class BigQueryLoader:
    """Classe pour gérer le chargement des données vers BigQuery"""
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            self.client = bigquery.Client(project=project_id)
        
        self.project_id = project_id
        logger.info(f"Client BigQuery initialisé pour le projet {project_id}")
    
    @staticmethod
    def _stringify_leaves(record: Dict) -> Dict:
        """Convertit toutes les valeurs feuilles en STRING pour éviter les conflits de type BQ."""
        result = {}
        for key, value in record.items():
            if isinstance(value, dict):
                result[key] = BigQueryLoader._stringify_leaves(value)
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    result[key] = [BigQueryLoader._stringify_leaves(item) for item in value]
                else:
                    # Liste de primitives → chaque élément en STRING (évite str([...]) invalide en JSON)
                    result[key] = [str(v) if v is not None else None for v in value]
            elif value is None:
                result[key] = None
            else:
                result[key] = str(value)
        return result

    @staticmethod
    def _build_string_schema(records: List[Dict]) -> List['bigquery.SchemaField']:
        """
        Construit un schéma BQ où tous les champs feuilles sont STRING.
        Utilisé quand autodetect=False pour empêcher BQ d'inférer DATE/TIMESTAMP
        depuis des chaînes de dates (ex: "2026-01-01").
        """
        seen: Dict = {}
        for record in records:
            BigQueryLoader._collect_fields_into(record, seen)
        return BigQueryLoader._schema_from_seen(seen)

    @staticmethod
    def _collect_fields_into(record: Dict, seen: Dict):
        for key, value in record.items():
            if isinstance(value, dict):
                if key not in seen:
                    seen[key] = ('RECORD', {})
                if seen[key][0] == 'RECORD':
                    BigQueryLoader._collect_fields_into(value, seen[key][1])
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                if key not in seen:
                    seen[key] = ('RECORD_REPEATED', {})
                if seen[key][0] == 'RECORD_REPEATED':
                    for item in value:
                        if isinstance(item, dict):
                            BigQueryLoader._collect_fields_into(item, seen[key][1])
            else:
                if key not in seen:
                    seen[key] = ('STRING', None)

    @staticmethod
    def _schema_from_seen(seen: Dict) -> List['bigquery.SchemaField']:
        fields = []
        for key, (ftype, sub) in seen.items():
            if ftype == 'RECORD':
                sub_fields = BigQueryLoader._schema_from_seen(sub)
                fields.append(bigquery.SchemaField(key, 'RECORD', mode='NULLABLE', fields=sub_fields))
            elif ftype == 'RECORD_REPEATED':
                sub_fields = BigQueryLoader._schema_from_seen(sub)
                fields.append(bigquery.SchemaField(key, 'RECORD', mode='REPEATED', fields=sub_fields))
            else:
                fields.append(bigquery.SchemaField(key, 'STRING', mode='NULLABLE'))
        return fields

    def load_data(self, data: List[Dict], dataset_id: str, table_id: str,
                  write_disposition: str = "WRITE_APPEND",
                  stringify: bool = False,
                  autodetect: bool = True) -> bool:
        """Charge les données dans BigQuery"""
        if not data:
            logger.warning(f"Aucune donnée à charger pour {dataset_id}.{table_id}")
            return False

        try:
            cleaned_data = []
            for record in data:
                cleaned_record = self._clean_field_names(record)
                if stringify:
                    cleaned_record = self._stringify_leaves(cleaned_record)
                cleaned_record['_extracted_at'] = datetime.now().isoformat()
                cleaned_data.append(cleaned_record)

            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

            # autodetect=False → schéma explicite tout-STRING pour éviter que BQ
            # infère DATE/TIMESTAMP depuis des chaînes de dates après stringify
            schema = self._build_string_schema(cleaned_data) if not autodetect else None

            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=autodetect,
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema_update_options=(
                    [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                    if write_disposition == "WRITE_APPEND" else []
                ),
            )
            
            job = self.client.load_table_from_json(
                cleaned_data, table_ref, job_config=job_config
            )
            job.result()
            
            logger.info(f"✓ {len(data)} enregistrements chargés dans {table_ref}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Erreur lors du chargement dans {dataset_id}.{table_id} : {e}")
            self._save_backup(data, dataset_id, table_id)
            return False
    
    def _clean_field_names(self, record: Dict) -> Dict:
        """Nettoie les noms de champs pour BigQuery"""
        cleaned = {}
        for key, value in record.items():
            clean_key = key.replace('$', '_').replace('@', '_').replace('#', '_')
            clean_key = clean_key.replace('.', '_').replace('-', '_').replace(' ', '_')
            
            if isinstance(value, dict):
                cleaned[clean_key] = self._clean_field_names(value)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                cleaned[clean_key] = [self._clean_field_names(item) for item in value]
            else:
                cleaned[clean_key] = value
        
        return cleaned
    
    def query(self, sql: str) -> List[Dict]:
        """Exécute une requête SQL et retourne les résultats sous forme de liste de dicts"""
        try:
            results = self.client.query(sql).result()
            return [dict(row) for row in results]
        except Exception as e:
            logger.warning(f"⚠️ Requête BQ échouée : {e}")
            return []

    def _save_backup(self, data: List[Dict], dataset_id: str, table_id: str):
        """Sauvegarde locale en cas d'échec"""
        import os
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = os.path.join(backup_dir, f"backup_{dataset_id}_{table_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        try:
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Backup sauvegardé : {backup_file}")
        except Exception as e:
            logger.error(f"❌ Impossible de créer le backup : {e}")

