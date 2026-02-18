import requests
import json
import logging
from datetime import datetime, timedelta
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
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
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
    
    def run(self, endpoints_config: List[Dict], credentials_path: str = None) -> Dict:
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
            
            for config in endpoints_config:
                endpoint = config["endpoint"]
                table = config["table"]
                params = config.get("params", {})
                
                logger.info(f"\n--- 📥 Traitement de {endpoint} ---")
                
                # Extraction
                data = self.fetch_data(endpoint, params=params)
                
                # Chargement BigQuery
                if data:
                    success = bq.load_data(
                        data=data,
                        dataset_id=self.bq_dataset,
                        table_id=table,
                        write_disposition="WRITE_APPEND"
                    )
                    
                    if success:
                        stats["success"] += 1
                        stats["total_records"] += len(data)
                    else:
                        stats["failed"] += 1
                else:
                    stats["failed"] += 1
            
            stats["duration"] = time.time() - start_time
            
            logger.info(f"\n========== ✅ EXTRACTION SOFIA TERMINEE ==========")
            logger.info(f"⏱️  Durée : {stats['duration']:.2f}s")
            logger.info(f"✅ Succès : {stats['success']}/{len(endpoints_config)}")
            logger.info(f"❌ Échecs : {stats['failed']}")
            logger.info(f"📊 Total : {stats['total_records']} enregistrements")
            
            return stats
            
        except Exception as e:
            stats["duration"] = time.time() - start_time
            logger.error(f"❌ Erreur critique : {e}", exc_info=True)
            raise



class BigQueryLoader:
    """Classe pour gérer le chargement des données vers BigQuery"""
    
    def __init__(self, project_id: str, credentials_path: str = None):
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            self.client = bigquery.Client(project=project_id)
        
        self.project_id = project_id
        logger.info(f"Client BigQuery initialisé pour le projet {project_id}")
    
    def load_data(self, data: List[Dict], dataset_id: str, table_id: str,
                  write_disposition: str = "WRITE_APPEND") -> bool:
        """Charge les données dans BigQuery"""
        if not data:
            logger.warning(f"Aucune donnée à charger pour {dataset_id}.{table_id}")
            return False
        
        try:
            cleaned_data = []
            for record in data:
                cleaned_record = self._clean_field_names(record)
                cleaned_record['_extracted_at'] = datetime.now().isoformat()
                cleaned_data.append(cleaned_record)
            
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
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
    
    def _save_backup(self, data: List[Dict], dataset_id: str, table_id: str):
        """Sauvegarde locale en cas d'échec"""
        backup_file = f"backup_{dataset_id}_{table_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Backup sauvegardé : {backup_file}")
        except Exception as e:
            logger.error(f"❌ Impossible de créer le backup : {e}")


# ============================================================
# Pour exécuter extract_sofia.py directement
# ============================================================
if __name__ == "__main__":
    
    monthly_periods = generate_monthly_periods('2024-01-01', '2024-31-01')

    # Configuration standalone
    API_CONFIG = {
        'API_BASE_URL' : "https://envergure.sc-form.net",
        'API_USERNAME' : "nelson@avdata.fr",
        'API_PASSWORD' : "EGMKQ6WmpxZnMRQ@"
    }
    
    BQ_CONFIG = {
        'BQ_PROJECT_ID':"dynamic-camp-465312-b6",
        'BQ_DATASET_ID':"Extract_Sofia",
        'BQ_CREDENTIALS_PATH': None
    }
    
    sofia_endpoints = []
    #sofia_endpoints.append(  {
           # "endpoint": "/SofiaEDC/API/Entite/GetEntite",
            #"params": {"IDSociete": 2, "ACTION": "ACTION"},
            #"table": "entites"
       # })
    #sofia_endpoints.append({
           # "endpoint": "/SofiaEDC/API/Entite/GetEntite",
           # "params": {"IDSociete": 3, "ACTION": "INSCRIT"},
           # "table": "Inscrite_Soc_3"
      #  })
    for period in monthly_periods:
        sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
            "params": {
                "IDSociete": 4, 
                "Debut": period['Debut'], 
                "Fin": period['Fin']
            },
            "table": "Presence_Soc_4"  # ← Même table pour tous !
            })
    for period in monthly_periods:
        sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
            "params": {
                "IDSociete": 3, 
                "Debut": period['Debut'], 
                "Fin": period['Fin']
            },
            "table": "Presence_Soc_3"  # ← Même table pour tous !
            })
    for period in monthly_periods:
        sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
            "params": {
                "IDSociete": 2, 
                "Debut": period['Debut'], 
                "Fin": period['Fin']
        },
            "table": "Presence_Soc_2"  # ← Même table pour tous !
            })
    
    # Lancement standalone
    extractor = APIExtractor(API_CONFIG, BQ_CONFIG)
    results = extractor.run(sofia_endpoints, credentials_path=None)