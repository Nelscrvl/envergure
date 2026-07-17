"""
Orchestration principale - Exécute les deux extractions
"""

import os
import logging
import subprocess
import time
import base64
from datetime import datetime
from extract_si_emploi import MySQLExtractor
from extract_sofia import APIExtractor, build_sofia_endpoints

import requests as _requests
import google.auth
import google.auth.transport.requests


def _get_ssh_key(project_id: str) -> str:
    local_path = os.getenv('SSH_KEY_PATH')
    if local_path:
        return open(local_path).read()
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    credentials.refresh(google.auth.transport.requests.Request())
    url = (
        f"https://secretmanager.googleapis.com/v1/projects/{project_id}"
        f"/secrets/Ssh_Rsa/versions/latest:access"
    )
    resp = _requests.get(url, headers={"Authorization": f"Bearer {credentials.token}"})
    resp.raise_for_status()
    return base64.b64decode(resp.json()["payload"]["data"]).decode("UTF-8")


def _load_env(path='.env'):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, val = line.partition('=')
                os.environ.setdefault(key.strip(), val.strip().strip('"\''))
    except FileNotFoundError:
        pass

_load_env()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Orchestration principale"""
    logger.info("\n" + "="*80)
    logger.info("🌅 DÉMARRAGE DU PIPELINE QUOTIDIEN - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("="*80)

    start_time = time.time()

    # ==========================================================================
    # CONFIGURATION À MODIFIER ICI ⬇️
    # ==========================================================================

    GCP_PROJECT_ID = 'dynamic-camp-465312-b6'

    # Configuration API Sofia
    sofia_config = {
        'api': {
            'base_url': 'https://envergure.sc-form.net',
            'username': 'nelson@avdata.fr',
            'password': os.getenv('SOFIA_PASSWORD')
        },
        'bq': {
            'project_id': GCP_PROJECT_ID,
            'dataset_id': 'Extract_Sofia'
        }
    }

    # Configuration MySQL
    mysql_config = {
        'ssh': {
            'user': 'envergure',
            'host': 'app02027.global-sp.net',
            'key': _get_ssh_key(GCP_PROJECT_ID)
        },
        'mysql': {
            'user': 'utilENVERGURE',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'datamart_sie_envergure'
        },
        'bq': {
            'project_id': GCP_PROJECT_ID,
            'dataset_id': 'mysql_data'
        }
    }

    # ==========================================================================
    # EXÉCUTION DES PIPELINES
    # ==========================================================================

    logger.info("\n📡 Lancement du pipeline API Sofia...")

    try:
        sofia_extractor = APIExtractor(sofia_config['api'], sofia_config['bq'])
        sofia_stats = sofia_extractor.run(
            endpoints_config=build_sofia_endpoints('2026-01-01', datetime.now().strftime('%Y-%m-%d')),
            credentials_path=None
        )
    except Exception as e:
        logger.error(f"❌ Erreur critique pipeline Sofia : {e}")
        sofia_stats = {
            'success': 0,
            'failed': 1,
            'total_records': 0,
            'duration': 0
        }

    logger.info("\n🗄️  Lancement du pipeline MySQL...")
    mysql_extractor = MySQLExtractor(mysql_config['ssh'], mysql_config['mysql'], mysql_config['bq'])
    mysql_stats = mysql_extractor.run()

    # ==========================================================================
    # RÉSUMÉ GLOBAL
    # ==========================================================================

    elapsed = time.time() - start_time

    logger.info("\n" + "="*80)
    logger.info("📊 RÉSUMÉ GLOBAL DU PIPELINE QUOTIDIEN")
    logger.info("="*80)
    logger.info(f"⏱️  Durée totale : {elapsed/60:.2f} minutes")
    logger.info(f"\n📡 API Sofia :")
    logger.info(f"   ✅ Succès : {sofia_stats['success']}")
    logger.info(f"   ❌ Échecs : {sofia_stats['failed']}")
    logger.info(f"   📈 Enregistrements : {sofia_stats['total_records']:,}")
    logger.info(f"\n🗄️  MySQL :")
    logger.info(f"   ✅ Succès : {mysql_stats['success']}")
    logger.info(f"   ❌ Échecs : {mysql_stats['failed']}")
    logger.info(f"   📈 Enregistrements : {mysql_stats['total_records']:,}")
    logger.info("="*80 + "\n")

    # ==========================================================================
    # RÉSUMÉ
    # ==========================================================================

    total_failures = sofia_stats['failed'] + mysql_stats['failed']
    if total_failures > 0:
        logger.warning(f"⚠️  Échecs au total (extraction)")
        exit(1)

    logger.info("✅ Extraction terminée avec succès — le job dbt-build prend le relai.")


if __name__ == "__main__":
    main()
