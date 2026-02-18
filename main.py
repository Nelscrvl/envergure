"""
Orchestration principale - Exécute les deux extractions
"""

import os
import logging
import time
from datetime import datetime, timedelta
import calendar
from extract_si_emploi import MySQLExtractor
from extract_sofia import APIExtractor, generate_monthly_periods

# Configuration du logging
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
            'password': "EGMKQ6WmpxZnMRQ@"              
        },
        'bq': {
            'project_id': GCP_PROJECT_ID,
            'dataset_id': 'Extract_Sofia'
        }
        }
    
   # Génération des périodes mensuelles pour les endpoints avec dates
def format_date_for_societe(date_str, id_societe):
    """
    Formate la date selon les exigences bizarres de chaque société
    IDSociete 1 et 2 : format normal AAAA-MM-JJ
    IDSociete 3 : format inversé AAAA-JJ-MM
    """
    if id_societe == 3:
        # Format bizarre pour société 3 : AAAA-JJ-MM
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return f"{dt.year}-{dt.day:02d}-{dt.month:02d}"
    else:
        # Format normal pour sociétés 1 et 2 : AAAA-MM-JJ
        return date_str


# Génération des périodes (format normal)
monthly_periods = generate_monthly_periods('2025-01-01', '2025-12-31')

sofia_endpoints = []

# Pour IDSociete 3 (format inversé)
for period in monthly_periods:
    sofia_endpoints.append({
        "endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
        "params": {
            "IDSociete": 3,
            "debut": format_date_for_societe(period['Debut'], 3),  # Convertit en AAAA-JJ-MM
            "fin": format_date_for_societe(period['Fin'], 3)        # Convertit en AAAA-JJ-MM
        },
        "table": "Presence_Soc_3"
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
sofia_endpoints.append(  {
            "endpoint": "/SofiaEDC/API/Entite/GetEntite",
            "params": {"IDSociete": 2, "ACTION": "ACTION"},
            "table": "entites"
        })
sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/Entite/GetEntite",
            "params": {"IDSociete": 3, "ACTION": "INSCRIT"},
            "table": "Inscrite_Soc_3"
        })
sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/StageEntreprise/GetStageEntrep",
            "params": {"IDSociete": 2, "Debut":"2025-01-01","Fin":"2025-12-31"},
            "table":"Stage_Soc_2"
         })
sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/StageEntreprise/GetStageEntrep",
            "params": {"IDSociete": 3, "Debut":"2025-01-01","Fin":"2025-12-31"},
            "table":"Stage_Soc_3"
         })
sofia_endpoints.append({
            "endpoint": "/SofiaEDC/API/Entite/GetEntite",
            "params": {"IDSociete": 2, "ACTION": "INSCRIT"},
            "table": "Inscrite_Soc_2"
        })
   
    #for period in monthly_periods:
        #sofia_endpoints.append({
            #"endpoint": "/SofiaEDC/API/PresenceSta/GetSeance",
           # "params": {
            #    "IDSociete": 2, 
            #    "Debut": period['Debut'], 
            #    "Fin": period['Fin']
          #  },
          #  "table": "Presence_Soc_2"  # ← Même table pour tous !
          #  })
        
    # Configuration MySQL
mysql_config = {
        'ssh': {
            'user': 'envergure',
            'host': 'app02027.global-sp.net',
            'key': open('/Users/nelson/envergures').read()  # ⬅️ Vérifier le chemin
        },
        'mysql': {
            'user': 'utilENVERGURE',         
            'password': 'F50dxWMRB4cJb1kYEv', 
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
            endpoints_config=sofia_endpoints,
            credentials_path=None  # Ou votre chemin de credentials
        )
except Exception as e:
        logger.error(f"❌ Erreur critique pipeline Sofia : {e}")
        sofia_stats = {
            'success': 0,
            'failed': 1,
            'total_records': 0,
            'duration': 0
        }
    
    # Pipeline 2 : MySQL
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

# Retourner un code d'erreur si échecs critiques
total_failures = sofia_stats['failed'] + mysql_stats['failed']
if total_failures > 0:
    logger.warning(f"⚠️  {total_failures} échecs au total")
    exit(1)

logger.info("✅ Pipeline quotidien terminé avec succès !")


if __name__ == "__main__":
    main()