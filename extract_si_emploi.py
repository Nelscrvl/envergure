import os
import subprocess
import time
import logging
from typing import Dict, List

import mysql.connector
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class MySQLExtractor:
    """Extraction des données depuis MySQL via tunnel SSH"""
    
    def __init__(self, ssh_config: Dict, mysql_config: Dict, bq_config: Dict):
        self.ssh_config = ssh_config
        self.mysql_config = mysql_config
        self.bq_config = bq_config
        self.tunnel_active = False
        self.ssh_key_path = '/tmp/ssh_key'
        self.bq_client = bigquery.Client(project=bq_config['project_id'])
    
    def setup_ssh_key(self, ssh_key_content: str):
        """Configure la clé SSH"""
        logger.info("🔧 MySQL - Configuration de la clé SSH...")
        with open(self.ssh_key_path, 'w') as f:
            f.write(ssh_key_content)
        os.chmod(self.ssh_key_path, 0o600)
        logger.info("✅ MySQL - Clé SSH configurée")
    
    def start_ssh_tunnel(self):
        """Démarre le tunnel SSH"""
        logger.info("🔌 MySQL - Démarrage du tunnel SSH...")
        
        ssh_command = [
            'ssh', '-f', '-N',
            '-i', self.ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'HostKeyAlgorithms=+ssh-rsa',
            '-o', 'PubkeyAcceptedAlgorithms=+ssh-rsa',
            '-o', 'ServerAliveInterval=60',
            '-L', '13306:127.0.0.1:3306',
            f'{self.ssh_config["user"]}@{self.ssh_config["host"]}'
        ]
        
        result = subprocess.run(ssh_command, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"❌ MySQL - Erreur SSH: {result.stderr}")
            raise Exception("Impossible d'établir le tunnel SSH")
        
        time.sleep(3)
        self.tunnel_active = True
        logger.info("✅ MySQL - Tunnel SSH établi")
    
    def stop_ssh_tunnel(self):
        """Arrête le tunnel SSH"""
        if self.tunnel_active:
            logger.info("🔌 MySQL - Fermeture du tunnel SSH...")
            subprocess.run(
                ['pkill', '-f', f'{self.ssh_config["user"]}@{self.ssh_config["host"]}'], 
                capture_output=True
            )
            self.tunnel_active = False
            if os.path.exists(self.ssh_key_path):
                os.remove(self.ssh_key_path)
            logger.info("✅ MySQL - Tunnel fermé")
    
    def get_mysql_connection(self):
        """Crée une connexion MySQL"""
        return mysql.connector.connect(
            host='localhost',
            port=13306,
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database=self.mysql_config['database']
        )
    
    def get_all_tables(self) -> List[str]:
        """Récupère la liste des tables"""
        conn = self.get_mysql_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            logger.info(f"✅ MySQL - {len(tables)} tables trouvées")
            return tables
        finally:
            conn.close()
    
    def extract_table(self, table_name: str) -> pd.DataFrame:
        """Extrait les données d'une table"""
        logger.info(f"📥 MySQL - Extraction de '{table_name}'...")
        conn = self.get_mysql_connection()
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            logger.info(f"✅ MySQL - {len(df)} lignes extraites de '{table_name}'")
            return df
        finally:
            conn.close()
    
    def ensure_dataset_exists(self):
        """Crée le dataset BigQuery s'il n'existe pas"""
        dataset_id = self.bq_config['dataset_id']
        dataset_ref = self.bq_client.dataset(dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
        except Exception:
            logger.info(f"📦 Création du dataset '{dataset_id}'...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"
            self.bq_client.create_dataset(dataset)
            logger.info(f"✅ Dataset '{dataset_id}' créé")
    
    def load_to_bigquery(self, df: pd.DataFrame, table_name: str) -> bool:
        """Charge un DataFrame dans BigQuery"""
        if df.empty:
            logger.warning(f"⚠️  DataFrame vide pour {table_name}")
            return False
        
        try:
            from datetime import datetime
            df['_extracted_at'] = datetime.now().isoformat()
            
            table_ref = f"{self.bq_config['project_id']}.{self.bq_config['dataset_id']}.{table_name}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
            )
            
            job = self.bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            
            logger.info(f"✅ {len(df)} lignes chargées dans {table_ref}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement : {e}")
            return False
    
    def run(self) -> Dict:
        """Exécute l'extraction complète MySQL"""
        logger.info("\n" + "="*60)
        logger.info("🚀 EXTRACTION MYSQL")
        logger.info("="*60)
        
        stats = {"success": 0, "failed": 0, "total_records": 0}
        
        try:
            # Setup SSH
            self.setup_ssh_key(self.ssh_config['key'])
            self.start_ssh_tunnel()
            
            # Création dataset
            self.ensure_dataset_exists()
            
            # Récupération des tables
            tables = self.get_all_tables()
            
            # Extraction et chargement
            for i, table_name in enumerate(tables, 1):
                logger.info(f"\n--- [{i}/{len(tables)}] {table_name} ---")
                try:
                    df = self.extract_table(table_name)
                    success = self.load_to_bigquery(df, table_name)
                    
                    if success:
                        stats['success'] += 1
                        stats['total_records'] += len(df)
                    else:
                        stats['failed'] += 1
                except Exception as e:
                    logger.error(f"❌ Erreur avec '{table_name}' : {e}")
                    stats['failed'] += 1
            
            logger.info(f"\n✅ MySQL terminé - {stats['success']}/{len(tables)} succès")
            
        except Exception as e:
            logger.error(f"❌ Erreur dans le pipeline MySQL : {e}", exc_info=True)
            stats['failed'] += 1
        
        finally:
            self.stop_ssh_tunnel()
        
        return stats