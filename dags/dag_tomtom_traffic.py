from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

SCRAPERS_PATH = '/opt/airflow/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from scrap_tomtom import get_warkop_traffic

def load_csv_to_postgres():
    path_staging = '/opt/airflow/data/raw/traffic_staging.csv'
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')
    
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS traffic_history (
            id SERIAL PRIMARY KEY,
            waktu TIMESTAMP,
            lokasi VARCHAR(100),
            kecepatan_sekarang INT,
            kecepatan_bebas INT,
            tingkat_kelancaran FLOAT
        );
    """
    pg_hook.run(create_table_sql)
    print("Pengecekan/Pembuatan tabel traffic_history berhasil dieksekusi.")
    
    if not os.path.exists(path_staging):
        raise FileNotFoundError(f"GAGAL: File staging tidak ditemukan di {path_staging}!")
        
    df = pd.read_csv(path_staging)
    
    rows_to_insert = [tuple(x) for x in df.to_numpy()]
    
    if rows_to_insert:
        pg_hook.insert_rows(
            table='traffic_history',
            rows=rows_to_insert,
            target_fields=['waktu', 'lokasi', 'kecepatan_sekarang', 'kecepatan_bebas', 'tingkat_kelancaran']
        )
        print(f"Berhasil load {len(rows_to_insert)} baris dari CSV ke PostgreSQL")

default_args = {
    'owner': 'zaki',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='tomtom_traffic_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: TomTom API -> CSV -> PostgreSQL',
    schedule='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['warkop', 'etl', 'postgres'],
) as dag:

    # Task 1: Extract (API ke CSV)
    extract_task = PythonOperator(
        task_id='extract_api_to_csv',
        python_callable=get_warkop_traffic
    )

    # Task 2: Load (CSV ke Postgres)
    load_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    extract_task >> load_task