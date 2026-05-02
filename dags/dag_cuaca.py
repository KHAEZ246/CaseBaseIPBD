from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

SCRAPERS_PATH = '/opt/airflow/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from scrap_cuaca import get_cuaca_solo

def load_cuaca_to_postgres():
    path_staging = '/opt/airflow/data/raw/cuaca_staging.csv'

    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS cuaca_history (
            id               SERIAL PRIMARY KEY,
            waktu            TIMESTAMP,
            kota             VARCHAR(50),
            suhu             FLOAT,
            suhu_terasa      FLOAT,
            kelembapan       INT,
            kondisi          VARCHAR(50),
            deskripsi        VARCHAR(100),
            kecepatan_angin  FLOAT,
            curah_hujan      FLOAT,
            cloudiness       INT
        );
    """
    pg_hook.run(create_table_sql)
    print("Tabel cuaca_history siap.")

    if not os.path.exists(path_staging):
        print("Skip: File staging cuaca tidak ditemukan.")
        return

    df = pd.read_csv(path_staging)

    if df.empty:
        print("Skip: CSV staging cuaca kosong.")
        return

    rows_to_insert = [tuple(x) for x in df.to_numpy()]

    pg_hook.insert_rows(
        table='cuaca_history',
        rows=rows_to_insert,
        target_fields=['waktu', 'kota', 'suhu', 'suhu_terasa', 'kelembapan',
                       'kondisi', 'deskripsi', 'kecepatan_angin', 'curah_hujan', 'cloudiness']
    )
    print(f"Berhasil load {len(rows_to_insert)} baris cuaca ke PostgreSQL!")

default_args = {
    'owner': 'zaki',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='cuaca_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: OpenWeatherMap -> CSV -> PostgreSQL',
    schedule='0 5-14 * * *',  # Tiap 1 jam, jam 05-14 UTC = 12-21 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['warkop', 'cuaca', 'etl', 'postgres'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_cuaca_to_csv',
        python_callable=get_cuaca_solo,
    )

    load_task = PythonOperator(
        task_id='load_cuaca_to_postgres',
        python_callable=load_cuaca_to_postgres,
    )

    extract_task >> load_task