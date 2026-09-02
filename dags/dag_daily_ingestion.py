"""
DAG principal — orchestration du pipeline data lakehouse.

Option A (déclenchement planifié) : ce DAG vérifie périodiquement la zone
brute de MinIO (bucket 'raw') alimentée par NiFi, et lance les jobs Spark
du médaillon (Bronze -> Silver -> Gold) dès que des données y sont
présentes.

Déclenchement des jobs Spark : via 'docker exec spark-master spark-submit ...',
exécuté depuis l'intérieur du conteneur Airflow grâce au socket Docker de
l'hôte monté sur les conteneurs airflow-scheduler/webserver (voir
Dockerfile.airflow et docker-compose.yml).
"""

from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
RAW_BUCKET = "raw"

DOMAINS = ["products", "users", "carts"]
GOLD_TABLES = ["product_performance", "cart_summary", "category_trends"]

SPARK_SUBMIT_CMD = "docker exec spark-master /opt/spark/bin/spark-submit"

default_args = {
    "owner": "lakehouse-project",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def check_raw_zone_has_data(**context):
    """
    Vérifie que la zone brute contient au moins un fichier pour chaque
    domaine. Si ce n'est pas le cas, la tâche est marquée 'skipped' et le
    reste du DAG (Bronze/Silver/Gold) ne se déclenche pas ce run-ci.
    """
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    for domain in DOMAINS:
        response = client.list_objects_v2(
            Bucket=RAW_BUCKET,
            Prefix="snapshots/",
        )
        contents = response.get("Contents", [])
        matching = [obj for obj in contents if f"{domain}_" in obj["Key"]]
        if not matching:
            raise AirflowSkipException(
                f"Aucune donnée trouvée pour le domaine '{domain}' dans raw/ — "
                f"le pipeline est ignoré pour cette exécution."
            )
        print(f"[Check] {len(matching)} fichier(s) trouvé(s) pour '{domain}'")

    print("[Check] Zone raw complète pour les 3 domaines — pipeline autorisé.")


with DAG(
    dag_id="lakehouse_medallion_pipeline",
    description="Pipeline complet Bronze -> Silver -> Gold, déclenché après vérification de la zone raw",
    default_args=default_args,
    schedule_interval="@hourly",  # Option A : vérification planifiée
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["lakehouse", "medallion"],
) as dag:

    check_raw = PythonOperator(
        task_id="check_raw_zone",
        python_callable=check_raw_zone_has_data,
    )

    # --- Couche Bronze : une tâche par domaine, exécutables en parallèle ---
    bronze_tasks = {}
    for domain in DOMAINS:
        bronze_tasks[domain] = BashOperator(
            task_id=f"bronze_{domain}",
            bash_command=(
                f"{SPARK_SUBMIT_CMD} /opt/spark/jobs/bronze/bronze_job.py "
                f"--domain {domain}"
            ),
        )

    # --- Couche Silver : une tâche par domaine, en parallèle entre elles,
    #     mais chacune attend la fin de la Bronze du MÊME domaine ---
    silver_tasks = {}
    for domain in DOMAINS:
        silver_tasks[domain] = BashOperator(
            task_id=f"silver_{domain}",
            bash_command=(
                f"{SPARK_SUBMIT_CMD} /opt/spark/jobs/silver/silver_job.py "
                f"--domain {domain}"
            ),
        )
        bronze_tasks[domain] >> silver_tasks[domain]

    # --- Couche Gold : chaque table dépend de TOUTES les tables Silver
    #     (les jointures Gold combinent plusieurs domaines) ---
    gold_tasks = {}
    for table in GOLD_TABLES:
        gold_tasks[table] = BashOperator(
            task_id=f"gold_{table}",
            bash_command=(
                f"{SPARK_SUBMIT_CMD} /opt/spark/jobs/gold/gold_job.py "
                f"--table {table}"
            ),
        )
        for domain in DOMAINS:
            silver_tasks[domain] >> gold_tasks[table]

    # --- Dépendance initiale : rien ne démarre sans la vérification raw ---
    for domain in DOMAINS:
        check_raw >> bronze_tasks[domain]