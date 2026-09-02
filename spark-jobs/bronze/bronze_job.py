"""
Job Spark — Couche BRONZE

Lit les fichiers JSON bruts déposés par NiFi dans la zone `raw` de MinIO,
pour un domaine donné (products, users ou carts), et les copie tels quels
(pas de nettoyage ni de transformation métier) dans une table Iceberg
Bronze, via le catalogue Nessie.

Deux colonnes techniques sont ajoutées :
  - snapshot_date       : date extraite du chemin du fichier source
                          (raw/snapshots/{date}/{domain}_{date}.json)
  - ingestion_timestamp : horodatage réel de l'exécution de ce job

Usage :
    spark-submit bronze_job.py --domain products
    spark-submit bronze_job.py --domain users
    spark-submit bronze_job.py --domain carts
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract

RAW_BUCKET = "raw"
BRONZE_NAMESPACE = "nessie.bronze"

# Extrait la date (yyyy-MM-dd) depuis un chemin du type :
#   s3a://raw/snapshots/2026-08-22/products_2026-08-22.json
DATE_PATTERN = r"snapshots/(\d{4}-\d{2}-\d{2})/"


def parse_args():
    parser = argparse.ArgumentParser(description="Job Spark Bronze")
    parser.add_argument(
        "--domain",
        required=True,
        choices=["products", "users", "carts"],
        help="Domaine FakeStoreAPI à traiter",
    )
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def run_bronze(spark: SparkSession, domain: str) -> None:
    source_path = f"s3a://{RAW_BUCKET}/snapshots/*/{domain}_*.json"
    target_table = f"{BRONZE_NAMESPACE}.{domain}"

    print(f"[Bronze] Lecture des fichiers : {source_path}")
    df = spark.read.option("multiline", "true").json(source_path)

    row_count = df.count()
    print(f"[Bronze] {row_count} lignes lues pour le domaine '{domain}'")

    df_bronze = (
        df.withColumn("_source_file", input_file_name())
        .withColumn("snapshot_date", regexp_extract("_source_file", DATE_PATTERN, 1))
        .withColumn("ingestion_timestamp", current_timestamp())
        .drop("_source_file")
    )

    print(f"[Bronze] Écriture dans la table Iceberg : {target_table}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}")

    (
        df_bronze.writeTo(target_table)
        .using("iceberg")
        .createOrReplace()
    )

    print(f"[Bronze] Terminé — table {target_table} mise à jour.")


def main():
    args = parse_args()
    spark = build_spark_session(f"bronze-{args.domain}")
    try:
        run_bronze(spark, args.domain)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()