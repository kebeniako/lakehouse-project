"""
Job Spark — Couche SILVER

Lit une table Bronze (copie brute du JSON) pour un domaine donné, applique
un nettoyage/aplatissement spécifique à ce domaine (sans jointure ni
agrégation — ça, c'est le rôle de Gold), et écrit le résultat dans une
table Iceberg Silver via le catalogue Nessie.

Usage :
    spark-submit silver_job.py --domain products
    spark-submit silver_job.py --domain users
    spark-submit silver_job.py --domain carts
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode

BRONZE_NAMESPACE = "nessie.bronze"
SILVER_NAMESPACE = "nessie.silver"


def parse_args():
    parser = argparse.ArgumentParser(description="Job Spark Silver")
    parser.add_argument(
        "--domain",
        required=True,
        choices=["products", "users", "carts"],
        help="Domaine FakeStoreAPI à traiter",
    )
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ----------------------------------------------------------------------
# Transformations spécifiques à chaque domaine
# ----------------------------------------------------------------------

def transform_products(df: DataFrame) -> DataFrame:
    """Aplatit la structure imbriquée 'rating' en deux colonnes simples."""
    return (
        df.withColumn("rating_rate", col("rating.rate"))
        .withColumn("rating_count", col("rating.count"))
        .drop("rating")
    )


def transform_users(df: DataFrame) -> DataFrame:
    """Aplatit les structures imbriquées 'name' et 'address'."""
    return (
        df.withColumn("firstname", col("name.firstname"))
        .withColumn("lastname", col("name.lastname"))
        .withColumn("address_city", col("address.city"))
        .withColumn("address_street", col("address.street"))
        .withColumn("address_number", col("address.number"))
        .withColumn("address_zipcode", col("address.zipcode"))
        .withColumn("address_geo_lat", col("address.geolocation.lat"))
        .withColumn("address_geo_long", col("address.geolocation.long"))
        .drop("name", "address")
    )


def transform_carts(df: DataFrame) -> DataFrame:
    """
    Explose la liste imbriquée 'products' : une ligne par panier devient
    une ligne par (panier, produit, quantité). Le nom de colonne source
    'id' (identifiant du panier) est renommé 'cart_id' pour rester clair
    une fois joint aux autres domaines dans la couche Gold.
    """
    df_exploded = df.withColumn("product_item", explode(col("products")))
    return (
        df_exploded.withColumnRenamed("id", "cart_id")
        .withColumn("product_id", col("product_item.productId"))
        .withColumn("quantity", col("product_item.quantity"))
        .drop("products", "product_item")
    )


DOMAIN_TRANSFORMS = {
    "products": transform_products,
    "users": transform_users,
    "carts": transform_carts,
}


def run_silver(spark: SparkSession, domain: str) -> None:
    source_table = f"{BRONZE_NAMESPACE}.{domain}"
    target_table = f"{SILVER_NAMESPACE}.{domain}"

    print(f"[Silver] Lecture de la table Bronze : {source_table}")
    df_bronze = spark.table(source_table)

    transform_fn = DOMAIN_TRANSFORMS[domain]
    df_silver = transform_fn(df_bronze)

    row_count = df_silver.count()
    print(f"[Silver] {row_count} lignes après transformation pour '{domain}'")

    print(f"[Silver] Écriture dans la table Iceberg : {target_table}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")

    (
        df_silver.writeTo(target_table)
        .using("iceberg")
        .createOrReplace()
    )

    print(f"[Silver] Terminé — table {target_table} mise à jour.")


def main():
    args = parse_args()
    spark = build_spark_session(f"silver-{args.domain}")
    try:
        run_silver(spark, args.domain)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()