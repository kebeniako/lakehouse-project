"""
Job Spark — Couche GOLD

Construit, à partir des tables Silver, des tables d'agrégats répondant
chacune à une question métier précise. Contrairement à Bronze/Silver
(un job par domaine), Gold combine plusieurs domaines : ce script prend
en argument le NOM de la table Gold à construire.

Tables disponibles :
  - product_performance : quels produits sont les plus commandés,
                           et sont-ils bien notés ?
  - cart_summary         : quelle est la valeur d'un panier, et d'où
                            viennent les clients qui dépensent le plus ?
  - category_trends      : comment la popularité des catégories évolue-t-
                            elle dans le temps ? (pertinent une fois le
                            backfill de 6 mois réalisé)

Usage :
    spark-submit gold_job.py --table product_performance
    spark-submit gold_job.py --table cart_summary
    spark-submit gold_job.py --table category_trends
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, first, round as spark_round, sum as spark_sum

SILVER_NAMESPACE = "nessie.silver"
GOLD_NAMESPACE = "nessie.gold"


def parse_args():
    parser = argparse.ArgumentParser(description="Job Spark Gold")
    parser.add_argument(
        "--table",
        required=True,
        choices=["product_performance", "cart_summary", "category_trends"],
        help="Table Gold à construire",
    )
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ----------------------------------------------------------------------
# Construction de chaque table Gold
# ----------------------------------------------------------------------

def build_product_performance(spark: SparkSession):
    """
    Question métier : quels produits sont les plus commandés, et sont-ils
    bien notés ? Jointure carts x products, agrégation par produit.
    """
    carts = spark.table(f"{SILVER_NAMESPACE}.carts")
    products = spark.table(f"{SILVER_NAMESPACE}.products")

    joined = carts.join(products, carts.product_id == products.id, "inner")

    return joined.groupBy(
        products.id.alias("product_id"),
        products.title,
        products.category,
    ).agg(
        spark_sum(carts.quantity).alias("total_quantity_ordered"),
        countDistinct(carts.cart_id).alias("num_carts_containing"),
        first(products.rating_rate).alias("rating_rate"),
        first(products.rating_count).alias("rating_count"),
    )


def build_cart_summary(spark: SparkSession):
    """
    Question métier : quelle est la valeur d'un panier, et d'où viennent
    les clients qui dépensent le plus ? Jointure carts x products x users.
    """
    carts = spark.table(f"{SILVER_NAMESPACE}.carts")
    products = spark.table(f"{SILVER_NAMESPACE}.products")
    users = spark.table(f"{SILVER_NAMESPACE}.users")

    carts_with_price = carts.join(
        products, carts.product_id == products.id, "inner"
    ).withColumn("line_total", col("quantity") * col("price"))

    cart_totals = carts_with_price.groupBy(carts.cart_id, carts.userId).agg(
        spark_round(spark_sum("line_total"), 2).alias("cart_total_value"),
        countDistinct(carts.product_id).alias("num_distinct_items"),
        first(carts.snapshot_date).alias("snapshot_date"),
    )

    return cart_totals.join(
        users, cart_totals.userId == users.id, "left"
    ).select(
        cart_totals.cart_id,
        cart_totals.userId,
        users.address_city,
        cart_totals.cart_total_value,
        cart_totals.num_distinct_items,
        cart_totals.snapshot_date,
    )


def build_category_trends(spark: SparkSession):
    """
    Question métier : comment la popularité des catégories évolue-t-elle
    dans le temps ? Agrégation simple sur products, par catégorie et par
    snapshot_date.
    """
    products = spark.table(f"{SILVER_NAMESPACE}.products")

    return products.groupBy("category", "snapshot_date").agg(
        count("*").alias("num_products"),
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_round(avg("rating_rate"), 2).alias("avg_rating"),
    )


TABLE_BUILDERS = {
    "product_performance": build_product_performance,
    "cart_summary": build_cart_summary,
    "category_trends": build_category_trends,
}


def run_gold(spark: SparkSession, table_name: str) -> None:
    target_table = f"{GOLD_NAMESPACE}.{table_name}"

    print(f"[Gold] Construction de la table : {target_table}")
    builder_fn = TABLE_BUILDERS[table_name]
    df_gold = builder_fn(spark)

    row_count = df_gold.count()
    print(f"[Gold] {row_count} lignes calculées pour '{table_name}'")

    print(f"[Gold] Écriture dans la table Iceberg : {target_table}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")

    (
        df_gold.writeTo(target_table)
        .using("iceberg")
        .createOrReplace()
    )

    print(f"[Gold] Terminé — table {target_table} mise à jour.")


def main():
    args = parse_args()
    spark = build_spark_session(f"gold-{args.table}")
    try:
        run_gold(spark, args.table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()