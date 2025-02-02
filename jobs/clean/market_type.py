import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    """
    TODO: Change to MERGE into market_type table
    """
    spark: SparkSession = setup_spark_environment(namespace, branch)

    market_type = (
        spark.table("betting.landing.raw")
        .filter(F.col("mc.marketDefinition").isNotNull())
        .select(
            F.col("mc.marketDefinition.marketType").alias("type"),
        )
        .distinct()
        .withColumn("id", F.monotonically_increasing_id())
    )

    save_table(spark, market_type, f"{namespace}.market_type", mode=WriteMode.REPLACE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a PySpark job")
    parser.add_argument(
        "--namespace",
        type=str,
        required=False,
        dest="namespace",
        help="If specified creates a new namespace and uses it",
    )
    parser.add_argument(
        "--branch",
        type=str,
        required=False,
        dest="branch",
        help="If specified creates a new branch in nessie and uses it",
    )
    args = parser.parse_args()

    save(args.namespace, args.branch)
