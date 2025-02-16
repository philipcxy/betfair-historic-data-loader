import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    """
    TODO: Needs to be updated to use MERG INTO
    """
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting.landing.raw")
        .filter(F.col("mc.marketDefinition").isNotNull())
        .select(F.col("mc.marketDefinition.runners"))
    )

    runners = (
        raw_df.select(F.explode(F.col("runners")).alias("runner"))
        .select(F.col("runner.*"))
        .select(F.col("id"), F.col("name"))
        .distinct()
    )

    save_table(spark, runners, f"{namespace}.runner")


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
