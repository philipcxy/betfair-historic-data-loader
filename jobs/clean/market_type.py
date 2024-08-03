import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)
    market_type = (
        flattened_df.select(F.col("marketType").alias("type"))
        .distinct()
        .withColumn("id", F.monotonically_increasing_id())
        .select(F.col("id"), F.col("type"))
    )

    market_type.write.format("iceberg").mode("append").save("market_type")


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
