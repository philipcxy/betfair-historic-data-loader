import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    """
    TODO: Needs to be updated to use MERG INTO
    """
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)

    runners = (
        flattened_df.select(F.explode(F.col("runners")).alias("runners"))
        .select(F.col("runners.*"))
        .select(F.col("id"), F.col("name"))
        .distinct()
    )

    runners.write.format("iceberg").mode("append").save("runner")


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
