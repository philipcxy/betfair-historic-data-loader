import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)

    window = Window.partitionBy(F.col("market_id"), F.col("id"))
    market_runner_df = (
        flattened_df.select(
            F.col("id").alias("market_id"), F.explode(F.col("runners")).alias("runner")
        )
        .select(F.col("market_id"), F.col("runner.*"))
        .withColumn(
            "winner", F.when(F.col("status") == "WINNER", True).otherwise(False)
        )
        .withColumn("winner", F.max(F.col("winner")).over(window))
        .select(
            F.col("market_id"),
            F.col("id").alias("runner_id"),
            F.col("winner"),
            F.col("sortPriority").alias("sort_priority"),
        )
        .distinct()
    )

    market_runner_df.write.format("iceberg").mode("append").save("market_runner")


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
