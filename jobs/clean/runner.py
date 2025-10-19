import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    """
    TODO: Needs to be updated to use MERG INTO
    """
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting_warehouse.landing.raw")
            .filter(F.col("mc.marketDefinition").isNotNull())
            .select(
                F.col("mc.marketDefinition.runners"),
                F.col("mc.id").alias("market_id"),
                F.col("mc.marketDefinition.name").alias("market_name"),
                F.col("pt"),
            )
        )

    runners_exploded = raw_df.select(
        F.explode(F.col("runners")).alias("runner"),
        F.col("market_id"),
        F.col("market_name"),
        F.col("pt"),
    ).select(F.col("runner.*"), F.col("market_id"), F.col("market_name"), F.col("pt"))

    window = (
        Window.partitionBy("market_id", "market_name", "id", "name")
        .orderBy(F.col("pt").asc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    runners = (
        runners_exploded.withColumn("last_status", F.last("status").over(window))
        .drop("status")
        .withColumnRenamed("last_status", "status")
        .dropDuplicates(
            ["market_id", "market_name", "id", "name"]
        )  # keep one row per runner
        .select(
            "market_id",
            "market_name",
            F.col("id").alias("runner_id"),
            F.col("name").alias("runner_name"),
            "status",
            F.col("sortPriority").alias("sort_priority"),
        )
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
