import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    df = spark.read.table("raw")

    rc_df = (
        df.where(F.col("mc.rc").isNotNull())
        .select(F.col("pt"), F.col("timestamp"), F.col("mc.*"))
        .withColumn("rc", F.explode(F.col("rc")))
        .select(
            F.col("id").alias("market_id"),
            F.col("pt"),
            F.col("timestamp"),
            F.col("rc.*"),
        )
    )

    batb_df = (
        rc_df.where(F.col("batb").isNotNull())
        .withColumn("batb", F.explode("batb"))
        .withColumn("position", F.element_at(F.col("batb"), 1).cast(T.IntegerType()))
        .withColumn("odds", F.element_at(F.col("batb"), 2))
        .withColumn("amount", F.element_at(F.col("batb"), 3))
        .withColumn("type", F.lit("b"))
        .drop(F.col("batb"), F.col("batl"), F.col("trd"))
        .withColumnRenamed("id", "runner_id")
    )

    batl_df = (
        rc_df.where(F.col("batl").isNotNull())
        .withColumn("batl", F.explode("batl"))
        .withColumn("position", F.element_at(F.col("batl"), 1).cast(T.IntegerType()))
        .withColumn("odds", F.element_at(F.col("batl"), 2))
        .withColumn("amount", F.element_at(F.col("batl"), 3))
        .withColumn("type", F.lit("l"))
        .drop(F.col("batb"), F.col("batl"), F.col("trd"))
    )

    odds_df = batb_df.union(batl_df).drop(F.col("trd"))
    odds_df.write.format("iceberg").mode("append").save("runner_change")


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
