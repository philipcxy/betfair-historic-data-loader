import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = spark.table("soccer.raw")

    rc_df = (
        raw_df.where(F.col("rc").isNotNull())
        .select(F.col("pt"), F.col("timestamp"), F.col("rc"), F.col("id"))
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
        .withColumn("batb", F.explode(F.col("batb")))
        .withColumns(
            {
                "position": F.element_at(F.col("batb"), 1).cast(T.IntegerType()),
                "odds": F.element_at(F.col("batb"), 2),
                "amount": F.element_at(F.col("batb"), 3),
                "type": F.lit("b"),
            }
        )
        .drop(F.col("batb"), F.col("batl"), F.col("trd"))
        .withColumnRenamed("id", "runner_id")
    )

    batl_df = (
        rc_df.where(F.col("batl").isNotNull())
        .withColumn("batl", F.explode(F.col("batl")))
        .withColumns(
            {
                "position": F.element_at(F.col("batl"), 1).cast(T.IntegerType()),
                "odds": F.element_at(F.col("batl"), 2),
                "amount": F.element_at(F.col("batl"), 3),
                "type": F.lit("l"),
            }
        )
        .drop(F.col("batb"), F.col("batl"), F.col("trd"))
    )

    odds_df = batb_df.union(batl_df).drop(F.col("trd"))

    save_table(spark, odds_df, "soccer.runner_change", mode=WriteMode.APPEND)


def rewrite_files(namespace: str, branch: str) -> None:
    spark = setup_spark_environment(namespace, branch)
    spark.sql(
        "CALL betting.system.rewrite_data_files(table => 'soccer.runner_change', strategy => 'sort', sort_order => 'market_id DESC NULLS LAST, runner_id DESC NULLS LAST, pt DESC NULLS LAST')"
    )


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
    parser.add_argument(
        "--rerwrite_files",
        type=bool,
        dest="rewrite_files",
        default=False,
        required=False,
    )
    args = parser.parse_args()

    if args.rewrite_files:
        rewrite_files(args.namespace, args.branch)
    else:
        save(args.namespace, args.branch)
