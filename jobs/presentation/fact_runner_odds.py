import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)
    rc_df = spark.read.table("soccer.runner_change").alias("rc")
    market_df = (
        spark.read.table("soccer.market")
        .select(F.col("id").alias("market_id"), F.col("kick_off"))
        .alias("m")
    )
    dim_runner = (
        spark.read.table("soccer.dim_runner")
        .select(F.col("id"), F.col("market_id"), F.col("runner_id"))
        .alias("dr")
    )

    window = Window.partitionBy(
        F.col("rc.market_id"), F.col("rc.runner_id"), F.col("type")
    ).orderBy(F.col("pt"))
    rc_df = (
        rc_df.groupBy(
            F.col("rc.market_id"),
            F.col("rc.runner_id"),
            F.col("rc.pt"),
            F.col("rc.hc"),
            F.col("rc.ltp"),
            F.col("rc.tv"),
            F.col("rc.type"),
        )
        .pivot("rc.position", ["0", "1", "2"])
        .agg(F.first(F.col("odds")), F.first(F.col("amount")))
        .withColumnRenamed("0_first(odds)", "first_odds")
        .withColumnRenamed("1_first(odds)", "second_odds")
        .withColumnRenamed("2_first(odds)", "third_odds")
        .withColumnRenamed("0_first(amount)", "first_amount")
        .withColumnRenamed("1_first(amount)", "second_amount")
        .withColumnRenamed("2_first(amount)", "third_amount")
        .join(
            F.broadcast(market_df),
            F.col("m.market_id") == F.col("rc.market_id"),
            "inner",
        )
        .withColumn(
            "minute",
            F.round(
                (F.col("rc.pt") - (F.unix_timestamp(F.col("m.kick_off")) * 1000))
                / 60000
            ).cast(T.IntegerType()),
        )
        .join(
            F.broadcast(dim_runner),
            (F.col("dr.market_id") == F.col("rc.market_id"))
            & (F.col("dr.runner_id") == F.col("rc.runner_id")),
            "inner",
        )
        .withColumn("first_odds", F.last("first_odds", ignorenulls=True).over(window))
        .withColumn("second_odds", F.last("second_odds", ignorenulls=True).over(window))
        .withColumn("third_odds", F.last("third_odds", ignorenulls=True).over(window))
        .withColumn(
            "first_amount", F.last("first_amount", ignorenulls=True).over(window)
        )
        .withColumn(
            "second_amount", F.last("second_amount", ignorenulls=True).over(window)
        )
        .withColumn(
            "third_amount", F.last("third_amount", ignorenulls=True).over(window)
        )
        .withColumn("runner_key", F.col("dr.id"))
        .drop(
            F.col("m.market_id"),
            F.col("m.runner_id"),
            F.col("m.kick_off"),
            F.col("dr.id"),
            F.col("dr.market_id"),
            F.col("dr.runner_id"),
        )
    )

    save_table(spark, rc_df, "soccer.fact_runner_odds", mode=WriteMode.APPEND)


def rewrite_files(namespace: str, branch: str) -> None:
    spark = setup_spark_environment(namespace, branch)
    spark.sql(
        "CALL betting.system.rewrite_data_files(table => 'soccer.fact_runner_odds', strategy => 'sort', sort_order => 'market_id DESC NULLS LAST, runner_id DESC NULLS LAST, pt DESC NULLS LAST')"
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
        "--rewrite_files",
        type=bool,
        dest="rewrite_files",
        default=False,
        required=False,
    )
    args = parser.parse_args()

    if args.rewrite_files:
        rewrite_files(args.namespace, args.branch)

    save(args.namespace, args.branch)
