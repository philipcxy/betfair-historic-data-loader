import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    runner = spark.read.table("betting.clean.runner").alias("r")
    runner_change = spark.read.table("betting.clean.runner_change").alias("rc")
    market_runner = spark.read.table("betting.clean.market_runner").alias("mr")
    market = spark.read.table("betting.clean.market").alias("m")
    market_type = spark.read.table("betting.clean.market_type").alias("mt")
    event = spark.read.table("betting.clean.event").alias("e")

    window = (
        Window.partitionBy(F.col("rc.market_id"), F.col("rc.runner_id"))
        .orderBy(F.col("epoch"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    runner_change_pre_ko_df = (
        runner_change.join(
            F.broadcast(market), F.col("m.id") == F.col("rc.market_id"), how="inner"
        )
        .withColumn("total_traded_volume", F.last(F.col("rc.tv")).over(window))
        .filter(F.col("rc.timestamp") < F.col("kick_off"))
        .groupBy(F.col("market_id"), F.col("runner_id"))
        .agg(
            F.last(F.col("rc.tv")).alias("pre_ko_traded_volume"),
            F.last(F.col("total_traded_volume")).alias("total_traded_volume"),
        )
    ).alias("rc_pko_volume")

    favourite_window = Window.partitionBy(F.col("rc.market_id")).orderBy(
        F.col("ltp"), F.col("odds")
    )
    odds_window = Window.partitionBy(
        F.col("rc.market_id"), F.col("rc.runner_id")
    ).orderBy(F.desc(F.col("rc.epoch")))

    runner_change_ko_odds_df = (
        runner_change.join(
            F.broadcast(market), F.col("m.id") == F.col("rc.market_id"), how="inner"
        )
        .filter((F.col("rc.timestamp") < F.col("kick_off")) & (F.col("rc.type") == "b"))
        .withColumn("odds_rank", F.row_number().over(odds_window))
        .filter(F.col("odds_rank") == 1)
        .withColumn("favourite_rank", F.row_number().over(favourite_window))
        .withColumn(
            "favourite", F.when(F.col("favourite_rank") == 1, True).otherwise(False)
        )
        .select(
            F.col("market_id"),
            F.col("runner_id"),
            F.col("rc.ltp").alias("ko_odds"),
            F.col("favourite"),
        )
        .distinct()
    ).alias("rc_pko_odds")

    dim_runner = (
        runner.join(market_runner, F.col("r.id") == F.col("mr.runner_id"))
        .join(market, F.col("mr.market_id") == F.col("m.id"))
        .join(market_type, F.col("m.type_id") == F.col("mt.id"))
        .join(event, F.col("m.event_id") == F.col("e.id"))
        .join(
            runner_change_pre_ko_df,
            (F.col("mr.market_id") == F.col("rc_pko_volume.market_id"))
            & (F.col("mr.runner_id") == F.col("rc_pko_volume.runner_id")),
        )
        .join(
            runner_change_ko_odds_df,
            (F.col("mr.market_id") == F.col("rc_pko_odds.market_id"))
            & (F.col("mr.runner_id") == F.col("rc_pko_odds.runner_id")),
        )
    )

    dim_runner = (
        dim_runner.withColumn(
            "scheduled_date_key",
            F.year(F.col("m.scheduled_time")) * 10000
            + F.month(F.col("m.scheduled_time")) * 100
            + F.dayofmonth(F.col("m.scheduled_time")),
        )
        .withColumn(
            "scheduled_time_key",
            F.date_format(F.col("m.scheduled_time"), "HHmm").cast(T.IntegerType()),
        )
        .withColumn(
            "kick_off_date_key",
            F.year(F.col("m.kick_off")) * 10000
            + F.month(F.col("m.kick_off")) * 100
            + F.dayofmonth(F.col("m.kick_off")),
        )
        .withColumn(
            "kick_off_time_key",
            F.date_format(F.col("m.kick_off"), "HHmm").cast(T.IntegerType()),
        )
        .select(
            F.monotonically_increasing_id().alias("id"),
            F.col("r.id").alias("runner_id"),
            F.col("r.name"),
            F.col("mr.sort_priority"),
            F.col("mr.winner"),
            F.col("m.id").alias("market_id"),
            F.col("mt.id").alias("market_type_id"),
            F.col("mt.type").alias("market_name"),
            F.col("e.id").alias("event_id"),
            F.col("e.name").alias("event_name"),
            F.col("scheduled_date_key"),
            F.col("scheduled_time_key"),
            F.col("kick_off_date_key"),
            F.col("kick_off_time_key"),
            F.col("e.country_code"),
            F.col("rc_pko_volume.pre_ko_traded_volume"),
            F.col("rc_pko_odds.ko_odds"),
            F.col("rc_pko_odds.favourite"),
            F.col("total_traded_volume"),
        )
        .distinct()
    )

    save_table(spark, dim_runner, f"{namespace}.dim_runner", mode=WriteMode.APPEND)


def rewrite_files(namespace: str, branch: str) -> None:
    spark = setup_spark_environment(namespace, branch)
    spark.sql(
        f"CALL betting.system.rewrite_data_files(table => '{namespace}.dim_runner', strategy => 'sort', sort_order => 'id NULLS LAST, market_id NULLS last')"
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
    else:
        save(args.namespace, args.branch)
