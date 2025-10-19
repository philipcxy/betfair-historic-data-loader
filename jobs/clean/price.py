
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)
    raw_df = (
        spark.table("betting_warehouse.landing.raw")
            .filter(F.col("timestamp") >= F.to_timestamp(F.lit("2025-01-01")))
            .filter(F.col("mc.rc").isNotNull())
            .withColumn("rc", F.explode("mc.rc"))
            .select(
                F.col("mc.id").alias("market_id"),
                F.col("pt").alias("epoch"),
                F.col("timestamp"),
                F.col("mc.rc").alias("rc"),
                F.col("rc.id").alias("runner_id"),
                F.col("rc.hc").cast(T.DoubleType()).alias("handicap"),
                F.col("rc.tv").cast(T.DoubleType()).alias("total_volume"),
                F.col("rc.batb").alias("batb"),
                F.col("rc.batl").alias("batl"),
            )
    )

    batb_df = (
        raw_df.where(F.col("batb").isNotNull())
        .withColumn("batb_entry", F.explode("batb"))
        .select(
            "epoch",
            "timestamp",
            "market_id",
            "runner_id",
            "handicap",
            F.element_at("batb_entry", 1).cast(T.IntegerType()).alias("position"),
            F.element_at("batb_entry", 2).alias("odds"),
            F.element_at("batb_entry", 3).alias("amount"),
            F.lit("b").alias("type"),
            "total_volume",
        )
    )

    batl_df = (
        raw_df.where(F.col("batl").isNotNull())
        .withColumn("batl_entry", F.explode("batl"))
        .select(
            "epoch",
            "timestamp",
            "market_id",
            "runner_id",
            "handicap",
            F.element_at("batl_entry", 1).cast(T.IntegerType()).alias("position"),
            F.element_at("batl_entry", 2).alias("odds"),
            F.element_at("batl_entry", 3).alias("amount"),
            F.lit("l").alias("type"),
            "total_volume",
        )
    )

    odds_df = batb_df.union(batl_df)

    save_table(spark, odds_df, "betting_warehouse.clean.price")
