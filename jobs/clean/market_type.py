import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)
    old_market_type = spark.read.table("market_type").alias("old")
    w = Window.partitionBy().rowsBetween(Window.unboundedPreceding, Window.currentRow)
    market_type = (
        flattened_df.select(F.col("marketType").alias("type"))
            .alias("new")
            .distinct()
            .join(old_market_type, F.col("old.type") == F.col("new.type"), how="left")
            .orderBy(F.col("old.id").asc_nulls_last())
            .withColumn(
                "new_id", F.sum(F.when(F.col("old.id") == F.lit(0), 0).otherwise(1)).over(w)
            )
            .filter(F.isnull(F.col("old.id")))
            .select(F.col("new_id").alias("id"), F.col("new.type").alias("type"))
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
