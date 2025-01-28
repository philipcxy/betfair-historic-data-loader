import argparse
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    start_date = datetime.date(2015, 1, 1)
    end_date = datetime.date(2030, 1, 1)

    schema = T.StructType(
        [
            T.StructField("start_date", T.TimestampType(), True),
            T.StructField("end_date", T.TimestampType(), True),
        ]
    )

    dates_df = spark.createDataFrame([(start_date, end_date)], schema)

    dates_df = dates_df.select(
        F.explode(
            F.sequence("start_date", "end_date", F.expr("interval 1 minute"))
        ).alias("datetime")
    )

    time_df = (
        dates_df.withColumn(
            "time_key", F.date_format(F.col("datetime"), "HHmm").cast(T.IntegerType())
        )
        .withColumn(
            "hour", F.date_format(F.col("datetime"), "HH").cast(T.IntegerType())
        )
        .withColumn(
            "minute", F.date_format(F.col("datetime"), "mm").cast(T.IntegerType())
        )
        .withColumn("time", F.date_format(F.col("datetime"), "HH:mm"))
    ).select(
        F.col("time_key"),
        F.col("time"),
        F.col("hour"),
        F.col("minute"),
    )

    save_table(spark, time_df, "soccer.dim_time", mode=WriteMode.APPEND)


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
