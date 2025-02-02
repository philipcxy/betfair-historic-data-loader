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
            T.StructField("start_date", T.DateType(), True),
            T.StructField("end_date", T.DateType(), True),
        ]
    )

    dates_df = spark.createDataFrame([(start_date, end_date)], schema)

    dates_df = dates_df.select(
        F.explode(F.sequence("start_date", "end_date", F.expr("interval 1 day"))).alias(
            "date"
        )
    )

    dates_df = (
        dates_df.withColumn(
            "date_key",
            F.year(F.col("date")) * 10000
            + F.month(F.col("date")) * 100
            + F.dayofmonth(F.col("date")),
        )
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("calendar_month", F.date_format(F.col("date"), "MMM"))
        .withColumn("day", F.dayofmonth(F.col("date")))
        .withColumn("calendar_day", F.date_format(F.col("date"), "EEE"))
        .withColumn("day_of_week", F.dayofweek(F.col("date")))
    ).select(
        F.col("date_key"),
        F.col("date"),
        F.col("year"),
        F.col("month"),
        F.col("calendar_month"),
        F.col("day"),
        F.col("day_of_week"),
        F.col("calendar_day"),
    )

    save_table(spark, dates_df, f"{namespace}.dim_date", mode=WriteMode.APPEND)


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
