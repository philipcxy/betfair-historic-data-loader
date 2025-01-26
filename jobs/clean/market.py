import argparse

from pyspark.sql import SparkSession

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    markets = spark.sql("""
                        SELECT raw.id, 
                            CAST(eventId as INT) as event_id
                            , mt.id as type_id
                            , cast(max(openDate) as timestamp) as scheduled_time
                            , cast(max(marketTime) as timestamp) as start_time
                            , cast(max(settledTime) as timestamp) as settled_time
                            , min(CASE WHEN inPlay == 'true' THEN timestamp ELSE NULL END) as kick_off
                            , numberOfWinners as num_winners
                        FROM soccer.raw
                        INNER JOIN soccer.market_type mt
                        ON raw.marketType = mt.type
                        GROUP BY raw.id, mt.id, eventId, marketType, numberOfWinners
                    """)

    save_table(spark, markets, "soccer.market", WriteMode.APPEND)


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
