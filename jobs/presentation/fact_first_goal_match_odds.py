import argparse

from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def load_data_to_table(namespace: str, branch: str) -> None:
    spark = setup_spark_environment(namespace, branch)

    df = spark.sql("""
        WITH filtered_markets as (
            SELECT *
            FROM soccer.dim_runner r
            WHERE r.market_type_id = 34
        )
        SELECT f.name,
            CAST(from_unixtime(first(fe.pt/1000)) AS date) as timestamp,
            f.event_name,
            f.runner_id,
            f.market_id,
            f.pre_ko_traded_volume,
            f.ko_odds,
            f.favourite,
            f.sort_priority,
            f.winner,
            f.country_code,
            min(fe.first_odds) as first_odds,
            min(fe.second_odds) as second_odds,
            min(fe.third_odds) as third_odds,
            e.first_goal_minute
        FROM filtered_markets f
        INNER JOIN soccer.fact_runner_odds fe
            ON f.id = fe.runner_key
        INNER JOIN soccer.dim_first_goal e
            ON e.event_id = f.event_id
        WHERE fe.minute between e.first_goal_minute AND e.first_goal_minute + 2
        GROUP BY f.name, 
            f.event_name,
            f.runner_id,
            f.market_id, 
            f. pre_ko_traded_volume, 
            f.ko_odds, 
            f.favourite, 
            f.sort_priority, 
            f.winner, 
            f.country_code, 
            e.first_goal_minute
    """)

    save_table(spark, df, "soccer.fact_first_goal_match_odds", mode=WriteMode.REPLACE)


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

    load_data_to_table(args.namespace, args.branch)
