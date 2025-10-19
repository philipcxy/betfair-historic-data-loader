from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

from shared.common import save_table, setup_spark_environment


def save():
    spark: SparkSession = setup_spark_environment()
    df_market = (
        spark.read.table("betting_warehouse.clean.market")
        .alias("m")
        .filter((F.col("m.kick_off").isNotNull()))
    )
    df_runner = spark.read.table("betting_warehouse.clean.runner").alias("r")
    df_price = spark.read.table("betting_warehouse.clean.price").alias("p")

    over_under_point_five_goals_market_name = "OVER_UNDER_05"
    correct_score_market_name = "CORRECT_SCORE"
    correct_score_2_market_name = "CORRECT_SCORE2"
    match_odds_market_name = "MATCH_ODDS"

    over_point_five_goals_runner_id = (
        df_runner.filter(F.col("runner_name") == F.lit("Over 0.5 Goals"))
        .select(F.col("runner_id"))
        .collect()[0][0]
    )

    market_runner_df = df_runner.join(
        df_market, (F.col("m.market_id") == F.col("r.market_id")), "inner"
    )
    first_goal_minute = (
        market_runner_df.filter(
            (F.col("m.market_type") == over_under_point_five_goals_market_name)
            & (F.col("r.status") == F.lit("WINNER"))
        )
        .withColumn(
            "first_goal_minute",
            F.when(
                (F.col("r.runner_id") == over_point_five_goals_runner_id),
                F.round(
                    (F.col("m.settled_time") - F.col("m.kick_off")).cast(T.LongType()) / 60,
                    0,
                ).cast(T.IntegerType()),
            ).otherwise(None),
        )
        .select(F.col("m.event_id"), F.col("first_goal_minute"))
    ).alias("fg")

    correct_score = (
        market_runner_df.filter(
            (F.col("m.market_type") == correct_score_market_name)
            & (F.col("r.status") == F.lit("WINNER"))
        )
        .withColumn(
            "final_score_home",
            F.split(F.col("r.runner_name"), " - ").getItem(0).cast("int"),
        )
        .withColumn(
            "final_score_away",
            F.split(F.col("r.runner_name"), " - ").getItem(1).cast("int"),
        )
        .withColumn(
            "match_result",
            F.when(F.col("runner_name").contains("Home Win"), "HOME")
            .when(F.col("runner_name").contains("Away Win"), "AWAY")
            .when(F.col("runner_name").contains("Draw"), "DRAW")
            .otherwise(None),
        )
        .select(
            F.col("m.event_id"),
            F.col("final_score_home"),
            F.col("final_score_away"),
            F.when(
                F.col("final_score_home").isNotNull(),
                F.when(
                    F.col("final_score_home") > F.col("final_score_away"), F.lit("HOME")
                ).otherwise(
                    F.when(
                        F.col("final_score_home") == F.col("final_score_away"),
                        F.lit("DRAW"),
                    ).otherwise(F.lit("AWAY"))
                ),
            )
            .otherwise(F.col("match_result"))
            .alias("match_result"),
        )
    ).alias("cs")

    correct_score_2 = (
        market_runner_df.filter(
            (F.col("m.market_type") == F.lit(correct_score_2_market_name))
            & (F.col("r.status") == F.lit("WINNER"))
            & (F.col("r.runner_name") != F.lit("Any unquoted"))
        )
        .withColumn(
            "final_score_home",
            F.split(F.col("r.runner_name"), " - ").getItem(0).cast("int"),
        )
        .withColumn(
            "final_score_away",
            F.split(F.col("r.runner_name"), " - ").getItem(1).cast("int"),
        )
        .select(
            F.col("m.event_id"),
            F.col("final_score_home"),
            F.col("final_score_away"),
        )
    ).alias("cs2")

    total_goals_df = (
        market_runner_df.filter(
            (F.col("market_type").startswith("OVER_UNDER"))
            & (F.col("runner_name").startswith("Over"))
        )
        .withColumn(
            "threshold", F.regexp_extract(F.col("runner_name"), r"(\d+)\.5", 1).cast("int")
        )
        .groupBy("event_name", "event_id")
        .agg(
            F.max(
                F.when(F.col("status") == "WINNER", F.col("threshold")).otherwise(F.lit(-1))
            ).alias("max_winning")
        )
        .withColumn("total_goals", F.col("max_winning") + 1)
        .select("event_id", "total_goals")
    ).alias("tg")

    price_with_kickoff = (
        df_price.filter((F.col("p.type") == F.lit("b")) & (F.col("p.position") == F.lit(1)))
        .join(
            df_market.filter(F.col("market_type") == match_odds_market_name),
            "market_id",
            "inner",
        )
        .filter(
            (F.col("p.timestamp") <= (F.col("m.kick_off") - F.expr("INTERVAL 30 SECOND")))
        )
    )

    # For each (event_id, sort_priority), take the odds & volume with latest timestamp
    agg_exprs = [
        F.expr("max_by(odds, timestamp)").cast("double").alias("pre_ko_odds"),
        F.expr("max_by(total_volume, timestamp)").cast("double").alias("pre_ko_volume"),
    ]
    # Join price_with_kickoff → runner, skip row_number
    pre_ko_odds_raw = (
        price_with_kickoff.groupBy(
            "event_id", "event_name", "scheduled_time", "market_id", "runner_id", "kick_off"
        )
        .agg(*agg_exprs)
        .join(
            df_runner.select("market_id", "runner_id", "sort_priority"),
            ["market_id", "runner_id"],
            "inner",
        )
        .selectExpr(
            "event_id",
            "event_name",
            "market_id",
            "runner_id",
            "kick_off",
            "scheduled_time",
            "sort_priority",
            "pre_ko_odds",
            "pre_ko_volume",
        )
    )
    pre_ko_odds = (
        pre_ko_odds_raw.groupBy(
            "event_id", "event_name", "market_id", "kick_off", "scheduled_time"
        )
        .pivot("sort_priority", ["1", "2", "3"])
        .agg(
            F.first("pre_ko_odds").alias("pre_ko_odds"),
            F.first("pre_ko_volume").alias("pre_ko_volume"),
            F.first("runner_id").alias("runner_id"),
        )
        .withColumnRenamed("1_pre_ko_odds", "pre_ko_odds_home")
        .withColumnRenamed("2_pre_ko_odds", "pre_ko_odds_away")
        .withColumnRenamed("3_pre_ko_odds", "pre_ko_odds_draw")
        .withColumnRenamed("1_pre_ko_volume", "pre_ko_volume_home")
        .withColumnRenamed("2_pre_ko_volume", "pre_ko_volume_away")
        .withColumnRenamed("3_pre_ko_volume", "pre_ko_volume_draw")
        .withColumnRenamed("1_runner_id", "home_runner_id")
        .withColumnRenamed("2_runner_id", "away_runner_id")
        .withColumnRenamed("3_runner_id", "draw_runner_id")
        .alias("pk")
    )

    event = (
        pre_ko_odds.join(first_goal_minute, "event_id", "left")
        .join(correct_score, "event_id", "left")
        .join(correct_score_2, "event_id", "left")
        .join(total_goals_df, "event_id", "left")
        .select(
            F.col("pk.event_id"),
            F.col("pk.event_name"),
            F.col("pk.kick_off"),
            F.col("pk.scheduled_time"),
            F.col("pk.market_id").alias("match_odds_market_id"),
            F.col("pk.home_runner_id"),
            F.col("pk.away_runner_id"),
            F.col("pk.draw_runner_id"),
            F.col("pk.pre_ko_odds_home"),
            F.col("pk.pre_ko_odds_away"),
            F.col("pk.pre_ko_odds_draw"),
            F.col("pk.pre_ko_volume_home"),
            F.col("pk.pre_ko_volume_away"),
            F.col("pk.pre_ko_volume_draw"),
            F.col("fg.first_goal_minute"),
            F.when(F.col("cs.final_score_home").isNull(), F.col("cs2.final_score_home"))
            .otherwise(F.col("cs.final_score_home"))
            .alias("final_score_home"),
            F.when(F.col("cs.final_score_away").isNull(), F.col("cs.final_score_away"))
            .otherwise(F.col("cs.final_score_away"))
            .alias("final_score_away"),
            F.col("tg.total_goals"),
            F.col("cs.match_result"),
        )
    )

    print(pre_ko_odds.count())
    print(correct_score.count())
    print(first_goal_minute.count())
    print(total_goals_df.count())

    save_table(spark, event, "betting_warehouse.clean.match_statistics")