import pytest
from datetime import date
from src.process_gold import GoldAggregator

def test_gold_aggregations(spark):
    source_db = "test_silver_db"
    target_db = "test_gold_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {source_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

    # Mock Silver OBT events
    silver_data = [
        ("U1", "E1", "play", date(2023, 10, 1), 500, "UK", 1000, "Ep 1"),
        ("U1", "E1", "complete", date(2023, 10, 1), 1000, "UK", 1000, "Ep 1"),
        ("U2", "E2", "play", date(2023, 10, 1), 200, "USA", 2000, "Ep 2"),
    ]
    spark.createDataFrame(
        silver_data, 
        ["user_id", "episode_id", "event_type", "event_date", "duration", "country", "episode_duration_seconds", "episode_title"]
    ).write.format("delta").mode("overwrite").saveAsTable(f"{source_db}.silver_events")

    aggregator = GoldAggregator(spark=spark, source_db=source_db, target_db=target_db)
    
    # Test episode daily aggregation
    aggregator.build_gold_episode_daily()
    ep_daily = spark.table(f"{target_db}.gold_episode_daily").filter("episode_id = 'E1'").first()
    assert ep_daily["play_count"] == 1
    assert ep_daily["complete_count"] == 1
    
    # Test country listen-through rate (LTR)
    aggregator.build_gold_country_ltr()
    ltr_df = spark.table(f"{target_db}.gold_country_ltr").filter("country = 'UK'").first()
    assert ltr_df["avg_listen_through_rate"] == 1.0  # 1000 duration / 1000 episode length
    
    # Test user engagement summary
    aggregator.build_gold_user_engagement()
    user_df = spark.table(f"{target_db}.gold_user_engagement").filter("user_id = 'U1'").first()
    assert user_df["listen_events"] == 2
    assert user_df["distinct_episodes"] == 1