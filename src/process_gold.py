from .logger import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, expr, approx_count_distinct, when

class GoldAggregator:
    def __init__(self, spark: SparkSession, source_db: str = "silver", target_db: str = "gold"):
        self.spark = spark
        self.source_db = source_db
        self.target_db = target_db

    def build_gold_episode_daily(self):
        """
        Builds daily play and complete counts per episode.
        """

        logger.info("Building gold_episode_daily.")
        events_df = self.spark.table(f"{self.source_db}.silver_events")
        
        gold_df = events_df.groupBy("episode_id", "episode_title", "event_date").agg(
            sum(when(col("event_type") == "play", 1).otherwise(0)).cast("bigint").alias("play_count"),
            sum(when(col("event_type") == "complete", 1).otherwise(0)).cast("bigint").alias("complete_count"),
            avg("duration").alias("avg_duration_seconds")
        )
        
        gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("event_date") \
            .saveAsTable(f"{self.target_db}.gold_episode_daily")

    def build_gold_user_engagement(self):
        """
        Builds per-user listening summary.
        """

        logger.info("Building gold_user_engagement.")
        events_df = self.spark.table(f"{self.source_db}.silver_events")
        
        gold_df = events_df.groupBy("user_id", "country").agg(
            sum(when(col("event_type").isin("play", "complete"), 1).otherwise(0)).cast("bigint").alias("listen_events"),
            avg("duration").alias("avg_session_duration_seconds"),
            approx_count_distinct("episode_id").alias("distinct_episodes")
        )
        
        gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{self.target_db}.gold_user_engagement")

    def build_gold_country_ltr(self):
        """
        Calculates the average listen-through rate by country.
        """

        logger.info("Building gold_country_ltr.")
        events_df = self.spark.table(f"{self.source_db}.silver_events")
        
        # Isolate complete events and ensure we avoid division by zero
        completed_events = events_df.filter(
            (col("event_type") == "complete") & 
            (col("episode_duration_seconds") > 0) &
            (col("duration").isNotNull())
        )
        
        # Calculate LTR (completion_duration / episode_duration)
        ltr_df = completed_events.withColumn("listen_through_rate", col("duration") / col("episode_duration_seconds")
        )
        
        gold_df = ltr_df.groupBy("country").agg(
            avg("listen_through_rate").alias("avg_listen_through_rate"),
            count("episode_id").alias("completions")
        )
        
        gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{self.target_db}.gold_country_ltr")