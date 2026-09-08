from .logger import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, expr, to_date

class SilverTransformer:
    def __init__(self, spark: SparkSession, source_db: str = "bronze", target_db: str = "silver"):
        self.spark = spark
        self.source_db = source_db
        self.target_db = target_db

    def process_events_obt(self):
        """
        Validates, deduplicates, and enriches event logs into a single wide Silver table.
        Failed DQ records are routed to the quarantine table.
        """
        logger.info("Starting Silver OBT processing...")
        
        # Read Bronze tables
        bronze_events = self.spark.table(f"{self.source_db}.bronze_events")
        bronze_users = self.spark.table(f"{self.source_db}.bronze_users")
        bronze_episodes = self.spark.table(f"{self.source_db}.bronze_episodes")
        
        # Parse timestamps on events
        parsed_events = bronze_events.withColumn(
            "event_ts", expr(r"try_to_timestamp(timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss')")
        )


        # Apply data quality rules
        valid_event_types = ["play", "pause", "seek", "complete"]
        
        is_valid_event = (col("user_id").isNotNull() & col("episode_id").isNotNull() &
            col("event_type").isin(valid_event_types) & col("event_ts").isNotNull()
        )
        
        valid_events = parsed_events.filter(is_valid_event)
        rejected_events = parsed_events.filter(~is_valid_event)
        
        # Route rejected records to quarantine
        rejected_events.select(
            col("event_type"), col("user_id"), col("episode_id"),
            col("timestamp"), col("duration"), col("event_ts"),
            col("_ingested_at").alias("_ingest_ts"),
            col("_input_file_name").alias("_source_file")
        ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{self.target_db}.silver_events_rejected")
        
        # Clean and deduplicate valid events
        cleaned_events = valid_events.withColumn("event_date", to_date(col("event_ts"))).withColumn(
            "duration", col("duration").cast("int")).dropDuplicates(["user_id", "episode_id", "event_type", "event_ts"])
        
        # Clean reference data on-the-fly
        clean_users = bronze_users.select(
            col("user_id"),
            to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
            col("country")
        ).dropDuplicates(["user_id"])
        
        clean_episodes = bronze_episodes.select(
            col("episode_id"),
            col("podcast_id"),
            col("title").alias("episode_title"), 
            to_date(col("release_date"), "yyyy-MM-dd").alias("release_date"),
            col("duration_seconds").cast("int").alias("episode_duration_seconds") 
        ).dropDuplicates(["episode_id"])
        
        # Enrich via left Joins 
        enriched_df = cleaned_events \
            .join(clean_users, "user_id", "left") \
            .join(clean_episodes, "episode_id", "left") \
            .select(
                cleaned_events["user_id"],
                cleaned_events["episode_id"],
                clean_episodes["podcast_id"],
                cleaned_events["event_type"],
                cleaned_events["event_ts"],
                cleaned_events["event_date"],
                cleaned_events["duration"],
                clean_users["country"],
                clean_users["signup_date"],
                clean_episodes["episode_title"],
                clean_episodes["release_date"],
                clean_episodes["episode_duration_seconds"],
                cleaned_events["_ingested_at"].alias("_ingest_ts"),
                cleaned_events["_input_file_name"].alias("_source_file")
            )
            
        # Write to silver table partitioned by event_date
        enriched_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("event_date") \
            .saveAsTable(f"{self.target_db}.silver_events")
            
        logger.info("Silver OBT events processed and loaded successfully.")