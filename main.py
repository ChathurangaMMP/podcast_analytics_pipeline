import os
import sys
import shutil
from src.logger import logger
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.ingest_bronze import BronzeIngestor
from src.process_silver import SilverTransformer
from src.process_gold import GoldAggregator

def clean_local_environment():
    """
    Completely removes the local spark-warehouse and metastore files.
    This guarantees a perfectly clean slate for local testing and prevents 
    Delta directory collision errors on repeated runs.
    """
    logger.warning("Cleaning local Spark environment for a fresh run.")
    
    # Paths to clean
    paths_to_delete = [
        os.path.join(os.getcwd(), "spark-warehouse"),
        os.path.join(os.getcwd(), "metastore_db")
    ]
    
    for path in paths_to_delete:
        if os.path.exists(path):
            logger.info(f"Deleting: {path}")
            shutil.rmtree(path, ignore_errors=True)

  
    derby_log = os.path.join(os.getcwd(), "derby.log")
    if os.path.exists(derby_log):
        try:
            os.remove(derby_log)
        except OSError:
            pass


def create_spark_session() -> SparkSession:
    """
    Create the main application SparkSession with Delta Lake support.
    """
    python_executable = sys.executable
    
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse").replace("\\", "/")

    builder = (
        SparkSession.builder
        .appName("PodcastAnalyticsPipeline")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.ui.enabled", "true")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def initialize_databases(spark: SparkSession):
    """
    Create the application databases if they don't already exist.
    """
    logger.info("Initializing application databases: bronze, silver, gold")
    databases = ["bronze", "silver", "gold"]

    for db in databases:
        logger.info(f"Ensuring database exists: {db}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def initialize_schema(spark: SparkSession, sql_file_path: str):
    """
    Reads a SQL DDL file and executes each semicolon-separated statement.
    """
    logger.info(f"Initializing schema from {sql_file_path}")

    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_content = file.read()

    sql_commands = [command.strip() for command in sql_content.split(";") if command.strip()]

    for i, command in enumerate(sql_commands, start=1):
        try:
            logger.info(f"Executing schema statement {i}")
            spark.sql(command)
        except Exception:
            logger.exception(f"Failed to execute schema statement {i}:\n{command}")
            raise


def run_analytical_queries(spark: SparkSession):
    """
    Executes and prints the results of the final analytical queries for Part 3.
    """
    logger.info("Executing final analytical queries.")

    print("\n--- Top Episodes by Completions ---")
    spark.sql("""
        SELECT 
            episode_id, 
            episode_title, 
            SUM(complete_count) AS total_completions,
            SUM(play_count) AS total_plays
        FROM gold.gold_episode_daily
        WHERE event_date >= (
            SELECT date_sub(MAX(event_date), 7) 
            FROM gold.gold_episode_daily
        )
        GROUP BY 
            episode_id, 
            episode_title
        ORDER BY 
            total_completions DESC
        LIMIT 10;
    """).show(truncate=False)

    print("\n--- Top Countries by Listen-Through Rate ---")
    spark.sql("""
        SELECT 
            country, 
            ROUND(avg_listen_through_rate * 100, 2) AS avg_ltr_percentage,
            completions AS total_completed_episodes
        FROM gold.gold_country_ltr
        WHERE completions > 0 -- Adjusted threshold for small sample datasets
        ORDER BY avg_ltr_percentage DESC
    """).show(truncate=False)

    print("\n--- Top Super Listeners ---")
    spark.sql("""
        WITH DailyUserListening AS (
            SELECT 
                user_id,
                event_date,
                COUNT(DISTINCT episode_id) AS distinct_episodes_listened
            FROM silver.silver_events
            WHERE event_type IN ('play', 'complete') 
            GROUP BY 
                user_id, 
                event_date
            HAVING COUNT(DISTINCT episode_id) >= 3
        )
        SELECT 
            COUNT(DISTINCT user_id) AS super_users_count
        FROM DailyUserListening
    """).show(truncate=False)


def main():
    logger.info("Starting Podcast Analytics Pipeline")
    clean_local_environment()
    spark = create_spark_session()

    try:
        spark.sparkContext.setLogLevel("ERROR")

        # Environment & DB Setup
        initialize_databases(spark)
        
        schema_path = "ddl/bronze_schema.sql"
        if os.path.exists(schema_path):
            initialize_schema(spark, schema_path)
        else:
            logger.warning(f"Schema file not found at {schema_path}.")

        # Bronze Layer
        logger.info("--- Phase 1: Bronze Ingestion ---")
        bronze_ingestor = BronzeIngestor(spark=spark, source_dir="data", target_db="bronze")
        
        bronze_ingestor.ingest_csv("raw/users.csv", "bronze_users")
        bronze_ingestor.ingest_csv("raw/episodes.csv", "bronze_episodes")
        bronze_ingestor.ingest_events("raw/event_logs.json", "bronze_events")

        users_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_users").first()[0]
        episodes_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_episodes").first()[0]
        events_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_events").first()[0]
        logger.info(f"Bronze layer row counts - Users: {users_count}, Episodes: {episodes_count}, Events: {events_count}")

        # Silver Layer
        logger.info("--- Phase 2: Silver Transformation ---")
        silver_transformer = SilverTransformer(spark=spark, source_db="bronze", target_db="silver")
        silver_transformer.process_events_obt()
        
        silver_count = spark.sql("SELECT COUNT(*) FROM silver.silver_events").first()[0]
        rejected_count = spark.sql("SELECT COUNT(*) FROM silver.silver_events_rejected").first()[0]
        logger.info(f"Silver layer row counts - Valid Events: {silver_count}, Quarantined Events: {rejected_count}")

        # Gold Layer
        logger.info("--- Phase 3: Gold Aggregation ---")
        gold_aggregator = GoldAggregator(spark=spark, source_db="silver", target_db="gold")
        
        gold_aggregator.build_gold_episode_daily()
        gold_aggregator.build_gold_user_engagement()
        gold_aggregator.build_gold_country_ltr()
        logger.info("Gold aggregations completed successfully.")

        # Analytics Queries (Part 3 Deliverable)
        logger.info("--- Phase 4: Business Analytics ---")
        run_analytical_queries(spark)

    except Exception as e:
        logger.exception(f"Podcast Analytics Pipeline failed: {e}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()