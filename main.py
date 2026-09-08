import os
from src.logger import logger
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.ingest_bronze import BronzeIngestor

def create_spark_session() -> SparkSession:
    """
    Create the main application SparkSession with Delta Lake support.
    """

    # Windows local Spark networking
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

    warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse").replace("\\", "/")

    builder = (
        SparkSession.builder
        .appName("PodcastAnalyticsPipeline")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
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


def main():
    logger.info("Starting Podcast Analytics Pipeline")
    spark = create_spark_session()

    try:
        spark.sparkContext.setLogLevel("ERROR")

        # Create application databases
        initialize_databases(spark)

        # Create Bronze ingestor
        bronze_ingestor = BronzeIngestor(spark=spark, source_dir="data", target_db="bronze")

        # Initialize table schema
        schema_path = "ddl/bronze_schema.sql"

        if os.path.exists(schema_path):
            initialize_schema(spark, schema_path)
        else:
            logger.warning(f"Schema file not found at {schema_path}.")

        # Ingest Bronze data
        bronze_ingestor.ingest_csv("raw/users.csv", "bronze_users")
        bronze_ingestor.ingest_csv("raw/episodes.csv", "bronze_episodes")
        bronze_ingestor.ingest_events("raw/event_logs.json", "bronze_events")

        # Verify ingestion
        users_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_users").first()[0]
        episodes_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_episodes").first()[0]
        events_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_events").first()[0]

        logger.info(f"Bronze layer row counts - Users: {users_count}, Episodes: {episodes_count}, Events: {events_count}")

    except Exception as e:
        logger.exception(f"Podcast Analytics Pipeline failed: {e}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()