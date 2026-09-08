from logger import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, col


def append_metadata_and_cast(df: DataFrame) -> DataFrame:
    """
    Casts all columns to string type to prevent ingestion crashes,
    and appends Bronze layer metadata for traceability.
    """
    # Cast all incoming columns to string
    for column_name in df.columns:
        df = df.withColumn(column_name, col(column_name).cast("string"))
    
    # Append auditing metadata
    return df.withColumn("_input_file_name", input_file_name()) \
             .withColumn("_ingested_at", current_timestamp())

class BronzeIngestor:
    def __init__(self, spark: SparkSession, source_dir: str, target_db: str):
        self.spark = spark
        self.source_dir = source_dir
        self.target_db = target_db

    def ingest_csv(self, file_name: str, table_name: str):
        """
        Ingests raw CSV files(users and episodes) into the Bronze layer.
        """

        file_path = f"{self.source_dir}/{file_name}"
        logger.info(f"Starting ingestion for {table_name} from {file_path}")
        try:
            raw_df = self.spark.read.csv(file_path, header=True, inferSchema=False)
            bronze_df = append_metadata_and_cast(raw_df)
            
            bronze_df.write.format("delta").mode("append").saveAsTable(table_name)
                
            logger.info(f"Successfully ingested {file_path} into {table_name}")
        except Exception as e:
            logger.error(f"Failed to ingest {table_name}: {str(e)}")
            raise

    def ingest_events(self, file_name: str, table_name: str):
        """
        Ingests raw JSON event logs into the Bronze layer.
        """

        file_path = f"{self.source_dir}/{file_name}"
        logger.info(f"Starting ingestion for {table_name} from {file_path}")
        try:
            raw_df = self.spark.read.json(file_path)
            bronze_df = append_metadata_and_cast(raw_df)
            
            bronze_df.write.format("delta").mode("append").saveAsTable(table_name)
                
            logger.info(f"Successfully ingested {file_path} into {table_name}")
        except Exception as e:
            logger.error(f"Failed to ingest {table_name}: {str(e)}")
            raise

