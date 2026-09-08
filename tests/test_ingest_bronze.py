from src.ingest_bronze import append_metadata_and_cast
from src.ingest_bronze import BronzeIngestor

def test_append_bronze_metadata_casts_types(spark):
    # Create dummy data with mixed types
    data = [
        (1, "Podcast A", 45.5),
        (2, "Podcast B", 30.0),
        (3, "Podcast C", 60.0)
    ]
    columns = ["id", "title", "duration_minutes"]
    input_df = spark.createDataFrame(data, columns)
    
    # Run the transformation
    result_df = append_metadata_and_cast(input_df)
    
    # Verify column types are all cast to strings
    schema_dict = dict(result_df.dtypes)
    assert schema_dict["id"] == "string"
    assert schema_dict["duration_minutes"] == "string"
    
    # Verify metadata columns were added
    assert "_input_file_name" in result_df.columns
    assert "_ingested_at" in result_df.columns
    assert schema_dict["_ingested_at"] == "timestamp"


def test_bronze_ingestor_writes_delta_table(spark):
    db_name = "test_bronze_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    
    ingestor = BronzeIngestor(spark=spark, target_db=db_name, source_dir=str("data"))
    
    # Execute the ingestion method
    ingestor.ingest_csv("raw/users.csv", "test_bronze_users")
    
    # Verify the Delta table exists and contains records
    result_df = spark.sql(f"SELECT * FROM {db_name}.test_bronze_users")
    assert result_df.count() == 100
    assert "user_id" in result_df.columns