import pytest
from src.ingest_bronze import append_metadata_and_cast, BronzeIngestor

def test_append_bronze_metadata_casts_types(spark):
    data = [
        (1, "Podcast A", 45.5),
        (2, "Podcast B", 30.0),
        (3, "Podcast C", 60.0)
    ]
    columns = ["id", "title", "duration_minutes"]
    input_df = spark.createDataFrame(data, columns)
    
    result_df = append_metadata_and_cast(input_df)
    
    schema_dict = dict(result_df.dtypes)
    assert schema_dict["id"] == "string"
    assert schema_dict["duration_minutes"] == "string"
    assert "_input_file_name" in result_df.columns
    assert "_ingested_at" in result_df.columns
    assert schema_dict["_ingested_at"] == "timestamp"

def test_bronze_ingestor_writes_delta_table(spark, tmp_path):
    db_name = "test_bronze_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    
    # Create a dummy CSV file dynamically in an isolated temp directory
    dummy_dir = tmp_path / "raw"
    dummy_dir.mkdir()
    dummy_file = dummy_dir / "dummy_users.csv"
    dummy_file.write_text("user_id,country,signup_date\nU1,UK,2023-01-01\nU2,USA,2023-01-02")
    
    ingestor = BronzeIngestor(spark=spark, target_db=db_name, source_dir=str(tmp_path))
    
    ingestor.ingest_csv("raw/dummy_users.csv", "test_bronze_users")
    
    result_df = spark.sql(f"SELECT * FROM {db_name}.test_bronze_users")
    assert result_df.count() == 2
    assert "country" in result_df.columns