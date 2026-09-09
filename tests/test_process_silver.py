import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

from src.process_silver import SilverTransformer


def test_process_events_obt(spark):
    source_db = "test_bronze_db"
    target_db = "test_silver_db"

    spark.sql(f"DROP DATABASE IF EXISTS {source_db} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS {target_db} CASCADE")

    spark.sql(f"CREATE DATABASE {source_db}")
    spark.sql(f"CREATE DATABASE {target_db}")

    spark.createDataFrame(
        [("U1", "2023-01-01", "UK")], ["user_id", "signup_date", "country"]
    ).write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{source_db}.bronze_users")

    spark.createDataFrame(
        [("E1", "P1", "Ep 1", "2023-01-01", "1000")], 
        ["episode_id", "podcast_id", "title", "release_date", "duration_seconds"] 
    ).write.format("delta").mode("overwrite").saveAsTable(f"{source_db}.bronze_episodes")

    events_data = [
        ("play","U1", "E1", "2023-10-01T10:00:00", 500, "f1", "2023-10-01"),
        ("play", "U1", "E1", "2023-10-01T10:00:00", 500, "f1", "2023-10-01"),
        ("fast_forward", "U1", "E1", "2023-10-01T11:00:00", 500, "f1", "2023-10-01"),
        ("play", None, "E1", "2023-10-01T12:00:00", 500, "f1", "2023-10-01")
    ]

    events_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("episode_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("_input_file_name", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ])

    (
        spark.createDataFrame(
            events_data,
            schema=events_schema
        )
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{source_db}.bronze_events")
    )

    transformer = SilverTransformer(spark=spark, source_db=source_db, target_db=target_db)

    transformer.process_events_obt()

    silver_df = spark.table(f"{target_db}.silver_events")

    assert silver_df.count() == 1
    assert silver_df.first()["country"] == "UK"

    rejected_df = spark.table(f"{target_db}.silver_events_rejected")

    assert rejected_df.count() == 2