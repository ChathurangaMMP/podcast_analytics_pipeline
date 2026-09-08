import os
import tempfile
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys


@pytest.fixture(scope="session")
def spark():

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    raw_temp_dir = tempfile.mkdtemp().replace("\\", "/")
    warehouse_uri = f"file:///{raw_temp_dir}"

    derby_url = f"jdbc:derby:;databaseName={raw_temp_dir}/metastore_db;create=true"

    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("podcast-pipeline-test")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse_uri)
        .config("javax.jdo.option.ConnectionURL", derby_url) 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark_session = (configure_spark_with_delta_pip(builder).getOrCreate())

    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    spark_session.stop()