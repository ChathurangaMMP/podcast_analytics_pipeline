import os
import tempfile
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():

    # Windows local networking
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

    warehouse_dir = tempfile.mkdtemp().replace("\\", "/")

    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("podcast-pipeline-test")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark_session = (configure_spark_with_delta_pip(builder).getOrCreate())

    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    spark_session.stop()