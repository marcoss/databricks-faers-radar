import os

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Databricks' session when running there; a local Delta-enabled session otherwise."""
    if "DATABRICKS_RUNTIME_VERSION" in os.environ or SparkSession.getActiveSession():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("faers-radar")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
        # Default 200 is far too many for one machine; AQE coalesces from here.
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
