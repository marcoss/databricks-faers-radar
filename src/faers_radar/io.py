"""Reading raw FAERS files and reading/writing lakehouse tables.

A target is either a filesystem path (local Delta, e.g. "data/lake") or a
Unity Catalog schema (e.g. "workspace.faers"). The same pipeline code runs on both.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw(spark: SparkSession, raw_root: str, table: str) -> DataFrame:
    """All quarters of one FAERS table, every column kept as a string (bronze)."""
    return (
        spark.read.option("sep", "$")
        .option("header", True)
        # FAERS has no quoting; a stray " in a drug name must not swallow lines.
        .option("quote", "")
        .option("pathGlobFilter", "*.txt")
        .csv(f"{raw_root}/{table}")
        .withColumn("source_quarter", F.regexp_extract("_metadata.file_name", r"(\d{4}Q\d)", 1))
    )


def read_deleted(spark: SparkSession, raw_root: str) -> DataFrame:
    """Case IDs FDA has withdrawn; one per line, no header."""
    return (
        spark.read.option("pathGlobFilter", "*.txt")
        .text(f"{raw_root}/deleted")
        .select(F.trim("value").alias("caseid"))
        .filter(F.col("caseid").rlike(r"^\d+$"))
        .distinct()
    )


def _is_path(target: str) -> bool:
    return "/" in target


def write_table(df: DataFrame, target: str, name: str) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    if _is_path(target):
        writer.save(f"{target}/{name}")
    else:
        writer.saveAsTable(f"{target}.{name}")


def read_table(spark: SparkSession, target: str, name: str) -> DataFrame:
    if _is_path(target):
        return spark.read.format("delta").load(f"{target}/{name}")
    return spark.table(f"{target}.{name}")
