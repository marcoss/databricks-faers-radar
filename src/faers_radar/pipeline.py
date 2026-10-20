"""Medallion pipeline entrypoint: `faers-radar {bronze,silver,gold,all} --raw ... --target ...`."""

import argparse

from pyspark.sql import SparkSession

from faers_radar import transforms
from faers_radar.io import read_deleted, read_raw, read_table, write_table
from faers_radar.spark import get_spark

RAW_TABLES = ("demo", "drug", "reac")


def bronze(spark: SparkSession, raw: str, target: str) -> None:
    for table in RAW_TABLES:
        write_table(read_raw(spark, raw, table), target, f"bronze_{table}")
    write_table(read_deleted(spark, raw), target, "bronze_deleted")


def silver(spark: SparkSession, target: str) -> None:
    cases = transforms.latest_case_versions(
        read_table(spark, target, "bronze_demo"), read_table(spark, target, "bronze_deleted")
    )
    write_table(cases, target, "silver_cases")
    cases = read_table(spark, target, "silver_cases")
    drug = read_table(spark, target, "bronze_drug")
    reac = read_table(spark, target, "bronze_reac")
    write_table(transforms.case_drugs(drug, cases), target, "silver_case_drugs")
    write_table(transforms.case_reactions(reac, cases), target, "silver_case_reactions")


def gold(spark: SparkSession, target: str, min_cases: int) -> None:
    signals = transforms.disproportionality(
        read_table(spark, target, "silver_case_drugs"),
        read_table(spark, target, "silver_case_reactions"),
        min_cases=min_cases,
    )
    write_table(signals, target, "gold_signals")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="faers-radar")
    parser.add_argument("layer", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--raw", default="data/raw", help="dir or UC Volume with raw files")
    parser.add_argument(
        "--target", default="data/lake", help="Delta path, or catalog.schema on Databricks"
    )
    parser.add_argument("--min-cases", type=int, default=3)
    args = parser.parse_args(argv)

    spark = get_spark()
    if args.layer in ("bronze", "all"):
        bronze(spark, args.raw, args.target)
    if args.layer in ("silver", "all"):
        silver(spark, args.target)
    if args.layer in ("gold", "all"):
        gold(spark, args.target, args.min_cases)


if __name__ == "__main__":
    main()
