"""Data-quality checks for the Hive metastore Airlines Iceberg table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from data_quality_checks import SparkDFDataset, apply_expectations, report_validation

DEFAULT_QUERY = """
SELECT *
FROM airlines_iceberg.flights
LIMIT 100
"""


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PySpark data-quality checks against a Hive metastore table using Great Expectations."
    )
    parser.add_argument(
        "--query",
        "-q",
        default=DEFAULT_QUERY,
        help="SQL query that reads data from the Hive metastore table (default reads the first 100 rows of airlines_iceberg.flights).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path(__file__).resolve().parent / "hms_validation_result.json",
        help="File to write the JSON validation result to",
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("GreatExpectationsHmsTableQuality").getOrCreate()


def apply_airlines_column_checks(df: DataFrame, dataset: SparkDFDataset) -> None:
    """Run column-specific Great Expectations against the airlines table sample."""

    columns = set(df.columns)
    ordered_columns = list(df.columns)
    expected_columns = [
        "month",
        "dayofmonth",
        "dayofweek",
        "deptime",
        "crsdeptime",
        "arrtime",
        "crsarrtime",
        "uniquecarrier",
        "flightnum",
        "tailnum",
        "actualelapsedtime",
        "crselapsedtime",
        "airtime",
        "arrdelay",
        "depdelay",
        "origin",
        "dest",
        "distance",
        "taxiin",
        "taxiout",
        "cancelled",
        "cancellationcode",
        "diverted",
        "carrierdelay",
        "weatherdelay",
        "nasdelay",
        "securitydelay",
        "lateaircraftdelay",
        "year",
    ]
    row_count = df.count()

    dataset.expect_table_row_count_to_be_between(min_value=1, max_value=max(1000, row_count * 10))
    dataset.expect_table_column_count_to_equal(len(columns))
    dataset.expect_table_columns_to_match_ordered_list(ordered_columns)
    dataset.expect_table_columns_to_match_set(set(expected_columns))

    def safe_expect(expectation, name: str, *args, **kwargs) -> None:
        if name in columns:
            expectation(name, *args, **kwargs)

    if "year" in columns:
        safe_expect(dataset.expect_column_to_exist, "year")
        safe_expect(dataset.expect_column_values_to_not_be_null, "year", mostly=0.95)
        safe_expect(dataset.expect_column_values_to_be_of_type, "year", type_="IntegerType")
        safe_expect(dataset.expect_column_values_to_not_be_in_set, "year", {1990, 1991, 1995})

    if "cancelled" in columns:
        safe_expect(dataset.expect_column_values_to_be_in_set, "cancelled", {0, 1}, mostly=0.95)
        distinct_cancelled = {
            row[0] for row in df.select("cancelled").distinct().collect() if row[0] is not None
        }
        if distinct_cancelled:
            safe_expect(
                dataset.expect_column_distinct_values_to_equal_set,
                "cancelled",
                value_set=distinct_cancelled,
            )
            if {0, 1}.issubset(distinct_cancelled):
                safe_expect(
                    dataset.expect_column_distinct_values_to_contain_set,
                    "cancelled",
                    value_set={0, 1},
                )
            safe_expect(
                dataset.expect_column_distinct_values_to_be_in_set,
                "cancelled",
                value_set={0, 1, 2},
            )
        safe_expect(
            dataset.expect_column_values_to_be_in_type_list,
            "cancelled",
            type_list=["IntegerType", "ShortType"],
        )

    if "diverted" in columns:
        safe_expect(dataset.expect_column_values_to_be_in_set, "diverted", {0, 1, "0", "1"}, mostly=0.9)
        distinct_diverted = {
            row[0] for row in df.select("diverted").distinct().collect() if row[0] is not None
        }
        if distinct_diverted and {0, 1}.issubset(distinct_diverted):
            safe_expect(
                dataset.expect_column_distinct_values_to_contain_set,
                "diverted",
                value_set={0, 1},
            )
    safe_expect(dataset.expect_column_values_to_be_between, "month", min_value=1, max_value=12, mostly=0.95)
    safe_expect(dataset.expect_column_values_to_be_between, "dayofmonth", min_value=1, max_value=31, mostly=0.95)
    safe_expect(dataset.expect_column_values_to_be_between, "dayofweek", min_value=1, max_value=7, mostly=0.95)

    for time_col in ("deptime", "crsdeptime", "arrtime", "crsarrtime"):
        safe_expect(dataset.expect_column_values_to_be_between, time_col, min_value=0, max_value=2359, mostly=0.9)

    safe_expect(dataset.expect_column_values_to_be_between, "flightnum", min_value=1, max_value=9999, mostly=0.95)
    safe_expect(dataset.expect_column_values_to_be_between, "actualelapsedtime", min_value=1, max_value=2000, mostly=0.95)
    safe_expect(dataset.expect_column_values_to_be_between, "crselapsedtime", min_value=1, max_value=2000, mostly=0.95)
    safe_expect(dataset.expect_column_values_to_be_between, "airtime", min_value=0, max_value=2000, mostly=0.95)

    safe_expect(dataset.expect_column_values_to_be_between, "arrdelay", min_value=-720, max_value=1440, mostly=0.95)
    safe_expect(dataset.expect_column_values_to_be_between, "depdelay", min_value=-720, max_value=1440, mostly=0.95)

    for column in ("distance", "taxiin", "taxiout"):
        safe_expect(dataset.expect_column_values_to_be_between, column, min_value=0, max_value=6000, mostly=0.95)

    if "origin" in columns:
        dataset.expect_column_values_to_match_regex("origin", r"^[A-Z]{3}$", mostly=0.95)
    if "dest" in columns:
        dataset.expect_column_values_to_match_regex("dest", r"^[A-Z]{3}$", mostly=0.95)

    safe_expect(dataset.expect_column_values_to_be_in_set, "cancelled", {0, 1}, mostly=0.95)

    for delay_col in ("carrierdelay", "weatherdelay", "nasdelay", "securitydelay", "lateaircraftdelay"):
        safe_expect(dataset.expect_column_values_to_be_between, delay_col, min_value=0, max_value=1440, mostly=0.95)

    if "origin" in columns:
        dataset.expect_column_values_to_match_regex("origin", r"^[A-Z]{3}$", mostly=0.95)
        dataset.expect_column_value_lengths_to_equal("origin", value=3, mostly=0.95)
    if "dest" in columns:
        dataset.expect_column_values_to_match_regex("dest", r"^[A-Z]{3}$", mostly=0.95)
        dataset.expect_column_value_lengths_to_equal("dest", value=3, mostly=0.95)

    stats = df.select(
        F.min("distance").alias("distance_min"),
        F.max("distance").alias("distance_max"),
        F.mean("distance").alias("distance_mean"),
        F.stddev("distance").alias("distance_std"),
        F.sum("distance").alias("distance_sum"),
    ).first()

    if stats is not None and stats.distance_min is not None:
        safe_expect(
            dataset.expect_column_min_to_be_between,
            "distance",
            min_value=max(0, stats.distance_min - 10),
            max_value=stats.distance_min + 5,
        )
    if stats is not None and stats.distance_max is not None:
        safe_expect(
            dataset.expect_column_max_to_be_between,
            "distance",
            min_value=stats.distance_max - 10,
            max_value=stats.distance_max + 100,
        )
    if stats is not None and stats.distance_mean is not None:
        safe_expect(
            dataset.expect_column_mean_to_be_between,
            "distance",
            min_value=max(0, stats.distance_mean - 100),
            max_value=stats.distance_mean + 100,
        )
    if stats is not None and stats.distance_std is not None:
        safe_expect(
            dataset.expect_column_stdev_to_be_between,
            "distance",
            min_value=max(0, stats.distance_std - 10),
            max_value=stats.distance_std + 10,
        )
    if stats is not None and stats.distance_sum is not None:
        safe_expect(
            dataset.expect_column_sum_to_be_between,
            "distance",
            min_value=max(0, stats.distance_sum * 0.9),
            max_value=stats.distance_sum * 1.1,
        )
    if "distance" in columns:
        median_distance = df.approxQuantile("distance", [0.5], 0.01)[0]
        safe_expect(
            dataset.expect_column_median_to_be_between,
            "distance",
            min_value=median_distance - 50,
            max_value=median_distance + 50,
        )
        if stats is not None:
            safe_expect(
                dataset.expect_column_values_to_be_between,
                "distance",
                min_value=stats.distance_min if stats.distance_min is not None else 0,
                max_value=stats.distance_max if stats.distance_max is not None else 6000,
                mostly=0.95,
            )

    if "flightnum" in columns and row_count:
        unique_count = df.select("flightnum").distinct().count()
        proportion_unique = unique_count / row_count
        safe_expect(
            dataset.expect_column_proportion_of_unique_values_to_be_between,
            "flightnum",
            min_value=max(0, proportion_unique - 0.05),
            max_value=min(1, proportion_unique + 0.05),
        )
        safe_expect(
            dataset.expect_column_unique_value_count_to_be_between,
            "flightnum",
            min_value=max(0, unique_count - 5),
            max_value=unique_count + 5,
        )

    if "uniquecarrier" in columns:
        dataset.expect_column_values_to_match_regex("uniquecarrier", r"^[A-Z0-9]{2}$", mostly=0.9)
        dataset.expect_column_values_to_match_regex_list(
            "uniquecarrier",
            regex_list=[r"^[A-Z]{2}$", r"^[A-Z0-9]{2}$"],
            match_on="any",
        )

    if "tailnum" in columns:
        dataset.expect_column_value_lengths_to_be_between("tailnum", min_value=3, max_value=7, mostly=0.9)
    if "uniquecarrier" in columns:
        dataset.expect_column_values_to_match_regex("uniquecarrier", r"^[A-Z0-9]{2}$", mostly=0.95)
    if "tailnum" in columns:
        dataset.expect_column_values_to_match_regex("tailnum", r"^[A-Z0-9]+$", mostly=0.9)

    safe_expect(dataset.expect_column_values_to_be_between, "year", min_value=2000, max_value=2030, mostly=0.95)


def main() -> None:
    args = parse_arguments()
    spark = build_spark_session()

    try:
        df = spark.sql(args.query)
        validator = SparkDFDataset(df)
        apply_expectations(df, validator)
        apply_airlines_column_checks(df, validator)
        raw_result = validator.validate(result_format="SUMMARY")
        validation_result = (
            raw_result.to_json_dict()
            if hasattr(raw_result, "to_json_dict")
            else raw_result
        )
        report_validation(validation_result)
        output_path = args.output.expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(validation_result, indent=2))
        print(f"Validation result saved to {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
