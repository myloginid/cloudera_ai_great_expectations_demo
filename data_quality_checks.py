"""PySpark job that demonstrates Great Expectations data-quality assertions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    NumericType,
    StringType,
    TimestampType,
)

try:
    from great_expectations.dataset import SparkDFDataset
except ImportError:
    try:
        from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
    except ImportError:
        try:
            from great_expectations.core.batch import Batch
            from great_expectations.core.expectation_suite import ExpectationSuite
            from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
            from great_expectations.execution_engine.sparkdf_execution_engine import (
                SparkDFExecutionEngine,
            )
            from great_expectations.validator.validator import Validator
        except ImportError as exc:  # pragma: no cover - depends on runtime environment
            raise ModuleNotFoundError(
                "great_expectations is required for these data-quality checks. "
                "Install it in your environment before running this job."
            ) from exc

        class _LocalValidator(Validator):
            @property
            def _include_rendered_content(self) -> bool:
                return False


        class SparkDFDataset:
            """Minimal adapter that replaces legacy SparkDFDataset with the Validator API."""

            EXPECTATION_SUITE_NAME = "sparkdf_dataset_adapter"

            def __init__(self, dataframe: DataFrame) -> None:
                execution_engine = SparkDFExecutionEngine(spark=dataframe.sparkSession)
                expectation_suite = ExpectationSuite(self.EXPECTATION_SUITE_NAME)
                batch_data = SparkDFBatchData(
                    execution_engine=execution_engine,
                    dataframe=dataframe,
                )
                batch = Batch(data=batch_data)
                self._validator = _LocalValidator(
                    execution_engine=execution_engine,
                    expectation_suite=expectation_suite,
                    batches=[batch],
                )

            def validate(self, *args: object, **kwargs: object) -> dict[str, object]:
                """Proxy validate call to the underlying Validator."""

                return self._validator.validate(*args, **kwargs)

            def __getattr__(self, name: str) -> object:
                if name.startswith("expect_"):
                    return getattr(self._validator, name)
                raise AttributeError(
                    f"{type(self).__name__} has no attribute {name}"
                )


DEFAULT_DATA_PATH = (
    Path(__file__).resolve().parent / "resources" / "users.parquet"
)
POSITIVE_HINTS = (
    "age",
    "count",
    "quantity",
    "qty",
    "total",
    "amount",
    "score",
    "balance",
    "value",
)
PERCENT_HINTS = ("rate", "percent", "ratio")
DATE_HINTS = ("date", "timestamp", "dob", "signup", "created", "updated")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PySpark data-quality checks with Great Expectations."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help="Path to the parquet file to validate (default: resources/users.parquet).",
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("GreatExpectationsDataQuality").getOrCreate()


def read_source_data(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    return spark.read.parquet(str(path))


def has_hint(column_name: str, hints: Iterable[str]) -> bool:
    lower = column_name.lower()
    return any(hint in lower for hint in hints)


def apply_expectations(df: DataFrame, dataset: SparkDFDataset) -> None:
    for field in df.schema:
        column_name = field.name
        data_type = field.dataType

        dataset.expect_column_to_exist(column_name)
        dataset.expect_column_values_to_not_be_null(column_name, mostly=0.95)

        if isinstance(data_type, StringType):
            dataset.expect_column_value_lengths_to_be_between(
                column_name, min_value=1, mostly=0.9
            )
            if "email" in column_name.lower():
                dataset.expect_column_values_to_match_regex(
                    column_name,
                    r"[^@\s]+@[^@\s]+\.[^@\s]+",
                    mostly=0.95,
                )
            if has_hint(column_name, ("zip", "postal", "pin")):
                dataset.expect_column_values_to_match_regex(
                    column_name,
                    r"^\d{5}(-\d{4})?$",
                    mostly=0.9,
                )

        if isinstance(data_type, NumericType):
            if has_hint(column_name, POSITIVE_HINTS):
                dataset.expect_column_values_to_be_between(
                    column_name, min_value=0, mostly=0.9
                )
            if has_hint(column_name, PERCENT_HINTS):
                dataset.expect_column_values_to_be_between(
                    column_name, min_value=0, max_value=1, mostly=0.9
                )

        if "id" in column_name.lower():
            dataset.expect_column_values_to_be_unique(column_name)

        if (
            isinstance(data_type, (DateType, TimestampType))
            or has_hint(column_name, DATE_HINTS)
        ):
            dataset.expect_column_values_to_match_regex(
                column_name, r"^\d{4}-\d{2}-\d{2}", mostly=0.9
            )


def report_validation(validation_result: dict[str, object]) -> None:
    stats = validation_result.get("statistics", {}) or {}
    failed = [
        result
        for result in validation_result.get("results", [])
        if not result.get("success")
    ]

    print("\n--- Great Expectations validation ---")
    print(f"Success: {validation_result.get('success')}")
    print(
        f"Evaluated expectations: {stats.get('evaluated_expectations', 0)}, "
        f"successful: {stats.get('successful_expectations', 0)}, "
        f"unsuccessful: {stats.get('unsuccessful_expectations', 0)}"
    )

    if failed:
        print("Failed expectations:")
        for result in failed:
            expectation_type = result.get("expectation_config", {}).get("expectation_type")
            kwargs = result.get("expectation_config", {}).get("kwargs", {})
            column = kwargs.get("column", "<unknown>")
            print(f"  - {expectation_type} on {column}")
    else:
        print("All expectations succeeded.")

    print("Validation detail (JSON snippet):")
    snippet = json.dumps(validation_result, indent=2)
    print(snippet)


def main() -> None:
    args = parse_arguments()
    spark = build_spark_session()

    try:
        df = read_source_data(spark, args.input)
        validator = SparkDFDataset(df)
        apply_expectations(df, validator)
        raw_result = validator.validate(result_format="SUMMARY")
        validation_result = (
            raw_result.to_json_dict()
            if hasattr(raw_result, "to_json_dict")
            else raw_result
        )
        report_validation(validation_result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
