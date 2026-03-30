"""Spark job for Great Expectations checks on the gx_demo_table."""

from __future__ import annotations

import argparse
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Iterable
import re

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--conf spark.yarn.principal=manishm@GO01-DEM.YLCU-ATMI.CLOUDERA.SITE "
    "--conf spark.yarn.keytab=/home/cdsw/go01-demo-aws-manishm.keytab pyspark-shell",
)

import cml.data_v1 as cmldata
from pyspark.sql import DataFrame, SparkSession

from data_quality_checks import SparkDFDataset, apply_expectations, report_validation

DEFAULT_CONFIG = {
    "completeness": {
        "nullable_comment": {"min": 0.5, "max": 1.0},
        "required_col": {},
    },
    "numeric": {
        "amount": {
            "min": 0.0,
            "max": 10000.0,
            "mean_min": 50.0,
            "mean_max": 500.0,
            "median_min": 40.0,
            "median_max": 400.0,
            "std_min": 0.0,
            "std_max": 1000.0,
            "sum_min": 100.0,
            "sum_max": 1_000_000.0,
            "quantiles": [
                {"quantile": 0.25, "min_value": 10, "max_value": 200},
                {"quantile": 0.5, "min_value": 20, "max_value": 400},
                {"quantile": 0.75, "min_value": 50, "max_value": 800},
            ],
            "values_min": 0.0,
            "values_max": 2000.0,
        },
        "measure_a": {"min": 0, "max": 2000},
        "measure_b": {"min": 0, "max": 2000},
    },
    "schema": {
        "columns": [
            "row_id",
            "nullable_comment",
            "all_null_col",
            "required_col",
            "amount",
            "score",
            "measure_a",
            "measure_b",
            "fixed_sum_1",
            "fixed_sum_2",
            "fixed_sum_3",
            "category",
            "business_key",
            "status",
            "country_cd",
            "currency_cd",
            "name_txt",
            "exact_len_2",
            "forbidden_token",
            "like_code",
            "free_text",
            "email",
            "regex_id",
            "regex_list_id",
            "phone10",
            "postal_code",
            "equal_col_a",
            "equal_col_b",
            "code_a",
            "code_b",
            "code_c",
            "compound_key_1",
            "compound_key_2",
        ],
        "expected_count": 32,
        "column_count_min": 30,
        "column_count_max": 40,
    },
    "uniqueness": {
        "category_set": {"A", "B", "C"},
        "business_key": {"min_proportion": 0.7, "max_proportion": 1.0, "min_unique": 10, "max_unique": 1000},
        "compound_keys": ["compound_key_1", "compound_key_2"],
    },
    "validity": {
        "status_set": ["ACTIVE", "PENDING", "INACTIVE"],
        "country_currency_pairs": [("IN", "INR"), ("US", "USD"), ("SG", "SGD")],
        "forbidden_tokens": ["DROP", "NULL", "INVALID"],
        "free_text_disallowed_patterns": ["UNWANTED%", "BAD-%"],
        "email_regex": r"[^@\s]+@[^@\s]+\.[^@\s]+",
        "regex_id": r"^EMP[0-9]{4}$",
        "regex_list": [r"^(AA|BB)-[0-9]{3}$"],
        "phone_regex": r"^[0-9]{10}$",
        "postal_regex": r"^[0-9]{5}(-[0-9]{4})?$",
        "name_len_min": 3,
        "name_len_max": 50,
    },
    "volume": {"row_min": 50, "row_max": 10000, "expected_rows": 100},
}

GH_CONFIG_FILES = [
    "/home/cdsw/gx_demo_config.json",
]

DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parent / "gx_demo_validation_result.json"

QUERY_SQL = "SELECT * FROM manishm.gx_demo_table"

SQL_CUSTOM_QUERIES = [
    "SELECT * FROM manishm.gx_demo_table WHERE measure_a <= measure_b",
    "SELECT * FROM manishm.gx_demo_table WHERE equal_col_a <> equal_col_b",
    "SELECT * FROM manishm.gx_demo_table WHERE fixed_sum_1 + fixed_sum_2 + fixed_sum_3 <> 100",
]

CONNECTION_NAME = "go01-aw-dl"

def run_if_columns_present(
    columns: set[str], required_columns: Iterable[str], func, *args, **kwargs
) -> None:
    if set(required_columns).issubset(columns):
        func(*args, **kwargs)


def like_pattern_to_regex(pattern: str) -> str:
    """Translate a SQL LIKE pattern into an equivalent regex."""
    builder: list[str] = ["^"]
    for char in pattern:
        if char == "%":
            builder.append(".*")
        elif char == "_":
            builder.append(".")
        else:
            builder.append(re.escape(char))
    builder.append("$")
    return "".join(builder)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Great Expectations checks for manishm.gx_demo_table."
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        default=None,
        help="Path to JSON overrides for expectation thresholds.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Where to write the validation JSON result.",
    )
    parser.add_argument(
        "--query",
        "-q",
        default=QUERY_SQL,
        help="SQL query to load the table rows (default selects the entire table).",
    )
    return parser.parse_args()


def load_config(path: Path | None) -> dict:
    config = deepcopy(DEFAULT_CONFIG)
    if path and path.exists():
        overrides = json.loads(path.read_text())
        merge_dict(config, overrides)
    for file in GH_CONFIG_FILES:
        p = Path(file)
        if p.exists():
            overrides = json.loads(p.read_text())
            merge_dict(config, overrides)
    return config


def merge_dict(base: dict, overrides: dict) -> None:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_dict(base[key], value)
        else:
            base[key] = value


def build_spark_session() -> SparkSession:
    try:
        conn = cmldata.get_connection(CONNECTION_NAME)
        return conn.get_spark_session()
    except Exception:
        return (
            SparkSession.builder.appName("GreatExpectationsGxDemoQuality")
            .enableHiveSupport()
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .getOrCreate()
        )


def apply_completeness_checks(
    df: DataFrame,
    dataset: SparkDFDataset,
    config: dict,
    columns: set[str],
) -> None:
    completeness = config["completeness"]
    run_if_columns_present(
        columns,
        {"all_null_col"},
        dataset.expect_column_values_to_be_null,
        "all_null_col",
    )
    run_if_columns_present(
        columns,
        {"required_col"},
        dataset.expect_column_values_to_not_be_null,
        "required_col",
        mostly=0.99,
    )


def apply_numeric_checks(
    df: DataFrame,
    dataset: SparkDFDataset,
    config: dict,
    columns: set[str],
) -> None:
    amount_config = config["numeric"]["amount"]
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_max_to_be_between,
        "amount",
        min_value=amount_config["min"],
        max_value=amount_config["max"],
    )
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_mean_to_be_between,
        "amount",
        min_value=amount_config["mean_min"],
        max_value=amount_config["mean_max"],
    )
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_median_to_be_between,
        "amount",
        min_value=amount_config["median_min"],
        max_value=amount_config["median_max"],
    )
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_min_to_be_between,
        "amount",
        min_value=amount_config["min"],
        max_value=amount_config["max"],
    )
    quantile_configs = amount_config.get("quantiles", [])
    if quantile_configs:
        quantile_ranges = {
            "quantiles": [q["quantile"] for q in quantile_configs],
            "value_ranges": [
                [q.get("min_value"), q.get("max_value")] for q in quantile_configs
            ],
        }
        run_if_columns_present(
            columns,
            {"amount"},
            dataset.expect_column_quantile_values_to_be_between,
            "amount",
            quantile_ranges=quantile_ranges,
        )
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_stdev_to_be_between,
        "amount",
        min_value=amount_config["std_min"],
        max_value=amount_config["std_max"],
    )
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_sum_to_be_between,
        "amount",
        min_value=amount_config["sum_min"],
        max_value=amount_config["sum_max"],
    )
    run_if_columns_present(
        columns,
        {"amount"},
        dataset.expect_column_values_to_be_between,
        "amount",
        min_value=amount_config["values_min"],
        max_value=amount_config["values_max"],
    )
    run_if_columns_present(
        columns,
        {"measure_a", "measure_b"},
        dataset.expect_column_pair_values_a_to_be_greater_than_b,
        column_A="measure_a",
        column_B="measure_b",
    )
    run_if_columns_present(
        columns,
        {"fixed_sum_1", "fixed_sum_2", "fixed_sum_3"},
        dataset.expect_multicolumn_sum_to_equal,
        [
            "fixed_sum_1",
            "fixed_sum_2",
            "fixed_sum_3",
        ],
        sum_total=100,
    )
    run_if_columns_present(
        columns,
        {"category"},
        dataset.expect_column_kl_divergence_to_be_less_than,
        "category",
        partition_object={"values": ["A", "B", "C"], "weights": [0.3, 0.3, 0.4]},
        threshold=0.5,
    )


def apply_schema_checks(dataset: SparkDFDataset, config: dict) -> None:
    schema_config = config["schema"]
    dataset.expect_column_to_exist("row_id")
    dataset.expect_column_values_to_be_of_type("amount", type_="DoubleType")
    dataset.expect_column_values_to_be_in_type_list("measure_a", type_list=["IntegerType", "LongType"])
    dataset.expect_table_column_count_to_be_between(
        min_value=schema_config["column_count_min"],
        max_value=schema_config["column_count_max"],
    )
    dataset.expect_table_column_count_to_equal(schema_config["expected_count"])
    dataset.expect_table_columns_to_match_ordered_list(schema_config["columns"])
    dataset.expect_table_columns_to_match_set(set(schema_config["columns"]))


def apply_sql_constraints(spark: SparkSession) -> None:
    for sql_query in SQL_CUSTOM_QUERIES:
        invalid_rows = spark.sql(sql_query).count()
        if invalid_rows != 0:
            raise RuntimeError(f"Custom SQL expectation failed ({sql_query}): {invalid_rows} rows returned")


def apply_uniqueness_checks(
    df: DataFrame,
    dataset: SparkDFDataset,
    config: dict,
    columns: set[str],
) -> None:
    uniqueness = config["uniqueness"]
    run_if_columns_present(
        columns,
        {"category"},
        dataset.expect_column_distinct_values_to_be_in_set,
        "category",
        value_set=uniqueness["category_set"],
    )
    run_if_columns_present(
        columns,
        {"category"},
        dataset.expect_column_distinct_values_to_contain_set,
        "category",
        value_set=uniqueness["category_set"],
    )
    run_if_columns_present(
        columns,
        {"category"},
        dataset.expect_column_distinct_values_to_equal_set,
        "category",
        value_set=uniqueness["category_set"],
    )
    run_if_columns_present(
        columns,
        {"business_key"},
        dataset.expect_column_proportion_of_unique_values_to_be_between,
        "business_key",
        min_value=uniqueness["business_key"]["min_proportion"],
        max_value=uniqueness["business_key"]["max_proportion"],
    )
    run_if_columns_present(
        columns,
        {"business_key"},
        dataset.expect_column_unique_value_count_to_be_between,
        "business_key",
        min_value=uniqueness["business_key"]["min_unique"],
        max_value=uniqueness["business_key"]["max_unique"],
    )
    run_if_columns_present(
        columns,
        {"row_id"},
        dataset.expect_column_values_to_be_unique,
        "row_id",
    )
    run_if_columns_present(
        columns,
        set(uniqueness["compound_keys"]),
        dataset.expect_compound_columns_to_be_unique,
        uniqueness["compound_keys"],
    )
    run_if_columns_present(
        columns,
        {"code_a", "code_b", "code_c"},
        dataset.expect_select_column_values_to_be_unique_within_record,
        ["code_a", "code_b", "code_c"],
        ignore_row_if="any_value_is_missing",
    )


def apply_validity_checks(
    dataset: SparkDFDataset,
    config: dict,
    columns: set[str],
) -> None:
    validity = config["validity"]
    run_if_columns_present(
        columns,
        {"status"},
        dataset.expect_column_most_common_value_to_be_in_set,
        "status",
        value_set=validity["status_set"],
    )
    run_if_columns_present(
        columns,
        {"equal_col_a", "equal_col_b"},
        dataset.expect_column_pair_values_to_be_equal,
        column_A="equal_col_a",
        column_B="equal_col_b",
    )
    run_if_columns_present(
        columns,
        {"country_cd", "currency_cd"},
        dataset.expect_column_pair_values_to_be_in_set,
        column_A="country_cd",
        column_B="currency_cd",
        value_pairs_set=validity["country_currency_pairs"],
    )
    run_if_columns_present(
        columns,
        {"name_txt"},
        dataset.expect_column_value_lengths_to_be_between,
        "name_txt",
        min_value=validity["name_len_min"],
        max_value=validity["name_len_max"],
    )
    run_if_columns_present(
        columns,
        {"exact_len_2"},
        dataset.expect_column_value_lengths_to_equal,
        "exact_len_2",
        value=2,
    )
    run_if_columns_present(
        columns,
        {"category"},
        dataset.expect_column_values_to_be_in_set,
        "category",
        value_set={"A", "B", "C"},
    )
    run_if_columns_present(
        columns,
        {"forbidden_token"},
        dataset.expect_column_values_to_not_be_in_set,
        "forbidden_token",
        value_set=set(validity["forbidden_tokens"]),
    )
    run_if_columns_present(
        columns,
        {"email"},
        dataset.expect_column_values_to_match_regex,
        "email",
        regex=validity["email_regex"],
        mostly=0.95,
    )
    run_if_columns_present(
        columns,
        {"regex_id"},
        dataset.expect_column_values_to_match_regex,
        "regex_id",
        regex=validity["regex_id"],
    )
    run_if_columns_present(
        columns,
        {"regex_list_id"},
        dataset.expect_column_values_to_match_regex_list,
        "regex_list_id",
        regex_list=validity["regex_list"],
    )
    disallowed_like_regex = [
        like_pattern_to_regex(pattern) for pattern in validity["free_text_disallowed_patterns"]
    ]
    disallowed_regex_list = [r"BAD-.*", r"FORBIDDEN.*"] + disallowed_like_regex
    run_if_columns_present(
        columns,
        {"free_text"},
        dataset.expect_column_values_to_not_match_regex_list,
        "free_text",
        regex_list=disallowed_regex_list,
    )
    run_if_columns_present(
        columns,
        {"phone10"},
        dataset.expect_column_values_to_match_regex,
        "phone10",
        regex=validity["phone_regex"],
    )
    run_if_columns_present(
        columns,
        {"postal_code"},
        dataset.expect_column_values_to_match_regex,
        "postal_code",
        regex=validity["postal_regex"],
    )


def apply_volume_checks(df: DataFrame, dataset: SparkDFDataset, config: dict) -> None:
    row_count = df.count()
    volume = config["volume"]
    dataset.expect_table_row_count_to_be_between(volume["row_min"], volume["row_max"])
    dataset.expect_table_row_count_to_equal(volume["expected_rows"])


def main() -> None:
    args = parse_arguments()
    config = load_config(args.config)
    spark = build_spark_session()

    try:
        df = spark.sql(args.query)
        validator = SparkDFDataset(df)
        columns = set(df.columns)
        apply_expectations(df, validator)
        apply_completeness_checks(df, validator, config, columns)
        apply_numeric_checks(df, validator, config, columns)
        apply_schema_checks(validator, config)
        apply_validity_checks(validator, config, columns)
        apply_uniqueness_checks(df, validator, config, columns)
        apply_volume_checks(df, validator, config)
        apply_sql_constraints(spark)
        result = validator.validate(result_format="SUMMARY")
        validation_result = (
            result.to_json_dict() if hasattr(result, "to_json_dict") else result
        )
        report_validation(validation_result)
        output_path = args.output.expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(validation_result, indent=2))
        print(f"Validation saved to {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
