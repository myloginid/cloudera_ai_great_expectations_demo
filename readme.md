# Data Quality Job / Great Expectations Notes

This repo now includes several PySpark jobs that wrap Hive metastore or sample data in `SparkDFDataset`
and assert Great Expectations runtimes. Each job writes its validation result to a JSON file (`--output`)
so downstream workflows can inspect pass/fail states.

## Environment & dependencies

- **Python**: 3.11.14 (the environment already provides all dependencies in `/home/cdsw/.local/lib/python3.11/site-packages`).
- **PySpark**: 3.5.1 (Scala 2.12.18) with Kerberos-aware configuration, so the jobs expect a valid `kinit` ticket for
  any Hive or S3 access.
- **Great Expectations**: 1.15.1 is already installed; use `python -m pip install "great_expectations[spark]"` in a project
  virtualenv or `requirements.txt` entry if you need to replicate the environment elsewhere. The `[spark]` extra
  pulls in the Spark execution engine plus any metric dependencies required for the checks.

## Data-quality jobs

### `data_quality_checks.py` (local sample)

Targets `resources/users.parquet`. It cycles through every column, verifying existence, mostly-not-null, pattern
matches (email, postal, dates), unique IDs, and heuristics for numeric hints like `amount`/`score`. This script
serves as a template for how to wrap `SparkDFDataset` against arbitrary DataFrames.

### `data_quality_checks_hms_table.py` (Hive metastore: `airlines_iceberg.flights`)

Runs a configurable query (default: first 100 rows) and executes column-aware expectations such as:

- row/column counts and schema comparisons (ordered/unordered sets).  
- individual column ranges (time values, flight numbers, distances, airports, etc.).  
- uniqueness and set containment expectations on carrier/delay columns.  
- regex constraints for IATA codes and supplemental type checks.

The script writes the Great Expectations result JSON to `hms_validation_result.json` by default and echoes the
applied statistics in the console.

### `data_quality_checks_gx_demo.py` (`manishm.gx_demo_table`)

This is the most comprehensive job and drives the documentation below:

1. **Completeness** – proportion of non-null values, required-not-null columns, and all-null columns.
2. **Numeric** – amount/score statistics (min, max, mean, median, quantiles, standard deviation, sum, value bounds),
   column-pair comparisons (`measure_a > measure_b`), `fixed_sum_*` validation, and KL divergence against an
   expected distribution for `category`.
3. **Schema** – column existence, type assertions, column count guardrails, and exact ordered/unordered lists.
4. **SQL expectations** – custom queries that must return zero rows (e.g., the invalid sums or mismatched columns).
5. **Uniqueness** – column distinct-set checks, business key uniqueness ratios, compound-key uniqueness, and
   intra-record uniqueness for grouped code fields.
6. **Validity** – value-in-set, regex/format, like-pattern, length, forbidden token/regex exclusions, phone/postal
   patterns, and country-currency pair validation.
7. **Volume** – row count range assertions, expected total count, and peer table parity.
8. **Multi-source comparison** – aggregated result equality with a compare table and content parity with a peer table.

The job reads optional JSON overrides (`--config`) and writes the validation result to a JSON file (`--output`).

## Extending these checks to new tables

1. Copy `data_quality_checks_gx_demo.py` (or create a new driver) and adjust `DEFAULT_CONFIG` to include the
   new table’s columns, type hints, value ranges, regexes, and peer comparisons.
2. Implement helper functions per quality category (`apply_completeness_checks`, `apply_numeric_checks`, etc.)
   referencing the new configuration entries.
3. Guard expensive checks with `run_if_columns_present` (or similar) so the expectation suite is resilient to schema drift.
4. Wire the new script into your orchestration tool with the appropriate query/setup and direct the output JSON to
   your desired location.

For small projects, the `apply_expectations` helper in `data_quality_checks.py` shows a minimal expectation set that can
be reused.

## Checking most-recent partition data only

When you only want to validate the most recent partition, parameterize the SQL query that feeds each job. For
example, replace the default `SELECT * FROM manishm.gx_demo_table` with:

```sql
SELECT * FROM manishm.gx_demo_table
WHERE ingestion_date = (
  SELECT MAX(ingestion_date) FROM manishm.gx_demo_table
)
```

Or, in PySpark, build a DataFrame with `spark.table("manishm.gx_demo_table").filter("ingestion_date = current_date")`
before passing it to `SparkDFDataset`. This ensures every expectation runs only on the latest slice.
