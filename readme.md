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

## Running this suite in Cloudera AI (CAI)

These scripts are designed to be run inside a CAI workspace that already configures Spark, Hive, Kerberos, and the
`go01-aw-dl` CML connection. To onboard external customers:

1. **Prepare the workspace** – create a project with the standard CAI runtime (Python 3.11 + Spark 3.5.1). Upload the
   `gx_demo` repo and ensure the `go01-demo-aws-manishm.keytab` file resides in `/home/cdsw`.
2. **Authenticate** – run `kinit -kt /home/cdsw/go01-demo-aws-manishm.keytab manishm@GO01-DEM.YLCU-ATMI.CLOUDERA.SITE` so
   the CAI driver pod has a valid TGT for Hive/S3 access. CAI already injects the `CDSW_*` env vars referenced by the Core
   ML APIs (`cml.data_v1`).
3. **Execute the GX demo** – trigger `python data_quality_checks_gx_demo.py -o gx_demo_test_output.json` from the project
   root. The script will either reuse the shared Spark session via `cmldata.get_connection("go01-aw-dl")` or fall back to
   a local builder that already configures the Iceberg catalog and Kerberos extensions. Validation results land in
   `gx_demo_test_output.json`.
4. **Interpret the output** – open the generated JSON or inspect `gx_demo_run.log` for the GE statistics (success percent,
   failing expectations, observed values). The job writes the final line `Validation saved to gx_demo_test_output.json`.
5. **Extend to other tables** – copy `data_quality_checks_gx_demo.py`, edit `DEFAULT_CONFIG` for the new schema, and run
   the script with `--config my_overrides.json` and `--query "SELECT ..."` against any CAI cluster-accessible table.

Because CAI already sets the Spark defaults (see `spark-defaults.conf`), you do not need to pass additional JVM
properties. Validating inside CAI ensures the same Kerberos/IDBroker context used by production workloads is applied here
too.

## Data-quality jobs

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
It now reuses the `go01-aw-dl` CML connection (`cmldata.get_connection`) so it inherits the platform’s Kerberos/IDBroker
and S3 configurations instead of building a plain `SparkSession` manually.
This also means the job shares the same Spark session that the platform already bootstraps, avoiding duplicate sessions and leveraging the existing Kerberos tickets/delegation tokens.
The expectation suite only uses the Spark-supported GE expectations that ship with this environment; unsupported expectations (like the removed z-score and match-like-pattern list calls) were dropped to prevent missing-provider errors.

## Extending these checks to new tables

1. Copy `data_quality_checks_gx_demo.py` (or create a new driver) and adjust `DEFAULT_CONFIG` to include the
   new table’s columns, type hints, value ranges, regexes, and peer comparisons.
2. Implement helper functions per quality category (`apply_completeness_checks`, `apply_numeric_checks`, etc.)
   referencing the new configuration entries.
3. Guard expensive checks with `run_if_columns_present` (or similar) so the expectation suite is resilient to schema drift.
4. Wire the new script into your orchestration tool with the appropriate query/setup and direct the output JSON to
   your desired location.

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

If your table partitions by `snapshot_date` or numeric `yyyymmdd`, swap `ingestion_date` for the correct column:
```sql
WHERE snapshot_date = (
  SELECT MAX(snapshot_date) FROM manishm.gx_demo_table
)
```
or
```sql
WHERE yyyymmdd = (
  SELECT MAX(yyyymmdd) FROM manishm.gx_demo_table
)
```
In PySpark, read the max partition value first and filter the DataFrame before creating the `SparkDFDataset` so the
validation always targets the very latest partition regardless of the column naming convention.

If your table partitions by `snapshot_date` or numeric `yyyymmdd`, swap `ingestion_date` for the correct column:
```sql
WHERE snapshot_date = (
  SELECT MAX(snapshot_date) FROM manishm.gx_demo_table
)
```
or
```sql
WHERE yyyymmdd = (
  SELECT MAX(yyyymmdd) FROM manishm.gx_demo_table
)
```
In PySpark, read the max partition value first and filter the DataFrame before creating the `SparkDFDataset` so the
validation always targets the very latest partition regardless of the column naming convention.

## Running this suite in Cloudera AI (CAI)

These scripts are designed to be run inside a CAI workspace that already configures Spark, Hive, Kerberos, and the
`go01-aw-dl` CML connection. To onboard external customers:

1. **Prepare the workspace** – create a project with the standard CAI runtime (Python 3.11 + Spark 3.5.1). Upload the
   `gx_demo` repo and ensure the `go01-demo-aws-manishm.keytab` file resides in `/home/cdsw`.
2. **Authenticate** – run `kinit -kt /home/cdsw/go01-demo-aws-manishm.keytab manishm@GO01-DEM.YLCU-ATMI.CLOUDERA.SITE` so
   the CAI driver pod has a valid TGT for Hive/S3 access. CAI already injects the `CDSW_*` env vars referenced by the Core
   ML APIs (`cml.data_v1`).
3. **Execute the GX demo** – trigger `python data_quality_checks_gx_demo.py -o gx_demo_test_output.json` from the project
   root. The script will either reuse the shared Spark session via `cmldata.get_connection("go01-aw-dl")` or fall back to
   a local builder that already configures the Iceberg catalog and Kerberos extensions. Validation results land in
   `gx_demo_test_output.json`.
4. **Interpret the output** – open the generated JSON or inspect `gx_demo_run.log` for the GE statistics (success percent,
   failing expectations, observed values). The job writes the final line `Validation saved to gx_demo_test_output.json`.
5. **Extend to other tables** – copy `data_quality_checks_gx_demo.py`, edit `DEFAULT_CONFIG` for the new schema, and run
   the script with `--config my_overrides.json` and `--query "SELECT ..."` against any CAI cluster-accessible table.

Because CAI already sets the Spark defaults (see `spark-defaults.conf`), you do not need to pass additional JVM
properties. Validating inside CAI ensures the same Kerberos/IDBroker context used by production workloads is applied here
too.
