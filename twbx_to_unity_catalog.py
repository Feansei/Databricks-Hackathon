# Databricks notebook source
# MAGIC %md
# MAGIC # Tableau Packaged Workbook (.twbx) -> Unity Catalog
# MAGIC
# MAGIC Extracts the embedded `.hyper` data extract(s) from a Tableau packaged
# MAGIC workbook and writes each table into Unity Catalog. Designed to run
# MAGIC interactively or as a scheduled Databricks Job.
# MAGIC
# MAGIC **Requirements / notes**
# MAGIC - Only works for workbooks with an *extract* data source (embedded `.hyper`
# MAGIC   file). If the workbook uses a *live* connection (e.g. live to Snowflake,
# MAGIC   SQL Server, etc.), there is no data embedded in the .twbx to extract —
# MAGIC   you'd instead need to connect Databricks directly to that source.
# MAGIC - Put the `.twbx` file somewhere the cluster can read it, e.g. a Unity
# MAGIC   Catalog Volume (`/Volumes/catalog/schema/volume/file.twbx`).
# MAGIC - Requires the `tableauhyperapi` package on the cluster (installed below).

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import datetime
import os
import shutil
import zipfile
from pathlib import Path
 
import pandas as pd
from tableauhyperapi import (
    HyperProcess,
    Telemetry,
    Connection,
    Date as HyperDate,
    Timestamp as HyperTimestamp,
)
 
# COMMAND ----------
 
# ---- Job parameters (settable via Databricks widgets / job params) ----
dbutils.widgets.text("twbx_path", "/Volumes/main/default/tableau_uploads/workbook.twbx")
dbutils.widgets.text("target_catalog", "main")
dbutils.widgets.text("target_schema", "tableau_extracts")
dbutils.widgets.text("write_mode", "overwrite")  # overwrite | append
dbutils.widgets.text("extract_dir", "/tmp/tableau_extract")

twbx_path = dbutils.widgets.get("twbx_path")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
write_mode = dbutils.widgets.get("write_mode")
extract_dir = dbutils.widgets.get("extract_dir")
 
# COMMAND ----------
 
def extract_hyper_files(twbx_path: str, extract_dir: str):
    """Unzip the .twbx and return paths to any .hyper files found inside."""
    if not os.path.exists(twbx_path):
        raise FileNotFoundError(f"Workbook not found at {twbx_path}")
 
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir, exist_ok=True)
 
    with zipfile.ZipFile(twbx_path, "r") as z:
        z.extractall(extract_dir)
 
    hyper_files = list(Path(extract_dir).rglob("*.hyper"))
    return hyper_files
 
 
def sanitize_name(name: str) -> str:
    """Make a Tableau table/schema name safe for Unity Catalog identifiers."""
    clean = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    clean = clean.strip("_").lower()
    return clean or "unnamed_table"
 
 
def _convert_value(value):
    """Convert Hyper API's custom Date/Timestamp objects to native Python
    types so pandas/PyArrow/Spark can handle them correctly."""
    if isinstance(value, HyperDate):
        return datetime.date(value.year, value.month, value.day)
    if isinstance(value, HyperTimestamp):
        return datetime.datetime(
            value.year, value.month, value.day,
            value.hour, value.minute, value.second, value.microsecond,
        )
    return value
 
 
def hyper_to_pandas(hyper_path: str) -> dict:
    """Read every table in a .hyper file into a dict of {name: pandas.DataFrame}."""
    tables = {}
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters={"log_dir": "/tmp"},) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path) as connection:
            schema_names = connection.catalog.get_schema_names()
            for schema in schema_names:
                table_names = connection.catalog.get_table_names(schema=schema)
                for table in table_names:
                    table_def = connection.catalog.get_table_definition(table)
                    columns = [col.name.unescaped for col in table_def.columns]
                    rows = connection.execute_list_query(query=f"SELECT * FROM {table}")
                    rows = [[_convert_value(v) for v in row] for row in rows]
                    df = pd.DataFrame(rows, columns=columns)
                    tables[table.name.unescaped] = df
    return tables
 
 
def dedupe_columns(columns) -> list:
    """Sanitize column names and make sure no two collide after sanitizing."""
    seen = {}
    result = []
    for col in columns:
        name = sanitize_name(col)
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        result.append(name)
    return result
 
 
def write_to_unity_catalog(df_pandas: pd.DataFrame, table_name: str, catalog: str, schema: str, mode: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    df_pandas = df_pandas.copy()
    df_pandas.columns = dedupe_columns(df_pandas.columns)
    spark_df = spark.createDataFrame(df_pandas)
    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    spark_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(full_table_name)
    print(f"Wrote {full_table_name} ({spark_df.count()} rows, {len(spark_df.columns)} cols)")
 
 
# COMMAND ----------
 
def main():
    print(f"Extracting {twbx_path} ...")
    hyper_files = extract_hyper_files(twbx_path, extract_dir)
 
    if not hyper_files:
        raise RuntimeError(
            "No .hyper files found inside this workbook. It likely uses a live "
            "connection rather than an embedded extract, so there's no data "
            "to pull out of the file itself."
        )
 
    created_tables = []  # track fully qualified table names for downstream tasks
 
    for hyper_file in hyper_files:
        print(f"\nReading hyper extract: {hyper_file}")
        tables = hyper_to_pandas(str(hyper_file))
 
        if not tables:
            print(f"  No tables found in {hyper_file}")
            continue
 
        for raw_name, df in tables.items():
            table_name = sanitize_name(raw_name)
            write_to_unity_catalog(df, table_name, target_catalog, target_schema, write_mode)
            created_tables.append(f"{target_catalog}.{target_schema}.{table_name}")
 
    # Expose created table names as task values so downstream tasks can reference them
    # via {{tasks.Extract_Data_Source_from_Tableau_Workbook.values.created_tables}}
    dbutils.jobs.taskValues.set(key="created_tables", value=", ".join(created_tables))
    dbutils.jobs.taskValues.set(key="twbx_path", value=twbx_path)
    dbutils.jobs.taskValues.set(key="uc_schema", value=f"{target_catalog}.{target_schema}")
    print(f"\nTask values set — created_tables: {created_tables}")
    print("\nDone.")
 
 
main()