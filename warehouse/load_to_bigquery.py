"""Provision / migrate the BigQuery warehouse for the IDP tax pipeline.

Run once per environment (and again after schema changes):

    GCP_PROJECT_ID=my-proj python warehouse/load_to_bigquery.py

* Creates the dataset and the partitioned + clustered table if missing.
* Additively migrates an existing table (adds new NULLABLE columns; never drops).
* Creates/refreshes the Looker Studio view from looker_schema.sql.

Row inserts are NOT done here: the ingestion function and the HITL dashboard
stream rows with insertId=document_id using shared/tax_record.to_bq_row.
"""
from __future__ import annotations

import os
import pathlib
import re
import sys

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "shared"))
import tax_record as tr  # noqa: E402

SCHEMA = [
    bigquery.SchemaField("document_id", "STRING", description="Deterministic id: sha256(bucket/object#generation)"),
    bigquery.SchemaField("employer_name", "STRING", description="Box c - employer legal name"),
    bigquery.SchemaField("employer_ein", "STRING", description="Box b - employer EIN, NN-NNNNNNN"),
    bigquery.SchemaField("wages", "NUMERIC", description="Box 1 - wages, tips, other compensation"),
    bigquery.SchemaField("fed_income_tax_withheld", "NUMERIC", description="Box 2 - federal income tax withheld"),
    bigquery.SchemaField("tax_year", "INT64", description="Tax year of the form"),
    bigquery.SchemaField("review_source", "STRING", description="AUTOMATED or HUMAN"),
    bigquery.SchemaField("reviewed_by", "STRING", description="Reviewer email for HUMAN rows"),
    bigquery.SchemaField("low_confidence_fields", "STRING", mode="REPEATED", description="Fields below threshold at ingestion"),
    bigquery.SchemaField("source_bucket", "STRING", description="Raw upload bucket"),
    bigquery.SchemaField("source_file", "STRING", description="Raw upload object name"),
    bigquery.SchemaField("schema_version", "STRING", description="Payload contract version"),
    bigquery.SchemaField("processed_at", "TIMESTAMP", description="UTC time the row was written"),
]
assert tuple(f.name for f in SCHEMA) == tr.BQ_COLUMNS, "SCHEMA out of sync with tax_record.BQ_COLUMNS"


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(ref)
        print(f"dataset {dataset_id}: exists")
    except NotFound:
        ds = bigquery.Dataset(ref)
        ds.location = location
        client.create_dataset(ds)
        print(f"dataset {dataset_id}: created in {location}")


def ensure_table(client: bigquery.Client, dataset_id: str, table_id: str) -> None:
    full_id = f"{client.project}.{dataset_id}.{table_id}"
    try:
        table = client.get_table(full_id)
    except NotFound:
        table = bigquery.Table(full_id, schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="processed_at")
        table.clustering_fields = ["tax_year", "employer_ein"]
        table.description = "Verified W-2 records (automated + human-reviewed). Query via vw_looker_tax_analytics for de-duplicated data."
        client.create_table(table)
        print(f"table {table_id}: created")
        return

    existing = {f.name for f in table.schema}
    to_add = [f for f in SCHEMA if f.name not in existing]
    if to_add:
        table.schema = list(table.schema) + to_add
        client.update_table(table, ["schema"])
        print(f"table {table_id}: added columns {[f.name for f in to_add]}")
    else:
        print(f"table {table_id}: schema up to date")


def ensure_view(client: bigquery.Client, dataset_id: str) -> None:
    sql_path = pathlib.Path(__file__).with_name("looker_schema.sql")
    sql = sql_path.read_text().replace("YOUR_PROJECT_ID", client.project).replace("tax_processing_ds", dataset_id)
    view_sql = re.search(r"(CREATE OR REPLACE VIEW.*?;)", sql, re.S | re.I)
    if not view_sql:
        raise RuntimeError("view definition not found in looker_schema.sql")
    client.query(view_sql.group(1)).result()
    print("view vw_looker_tax_analytics: created/refreshed")


def main() -> int:
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        print("Set GCP_PROJECT_ID", file=sys.stderr)
        return 2
    dataset = os.environ.get("BQ_DATASET", "tax_processing_ds")
    table = os.environ.get("BQ_TABLE", "verified_w2_records")
    location = os.environ.get("BQ_LOCATION", "US")

    client = bigquery.Client(project=project)
    ensure_dataset(client, dataset, location)
    ensure_table(client, dataset, table)
    if table == "verified_w2_records":
        ensure_view(client, dataset)
    else:
        print("custom BQ_TABLE set: edit looker_schema.sql and create the view manually")
    return 0


if __name__ == "__main__":
    sys.exit(main())
