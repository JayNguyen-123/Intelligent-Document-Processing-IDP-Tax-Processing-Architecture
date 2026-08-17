import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def create_table_if_not_exists(client, project_id, dataset_id, table_id):
    """Verifies existence of and builds the schema-enforced target BigQuery data warehouse."""
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # 1. Verify Dataset existence container block structure, create if missing
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Found existing BigQuery dataset reference: '{dataset_id}'")
    except NotFound:
        print(f"✨ Dataset '{dataset_id}' not found. Initializing analytics dataset space...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Modify region configuration here if necessary
        client.create_dataset(dataset)
        print(f"✅ Successfully created BigQuery dataset: '{dataset_id}'")

    # 2. Define the schema layout definitions mapping tax document abstractions
    schema = [
        bigquery.SchemaField("employer_name", "STRING", mode="NULLABLE", description="Legal name of filing organization"),
        bigquery.SchemaField("employer_ein", "STRING", mode="NULLABLE", description="Federal Employer Identification Number"),
        bigquery.SchemaField("wages", "NUMERIC", mode="NULLABLE", description="Total gross taxable income reported"),
        bigquery.SchemaField("fed_income_tax_withheld", "NUMERIC", mode="NULLABLE", description="Federal withholding value pulled"),
        bigquery.SchemaField("tax_year", "INTEGER", mode="NULLABLE", description="Filing year code metric identifier"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE", description="System timestamp marker record row insertion")
    ]

    # 3. Check for structural table instance, build if missing
    try:
        client.get_table(table_ref)
        print(f"✅ Target BigQuery analytics table '{table_id}' is ready.")
    except NotFound:
        print(f"✨ Table '{table_id}' not found. Compiling schema fields matrix...")
        table = bigquery.Table(table_ref, schema=schema)
        
        # Configure table behavior to support streaming buffers
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="processed_at"  # Optimizes big table queries slicing time domains
        )
        
        client.create_table(table)
        print(f"✅ Successfully deployed schema-enforced table: '{table_id}'")

def insert_verified_record_to_bigquery(project_id, dataset_id, table_id, row_data):
    """Streams data dictionaries straight into BigQuery columns."""
    client = bigquery.Client(project=project_id)
    
    # Ensure system environment targets exist structurally
    create_table_if_not_exists(client, project_id, dataset_id, table_id)
    
    table_ref = client.dataset(dataset_id).table(table_id)
    
    print(f"📈 Attempting streaming insertion row into {dataset_id}.{table_id}...")
    errors = client.insert_rows_json(table_ref, [row_data])
    
    if not errors:
        print("✅ Structured extraction record securely written to BigQuery warehouse grid.")
        return True
    else:
        print(f"❌ Failed to append streaming telemetry matrix: {errors}")
        return False

if __name__ == "__main__":
    # Test script block to verify connection configurations manually
    TEST_PROJECT = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
    TEST_DATASET = "tax_processing_ds"
    TEST_TABLE = "verified_w2_records"
    
    # Instantiate standalone execution connection check
    print("🧪 Running local infrastructure verification test sequence...")
    client_test = bigquery.Client(project=TEST_PROJECT)
    create_table_if_not_exists(client_test, TEST_PROJECT, TEST_DATASET, TEST_TABLE)
