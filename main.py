import os
import time
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from bq_schemas import BigQuerySchemaManager

# Variáveis de ambiente configuradas no Cloud Run Job
PROJECT_ID = os.getenv("GCP_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET", "gestao_manutencao")
BUCKET_NAME = os.getenv("GCS_BUCKET", "meu-bucket")
INPUT_PREFIX = os.getenv("GCS_INPUT_PREFIX", "raw/")
PROCESSED_PREFIX = os.getenv("GCS_PROCESSED_PREFIX", "processed/")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "gestao_manutencao_industrial_schema.yaml")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
schema_manager = BigQuerySchemaManager(SCHEMA_PATH)

def ensure_dataset():
    dataset_ref = bigquery.DatasetReference(bq_client.project, DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = BQ_LOCATION
    bq_client.create_dataset(dataset, exists_ok=True)

def ensure_table(table_name: str, schema: list[bigquery.SchemaField]):
    table_ref = f"{bq_client.project}.{DATASET_ID}.{table_name}"
    table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table, exists_ok=True)

def write_log(table_name: str, file_path: str, status: str, rows: int = 0, error: str = None, duration: float = 0.0):
    log_table_id = f"{bq_client.project}.{DATASET_ID}.ingestion_logs"
    ensure_table("ingestion_logs", schema_manager.get_log_schema())

    rows_data = [{
        "log_id": str(uuid.uuid4()),
        "execution_timestamp": datetime.now(timezone.utc).isoformat(),
        "table_name": table_name,
        "source_file": file_path,
        "status": status,
        "rows_inserted": rows,
        "error_message": error,
        "duration_seconds": round(duration, 3)
    }]
    bq_client.insert_rows_json(log_table_id, rows_data)

def move_to_processed(bucket_name: str, blob_name: str, prefix: str):
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    file_name = os.path.basename(blob_name)
    dest_name = f"{prefix.rstrip('/')}/{file_name}"

    bucket.copy_blob(source_blob, bucket, dest_name)
    source_blob.delete()
    return dest_name

def process_file(blob_name: str, table_name: str):
    start_time = time.time()
    source_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    print(f"Iniciando ingestão de {source_uri} para {DATASET_ID}.{table_name}...")

    try:
        schema = schema_manager.get_schema(table_name)
        ensure_table(table_name, schema)

        table_ref = f"{bq_client.project}.{DATASET_ID}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )

        load_job = bq_client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
        load_job.result()

        rows = load_job.output_rows or 0
        new_path = move_to_processed(BUCKET_NAME, blob_name, PROCESSED_PREFIX)
        duration = time.time() - start_time

        write_log(table_name, blob_name, "SUCCESS", rows=rows, duration=duration)
        print(f"Sucesso: {rows} linhas inseridas. Arquivo movido para {new_path} ({duration:.2f}s)")

    except Exception as e:
        duration = time.time() - start_time
        write_log(table_name, blob_name, "ERROR", error=str(e), duration=duration)
        print(f"Erro ao processar {blob_name}: {e}")

def run():
    ensure_dataset()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=INPUT_PREFIX)

    for blob in blobs:
        if not blob.name.endswith(".csv"):
            continue

        for table in schema_manager.table_names:
            if table in blob.name.lower():
                process_file(blob.name, table)
                break

if __name__ == "__main__":
    run()
