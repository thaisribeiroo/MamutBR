from google.cloud import bigquery
from schema_loader import load_yaml_schema

class BigQuerySchemaManager:
    """
    Classe que converte a definição das tabelas do YAML
    em schemas nativos do BigQuery (SchemaField).
    """
    TYPE_MAPPING = {
        "int_seq": "INTEGER",
        "int": "INTEGER",
        "str": "STRING",
        "text": "STRING",
        "uuid": "STRING",
        "name": "STRING",
        "email": "STRING",
        "phone": "STRING",
        "float": "FLOAT64",
        "date": "DATE",
    }

    def __init__(self, yaml_path: str = "gestao_manutencao_industrial_schema.yaml"):
        self.raw_schema = load_yaml_schema(yaml_path)
        self.domain = self.raw_schema.get("domain", "gestao_manutencao_industrial")
        self.tables = self.raw_schema.get("tables", {})

    @property
    def table_names(self) -> list[str]:
        return list(self.tables.keys())

    def get_schema(self, table_name: str) -> list[bigquery.SchemaField]:
        table_info = self.tables.get(table_name)
        if not table_info:
            raise ValueError(f"Tabela '{table_name}' não existe no YAML. Disponíveis: {self.table_names}")

        columns = table_info.get("columns", {})
        schema = []

        for col_name, meta in columns.items():
            dtype = meta.get("dtype", "str")
            bq_type = self.TYPE_MAPPING.get(dtype, "STRING")
            mode = "REQUIRED" if meta.get("primary_key") else "NULLABLE"
            
            description = f"Tipo de dados: {dtype}"
            if "foreign_key" in meta:
                fk = meta["foreign_key"]
                description += f" | FK: {fk.get('table')}.{fk.get('column')}"

            schema.append(
                bigquery.SchemaField(
                    name=col_name,
                    field_type=bq_type,
                    mode=mode,
                    description=description
                )
            )
        return schema

    @staticmethod
    def get_log_schema() -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("log_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("execution_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows_inserted", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("duration_seconds", "FLOAT64", mode="NULLABLE"),
        ]
