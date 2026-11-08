import yaml
import os

def load_yaml_schema(yaml_path: str = "gestao_manutencao_industrial_schema.yaml") -> dict:
    """Carrega o arquivo YAML com os metadados das tabelas."""
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"Arquivo YAML não encontrado: {yaml_path}")
    
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
