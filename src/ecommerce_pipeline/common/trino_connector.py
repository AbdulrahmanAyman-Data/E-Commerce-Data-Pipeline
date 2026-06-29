import trino
from ecommerce_pipeline.common.config_loader import load_config

def get_trino_connection():
    config = load_config()
    trino_cfg = config.get("trino", {})
    return trino.dbapi.connect(
        host = trino_cfg.get("host", "trino"),
        port = trino_cfg.get("port", 8080),
        user = trino_cfg.get("user", "admin"),
        catalog = trino_cfg.get("catalog", "hive"),
        schema = trino_cfg.get("database", "ecommerce_gold")
    )

def run_trino_query(sql: str) -> list:
    conn = get_trino_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    
    finally:
        cursor.close()
        conn.close()