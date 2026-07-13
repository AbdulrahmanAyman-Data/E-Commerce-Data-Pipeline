import json
from pathlib import Path
from ecommerce_pipeline.common.config_loader import load_config
from ecommerce_pipeline.common.hdfs_utils import hdfs_mkdir, hdfs_exists, hdfs_put
from ecommerce_pipeline.common.logger import get_logger

logger = get_logger(__name__)

BATCH_TABLES = [
    "distribution_centers",
    "inventory_items",
    "order_items",
    "orders",
    "products",
    "users"
]


def load_schema(table: str) -> dict:
    schema_path =(
         Path(__file__).resolve().parents[3]
        / "config" / "schemas" / f"{table}_schema.json"
    )
    with open(schema_path, "r") as f:
        return json.load(f)
    

def validate_csv_exists(local_path: Path, table: str) -> bool:
    if not local_path.exists():
        logger.error(f"csv not found: {local_path}")
        return False
    return True


def ingest_table(table: str, raw_dir: Path, bronze_path: str) -> bool:
    local_file = raw_dir / f"{table}.csv"
    hdfs_target = f"{bronze_path}/{table}/{table}.csv" 
    hdfs_dir = f"{bronze_path}/{table}"

    if not validate_csv_exists(local_file, table):
        return False
    
    try:
        schema = load_schema(table)
        logger.info(f"Schema loaded: {table} - {len(schema['columns'])} columns")
    except FileNotFoundError:
        logger.error(f"Schema not found: {table}_schema.json")
        return False
    
    if not hdfs_exists(hdfs_dir):
        logger.info(f"Creating HDFS dir: {hdfs_dir}")
        hdfs_mkdir(hdfs_dir)

    logger.info(f"Uploading {local_file} → {hdfs_target}")
    success = hdfs_put(str(local_file), hdfs_target, overwrite=True)

    if success:
        logger.info(f"✅ {table} uploaded successfully")
    else:
        logger.error(f"❌ {table} upload FAILED")

    return success


def run_bronze_ingestion() -> None:
    config = load_config()
    hdfs_cfg = config.get("hdfs", {})

    namenode = hdfs_cfg.get("namenode_uri", "hdfs://hadoop-master:9000")
    bronze_path = hdfs_cfg.get("bronze_path", "/data/bronze")

    raw_dir = Path(__file__).resolve().parents[3] / "Data" / "raw"

    logger.info("="*50)
    logger.info("Starting bronze ingestion")
    logger.info(f"Source: {raw_dir}")
    logger.info(f"Target: {namenode}{bronze_path}")
    logger.info("="*50)

    hdfs_mkdir(bronze_path)

    results = {}
    for table in BATCH_TABLES:
        results[table] = ingest_table(table, raw_dir, bronze_path)

    logger.info("="*50)
    logger.info("Ingestion Summary:")
    for table, success in results.items():
        status = "✅ OK" if success else "❌ FAILED"
        logger.info(f"{table}: {status}")

    failed = [t for t, s in results.items() if not s ]
    if failed:
        logger.error(f"Failed tables: {failed}")
        raise RuntimeError(f"Ingestion failed for: {failed}")

    logger.info("Bronze Ingestion completed successfully!")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_bronze_ingestion()
