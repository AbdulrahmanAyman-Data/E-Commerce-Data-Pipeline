import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from ecommerce_pipeline.common.config_loader import load_config
from ecommerce_pipeline.common.spark_session import get_spark_session
from ecommerce_pipeline.common.logger import get_logger

logger = get_logger(__name__)

BATCH_TABLES = [
    "distribution_centers",
    "products",
    "orders",
    "users",
    "order_items",
    "inventory_items",
]


def load_schema(table: str) -> dict:
    schema_path = (
        Path(__file__).resolve().parents[3]
        / "config" / "schemas" / f"{table}_schema.json"
    )
    with open(schema_path, "r") as f:
        return json.load(f)
    

def validate_table(spark: SparkSession, table: str, bronze_path: Path) -> dict:
    result = {
        "table": table,
        "file_exicts": False,
        "column_count_ok": False,
        "pk_null_count": -1,
        "row_count": 0,
        "passed": False
    }

    hdfs_path = f"{bronze_path}/{table}/{table}.csv"

    # ──>> Check 1: file exicts <<──
    try:
        df = spark.read.option("header", "true").csv(hdfs_path)
        if df.first() is None:
            logger.warning(f"{table}: file is empty")
            return result
        result["file_exicts"] = True
    except Exception as e:
        logger.error(f"{table}: file not found - {e}")
        return result

    # ──>> Check 2: column count <<──
    schema = load_schema(table)
    expected_cols = len(schema["columns"])
    actual_cols = len(df.columns)
    result["column_count_ok"] = actual_cols == expected_cols

    if result["column_count_ok"]:
        logger.info(f"{table}: columns OK ({actual_cols}/{expected_cols})")
    else:
        logger.warning(f"{table}: column mismatch — expected {expected_cols}, got {actual_cols}")

    # ──>> Check 3: Primary Key nulls + row count <<──
    pk = schema["primary_key"]
    if pk in df.columns:
        result["pk_null_count"] = df.filter(col(pk).isNull()).count()
        result["row_count"] = df.count()
        logger.info(f"{table}: {result['row_count']:,} rows | PK nulls={result['pk_null_count']}")
    else:
        logger.warning(f"{table}: PK '{pk}' not found in columns")

    result["passed"] = (
        result["file_exicts"]
        and result["column_count_ok"]
        and result["pk_null_count"] == 0
    )

    return result


def run_bronze_validation() -> None:
    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode = hdfs_cfg.get("namenode_uri", "hdfs://master:9000")
    bronze_path = hdfs_cfg.get("bronze_path", "/data/bronze")
    full_bronze = f"{namenode}{bronze_path}"

    spark = get_spark_session("bronze_validation")

    logger.info("=" * 50)
    logger.info("Starting Bronze Validation")
    logger.info(f"Bronze path: {full_bronze}")
    logger.info("=" * 50)

    results = []
    for table in BATCH_TABLES:
        logger.info(f"--- Validating: {table} ---")
        r = validate_table(spark, table, full_bronze)
        results.append(r)

    logger.info("=" * 50)
    logger.info("Validation Summary:")

    failed = []
    for r in results:
        status = "✅ PASSED" if r["passed"] else "❌ FAILED"
        logger.info(
            f"  {r['table']}: {status} "
            f"| rows={r['row_count']:,} "
            f"| cols_ok={r['column_count_ok']} "
            f"| pk_nulls={r['pk_null_count']}"
        )
        if not r["passed"]:
            failed.append(r["table"])

    spark.stop()

    if failed:
        logger.error(f"Failed tables: {failed}")
        raise RuntimeError(f"Bronze validation failed for: {failed}")
    
    logger.info("Bronze Validation passed! ✅")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_bronze_validation()


