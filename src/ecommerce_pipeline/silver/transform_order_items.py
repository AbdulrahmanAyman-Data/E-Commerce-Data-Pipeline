import json
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, TimestampType
from ecommerce_pipeline.common.spark_session import get_spark_session
from ecommerce_pipeline.common.config_loader import load_config
from ecommerce_pipeline.common.logger import get_logger

logger = get_logger(__name__)

TYPE_MAP = {
    "integer": IntegerType(),
    "string": StringType(),
    "double": DoubleType(),
    "float": FloatType(),
    "timestamp": TimestampType(),
}

def load_spark_schema(table: str) ->StructType:
    schema_path = (
        Path(__file__).resolve().parents[3]
        / "config" / "schema" / f"{table}_schema.json"
    )
    with open(schema_path) as f:
        schema_def = json.load(f)
    
    fields = []
    for col in schema_def["columns"]:
        spark_type = TYPE_MAP.get(col["type"], StringType())
        fields.append(StructField(col["name"], spark_type, col["nullable"]))

    return StructType(fields)