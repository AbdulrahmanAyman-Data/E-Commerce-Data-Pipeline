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

def transform(spark, bronze_path: str, silver_path: str):
    input_path = f"{bronze_path}/distribution_centers/distribution_centers.csv"
    output_path = f"{silver_path}/distribution_centers"

    schema = load_spark_schema("distribution_centers")

    logger.info(f"Reading from: {input_path}")
    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(input_path)
    )

    df_clean = (
        df
        .dropDuplicates(["id"])
        .filter(F.col("id").isNotNull())
        .filter(
            (F.col("latitude").between(-90, 90))&
            (F.col("longitude").between(-180, 180))
        )
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("ingestion_date", F.current_date())
    )

    logger.info(f"Writing {df_clean.count()} rows to: {output_path} ")
    df_clean.write.mode("overwrite").parquet(output_path)
    logger.info("✅ distribution_centers Silver done!")


def run():
    config = load_config()
    hdfs = config.get("hdfs", {})
    namenode = hdfs.get("namenode_uri", "hdfs://master:9000")
    bronze = namenode + hdfs.get("bronze_path", "/data/bronze")  
    silver = namenode + hdfs.get("silver_path", "/data/silver")

    spark = get_spark_session("silver-distribution-centers")
    transform(spark, bronze, silver)
    spark.stop()

if __name__ == "__main__":
    run()