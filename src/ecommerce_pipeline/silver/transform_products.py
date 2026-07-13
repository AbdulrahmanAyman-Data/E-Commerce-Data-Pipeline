import json
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DoubleType, TimestampType
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


def load_spark_schema(tabel: str) ->StructType:
    schema_path =( 
        Path(__file__).resolve().parents[3]
        / "config" / "schemas" / f"{tabel}_schema.json"
    )
    with open(schema_path) as f:
        schema_def = json.load(f)

    fields = []
    for col in schema_def["columns"]:
        spark_type = TYPE_MAP.get(col["type"], StringType())
        fields.append(StructField(col["name"], spark_type, col["nullable"]))

    return StructType(fields)


def transform(spark, bronze_Path: str, silver_Path: str):
    input_path = f"{bronze_Path}/products/products.csv" 
    output_path = f"{silver_Path}/products"

    schema = load_spark_schema("products")
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
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("department", F.trim(F.col("department")))
        .withColumn("ingestion_date", F.current_date())
    )

    count = df_clean.count()
    logger.info(f"Writing {count:,} rows to: {output_path}")
    df_clean.write.mode("overwrite").parquet(output_path)
    logger.info("✅ products Silver done!")

def run():
    config = load_config()
    hdfs = config.get("hdfs", {})
    namenode = hdfs.get("namenode_uri", "hdfs://master:9000")
    bronze = namenode + hdfs.get("bronze", "/data/bronze")
    silver = namenode + hdfs.get("silver", "/data/silver")

    spark = get_spark_session("silver-products")
    transform(spark, bronze, silver)
    spark.stop()


if __name__ == "__main__":
    run()