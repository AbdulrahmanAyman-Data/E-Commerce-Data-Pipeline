import os
from pyspark.sql import SparkSession
from ecommerce_pipeline.common.config_loader import load_config

def get_spark_session(app_name: str = None) -> SparkSession:
    config = load_config
    spark_cfg = config.get("spark", {})
    hdfs_cfg = config.get("hdfs", {})

    name = app_name or spark_cfg.get("app_name", "ecommerce-pipeline")

    namenode = hdfs_cfg.get("namenode_uri", "hdfs://master:9000")
    hive_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

    spark =(
        SparkSession.builder
        .appName(name)
        .config("spark.sql.shuffle.partitions",
                spark_cfg.get("shuffle_partitions", 2))
        .config("spark.hadoop.fs.defaultFS",namenode)
        .config("hive.metastore.uris", hive_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark