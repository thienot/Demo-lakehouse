from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Create Iceberg")

    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        "s3a://warehouse/iceberg"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minio"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minio123"
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)


print("--CREATE DATABASE--")

spark.sql("""
    CREATE DATABASE IF NOT EXISTS local.bhxh
""")


print("========== CREATE ICEBERG TABLE ==========")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bhxh.masters (
        SO_SO_BHXH STRING,
        HO_TEN STRING,
        GIOI_TINH STRING,
        MA_TINH STRING,
        TUOI INT
    )
    USING iceberg
""")


print("--SHOW TABLES--")

spark.sql("""
    SHOW TABLES IN local.bhxh
""").show()


spark.stop()