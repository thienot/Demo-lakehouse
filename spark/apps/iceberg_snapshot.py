from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Snapshot Iceberg")

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


print("--SNAPSHOTS--")

spark.sql("""
    SELECT *
    FROM local.bhxh.masters.snapshots
""").show(truncate=False)


spark.stop()