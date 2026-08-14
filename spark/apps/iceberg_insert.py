from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Insert Iceberg")

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


spark.sql("""
    INSERT INTO local.bhxh.masters VALUES
    ('BH000001', 'Nguyen Van An', 'M', '01', 25),
    ('BH000002', 'Tran Thi Binh', 'F', '02', 31),
    ('BH000003', 'Le Van Cuong', 'M', '01', 42),
    ('BH000004', 'Pham Thi Dung', 'F', '03', 28),
    ('BH000005', 'Hoang Van Em', 'M', '04', 35)
""")


print("========== DATA ==========")

spark.sql("""
    SELECT *
    FROM local.bhxh.masters
""").show()


spark.stop()