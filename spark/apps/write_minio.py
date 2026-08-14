from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

schema_bhxh = StructType([
    StructField("ID", LongType(), nullable=False),
    StructField("NLD_ID", LongType(), nullable=False),

    StructField("SO_SO_BHXH", StringType(), nullable=True),
    StructField("THANG_BD", StringType(), nullable=True),
    StructField("THANG_KT", StringType(), nullable=True),

    StructField("TT_TG_BHXH", StringType(), nullable=True),
    StructField("DT_TG_BHXH", StringType(), nullable=True),
    StructField("NAM_TG_BHXH", IntegerType(), nullable=True),
    StructField("THANG_TG_BHXH", IntegerType(), nullable=True),

    StructField("NAM_TG_BHXH_BB", IntegerType(), nullable=True),
    StructField("THANG_TG_BHXH_BB", IntegerType(), nullable=True),

    StructField("TT_TG_BHTN", StringType(), nullable=True),
    StructField("DT_TG_BHTN", StringType(), nullable=True),
    StructField("NAM_TG_BHTN", IntegerType(), nullable=True),
    StructField("THANG_TG_BHTN", IntegerType(), nullable=True),

    StructField("TT_TG_BHYT", StringType(), nullable=True),
    StructField("DT_TG_BHYT", StringType(), nullable=True),
    StructField("NAM_TG_BHYT", IntegerType(), nullable=True),
    StructField("THANG_TG_BHYT", IntegerType(), nullable=True),

    StructField("NAM_NO_BHXH", IntegerType(), nullable=True),
    StructField("THANG_NO_BHXH", IntegerType(), nullable=True),

    StructField("NAM_NO_BHTN", IntegerType(), nullable=True),
    StructField("THANG_NO_BHTN", IntegerType(), nullable=True),

    StructField("TT_TG_BH", StringType(), nullable=True),
    StructField("DT_TG_BH", StringType(), nullable=True),

    StructField("TU_THANG_DVI", StringType(), nullable=True),
    StructField("DEN_THANG_DVI", StringType(), nullable=True),
    StructField("DEN_THANG_HTTT", StringType(), nullable=True),
    StructField("DEN_THANG_BHTN", StringType(), nullable=True),

    StructField("THANG_BD_LT", StringType(), nullable=True),
    StructField("THANG_KT_LT", StringType(), nullable=True),
    StructField("SO_THANG_LT", IntegerType(), nullable=True),

    StructField("IS_ERRORS", IntegerType(), nullable=True),
    StructField("NGHI_VIEC", IntegerType(), nullable=True),
    StructField("IS_CONTINUE", IntegerType(), nullable=True),
    StructField("TRUY_DONG", IntegerType(), nullable=True),

    StructField("DEN_NGAY", StringType(), nullable=True),

    StructField("MA_CD", StringType(), nullable=True),
    StructField("MA_NHH", StringType(), nullable=True),
    StructField("DD_MA_DON_VI", StringType(), nullable=True),

    StructField("DD_THANG_DONG_DEN_XH", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_BHXH", DecimalType(10, 4), nullable=True),

    StructField("DD_THANG_DONG_DEN_YT", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_BHYT", DecimalType(10, 4), nullable=True),

    StructField("DD_THANG_DONG_DEN_TN", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_BHTN", DecimalType(10, 4), nullable=True),

    StructField("DD_THANG_DONG_DEN_TNLD", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_TNLD", DecimalType(10, 4), nullable=True),

    StructField("RAW_RESPONSE", StringType(), nullable=True),

    StructField(
        "CREATED_AT",
        TimestampType(),
        nullable=False,
    ),
])


spark = (
    SparkSession.builder
    .appName("Write MinIO")
    
    #Cấu hình config cho MinIO
    .config(
        "spark.hadoop.fs.s3a.endpoint", #nói với spark S3 endpoint nằm ở MinIO http://minio:9000
        "http://minio:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key", #đoạn này để biết username/access key để truy cập
        "minio"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key", #đoạn này để biết password/secret key để truy cập
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


#Đọc file raw master
#Extract
df = spark.read.csv("/opt/spark/data/raw/RAW_QTTG_BHXH.csv", header=True, schema=schema_bhxh)
print("--Raw--")
df.count()

#Transform
window = Window.partitionBy("SO_SO_BHXH").orderBy(desc("CREATED_AT"), desc("ID"))

#Lọc dữ liệu để lấy bản ghi mới nhất của master(SO_SO_BHXH) theo cột created_at, ID
#Dùng row_number để có thể lấy được vị trí mới nhất, sau đó lọc theo yêu cầu
df_clean = df.withColumn("rank", row_number().over(window))\
            .filter(
                (col("rank") == 1)  &
                when(col("NLD_ID").isNotNull(), True) &
                when(col("SO_SO_BHXH").isNotNull(), True)
            ) \
            .withColumn("NLD_ID", trim(col("NLD_ID"))) \
            .withColumn("SO_SO_BHXH", trim(col("SO_SO_BHXH")))

print("--Clean--")
df_clean.count()


#Viết data xuống MinIO
#Đường dẫn lưu vào MinIO
output_path = "s3a://warehouse/bronze/masters"


#Lưu file dưới dạng parquet
df_clean.write.parquet(output_path, mode="overwrite")
print("--Write--")
print(output_path)


spark.stop()