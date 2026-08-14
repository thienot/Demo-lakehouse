from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Write Masters").getOrCreate()

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


#Load          
output_path = "/opt/spark/data/output/master"

#Partition theo SO_SO_BHXH, 200 partition
num_partitions = 200

df_clean = df_clean.withColumn(
    "hash_partition",
    abs(hash(col("SO_SO_BHXH"))) % num_partitions
)
#Lưu file dưới dạng parquet và chia partiton theo cột SO_SO_BHXH
df_clean.write.parquet(output_path, mode="overwrite", partitionBy="hash_partition")

print(f"Output: {output_path}")

spark.stop()