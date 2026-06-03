from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

schema = StructType([
    StructField("imsi", StringType(), False),
    StructField("laccell", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("procedureStartTime", LongType(), False),
])

spark = (
    SparkSession.builder
    .appName("ReadTelecomHdfs")
    .master("local[*]")
    .getOrCreate()
)

input_path = "hdfs://hadoop:9000/flume/telecom/*/*/*/*"
df = (
    spark.read
    .option("delimiter", "|")
    .schema(schema)
    .csv(input_path)
)

print("Schema:")
df.printSchema()
print("Sample rows:")
df.show(10, truncate=False)

spark.stop()
