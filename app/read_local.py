from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, year, month, mean, median, udf

spark = (
    SparkSession.builder.master("local[*]")
    .appName("TaxiDataProcessing")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6",
    )
    .getOrCreate()
)

df_local = spark.read.parquet("./data/result/")

print("---Local Data Schema---".center(50, "#"))
df_local.printSchema()

print("---Local Data Rows---".center(50, "#"))
df_local.limit(10).orderBy("paymentType", "year", "month").show()

print("---Total Rows---".center(50, "#"))
print(df_local.count())

spark.stop()
