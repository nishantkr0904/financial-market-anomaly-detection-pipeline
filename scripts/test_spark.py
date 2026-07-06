from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FinancialPipeline")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "Apple"), (2, "Tesla")],
    ["id", "company"]
)

df.show()

spark.stop()
