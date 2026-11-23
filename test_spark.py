import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataCenterOperationsPlatform")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

data = [
    ("DC01", "SERVER001", 45.5),
    ("DC01", "SERVER002", 78.2),
    ("DC02", "SERVER003", 91.3),
]

columns = ["datacenter_id", "server_id", "cpu_utilization"]

df = spark.createDataFrame(data, columns)

df.show()

print("Total servers:", df.count())

spark.stop()