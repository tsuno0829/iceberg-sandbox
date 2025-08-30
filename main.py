"""
Simple Iceberg test code using Spark
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect-server:15002").getOrCreate()

print(f"✅ Connected to Spark! Version: {spark.version}")

spark.catalog.setCurrentCatalog("iceberg")

# サンプルデータの作成
data = [(1, 'apple'), (2, 'banana'), (3, 'cherry')]
df = spark.createDataFrame(data, ["id", "data"])

# テーブル名を指定
table_name = "default.fruits"

# DBにあるテーブルを表示
# spark.sql("SHOW TABLES FROM default;").show()

# Icebergテーブルに書き込み
df.writeTo(table_name).createOrReplace()
print(f"✅ Wrote data to {table_name}")

# Icebergテーブルから読み込み
spark.read.table(table_name).show()

# Sparkセッションを停止
spark.stop()
