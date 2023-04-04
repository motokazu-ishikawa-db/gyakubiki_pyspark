# Databricks notebook source
# MAGIC %md
# MAGIC # 逆引きPySpark 1.基本操作編
# MAGIC Apache Spark 3.3のPySparkに準拠しています。一部、Databricks特有の関数を使っています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-1 ファイルからデータフレームにデータを読み込む (入力)
# MAGIC [spark.read](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html)オブジェクトを使って、ファイルからデータを読み込みます。

# COMMAND ----------

df = ( spark
       .read
       .format("csv")
       .option("header","true")
       .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/") )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-2 カラムを追加する
# MAGIC [withColumn()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html)で、データフレームに新しいカラムを追加できます。

# COMMAND ----------

from pyspark.sql import functions as f
df_2 = df.withColumn("pickup_date", f.to_date("pickup_datetime"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-3 特定の条件にあてはまるデータを取り出す
# MAGIC [filter()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html)で、データフレームから、特定の条件を持つデータだけからなる新しいデータフレームを作ります。

# COMMAND ----------

# 例文その１
from pyspark.sql import functions as F
df_3 = df_2.filter( F.col("trip_distance") > 10 )

# COMMAND ----------

# 例文その２
df_3 = df_2.filter( "trip_distance > 10" )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-4 データ件数を集計する
# MAGIC [count()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html)で、データフレームの中のデータ件数を表示できます。

# COMMAND ----------

df_3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-5 カラムを削除する
# MAGIC [drop()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html)で、指定したカラムを削除できます。

# COMMAND ----------

df_4 = df_3.drop( "pickup_datetime" )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-6 データフレーム同士を結合する（join）
# MAGIC [join()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)で、データフレーム同士を結合します。

# COMMAND ----------

# 結合相手のデータを別途読み込みます
df_code = spark.read.format("csv").option("header","true").load("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")

# COMMAND ----------

df_5 = df_4.join( df_code, df_4.rate_code == df_code.RateCodeID )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-7 値ごとの件数をカウントして降順に整列する
# MAGIC [groupby()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.groupby.html)で、カラムの値ごとに集約し、[count()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.count.html)で件数を集計し、[orderBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html)で降順に整列します。

# COMMAND ----------

display( df_5.groupby("vendor_id").count().orderBy("count", ascending=False) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-8 データフレームからデータを書き出す（出力）
# MAGIC [データフレームのwriteインターフェース](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)を使って、ファイルにデータを出力します。
# MAGIC ご利用の環境でのファイルパスをsave()に指定してから、コメントを外して実行してください

# COMMAND ----------

#( df.write
#    .format("parquet")
#    .mode("append")
#    .save(<ファイルパス>) )
