# Databricks notebook source
# MAGIC %md
# MAGIC # 逆引きPySpark 2.日付時刻編
# MAGIC Apache Spark 3.3のPySparkに準拠しています。一部、Databricks特有の関数を使っています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの準備
# MAGIC 以下のセルを実行してください。

# COMMAND ----------

from pyspark.sql import functions as F

# 元データのtimestampはバラツキが少ないため、デモのために乱数を足しています
df = spark.read.format("json").load("dbfs:/databricks-datasets/iot/iot_devices.json").select("device_id","device_name","humidity",((F.col("timestamp")/1000) + F.rand() * 100000000).alias("timestamp") ).withColumn( "time", F.to_timestamp( "timestamp" ) ).withColumn( "date", F.to_date( "time" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-1 現在の取得

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1-1 現在の時刻を取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "current_time_utc", F.current_timestamp() )
display( df.select( "device_id", "current_time_utc" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1-2 現在の日付を取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "today", F.current_date() )
display( df.select( "device_id", "today" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-2 抽出

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-1 時刻から年の値を取り出す

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "year", F.year( "time" ) )
display( df.select( "time", "year" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-2 時刻から月の値を取り出す

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "month", F.month( "time" ) )
display( df.select( "time", "month" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-3 時刻から時の値を取り出す

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "hour", F.hour( "time" ) )
display( df.select( "time", "hour" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-4 時刻から分の値を取り出す

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "minute", F.minute( "time" ) )
display( df.select( "time", "minute" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-5 時刻から秒の値を取り出す

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "second", F.second( "time" ) )
display( df.select( "time", "second" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-6 時刻を特定の単位で切り捨てる

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "date_trunc", F.date_trunc( "month", "time" ) )
display( df.select( "time","date_trunc" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-7 日付を特定の単位で切り捨てる

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "trunc", F.trunc( "date", "month" ) )
display( df.select( "date", "trunc" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-8 日付が年の何日目かに該当するかを求める

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "dayofyear", F.dayofyear( "date" ) )
display( df.select( "date", "dayofyear" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-9 日付が年の何週目に該当するかを求める

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "weekofyear", F.weekofyear( "date" ) )
display( df.select( "date", "weekofyear" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-10 日付が年の第何四半期に該当するかを求める

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "quarter", F.quarter( "date" ) )
display( df.select( "date", "quarter" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-11 日付が月の何日目かに該当するかを求める

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "day", F.dayofmonth( "date" ) )
display( df.select( "date", "day" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-12 日付が週の何日目かに該当するかを求める

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "dayofweek", F.dayofweek( "date" ) )
display( df.select( "date", "dayofweek" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2-13 その月の月末に該当する日付を求める

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "last_day_of_month", F.last_day( "date" ) )
display( df.select( "date", "last_day_of_month" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-3 変換

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-1 UNIX時間から時刻へ変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "time", F.timestamp_seconds("timestamp") )
display( df.select( "timestamp", "time" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-2 時刻からUNIX時間へ変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "unixtime", F.unix_timestamp("time") )
display( df.select( "time", "unixtime" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-3 時刻をUTCから特定のタイムゾーンへ変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "time_jst", F.from_utc_timestamp("time", "Asia/Tokyo") )
display( df.select( "time", "time_jst" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-4 時刻を特定のタイムゾーンからUTCへ変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "time_utc", F.to_utc_timestamp("time_jst", "Asia/Tokyo") )
display( df.select( "time_jst", "time_utc" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-5 時刻/日付から文字列へ変換する
# MAGIC [時刻/日付のフォーマット形式](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "timestamp_string", F.date_format( "time", "yyyyMMdd-HHmmss" ) )
display( df.select( "time", "timestamp_string" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-6 文字列から時刻へ変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "to_timestamp", F.to_timestamp( "timestamp_string", "yyyyMMdd-HHmmss" ) )
display( df.select( "timestamp_string", "to_timestamp" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-7 文字列から日付へ変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "to_date", F.to_date( "timestamp_string", "yyyyMMdd-HHmmss" ) )
display( df.select( "timestamp_string", "to_date" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-8 時刻を日付に変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "date", F.to_date( "time_jst" ) )
display( df.select( "time_jst", "date" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3-9 年、月、日から、日付を作成する

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn( "made_date", F.make_date( "year", "month", "day" ) )
display( df.select( "year", "month", "day", "made_date" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## データ準備

# COMMAND ----------

from pyspark.sql import functions as F

# 元データのtimestampはバラツキが少ないため、デモのために乱数を足しています
df2 = spark.read.format("json").load("dbfs:/databricks-datasets/iot/iot_devices.json").select("device_id","device_name","humidity", F.timestamp_seconds(( F.col("timestamp")/1000) + F.rand() * 100000000).cast("timestamp").alias("start_time"),F.timestamp_seconds((F.col("timestamp")/1000) + F.rand() * 100000000).cast("timestamp").alias("end_time") ).withColumn( "start_date", F.col( "start_time" ).cast( "date" ) ).withColumn( "end_date", F.col( "end_time" ).cast( "date" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-4 日付の足し算、引き算

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4-1 ２つの日付の間の日数を求める

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df2.withColumn( "datediff", F.datediff( "end_date", "start_date" ) )
display( df2.select( "start_date", "end_date", "datediff" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4-2 ２つの日付の間の月数を求める

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df2.withColumn( "months_between", F.months_between( "end_date", "start_date" ) )
display( df2.select( "start_date", "end_date", "months_between" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4-3 次の○曜日の日付を求める（例：次の月曜日の日付）

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df2.withColumn( "next_day_of_end_date", F.next_day( "end_date", "Mon" ) )
display( df2.select( "end_date", "next_day_of_end_date" )  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4-4 ○日後の日付を求める

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df2.withColumn( "three_days_after_end_date", F.date_add( "end_date", 3 ) )
display( df2.select( "end_date", "three_days_after_end_date" )  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4-5 ○日前の日付を求める

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df2.withColumn( "three_days_before_start_date", F.date_sub( "start_date", 3 ) )
display( df2.select( "start_date", "three_days_before_start_date" )  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4-6 ○ヶ月後の日付を求める

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df2.withColumn( "two_months_after_end_date", F.add_months( "end_date", 2 ) )
display( df2.select( "end_date", "two_months_after_end_date" )  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-5 その他

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-5-1 特定の長さのウィンドウについて統計値を集計する

# COMMAND ----------

from pyspark.sql import functions as F

df_group = df2.groupBy( F.window("end_time", "5 days", "1 day" ) ).agg( F.avg("humidity").alias("average_humidity") )
display( df_group )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-5-2 セッションウィンドウについて統計値を集計する

# COMMAND ----------

from pyspark.sql import functions as F

df_group = df2.groupBy( F.session_window( "end_time", "1 hour" ) ).count()
display( df_group )
