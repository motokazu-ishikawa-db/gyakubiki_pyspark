# Databricks notebook source
# MAGIC %md
# MAGIC # 逆引きPySpark 4.データ分析編
# MAGIC Apache Spark 3.3のPySparkに準拠しています。一部、Databricks特有の関数を使っています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの準備
# MAGIC 以下のセルを実行してください。

# COMMAND ----------

df = ( spark
      .read
      .format("csv")
      .schema("artist_id string, artist_latitude double, artist_longitude double, artist_location string, artist_name string, duration double, end_of_fade_in double, key int, key_confidence double, loudness double, release string, song_hotnes double, song_id string, start_of_fade_out double, tempo double, time_signature double, time_signature_confidence double, title string, year double, partial_sequence int")
      .option("header", False)
      .option("delimiter", "\t")
      .load("dbfs:/databricks-datasets/songs/data-001") )

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4-1 欠損値

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-1 カラムごとの欠損率を求める（方法その１)

# COMMAND ----------

from pyspark.sql import functions as F
( df.select([F.round((F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) * 100 / df.count()),2).alias(c) for c in df.columns])
    .select( "duration", "end_of_fade_in", "key" )
    .show() )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-2 カラムごとの欠損率を求める（Databricks限定機能を利用)

# COMMAND ----------

# 以下のコマンド実行後に、画面上でデータプロファイルの新規タブを作成します
display( df )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-3 カラムごとの欠損値の数を求める

# COMMAND ----------

from pyspark.sql import functions as F
display( df.select([F.round((F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) * 100 ),2).alias(c) for c in df.columns])
    .select( "duration", "end_of_fade_in", "key" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-4 特定のカラムの値が欠損していない行を取得する

# COMMAND ----------

from pyspark.sql import functions as F
df_4_1_4 = df.filter( F.col("artist_latitude").isNotNull() )
display( df_4_1_4 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-5 特定のカラムの値が欠損している行を取得する

# COMMAND ----------

from pyspark.sql import functions as F
df_4_1_5 = df.filter( F.col("artist_latitude").isNull() )
display( df_4_1_5 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-6 欠損値のある行を除外する

# COMMAND ----------

# 例文1(特定の複数カラムが全て欠損している場合に除外する)
df_4_1_6_1 = df.dropna( how="all", subset=[ "artist_latitude", "artist_longitude", "artist_location" ] )
display( df_4_1_6_1 )

# 例文2(特定の複数カラムのいずれかが欠損している場合に除外する)
df_4_1_6_2 = df.dropna( how="any", subset=[ "artist_latitude", "artist_longitude", "artist_location" ] )
display( df_4_1_6_2 )

# 例文3(特定の複数カラムのうち欠損していないカラム数がN個以上の場合に残す)
df_4_1_6_3 = df.dropna( thresh=1, subset=[ "artist_latitude", "artist_longitude", "artist_location" ] )
display( df_4_1_6_3 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-7 欠損値を特定の値で置換する

# COMMAND ----------

# 例文1(１つ以上のカラムの欠損値を同じ値で置換する)
df_4_1_7_1 = df.fillna( 0.0, subset=[ "duration", "key", "loudness" ] )
display( df_4_1_7_1 )

# 例文2(複数のカラムの欠損値をカラムごとにそれぞれ別々の値で置換する)
df_4_1_7_2 = df.fillna( {"artist_latitude":40.730610, "artist_longitude":-73.935242, "artist_location":"NYC"} )
display( df_4_1_7_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-1-8 欠損値を平均値(中央値)で置換する

# COMMAND ----------

from pyspark.ml.feature import Imputer

# 例文1(欠損値を平均値で置換する)
imputer_mean = Imputer(
  strategy="mean",
  inputCols=[ "song_hotnes", "key", "loudness" ],
  outputCols=[ "song_hotnes", "key", "loudness" ]
)
df_4_1_8_1 = imputer_mean.fit( df ).transform( df )
display( df_4_1_8_1.select( "song_hotnes", "key", "loudness" ) )

# 例文2(欠損値を中央値で置換する)
imputer_median = Imputer(
  strategy="median",
  inputCols=[ "song_hotnes", "key", "loudness" ],
  outputCols=[ "song_hotnes", "key", "loudness" ]
)
df_4_1_8_2 = imputer_median.fit( df ).transform( df )
display( df_4_1_8_2.select( "song_hotnes", "key", "loudness" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4-2 基本統計量

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-2-1 カラムごとの基本統計量を出力する（方法その１)

# COMMAND ----------

df.select( "song_hotnes", "key", "loudness" ).summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-2-2 カラムごとの基本統計量を出力する（Databricks限定機能を利用)

# COMMAND ----------

# 以下のコマンド実行後に、画面上でデータプロファイルの新規タブを作成します
display( df )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4-3 グループ集計

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-3-1 グループごとの総数を集計して大きい順に整列する

# COMMAND ----------

from pyspark.sql import functions as F
df_4_3_1 = ( df
        .filter( F.col( "artist_location" ).isNotNull() )
        .groupBy( "artist_location" )
        .count()
        .orderBy( "count", ascending=False ) )
display( df_4_3_1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-3-2 クロス集計をしてピボットテーブルを作成する

# COMMAND ----------

from pyspark.sql import functions as F
df_4_3_2 =( df
        .filter( F.col( "artist_location" ).isNotNull() )
        .groupBy( "artist_location" )
        .pivot( "year", [2005, 2006, 2007, 2008, 2009, 2010 ] )
        .count()
        .withColumn( "total", F.col("2005") + F.col("2006") + F.col("2007") + F.col("2008") + F.col("2009") + F.col("2010") )
        .orderBy("total", ascending=False) )
display( df_4_3_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-3-3 グループごとの統計値を集計する

# COMMAND ----------

from pyspark.sql import functions as F
df_4_3_3 = ( df
        .filter( F.col( "artist_location" ).isNotNull() )
        .groupBy( "artist_location" )
        .agg( F.min( "tempo" ).alias( "min_tempo" ),
              F.avg( "tempo" ).alias( "avg_tempo" ),
              F.max( "tempo" ).alias( "max_tempo" ),
              F.count( "tempo" ).alias( "count" ) )
        .orderBy( "count", ascending=False ) )
display( df_4_3_3 )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4-4 ウィンドウ関数

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-4-1 パーティション（グループ)の統計値を取得する

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
df_4_4_1 = ( df
             .filter( F.col( "tempo" ).isNotNull() & F.col( "artist_location" ).isNotNull() )
             .withColumn( "avg_tempo_of_city", F.avg( "tempo" ).over( Window.partitionBy( "artist_location" ) ) )
             .select( "artist_location", "artist_name", "title", "tempo", "avg_tempo_of_city" )
             .filter( F.col( "artist_location" ).startswith( "U" ) ))
display( df_4_4_1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-4-2 パーティション（グループ）内での値の順位を取得する

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
df_4_4_2 = ( df
             .filter( F.col( "tempo" ).isNotNull() & F.col( "artist_location" ).isNotNull() )
             .withColumn( "tempo_rank_in_city", F.rank().over( Window.partitionBy( "artist_location" ).orderBy( F.col("tempo").desc() ) ) )
             .select( "artist_location", "artist_name", "title", "tempo", "tempo_rank_in_city" )
             .filter( F.col( "artist_location" ).startswith( "U" ) ))
display( df_4_4_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-4-3 パーティション（グループ）内で1つ前(後)の値を取得する

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
df_4_4_3 = ( df
             .filter( F.col( "tempo" ).isNotNull() & F.col( "artist_location" ).isNotNull() )
             .withColumn( "previous_tempo_in_city", F.lag( "tempo" ).over( Window.partitionBy( "artist_location" ).orderBy( F.col("tempo").desc() ) ) )
             .withColumn( "next_tempo_in_city", F.lead( "tempo" ).over( Window.partitionBy( "artist_location" ).orderBy( F.col("tempo").desc() ) ) )
             .select( "artist_location", "artist_name", "title", "tempo", "previous_tempo_in_city", "next_tempo_in_city" )
             .filter( F.col( "artist_location" ).startswith( "U" ) ))
display( df_4_4_3 )
