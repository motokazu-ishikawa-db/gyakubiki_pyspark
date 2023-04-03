# Databricks notebook source
# MAGIC %md
# MAGIC # 逆引きPySpark 5.結合編
# MAGIC Apache Spark 3.3のPySparkに準拠しています。一部、Databricks特有の関数を使っています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの準備
# MAGIC 以下のセルを実行してください。

# COMMAND ----------

df_ramen = spark.createDataFrame([(1, "醤油ラーメン", 600),(2, "塩ラーメン", 700),(3, "豚骨醤油ラーメン", 900),(4, "味噌ラーメン", 800)],("ramen_id","name","price"))
display( df_ramen )

df_topping = spark.createDataFrame([(1, "チャーシュー"),(2, "味玉"),(3, "チャーシュー"),(3, "ニンニク")],("ramen_id","name"))
display( df_topping )

df_ramen_2 = spark.createDataFrame([(5, 700, "鶏白湯ラーメン"),(6, 800, "濃厚とんこつラーメン"),(7, 600, "塩つけめん")],("ramen_id","price","name"))
display( df_ramen_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5-1 Join

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-1-1 内部結合 (INNER JOIN)

# COMMAND ----------

df_5_1_1 = df_ramen.join( df_topping, "ramen_id", "inner" )

# 結合に使う列をデータフレームごとにそれぞれ以下のように指定することもできます
# df_5_1_1 = df_ramen.join( df_topping, df_ramen.ramen_id == df_topping.ramen_id, "inner" )

display( df_5_1_1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-1-2 左外部結合 (LEFT OUTER JOIN)

# COMMAND ----------

df_5_1_2 = df_ramen.join( df_topping, "ramen_id", "left" )

# 結合に使う列をデータフレームごとにそれぞれ以下のように指定することもできます
# df_5_1_2 = df_ramen.join( df_topping, df_ramen.ramen_id == df_topping.ramen_id, "left" )

display( df_5_1_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-1-3 クロス結合

# COMMAND ----------

df_5_1_3 = df_ramen.crossJoin( df_topping )
display( df_5_1_3 )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5-2 Union

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-2-1 Union データフレームを縦方向に結合する

# COMMAND ----------

df_5_2_1 = df_ramen.union( df_ramen_2 )
display( df_5_2_1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-2-2 列名を考慮してUnion （データフレームを縦方向に結合する）

# COMMAND ----------

df_5_2_1 = df_ramen.unionByName( df_ramen_2 )
display( df_5_2_1 )
