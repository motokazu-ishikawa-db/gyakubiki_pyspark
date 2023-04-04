# Databricks notebook source
# MAGIC %md
# MAGIC # 逆引きPySpark 3.文字列編
# MAGIC Apache Spark 3.3のPySparkに準拠しています。一部、Databricks特有の関数を使っています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-1 整形

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-1 大文字にする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "Lionel", "Messi"),(9, "Julian", "Alvarez"),(22, "Lautaro", "Martinez")],("number","first_name","last_name"))

df = ( df.withColumn( "first_name_upper", F.upper("first_name") )
         .withColumn( "last_name_upper", F.upper("last_name") ) )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-2 小文字にする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "Lionel", "Messi"),(9, "Julian", "Alvarez"),(22, "Lautaro", "Martinez")],("number","first_name","last_name"))

df = ( df.withColumn( "first_name_lower", F.lower("first_name") )
         .withColumn( "last_name_lower", F.lower("last_name") ) )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-3 単語の先頭文字を大文字にする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "LIONEL", "MESSI"),(9, "JULIAN", "ALVAREZ"),(22, "LAUTARO", "MARTINEZ")],("number","first_name","last_name"))

df = ( df.withColumn( "first_name_initcap", F.initcap("first_name") )
         .withColumn( "last_name_initcap", F.initcap("last_name") ) )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-4 数値を整形する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "Lionel", "Messi", 41000000),(9, "Julian", "Alvarez", 3130000),(22, "Lautaro", "Martinez", 6375000)],("number","first_name","last_name","salary"))
display( df.withColumn( "salary_formatted", F.format_number( "salary", 2 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-5 書式付きで文字列を出力する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "Lionel", "Messi"),(9, "Julian", "Alvarez"),(22, "Lautaro", "Martinez")],("number","first_name","last_name"))

display( df.withColumn( "string_formatted", F.format_string( "%s's number is %d", "last_name", "number" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-6 左側をパディング（右詰め）する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "Lionel", "Messi"),(9, "Julian", "Alvarez"),(22, "Lautaro", "Martinez")],("number","first_name","last_name"))

display( df.withColumn( "number_padded", F.lpad( "number", 4, "0" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-7 右側をパディング（左詰め）する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "Lionel", "Messi"),(9, "Julian", "Alvarez"),(22, "Lautaro", "Martinez")],("number","first_name","last_name"))

display( df.withColumn( "number_padded", F.rpad( "number", 4, "0" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-8 左側の空白文字を削除する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "  Lionel  ", "Messi"),(9, "  Julian  ", "Alvarez"),(22, "  Lautaro  ", "Martinez")],("number","first_name","last_name"))

display( df.withColumn( "first_name_trimmed", F.ltrim( "first_name" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-9 右側の空白文字を削除する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "  Lionel  ", "Messi"),(9, "  Julian  ", "Alvarez"),(22, "  Lautaro  ", "Martinez")],("number","first_name","last_name"))

display( df.withColumn( "first_name_trimmed", F.rtrim( "first_name" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1-10 両端の空白文字を削除する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(10, "  Lionel  ", "Messi"),(9, "  Julian  ", "Alvarez"),(22, "  Lautaro  ", "Martinez")],("number","first_name","last_name"))

display( df.withColumn( "first_name_trimmed", F.trim( "first_name" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-2 検索、抽出

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2-1 指定した文字が登場する最初の位置を調べる

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("MLflow", "MLflow is an open source platform for managing the end-to-end machine learning lifecycle. It tackles four primary functions"),("Delta Lake", "Delta Lake is an open-source storage framework that enables building a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs for Scala, Java, Rust, Ruby, and Python."),("Apache Spark", "Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.")],("name","what_is"))

display( df.withColumn( "instr_position", F.instr( "what_is", "open" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2-2 指定した文字が指定した位置以降で登場する最初の位置を調べる

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

df = spark.createDataFrame([Row("Time is money"),Row("Speech is silver, silence is golden"),Row("Art is long, life is short")],("proverb string"))

display( df.withColumn( "locate_position", F.locate( "is", "proverb", 10 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2-3 部分文字列を抽出する

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

df = spark.createDataFrame([Row("Time is money"),Row("Speech is silver, silence is golden"),Row("Art is long, life is short")],("proverb string"))

display( df.withColumn( "substring", F.substring( "proverb", 22, 5 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2-4 正規表現にマッチした部分を抽出する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("MLflow", "MLflow is an open source platform for managing the end-to-end machine learning lifecycle. It tackles four primary functions"),("Delta Lake", "Delta Lake is an open-source storage framework that enables building a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs for Scala, Java, Rust, Ruby, and Python."),("Apache Spark", "Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.")],("name","what_is"))

display( df.withColumn( "hyphon_word", F.regexp_extract( "what_is", r"\w+-\w+", 0 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2-5 特定の区切り文字が指定回数登場するまでの部分文字列を抽出する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("Apache Spark", "spark.apache.org"),("Apache Kafka", "kafka.apache.org"),("Apache Parquet", "parquet.apache.org")],("name","domain"))

display( df.withColumn( "subdomain", F.substring_index( "domain", ".", 1 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-3 置換

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-3-1 正規表現にマッチした部分を置換する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("Yamada", "My phone is 090-0012-3456. please call me back"),("Tanaka", "電話番号は090-0123-4567です"),("Suzuki", "070-0012-3456に連絡欲しいとのことです")],("note_taker","note"))

display( df.withColumn( "note_wo_phone", F.regexp_replace( "note", r"\d{3}-\d{4}-\d{4}", "<redacted>" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-3-2 指定した位置に文字列を上書きする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("SPARK_SQL", "CORE", 7, -1),("SPARK_SQL", "STREAMING", 7, 2),("SPARK_SQL", "PY", 1, 0)],("original", "phrase", "pos", "len"))

display( df.withColumn( "overlayed", F.overlay( "original", "phrase", "pos", "len" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-3-3 1文字ずつ別の文字に置換する 

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("sofa", "l234S"),("chair", "g9"),("table", "So0")],("item", "order"))

display( df.withColumn( "order_corrected", F.translate( "order", "olSeg", "01569" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-4 分割・結合

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-4-1 正規表現で指定した箇所で分割する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("ABC trading", "(06)1234-5678"),("XYZ company", "(03)1234-5678"),("shop123", "090-0012-3456")],("company", "number"))

display( df.withColumn( "number_split", F.split( "number", "[()-]", -1 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-4-2 （指定した言語で）単語に分割する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("これは私のペンです", "ja", "JP"),("This is my pen", "en", "US"),("PySparkは、Spark SQLをPythonで扱うことのできるライブラリです", "ja", "JP")],("example", "language", "country"))

display( df.withColumn( "example_split", F.sentences( "example", "language", "country" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-4-3 文字列等を結合する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("Ms", "Hanako", "Yamada"),("Mr", "Ichiro", "Tanaka"),("Dr", "Natsuko", "Suzuki")],("title", "first_name", "last_name"))

display( df.withColumn( "concatenated", F.concat( "title", F.lit(". "), "first_name", F.lit(" "), "last_name" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-4-4 区切り文字を使って文字列を結合する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("A", "0123", "45678"),("B", "0020", "33445"),("C", "1100", "09876")],("code", "number_1", "number_2"))

display( df.withColumn( "concatenated", F.concat_ws( "-", "code", "number_1", "number_2" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-4-5 文字列を繰り返し結合する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "abc"),(2, "xyz"),(3, "123")],("number", "code"))

display( df.withColumn( "code_repeated", F.repeat( "code", 2 ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-5 変換

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-5-1 最初の文字のアスキーコードを取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "abc"),(2, "xyz"),(3, "123")],("number", "code"))

display( df.withColumn( "ascii_code", F.ascii( "code" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-5-2 文字列をBASE64でエンコードする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "abc"),(2, "xyz"),(3, "123")],("number", "code"))

display( df.withColumn( "base64_encoded", F.base64( "code" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-5-3 BASE64エンコード文字列をデコードする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "YWJj"),(2, "eHl6"),(3, "MTIz")],("number", "base64_encoded"))

display( df.withColumn( "original_code", F.unbase64( "base64_encoded" ).cast("string") ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-5-4 指定したキャラクタセットでバイナリに変換する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "あいうえお"),(2, "かきくけこ"),(3, "さしすせそ")],("number", "nihongo"))

df.withColumn( "encoded", F.encode( "nihongo", "UTF-8" ) ).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-5-5 指定したキャラクタセットのバイナリを文字列にデコードする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "あいうえお"),(2, "かきくけこ"),(3, "さしすせそ")],("number", "nihongo"))

display( df.withColumn( "encoded", F.encode( "nihongo", "UTF-8" ) )
           .withColumn( "decoded", F.decode( "encoded", "UTF-8" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-5-6 SoundExにエンコードする

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "apple"),(2, "orange"),(3, "strawberry")],("number", "fruit"))

display( df.withColumn( "soundex", F.soundex( "fruit" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-6 その他

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-6-1 文字列長を取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "apple"),(2, "orange"),(3, "いちご")],("number", "fruit"))

display( df.withColumn( "length", F.length( "fruit" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-6-2 ビット長を取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "apple"),(2, "orange"),(3, "いちご")],("number", "fruit"))

display( df.withColumn( "bit_length", F.bit_length( "fruit" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-6-3 オクテット長を取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([(1, "apple"),(2, "orange"),(3, "いちご")],("number", "fruit"))

display( df.withColumn( "octet_length", F.octet_length( "fruit" ) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-6-4 レーベンシュタイン距離を取得する

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([("tuple", "apple"),("range", "orange"),("いなご", "いちご")],("left", "right"))

display( df.withColumn( "levenshtein_distance", F.levenshtein( "left", "right" ) ) )
