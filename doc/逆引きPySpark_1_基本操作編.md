PySparkでこういう場合はどうしたらいいのかをまとめた逆引きPySparkの基本操作編です。
（随時更新予定です。）

- 原則としてApache Spark 3.3のPySparkのAPIに準拠していますが、一部、便利なDatabricks限定の機能も利用しています（利用しているところはその旨記載しています）。
- Databricks Runtime 11.0 上で動作することを確認しています。
- ノートブックを[こちらのリポジトリ](https://github.com/motokazu-ishikawa-db/gyakubiki_pyspark) から[Repos](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)にてご使用のDatabricksの環境にダウンロードできます。

## 例文の前提条件
- SparkSessionオブジェクトがsparkという変数名で利用可能なこと

### 1-1 ファイルからデータフレームにデータを読み込む (入力)
[spark.read](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html)オブジェクトを使って、ファイルからデータを読み込みます。
```py
# 構文
df = ( spark
       .read
       .format(<フォーマット名>)
       [.option(<オプションのキー>,<オプションの値>)]
       .load(<ファイルパス>) )

# 例文
df = ( spark
       .read
       .format("csv")
       .option("header","true")
       .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/") )
```

### 1-2 カラムを追加する
[withColumn()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html)で、データフレームに新しいカラムを追加できます。
```py
# 構文
df_2 = df.withColumn( <追加するカラム名>, <追加するColumnオブジェクト> )

# 例文
from pyspark.sql import functions as f
df_2 = df.withColumn("pickup_date", f.to_date("pickup_datetime"))
```


### 1-3 特定の条件にあてはまるデータを取り出す
[filter()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html)で、データフレームから、特定の条件を持つデータだけからなる新しいデータフレームを作ります。
```py
# 構文
df_3 = df_2.filter(<データを絞り込む条件>)

# 例文その１
from pyspark.sql.functions import col
df_3 = df_2.filter( col("trip_distance") > 10 )

# 例文その２
df_3 = df_2.filter( "trip_distance > 10" )
```


### 1-4 データ件数を集計する
[count()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html)で、データフレームの中のデータ件数を表示できます。
```py
# 構文
df_3.count()

# 実行結果
64708276
```

### 1-5 カラムを削除する
[drop()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html)で、指定したカラムを削除できます。
```py
# 構文
df_4 = df_3.drop( <削除するカラム> )

# 例文
df_4 = df_3.drop( "pickup_datetime" )
```

### 1-6 データフレーム同士を結合する（join）
[join()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)で、データフレーム同士を結合します。
デフォルトの結合方法は内部結合(INNER JOIN)です。
```py
# 構文
df_5 = df_4.join( <結合相手のデータフレーム>, <結合条件>[, <結合方法>] )

# 例文
df_5 = df_4.join( df_code, df_4.rate_code == df_code.RateCodeID )
```

### 1-7 値ごとの件数をカウントして降順に整列する
[groupby()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.groupby.html)で、カラムの値ごとに集約し、[count()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.count.html)で件数を集計し、[orderBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html)で降順に整列します。
```py
# 構文
df.groupby(<集計するカラム名>).count().orderBy("count", ascending=False) 

# 例文
display( df_5.groupby("vendor_id").count().orderBy("count", ascending=False) )
```
実行結果
![gyakubiki_1_7.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/2663175/95cdf379-ecbf-3232-6c79-c81cc7cc9e46.png)


:::note info
display()は、データフレームの中身を見やすく表示するためのDatabricks独自のメソッドです。
:::

### 1-8 データフレームからデータを書き出す（出力）
[データフレームのwriteインターフェース](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)を使って、ファイルにデータを出力します。
```py
# 構文
( df.write
    .format(<フォーマット>)
    [.mode(<書き込みモード>)]
    .save(<ディレクトリパス>) )

# 例文
( df.write
    .format("parquet")
    .mode("append")
    .save("/tmp/hoge/fuga") )
```

書き込みモードでは、すでにファイルが存在していた場合の動作を以下の中から指定します。

| 書き込みモード | 説明 |
|:-:|:-:|
|  append | 既存のデータに追加します  |
| overwrite | 既存のデータを上書きします |
| error or errorifexists | データが既にある場合はExceptionを投げます |
| ignore|データが既にある場合は処理を実行しません |

