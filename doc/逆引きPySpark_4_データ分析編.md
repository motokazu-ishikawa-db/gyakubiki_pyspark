PySparkでこういう場合はどうしたらいいのかをまとめた逆引きPySparkのデータ分析編です。
（随時更新予定です。）

- 原則としてApache Spark 3.3のPySparkのAPIに準拠していますが、一部、便利なDatabricks限定の機能も利用しています（利用しているところはその旨記載しています）。
- Databricks Runtime 11.3 上で動作することを確認しています。
- ノートブックを[こちらのリポジトリ](https://github.com/motokazu-ishikawa-db/gyakubiki_pyspark) から[Repos](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)にてご使用のDatabricksの環境にダウンロードできます。

## 例文の前提条件
- SparkSessionオブジェクトがsparkという変数名で利用可能なこと

# 4-1 欠損値

## 4-1-1 カラムごとの欠損率を求める（方法その１)
Databricks限定の機能を用いない方法です。4-1-2の方は、Databricks限定機能を使ってシンプルにわかりやすい表示を得ることができる方法です。
```py
# 構文
df.select([F.round((F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) * 100 / df.count()),2).alias(c) for c in df.columns])

# 例文
from pyspark.sql import functions as F

( df.select([F.round((F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) * 100 / df.count()),2).alias(c) for c in df.columns])
    .select( "duration", "end_of_fade_in", "key" )
    .show() )
```
出力例
| duration | end_of_fade_in | key |
|:-:|:-:|:-:|
| 0.06 | 0.06 | 0.06 |

## 4-1-2 カラムごとの欠損率を求める（Databricks限定機能を利用)
:::note warn
注意
Databricks限定の機能を利用しており、他の環境では利用できません。
:::
display()関数を使って、カラムごとの欠損率を求めます。カラムごとの統計情報を表示する表のmissingに欠損率が表示されます。欠損率以外にも、データ数、平均、標準偏差、最小最大やデータ分布の可視化等なども表示されるため便利です。

参考）[Databricksのdisplayメソッドでデータプロファイリングをサポートしました](https://qiita.com/taka_yayoi/items/ab9a20f581847ad69080)

```py
# 構文 (以下のコマンド実行後に、画面上でデータプロファイルの新規タブを作成します）
display( <データフレーム> )

# 例文
display( df )
```
出力例
![スクリーンショット 2023-02-24 17.50.19.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/2663175/20652b49-2fb9-cf75-46ac-8d20fa0dd597.png)

## 4-1-3 カラムごとの欠損値の数を求める

```py
# 構文
df.select([F.round((F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) * 100 ),2).alias(c) for c in df.columns])

# 例文
from pyspark.sql import functions as F
display( df.select([F.round((F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) * 100 ),2).alias(c) for c in df.columns])
    .select( "duration", "end_of_fade_in", "key" ) )
```
出力例
| | duration | end_of_fade_in | key |
|:-:|:-:|:-:|:-:|
| 1 | 2000 | 2000 | 2000 |

## 4-1-4 特定のカラムの値が欠損していない行を取得する
[Column](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html)オブジェクトの[isNotNull()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNotNull.html)メソッドは、そのカラムの値がNullでないかどうかを判定します(Nullでない場合はTrue)。isNotNull()を使って、欠損していない行のみを取得することができます。
```py
# 構文
df.filter( F.col(<カラム名>).isNotNull() )

# 例文
from pyspark.sql import functions as F
df_4_1_4 = df.filter( F.col("artist_latitude").isNotNull() )
```

## 4-1-5 特定のカラムの値が欠損している行を取得する
[Column](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html)オブジェクトの[isNull()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNull.html)メソッドは、そのカラムの値がNullかどうかを判定します(Nullの場合はTrue)。isNull()を使って、欠損している行のみを取得することができます。
```py
# 構文
df.filter( F.col(<カラム名>).isNull() )

# 例文
from pyspark.sql import functions as F
df_4_1_5 = df.filter( F.col("artist_latitude").isNull() )
```

## 4-1-6 欠損値のある行を除外する
データフレームの[dropna()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropna.html)メソッドを使って、欠損値のある行を除外します。

<table>
  <caption>dropna()の引数</caption>
  <thead>
    <tr>
      <th>名前</th><th>型</th><th>説明</th>
    </tr>
  </thead>
  <tr>
    <td> how </td> <td>str</td> <td>"any"または"all"。"any"の場合は対象となるカラムのいずれかが欠損していると除外される。"all"の場合は対象となる全てのカラムが欠損していると除外される。</td>
  </tr>
  <tr>
    <td> thresh </td> <td>int</td> <td>指定しないのがデフォルト。指定されると、欠損していないカラム数がこの数より小さい場合に除外される。このパラメーターはhowの効果を上書きする。</td>
  </tr>
  <tr>
    <td> subset </td> <td>str, tuple, list</td> <td>考慮の対象となるカラムのリスト。</td>
  </tr>
</table>

```py
# 構文1(特定の複数カラムが全て欠損している場合に除外する)
df.dropna( how="all", subset=<特定の複数カラムのリスト> )

# 構文2(特定の複数カラムのいずれかが欠損している場合に除外する)
df.dropna( how="any", subset=<特定の複数カラムのリスト> )

# 構文3(特定の複数カラムのうち欠損していないカラム数がN個以上の場合に残す)
df.dropna( thresh=<欠損していないカラム数の閾値>, subset=<特定の複数カラムのリスト> )

# 例文1(特定の複数カラムが全て欠損している場合に除外する)
df_4_1_6_1 = df.dropna( how="all", subset=[ "duration", "end_of_fade_in", "key" ] )

# 例文2(特定の複数カラムのいずれかが欠損している場合に除外する)
df_4_1_6_2 = df.dropna( how="any", subset=[ "duration", "end_of_fade_in", "key" ] )

# 例文3(特定の複数カラムのうち欠損していないカラム数がN個以上の場合に残す)
df_4_1_6_3 = df.dropna( thresh=1, subset=[ "duration", "end_of_fade_in", "key" ] )
```

## 4-1-7 欠損値を特定の値で置換する
データフレームの[fillna()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.fillna.html)メソッドを使って、欠損値を、指定した特定の値で置換します。

<table>
  <caption>fillna()の引数</caption>
  <thead>
    <tr>
      <th>名前</th><th>指定</th><th>型</th><th>説明</th>
    </tr>
  </thead>
  <tr>
    <td>value</td><td>必須</td><td>int、float、string、bool、dict</td><td>欠損値を置換する値。valueが辞書型の場合、subsetは無視され、valueはカラム名から置換される値へのマップであることが求められます。置換する値のデータ型はint、float、boolean、stringのいずれかです。</td>
  </tr>
  <tr>
    <td>subset</td><td>オプション</td><td>str、tuple、list</td><td>オプション指定の、考慮されるカラムのリスト。指定されたデータ型と一致しないカラムがsubsetで指定されても無視されます。例えば、valueがstringの場合、subsetが文字列以外のデータ型のカラムを含む場合、そのカラムは単純に無視されます。</td>
  </tr>
</table>

```py
# 構文1(１つ以上のカラムの欠損値を同じ値で置換する)
df.fillna( <置換する値>, subset=<対象のカラム名のリスト> )

# 構文2(複数のカラムの欠損値をカラムごとにそれぞれ別々の値で置換する)
df.dropna( <カラム名と置換する値の辞書> )

# 例文1(１つ以上のカラムの欠損値を同じ値で置換する)
df_4_1_7_1 = df.fillna( 0.0, subset=[ "duration", "key", "loudness" ] )

# 例文2(複数のカラムの欠損値をカラムごとにそれぞれ別々の値で置換する)
df_4_1_7_2 = df.fillna( {"artist_latitude":40.730610, "artist_longitude":-73.935242, "artist_location":"NYC"} )
```

## 4-1-8 欠損値を平均値(中央値)で置換する
欠損値を推定して補完するための[Imputer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.Imputer.html)クラスを使います。置換の方法として平均値("mean")、中央値("median")、最頻値("mode")のいずれかを選択し、[setStrategy()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.Imputer.html#pyspark.ml.feature.Imputer.setStrategy)で設定します。なお、数値型の推定のみに対応しており、カテゴリ値には対応していません。

```py
# 構文1(欠損値を平均値で置換する)
imputer_mean = Imputer(
  strategy="mean",
  inputCols=<置換する対象のカラム名のリスト>,
  outputCols=<置換後のカラム名のリスト>
)
df_4_1_8_1 = imputer_mean.fit( df ).transform( df )

# 構文2(欠損値を中央値で置換する)
imputer_median = Imputer(
  strategy="median",
  inputCols=<置換する対象のカラム名のリスト>,
  outputCols=<置換後のカラム名のリスト>
)
df_4_1_8_2 = imputer_median.fit( df ).transform( df )

# 例文1(欠損値を平均値で置換する)
imputer_mean = Imputer(
  strategy="mean",
  inputCols=[ "song_hotnes", "key", "loudness" ],
  outputCols=[ "song_hotnes", "key", "loudness" ]
)
df_4_1_8_1 = imputer_mean.fit( df ).transform( df )

# 例文2(欠損値を中央値で置換する)
imputer_median = Imputer(
  strategy="median",
  inputCols=[ "song_hotnes", "key", "loudness" ],
  outputCols=[ "song_hotnes", "key", "loudness" ]
)
df_4_1_8_2 = imputer_median.fit( df ).transform( df )
```

# 4-2 基本統計量

## 4-2-1 カラムごとの基本統計量を出力する（方法その１)
データフレームの[summary()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.summary.html)メソッドを使って、カラムごとの基本統計量（総数、平均、標準偏差、最小、25%パーセンタイル、50%パーセンタイル、75%パーセンタイル、最大）を出力します。
```py
# 構文
df.select( <基本統計量を出力したいカラム> ).summary().show()

# 例文
df.select( "song_hotnes", "key", "loudness" ).summary().show()
```
出力例
|summary|       song_hotnes|              key|           loudness|
|:-:|:-:|:-:|:-:|
|  count|             18149|            31369|              31369|
|   mean|0.3556972746713994|5.334597851381938|-10.086819726481567|
| stddev|0.2341764776975724|3.598054733120626|   5.14788868290633|
|    min|               0.0|                0|            -57.871|
|    25%|    0.215080318509|                2|            -12.603|
|    50%|    0.376169924841|                5|             -8.951|
|    75%|    0.531983193341|                9|             -6.397|
|    max|               1.0|               11|              2.046|

## 4-2-2 カラムごとの基本統計量を出力する（Databricks限定機能を利用)
:::note warn
注意
Databricks限定の機能を利用しており、他の環境では利用できません。
:::
Databricks限定のdisplay()関数を使って、総数、平均、標準偏差、最小、中央値、最大等の基本統計量に加えて、データ分布のヒストグラム等を簡単に表示して、データの統計情報を簡単に把握することができます。

参考）[Databricksのdisplayメソッドでデータプロファイリングをサポートしました](https://qiita.com/taka_yayoi/items/ab9a20f581847ad69080)
```py
# 構文 (以下のコマンド実行後に、画面上でデータプロファイルの新規タブを作成します）
display( <データフレーム> )

# 例文
display( df )
```
出力例
![スクリーンショット 2023-03-01 22.06.04.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/2663175/d122facc-8af6-aeae-ca8d-15069cefa8e8.png)

# 4-3 グループ集計

## 4-3-1 グループごとの総数を集計して大きい順に整列する
データフレームを特定のカラムでグループ化して集計したい場合、[groupBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)メソッドを使います。groupBy()メソッドはグループ化されたデータを表す[GroupedData](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html)オブジェクトを返すので、これに対して各種処理を実施することでグループデータの集計が可能です。

[count()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.count.html)メソッドはグループごとの総数をカウントした結果をつけたデータフレームを返します。これに対して[orderBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html)メソッドを使って、総数の降順に並べます。

```py
# 構文
df.groupBy( <グループ化したいカラム> ).count().orderBy( "count", ascending=False )

# 例文
from pyspark.sql import functions as F
df_4_3_1 =( df
            .filter( F.col( "artist_location" ).isNotNull() )
            .groupBy( "artist_location" )
            .count()
            .orderBy( "count", ascending=False ) )
```
出力例
|artist_location|count|
|:-:|:-:|
|London, England|430|
|New York, NY|425|
|Los Angeles, CA|346|
|Chicago, IL|278|
|California - LA|267|
(以下略)

## 4-3-2 クロス集計をしてピボットテーブルを作成する
2つのカラムでクロス集計をしてピボットテーブルを作成する方法です。まず、データフレームを1つ目のカラムで[groupBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)して出来た[GroupedData](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html)に2つ目のカラムで[pivot()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html)メソッドを使用し、[count()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html)で総数をカウントすることでピボットテーブルが得られます。
```py
# 構文
( df
  .groupBy( <1つ目のカラム> )
  .pivot( <2つ目のカラム>, <出力したい値> )
  .count()
  .withColumn( "total", <合計対象のピボットテーブルのカラム> )
  .orderBy( "count", ascending=False ) )

# 例文
df_4_3_2 = ( df
            .filter( F.col( "artist_location" ).isNotNull() )
            .groupBy( "artist_location" )
            .pivot( "year", [2005, 2006, 2007, 2008, 2009, 2010 ] )
            .count()
            .withColumn( "total", F.col("2005") + F.col("2006") + F.col("2007") + F.col("2008") + F.col("2009") + F.col("2010") )
            .orderBy("total", ascending=False) )
```
出力例
|artist_location|2005|2006|2007|2008|2009|2010|total|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|London, England|18|23|16|15|12|4|88|
|New York, NY|8|18|11|4|10|6|57|
|Los Angeles, CA|8|11|9|10|4|2|44|
|California - LA|14|13|5|4|4|2|42|
|Chicago, IL|9|11|8|7|5|2|42|

(以下略)

## 4-3-3 グループごとの統計値を集計する
グループごとに、特定のカラムについて最小、平均、最大等の統計量を以下のように集計することができます。

まず、データフレームを特定のカラムで[groupBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)して[GroupedData](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html)オブジェクトを取得します。このオブジェクトに対して[agg()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.agg.html)メソッドで集計に使う関数を渡します。この際、複数の関数を渡すことや、[alias()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html)を使って、集計値のカラム名を指定することができます。
```py
# 構文
df
.groupBy( <グループ化するカラム> )
.agg( F.min( <最小値を集計するカラム> ).alias( <最小値を表すカラム名> ),
      F.avg( <平均値を集計するカラム> ).alias( <平均値を表すカラム名> ),
      F.max( <最大値を集計するカラム> ).alias( <最大値を表すカラム名> ),
      F.count( <個数を集計するカラム> ).alias( <総数を表すカラム名> ) )

# 例文
from pyspark.sql import functions as F
df_4_3_3 = ( df
            .filter( F.col( "artist_location" ).isNotNull() )
            .groupBy( "artist_location" )
            .agg( F.min( "tempo" ).alias( "min_tempo" ),
                  F.avg( "tempo" ).alias( "avg_tempo" ),
                  F.max( "tempo" ).alias( "max_tempo" ),
                  F.count( "tempo" ).alias( "count" ))
            .orderBy( "count", ascending=False ) )
```
出力例
|artist_location|min_tempo|avg_tempo|max_tempo|count|
|:-:|:-:|:-:|:-:|:-:|
|London, England|0|123.43878139534881|247.791|430|
|New York, NY|30.879|119.98964705882352|240.346|425|
|Los Angeles, CA|48.592|125.92399421965321|246.435|346|
|Chicago, IL|35.853|122.42928057553958|248.299|278|
|California - LA|45.919|126.99132584269661|229.903|267|


(以下略)

# 4-4 ウィンドウ関数

## 4-4-1 パーティション（グループ)の統計値を取得する

ウィンドウ関数を使うことで、パーティション(グループ)内の統計値を取得することができます。
グループに該当する[Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)を以下のように作成します。
```py
Window.partitionBy( "artist_location" )
```
これを次のようにavg()などの統計関数のover()関数に渡すことで、パーティション内での統計値を表現できます。
```py
F.avg().over( Window.partitionBy( "artist_location" ) )
```

```py
# 構文
from pyspark.sql import functions as F
from pyspark.sql import Window
df.withColumn( <新しいカラム名>, F.avg().over( Window.partitionBy( <グループ化するカラム名> ) ) )

# 例文
from pyspark.sql import functions as F
from pyspark.sql import Window
df_4_4_1 = ( df
             .filter( F.col( "tempo" ).isNotNull() & F.col( "artist_location" ).isNotNull() )
             .withColumn( "avg_tempo_of_city", F.avg( "tempo" ).over( Window.partitionBy( "artist_location" ) ) )
             .select( "artist_location", "artist_name", "title", "tempo", "avg_tempo_of_city" )
             .filter( F.col( "artist_location" ).startswith( "U" ) ))
```
出力例
|artist_location|artist_name|title|tempo|avg_tempo_of_city|
|:-:|:-:|:-:|:-:|:-:|
|UK|Morcheeba|Slow Down|168.06|110.45801818181816|
|UK|Smokey Robinson & The Miracles|Come Spy With Me|108.786|110.45801818181816|
|UK|Ivor Cutler|Mostly Tins|204.181|110.45801818181816|
|UK|Blancmange|Blind Vision|121.441|110.45801818181816|
|UK|Babyshambles|UnBiloTitled|84.511|110.45801818181816|

(以下略)

## 4-4-2 パーティション（グループ）内での値の順位を取得する

ウィンドウ関数を使うことで、グループ内での特定のカラムの値に基づいた順位を求めることができます。

グループに該当する[Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)を以下のように作成します。
```py
Window.partitionBy( "artist_location" )
```
これを次のようにrank()のover()関数に渡すことで、パーティション内での順位を表現できます。
```py
F.rank().over( Window.partitionBy( "artist_location" ) )
```
|関数|説明|
|:-:|:-:|
|[dense_rank()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html)|順位。同率のものが2つ以上あっても次の順位は飛ばされません。|
|[ntile(<タイル数>)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ntile.html)|パーティションを指定した数のタイルに分割した場合に何番目のタイルに該当するか。|
|[percent_rank()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percent_rank.html)|パーセンタイル順位（0.0〜1.0)。|
|[rank()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html)|順位。|
|[row_number()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html)|ウィンドウ内での1から始まる順位。|

```py
# 構文
from pyspark.sql import functions as F
from pyspark.sql import Window
df
.withColumn( <新しいカラム名>, F.rank().over( Window.partitionBy( <グループ化するカラム名> ).orderBy( <順位をつけるカラム名> ) ) )

# 例文
from pyspark.sql import functions as F
from pyspark.sql import Window
df_4_4_2 = ( df
             .filter( F.col( "tempo" ).isNotNull() & F.col( "artist_location" ).isNotNull() )
             .withColumn( "tempo_rank_in_city", F.rank().over( Window.partitionBy( "artist_location" ).orderBy( "tempo" ) ) )
             .select( "artist_location", "artist_name", "title", "tempo", "tempo_rank_in_city" )
             .filter( F.col( "artist_location" ).startswith( "U" ) ))
```
出力例
|artist_location|artist_name|title|tempo|tempo_rank_in_city|
|:-:|:-:|:-:|:-:|:-:|
|UK|Ivor Cutler|Mostly Tins|204.181|1|
|UK|Creaming Jesus|What The Harpy Said|180.893|2|
|UK|Bad Manners|Hokey Cokey|169.337|3|
|UK|Morcheeba|Slow Down|168.06|4|
|UK|Bad Manners|I Don't Care If The Sun Don't Shine|166.489|5|

(以下略)

## 4-4-3 パーティション（グループ）内で1つ前(後)の値を取得する

ウィンドウ関数を使うことで、パーティション（グループ)内での特定のカラムで値を整列した場合に１つ前の値や1つ後ろの値を取得することができます。

グループに該当する[Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)を以下のように作成します。
```py
Window.partitionBy( "artist_location" )
```
これを次のように[lag()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html)のover()関数に渡すことで、パーティション内での1つ前の値を取得できます。1つ後の値は[lead()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lead.html)を使うことで取得できます。
```py
F.lag( "tempo ).over( Window.partitionBy( "artist_location" ) )
```

```py
# 構文
from pyspark.sql import functions as F
from pyspark.sql import Window
( df
.withColumn( <新しいカラム名>, F.lag( <前の値を取りたいカラム名> ).over( Window.partitionBy( <グループ化するカラム名> ).orderBy( <前の値を取りたいカラム名> ) ) )
.withColumn( <新しいカラム名>, F.leard( <後の値を取りたいカラム名> ).over( Window.partitionBy( <グループ化するカラム名> ).orderBy( <後の値を取りたいカラム名> ) ) ) )

# 例文
from pyspark.sql import functions as F
from pyspark.sql import Window
df_4_4_3 = ( df
             .filter( F.col( "tempo" ).isNotNull() & F.col( "artist_location" ).isNotNull() )
             .withColumn( "previous_tempo_in_city", F.lag( "tempo" ).over( Window.partitionBy( "artist_location" ).orderBy( F.col("tempo").desc() ) ) )
             .withColumn( "next_tempo_in_city", F.lead( "tempo" ).over( Window.partitionBy( "artist_location" ).orderBy( F.col("tempo").desc() ) ) )
             .select( "artist_location", "artist_name", "title", "tempo", "previous_tempo_in_city", "next_tempo_in_city" )
             .filter( F.col( "artist_location" ).startswith( "U" ) ))
```
出力例
|artist_location|artist_name|title|tempo|previous_tempo_in_city|next_tempo_in_city|
|:-:|:-:|:-:|:-:|:-:|:-:|
|UK|Ivor Cutler|Mostly Tins|204.181|null|180.893|
|UK|Creaming Jesus|What The Harpy Said|180.893|204.181|169.337|
|UK|Bad Manners|Hokey Cokey|169.337|180.893|168.06|
|UK|Morcheeba|Slow Down|168.06|169.337|166.489|
|UK|Bad Manners|I Don't Care If The Sun Don't Shine|166.489|168.06|160.177|

(以下略)