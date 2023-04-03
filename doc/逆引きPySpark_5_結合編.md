PySparkでこういう場合はどうしたらいいのかをまとめた[逆引きPySparkシリーズ](https://qiita.com/motokazu_ishikawa/items/c55c55426d24edb43a36)の結合編です。
（随時更新予定です。）

- 原則としてApache Spark 3.3のPySparkのAPIに準拠していますが、一部、便利なDatabricks限定の機能も利用しています（利用しているところはその旨記載しています）。
- Databricks Runtime 11.3 上で動作することを確認しています。
- ノートブックを[こちらのリポジトリ](https://github.com/motokazu-ishikawa-db/gyakubiki_pyspark) から[Repos](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)にてご使用のDatabricksの環境にダウンロードできます。
- 逆引きPySparkの他の章については、[こちらの記事](https://qiita.com/motokazu_ishikawa/items/c55c55426d24edb43a36)をご覧ください。

# 5-1 Join

## 5-1-1 内部結合 (INNER JOIN)

内部結合 (INNER JOIN)は、結合のキーとなる列の値がマッチする行同士を連結することで2つのデータフレームを結合します。キーの列の値が**片方のデータフレームにしか存在しない場合は、その行は出力されません**。

データフレームの[join()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)メソッドを使い、最初の引数として内部結合をしたい相手のデータフレームを指定します。

以下のデータフレーム1、データフレーム2をramen_idという列を結合のキーとして内部結合するとした場合について説明します。

<table>
<caption>データフレーム1(df_ramen)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
</tbody>
</table>

<table>
<caption>データフレーム2(df_topping)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
</tbody>
</table>

結合のキーとなる列名が両方のデータフレームで共通している場合、構文１のように列名を第２引数で指定します。
```py
# 構文1
df_1.join( df_2, <結合のキーとなる列>, "inner" )

# 例文1
df_ramen.join( df_topping, "ramen_id", "inner" )
```
構文2のように、それぞれのデータフレームの列名を指定し、結合のための条件式を第2引数にすることもできます。
```py
# 構文2
df_1.join( df_2, <結合の条件>, "inner" )

# 例文2
df_ramen.join( df_topping, df_ramen.ramen_id == df_topping.ramen_id, "inner" )
```

構文1の場合の出力例は以下になります。
<table>
<caption>内部結合されたデータフレーム</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">ニンニク</td>
</tr>
</tbody>
</table>

## 5-1-2 左外部結合 (LEFT OUTER JOIN)

左外部結合 (LEFT OUTER JOIN)は、結合の基準となる左側のデータフレームの行に、キーとなる列の値がマッチする右側のデータフレームの行を連結することで2つのデータフレームを結合します。左側のデータフレームのキーとなる列の値が**右側のデータフレームに存在しない場合でも、左側のデータフレームの該当行は出力されます**（右側のデータフレームの該当の情報はnullとして出力されます）。

左外部結合の基準となる左側のデータフレームの[join()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)メソッドを使い、最初の引数として相手の（右側の）データフレームを指定します。

以下のデータフレーム1、データフレーム2をramen_idという列を結合のキーとして左外部結合するとした場合について説明します。

<table>
<caption>データフレーム1(df_ramen)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
</tbody>
</table>

<table>
<caption>データフレーム2(df_topping)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
</tbody>
</table>

結合のキーとなる列名が両方のデータフレームで共通している場合、構文１のように列名を第２引数で指定します。
```py
# 構文1
df_1.join( df_2, <結合のキーとなる列>, "left" )

# 例文1
df_ramen.join( df_topping, "ramen_id", "left" )
```
構文2のように、それぞれのデータフレームの列名を指定し、結合のための条件式を第2引数にすることもできます。
```py
# 構文2
df_1.join( df_2, <結合の条件>, "left" )

# 例文2
df_ramen.join( df_topping, df_ramen.ramen_id == df_topping.ramen_id, "left" )
```

構文1の場合の出力例は以下になります。
<table>
<caption>左外部結合されたデータフレーム</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">ニンニク</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
<td style="text-align:center">null</td>
</tr>
</tbody>
</table>

## 5-1-3 クロス結合 (CROSS JOIN)

クロス結合 (CROSS  JOIN)は、結合する両方のデータフレームの全ての行の組み合わせを出力します。結果として、両側のデータフレームそれぞれの行数を掛け合わせた数の行が出力されます。

データフレームの[crossJoin()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.crossJoin.html)メソッドを使い、引数としてクロス結合の相手のデータフレームを指定します。結合のキーは不要なため、引数は1つだけです。

以下、データフレーム1、データフレーム2をクロス結合するとした場合について説明します。

<table>
<caption>データフレーム1(df_ramen)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
</tbody>
</table>

<table>
<caption>データフレーム2(df_topping)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
</tbody>
</table>

結合のキーとなる列名が両方のデータフレームで共通している場合、構文１のように列名を第２引数で指定します。
```py
# 構文1
df_1.crossJoin( df_2 )

# 例文1
df_ramen.crossJoin( df_topping )
```

<table>
<caption>クロス結合されたデータフレーム</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
<td style="text-align:center">1</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
<td style="text-align:center">2</td>
<td style="text-align:center">味玉</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
<td style="text-align:center">3</td>
<td style="text-align:center">チャーシュー</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
<td style="text-align:center">3</td>
<td style="text-align:center">ニンニク</td>
</tr>
</tbody>
</table>

# 5-2 Union

## 5-2-1 Union データフレームを縦方向に結合する

2つのデータフレームを縦方向に結合(Union結合)します。SQLのUNION ALLとは異なり重複行がある場合でも許容されます。そのため、結合結果から重複を削除したい場合はUnion結合したデータフレームに対してさらに[distinct()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html)を実行する必要があります。また、列の結合は、列の順番のみが考慮されます。列名が同じもの同士をUnion結合したい場合はunionByName()を使う必要があります。

データフレームの[union()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html)メソッドを使い、最初の引数としてUnion結合をしたい相手のデータフレームを指定します。

以下のデータフレーム1、データフレーム2をUnion結合するとした場合について説明します。

<table>
<caption>データフレーム1(df_ramen)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
</tbody>
</table>

<table>
<caption>データフレーム2(df_topping)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">price</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">5</td>
<td style="text-align:center">700</td>
<td style="text-align:center">鶏白湯ラーメン</td>
</tr>
<tr>
<td style="text-align:center">6</td>
<td style="text-align:center">800</td>
<td style="text-align:center">濃厚とんこつラーメン</td>
</tr>
<tr>
<td style="text-align:center">7</td>
<td style="text-align:center">600</td>
<td style="text-align:center">塩つけめん</td>
</tr>
</tbody>
</table>

```py
# 構文
df_1.union( df_2 )

# 例文
df_ramen.union( df_ramen_2 )
```

<table>
<caption>Union(ByName)結合されたデータフレーム</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
<tr>
<td style="text-align:center">5</td>
<td style="text-align:center">700</td>
<td style="text-align:center">鶏白湯ラーメン</td>
</tr>
<tr>
<td style="text-align:center">6</td>
<td style="text-align:center">800</td>
<td style="text-align:center">濃厚とんこつラーメン</td>
</tr>
<tr>
<td style="text-align:center">7</td>
<td style="text-align:center">600</td>
<td style="text-align:center">塩つけめん</td>
</tr>
</tbody>
</table>

## 5-2-2 列名を考慮してUnion （データフレームを縦方向に結合する）

2つのデータフレームを縦方向に結合(Union結合)します。5-2-1とは異なり、列の順番に関わらず、結合相手のデータフレームで同じ列名の列をUnion結合します。SQLのUNION ALLとは異なり重複行がある場合でも許容されます。そのため、結合結果から重複を削除したい場合はUnion結合したデータフレームに対してさらに[distinct()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html)を実行する必要があります。

データフレームの[unionByName()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html)メソッドを使い、最初の引数としてUnion結合をしたい相手のデータフレームを指定します。

以下のデータフレーム1、データフレーム2をUnion結合するとした場合について説明します。

<table>
<caption>データフレーム1(df_ramen)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
</tbody>
</table>

<table>
<caption>データフレーム2(df_topping)</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">price</th>
<th style="text-align:center">name</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">5</td>
<td style="text-align:center">700</td>
<td style="text-align:center">鶏白湯ラーメン</td>
</tr>
<tr>
<td style="text-align:center">6</td>
<td style="text-align:center">800</td>
<td style="text-align:center">濃厚とんこつラーメン</td>
</tr>
<tr>
<td style="text-align:center">7</td>
<td style="text-align:center">600</td>
<td style="text-align:center">塩つけめん</td>
</tr>
</tbody>
</table>

```py
# 構文
df_1.unionByName( df_2 )

# 例文
df_ramen.unionByName( df_ramen_2 )
```

<table>
<caption>Union(ByName)結合されたデータフレーム</caption>
<thead>
<tr>
<th style="text-align:center">ramen_id</th>
<th style="text-align:center">name</th>
<th style="text-align:center">price</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center">1</td>
<td style="text-align:center">醤油ラーメン</td>
<td style="text-align:center">600</td>
</tr>
<tr>
<td style="text-align:center">2</td>
<td style="text-align:center">塩ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">3</td>
<td style="text-align:center">豚骨醤油ラーメン</td>
<td style="text-align:center">900</td>
</tr>
<tr>
<td style="text-align:center">4</td>
<td style="text-align:center">味噌ラーメン</td>
<td style="text-align:center">800</td>
</tr>
<tr>
<td style="text-align:center">5</td>
<td style="text-align:center">鶏白湯ラーメン</td>
<td style="text-align:center">700</td>
</tr>
<tr>
<td style="text-align:center">6</td>
<td style="text-align:center">濃厚とんこつラーメン</td>
<td style="text-align:center">800</td>
</tr>
<tr>
<td style="text-align:center">7</td>
<td style="text-align:center">塩つけめん</td>
<td style="text-align:center">600</td>
</tr>
</tbody>
</table>
