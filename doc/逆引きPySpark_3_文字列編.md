PySparkでこういう場合はどうしたらいいのかをまとめた[逆引きPySparkシリーズ](https://qiita.com/motokazu_ishikawa/items/c55c55426d24edb43a36)の文字列編です。
（随時更新予定です。）

- 原則としてApache Spark 3.3のPySparkのAPIに準拠していますが、一部、便利なDatabricks限定の機能も利用しています（利用しているところはその旨記載しています）。
- Databricks Runtime 11.3 上で動作することを確認しています。
- ノートブックを[こちらのリポジトリ](https://github.com/motokazu-ishikawa-db/gyakubiki_pyspark) から[Repos](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)にてご使用のDatabricksの環境にダウンロードできます。
- 逆引きPySparkの他の章については、[こちらの記事](https://qiita.com/motokazu_ishikawa/items/c55c55426d24edb43a36)をご覧ください。

## 例文の前提条件
- SparkSessionオブジェクトがsparkという変数名で利用可能なこと

# 3-1 整形

## 3-1-1 大文字にする
[upper()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.upper.html#pyspark.sql.functions.upper)関数を使って、文字列を大文字にします。
```py
# 構文
df.withColumn( <追加するカラム名>, F.upper(<文字列型のカラム>) )

# 例文
from pyspark.sql import functions as F

df = ( df.withColumn( "first_name_upper", F.upper("first_name") )
         .withColumn( "last_name_upper", F.upper("last_name") ) )
```
出力例
|  | number | first_name | last_name | first_name_upper | last_name_upper |
|:-:|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | Lionel | Messi | LIONEL | MESSI |
| 2 | 9 | Julian | Alvarez | JULIAN | ALVAREZ |
| 3 | 22 | Lautaro | Martinez | LAUTARO | MARTINEZ |

## 3-1-2 小文字にする
[lower()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lower.html#pyspark.sql.functions.lower)関数を使って、文字列を小文字にします。
```py
# 構文
df.withColumn( <追加するカラム名>, F.lower(<文字列型のカラム>) )

# 例文
from pyspark.sql import functions as F

df = ( df.withColumn( "first_name_lower", F.lower("first_name") )
         .withColumn( "last_name_lower", F.lower("last_name") ) )
```
出力例
|  | number | first_name | last_name | first_name_lower | last_name_lower |
|:-:|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | Julian | Alvarez | lionel | messi |
| 2 | 9 | Lautaro | Martinez | julian | alvarez |
| 3 | 22 | Lionel | Messi | lautaro | martinez |

## 3-1-3 単語の先頭文字を大文字にする
[initcap()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.initcap.html#pyspark.sql.functions.initcap)関数を使って、単語の先頭文字を大文字にします。
```py
# 構文
df.withColumn( <追加するカラム名>, F.initcap(<文字列型のカラム>) )

# 例文
from pyspark.sql import functions as F

df = ( df.withColumn( "first_name_initcap", F.initcap("first_name") )
         .withColumn( "last_name_initcap", F.initcap("last_name") ) )
```
出力例
|  | number | first_name | last_name | first_name_initcap | last_name_initcap |
|:-:|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | LIONEL | MESSI | Lionel | Messi |
| 2 | 9 | JULIAN | ALVAREZ | Julian | Alvarez |
| 3 | 22 | LAUTARO | MARTINEZ | Lautaro | Martinez |

## 3-1-4 数値を整形する
[format_number()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_number.html#pyspark.sql.functions.format_number)関数を使って、数値を整形します。
```py
# 構文
df.withColumn( <追加するカラム名>, F.format_number(<数値型のカラム>, <小数点以下の桁数>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "salary_formatted", F.format_number( "salary", 2 ) ) )
```
出力例
|  | number | first_name | last_name | salary | salary_formatted |
|:-:|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | Lionel | Messi | 41000000 | 41,000,000.00 |
| 2 | 9 | Julian | Alvarez | 3130000 | 3,130,000.00 |
| 3 | 22 | Lautaro | Martinez | 6375000 | 6,375,000.00 |

## 3-1-5 書式付きで文字列を出力する
[format_string()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_string.html#pyspark.sql.functions.format_string)関数を使って、[printfフォーマット](https://qiita.com/takahirocook/items/06d525be63eccd99ed49)の書式で文字列を出力します。
```py
# 構文
df.withColumn( <追加するカラム名>, F.format_string(<書式>, <カラム>[,<カラム>...]) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "string_formatted", F.format_string( "%s's number is %d", "last_name", "number" ) ) )
```
出力例
|  | number | first_name | last_name | string_formatted |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | Lionel | Messi | Messi's number is 10 |
| 2 | 9 | Julian | Alvarez | Alvarez's number is 9 |
| 3 | 22 | Lautaro | Martinez | Martinez's number is 22 |

## 3-1-6 左側をパディング（右詰め）する
[lpad()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lpad.html#pyspark.sql.functions.lpad)関数を使って、指定の文字列長になるまで、左側に指定された文字をパディングします。パディングの対象となるカラムは文字列型でも数値型でもOKです。

```py
# 構文
df.withColumn( <追加するカラム名>, F.lpad(<カラム>, <文字列長>, <パディングに使う文字>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "number_padded", F.lpad( "number", 4, "0" ) ) )
```
出力例
|  | number | first_name | last_name | number_padded |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | Lionel | Messi | 0010 |
| 2 | 9 | Julian | Alvarez | 0009 |
| 3 | 22 | Lautaro | Martinez | 0022 |

## 3-1-7 右側をパディング（左詰め）する
[rpad()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rpad.html#pyspark.sql.functions.rpad)関数を使って、指定の文字列長になるまで、右側に指定された文字をパディングします。パディングの対象となるカラムは文字列型でも数値型でもOKです。

```py
# 構文
df.withColumn( <追加するカラム名>, F.rpad(<カラム>, <文字列長>, <パディングに使う文字>) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "number_padded", F.rpad( "number", 4, "0" ) )
```
出力例
|  | number | first_name | last_name | number_padded |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 | Lionel | Messi | 1000 |
| 2 | 9 | Julian | Alvarez | 9000 |
| 3 | 22 | Lautaro | Martinez | 2200 |

## 3-1-8 左側の空白文字を削除する
[ltrim()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ltrim.html#pyspark.sql.functions.ltrim)関数を使って、文字列の左側にある空白文字を削除します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.ltrim(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "first_name_trimmed", F.ltrim( "first_name" ) )
```
出力例
|  | number | first_name | last_name | first_name_trimmed |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 |   Lionel   | Messi | Lionel   |
| 2 | 9 |   Julian   | Alvarez | Julian   |
| 3 | 22 |   Lautaro   | Martinez | Lautaro   |

## 3-1-9 右側の空白文字を削除する
[rtrim()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rtrim.html#pyspark.sql.functions.rtrim)関数を使って、文字列の右側にある空白文字を削除します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.rtrim(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "first_name_trimmed", F.rtrim( "first_name" ) )
```
出力例
|  | number | first_name | last_name | first_name_trimmed |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 |   Lionel   | Messi |   Lionel |
| 2 | 9 |   Julian   | Alvarez |   Julian |
| 3 | 22 |   Lautaro   | Martinez |   Lautaro |

## 3-1-10 両端の空白文字を削除する
[trim()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html#pyspark.sql.functions.trim)関数を使って、文字列の左右両端にある空白文字を削除します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.trim(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "first_name_trimmed", F.trim( "first_name" ) )
```
出力例
|  | number | first_name | last_name | first_name_trimmed |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 10 |   Lionel   | Messi | Lionel |
| 2 | 9 |   Julian   | Alvarez | Julian |
| 3 | 22 |   Lautaro   | Martinez | Lautaro |

# 3-2 検索、抽出

## 3-2-1 指定した文字が登場する最初の位置を調べる
[instr()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.instr.html#pyspark.sql.functions.instr)関数を使って、文字列から、部分文字列が最初に登場する位置を検索します(最初に登場する場合は1が返ります)。部分文字列が見つからない場合は0が返ります。

```py
# 構文
df.withColumn( <追加するカラム名>, F.instr(<検索対象の文字列型カラム>, <検索する部分文字列>) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "instr_position", F.instr( "what_is", "open" ) )
```
出力例
|  | name | what_is | position |
|:-:|:-:|:-:|:-:|
| 1 | MLflow | MLflow is an open source platform for managing the end-to-end machine learning lifecycle. It tackles four primary functions | 14 |
| 2 | Delta Lake | Delta Lake is an open-source storage framework that enables building a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs for Scala, Java, Rust, Ruby, and Python. | 18 |
| 3 | Apache Spark | Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. | 0 |

## 3-2-2 指定した文字が指定した位置以降で登場する最初の位置を調べる
[locate()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.locate.html#pyspark.sql.functions.locate)関数を使って、文字列から、指定した位置以降で部分文字列が最初に登場する位置を検索します(最初に登場する場合は1が返ります)。部分文字列が見つからない場合は0が返ります。


```py
# 構文
df.withColumn( <追加するカラム名>, F.instr(<検索する部分文字列>, <検索対象の文字列型カラム>[, <検索する最初の位置>]) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "locate_position", F.locate( "is", "proverb", 10 ) )
```
出力例
|  | proverb | locate_position |
|:-:|:-:|:-:|
| 1 | Time is money | 0 |
| 2 | Speech is silver, silence is golden | 27 |
| 3 | Art is long, life is short | 19 |

## 3-2-3 部分文字列を抽出する
[substring()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring.html#pyspark.sql.functions.substring)関数を使って、文字列から、位置と長さを指定して部分文字列を抽出します。部分文字列の開始位置の指定は、最初の文字を(0ではなく)1として数えます。

```py
# 構文
df.withColumn( <追加するカラム名>, F.substring(<文字列型カラム>, <部分文字列の開始位置>, <部分文字列の長さ>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "substring", F.substring( "proverb", 22, 5 ) ) )
```
出力例
|  | proverb | substring |
|:-:|:-:|:-:|
| 1 | Time is money |  |
| 2 | Speech is silver, silence is golden | ence |
| 3 | Art is long, life is short | short |

## 3-2-4 正規表現にマッチした部分を抽出する
[regexp_extract()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html#pyspark.sql.functions.regexp_extract)関数を使って、文字列から、正規表現を使って部分文字列を抽出します。

正規表現パターンの構文については、以下のページの一覧を参考にすることができます。
参考：[【Java】正規表現のまとめ](https://qiita.com/suema0331/items/5dde9f91671100a83905)

```py
# 構文
df.withColumn( <追加するカラム名>, F.regexp_extract(<文字列型カラム>, <正規表現パターン>, <マッチ文字列の何番目の要素を抽出するか>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "hyphon", F.regexp_extract( "what_is", r"\w+-\w+", 0 ) ) )
```
出力例
|  | name | what_is | hyphon_word |
|:-:|:-:|:-:|:-:|
| 1 | MLflow | MLflow is an open source platform for managing the end-to-end machine learning lifecycle. It tackles four primary functions | end-to |
| 2 | Delta Lake | Delta Lake is an open-source storage framework that enables building a Lakehouse architecture with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs for Scala, Java, Rust, Ruby, and Python. | open-source |
| 3 | Apache Spark | Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. | multi-language |

## 3-2-5 特定の区切り文字が指定回数登場するまでの部分文字列を抽出する
[substring_index()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring_index.html#pyspark.sql.functions.substring_index)関数を使って、文字列から、指定した区切り文字が指定した回数登場する箇所の前までの部分文字列を抽出します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.substring_index(<文字列型カラム>, <区切り文字>, <登場回数>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "subdomain", F.substring_index( "domain", ".", 1 ) ) )
```
出力例
|  | name | domain | subdomain |
|:-:|:-:|:-:|:-:|
| 1 | Apache Spark | spark.apache.org | spark |
| 2 | Apache Kafka | kafka.apache.org | kafka |
| 3 | Apache Parquet | parquet.apache.org | parquet |


# 3-3 置換

## 3-3-1 正規表現にマッチした部分を置換する
[regexp_replace()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html#pyspark.sql.functions.regexp_replace)関数を使って、文字列に対して、正規表現パターンに該当する文字列を、別の文字列で置換します。

正規表現パターンの構文については、以下のページの一覧を参考にすることができます。
参考：[【Java】正規表現のまとめ](https://qiita.com/suema0331/items/5dde9f91671100a83905)

```py
# 構文
df.withColumn( <追加するカラム名>, F.regexp_replace(<文字列型カラム>, <正規表現パターン>, <置換に使う文字列>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "note_wo_phone", F.regexp_replace( "note", r"\d{3}-\d{4}-\d{4}", "<redacted>" ) ) )
```
出力例
|  | note_taker | note | note_wo_phone |
|:-:|:-:|:-:|:-:|
| 1 | Yamada | My phone is 090-0012-3456. please call me back | My phone is \<redacted\>. please call me back |
| 2 | Tanaka | 電話番号は090-0123-4567です | 電話番号は\<redacted\>です |
| 3 | Suzuki | 070-0012-3456に連絡欲しいとのことです | \<redacted\>に連絡欲しいとのことです |

## 3-3-2 指定した位置に文字列を上書き(挿入)する
[overlay()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.overlay.html#pyspark.sql.functions.overlay)関数を使って、文字列に対して、指定した位置に文字列を上書きまたは挿入します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.overlay(<文字列型カラム>, <上書きする文字列型カラム>, <上書きする位置>[, <上書きする長さ>]) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "overlayed", F.overlay( "original", "phrase", "pos", "len" ) ) )
```
出力例
|  | original | phrase | pos | len | overlayed |
|:-:|:-:|:-:|:-:|:-:|:-:|
| 1 | SPARK_SQL | CORE | 7 | -1 | SPARK_CORE |
| 2 | SPARK_SQL | STREAMING | 7 | 2 | SPARK_STREAMINGL |
| 3 | SPARK_SQL | PY | 1 | 0 | PYSPARK_SQL |

## 3-3-3 1文字ずつ別の文字に置換する
[translate()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.translate.html#pyspark.sql.functions.translate)関数を使って、文字列に対して、指定した文字があった場合に、指定した文字で置換します。下記、<置換される文字>が文字列型カラムにあった場合、<置換する文字>で対応する位置にある文字で置換されます。

```py
# 構文
df.withColumn( <追加するカラム名>, F.translate(<文字列型カラム>, <置換される文字>, <置換する文字>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "order_corrected", F.translate( "order", "olSeg", "01569" ) ) )
```
出力例
|  | item | order | order_corrected |
|:-:|:-:|:-:|:-:|
| 1 | sofa | l234S | 12345 |
| 2 | chair | g9 | 99 |
| 3 | table | So0 | 500 |

# 3-4 分割・結合

## 3-4-1 正規表現で指定した箇所で分割する
[split()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split.html#pyspark.sql.functions.split)関数を使って、文字列を、正規表現のパターンを使って分割します。

- <最大回数>が0より大きい時： 最大でこの回数まで分割されます。
- <最大回数>が0以下の時：　回数に制限なく、正規表現のパターンに従って分割されます。

正規表現パターンの構文については、以下のページの一覧を参考にすることができます。
参考：[【Java】正規表現のまとめ](https://qiita.com/suema0331/items/5dde9f91671100a83905)

```py
# 構文
df.withColumn( <追加するカラム名>, F.split(<文字列型カラム>, <正規表現パターン>[, <最大回数>]) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "number_split", F.split( "number", "[()-]", -1 ) )
```
出力例
|  | company | number | number_split |
|:-:|:-:|:-:|:-:|
| 1 | ABC trading | (06)1234-5678 | ["", "06", "1234", "5678"] |
| 2 | XYZ company | (03)1234-5678 | ["", "03", "1234", "5678"] |
| 3 | shop123 | 090-0012-3456 | ["090", "0012", "3456"] |

## 3-4-2 （指定した言語で）単語に分割する
[sentences()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sentences.html#pyspark.sql.functions.sentences)関数を使って、文字列を、ロケールに従って単語に分割します。ロケールは、言語と国を指定します。ロケールを指定しない場合は環境のデフォルトロケールが使われます。

```py
# 構文
df.withColumn( <追加するカラム名>, F.sentences(<文字列型カラム>[, <言語を指定するカラム>, <国を指定するカラム>]) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "example_split", F.sentences( "example", "language", "country" ) ) )
```
出力例
|  | example | language | country | example_split |
|:-:|:-:|:-:|:-:|:-:|
| 1 | これは私のペンです | ja | JP | [["これは", "私", "の", "ペン", "です"]] |
| 2 | This is my pen | en | US | [["This", "is", "my", "pen"]] |
| 3 | PySparkは、Spark SQLをPythonで扱うことのできるライブラリです | ja | JP | [["PySpark", "は", "Spark", "SQL", "を", "Python", "で", "扱", "うことのできる", "ライブラリ", "です"]] |

## 3-4-3 文字列等を結合する
[concat()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat.html#pyspark.sql.functions.concat)関数を使って、文字列等を結合します。結合対象のカラムは複数設定可能です。

```py
# 構文
df.withColumn( <追加するカラム名>, F.concat(<文字列型カラム>[,...<文字列型カラム>]) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "concatenated", F.concat( "title", F.lit(". "), "first_name", F.lit(" "), "last_name" ) ) )
```
出力例
|  | title | first_name | last_name | concatenated |
|:-:|:-:|:-:|:-:|:-:|
| 1 | Ms | Hanako | Yamada | Ms. Hanako Yamada |
| 2 | Mr | Ichiro | Tanaka | Mr. Ichiro Tanaka |
| 3 | Dr | Natsuko | Suzuki | Dr. Natsuko Suzuki |

## 3-4-4 区切り文字を使って文字列を結合する
[concat_ws()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat_ws.html#pyspark.sql.functions.concat_ws)関数を使って、特定の区切り文字を使い、文字列等を結合します。結合対象のカラムは複数設定可能です。

```py
# 構文
df.withColumn( <追加するカラム名>, F.concat_ws(<区切り文字>,<文字列型カラム>[,...<文字列型カラム>]) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "concatenated", F.concat_ws( "-", "code", "number_1", "number_2" ) ) )
```
出力例
|  | code | number_1 | number_2 | concatenated |
|:-:|:-:|:-:|:-:|:-:|
| 1 | A | 0123 | 45678 | A-0123-45678 |
| 2 | B | 0020 | 33445 | B-0020-33445 |
| 3 | C | 1100 | 09876 | C-1100-09876 |

## 3-4-5 文字列を繰り返し結合する
[repeat()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.repeat.html#pyspark.sql.functions.repeat)関数を使って、文字列を、指定回数繰り返し結合します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.repeat(<文字列型カラム>,<繰り返し回数>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "code_repeated", F.repeat( "code", 2 ) ) )
```
出力例
|  | number | code | code_repeated |
|:-:|:-:|:-:|:-:|
| 1 | 1 | abc | abcabc |
| 2 | 2 | xyz | xyzxyz |
| 3 | 3 | 123 | 123123 |

# 3-5 変換

## 3-5-1 最初の文字のアスキーコードを取得する
[ascii()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ascii.html#pyspark.sql.functions.ascii)関数を使って、文字列の最初の文字のアスキーコードを取得します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.ascii(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "ascii_code", F.ascii( "code" ) ) )
```
出力例
|  | number | code | ascii_code |
|:-:|:-:|:-:|:-:|
| 1 | 1 | abc | 97 |
| 2 | 2 | xyz | 120 |
| 3 | 3 | 123 | 49 |

## 3-5-2 文字列をBASE64でエンコードする
[base64()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.base64.html#pyspark.sql.functions.base64)関数を使って、文字列をBASE64でエンコードします。

```py
# 構文
df.withColumn( <追加するカラム名>, F.base64(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "base64_encoded", F.base64( "code" ) ) )
```
出力例
|  | number | code | base64_encoded |
|:-:|:-:|:-:|:-:|
| 1 | 1 | abc | YWJj |
| 2 | 2 | xyz | eHl6 |
| 3 | 3 | 123 | MTIz |

## 3-5-3 BASE64エンコード文字列をデコードする
[unbase64()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unbase64.html#pyspark.sql.functions.unbase64)関数を使って、BASE64でエンコードされた文字列をデコードしバイナリ型のカラムとして抽出し、それを文字列型にキャストします。

```py
# 構文
df.withColumn( <追加するカラム名>, F.unbase64(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "original_code", F.unbase64( "base64_encoded" ).cast("string") ) )
```
出力例
|  | number | base64_encoded | original_code |
|:-:|:-:|:-:|:-:|
| 1 | 1 | YWJj | abc |
| 2 | 2 | eHl6 | xyz |
| 3 | 3 | MTIz | 123 |

## 3-5-4 指定したキャラクタセットでバイナリに変換する
[encode()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.encode.html#pyspark.sql.functions.encode)関数を使って、文字列を、指定したキャラクターセットでバイナリに変換します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.encode(<文字列型カラム>、<キャラクターセット>) )

# 例文
from pyspark.sql import functions as F

df.withColumn( "encoded", F.encode( "nihongo", "UTF-8" ) ).printSchema()
```
出力例
root
 |-- number: long (nullable = true)
 |-- nihongo: string (nullable = true)
 |-- encoded: binary (nullable = true)

## 3-5-5 指定したキャラクタセットのバイナリを文字列にデコードする
[decode()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.decode.html#pyspark.sql.functions.decode)関数を使って、特定のキャラクターセットでエンコードされたバイナリをデコードして文字列として抽出します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.decode(<バイナリ型カラム>, <キャラクターセット>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "encoded", F.encode( "nihongo", "UTF-8" ) )
           .withColumn( "decoded", F.decode( "encoded", "UTF-8" ) ) )
```
出力例
|  | nihongo | encoded | decoded |
|:-:|:-:|:-:|:-:|
| 1 | あいうえお | 44GC44GE44GG44GI44GK | あいうえお |
| 2 | かきくけこ | 44GL44GN44GP44GR44GT | かきくけこ |
| 3 | さしすせそ | 44GV44GX44GZ44Gb44Gd | さしすせそ |

## 3-5-6 SoundExにエンコードする
[soundex()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.soundex.html#pyspark.sql.functions.soundex)関数を使って、文字列を、SoundExにエンコードします。

```py
# 構文
df.withColumn( <追加するカラム名>, F.soundex(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "soundex", F.soundex( "fruit" ) ) )
```
出力例
|  | number | fruit | soundex |
|:-:|:-:|:-:|:-:|
| 1 | 1 | apple | A140 |
| 2 | 2 | orange | O652 |
| 3 | 3 | strawberry | S361 |

# 3-6 その他

## 3-6-1 文字列長を取得する
[length()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.length.html#pyspark.sql.functions.length)関数を使って、文字列の文字列長を取得します。前後の空白についてもカウントされます。

```py
# 構文
df.withColumn( <追加するカラム名>, F.length(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "length", F.length( "fruit" ) ) )
```
出力例
|  | number | fruit | length |
|:-:|:-:|:-:|:-:|
| 1 | 1 | apple | 5 |
| 2 | 2 | orange | 6 |
| 3 | 3 | いちご | 3 |

## 3-6-2 ビット長を取得する
[bit_length()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_length.html#pyspark.sql.functions.bit_length)関数を使って、文字列のビット長を取得します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.bit_length(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "bit_length", F.bit_length( "fruit" ) ) )
```
出力例
|  | number | fruit | bit_length |
|:-:|:-:|:-:|:-:|
| 1 | 1 | apple | 40 |
| 2 | 2 | orange | 48 |
| 3 | 3 | いちご | 72 |

## 3-6-3 オクテット長を取得する
[octet_length()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.octet_length.html#pyspark.sql.functions.octet_length)関数を使って、文字列のオクテット長を取得します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.octet_length(<文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "octet_length", F.octet_length( "fruit" ) ) )
```
出力例
|  | number | fruit | octet_length |
|:-:|:-:|:-:|:-:|
| 1 | 1 | apple | 5 |
| 2 | 2 | orange | 6 |
| 3 | 3 | いちご | 9 |

## 3-6-4 レーベンシュタイン距離を取得する
[levenshtein()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.levenshtein.html#pyspark.sql.functions.levenshtein)関数を使って、文字列同士の間の[レーベンシュタイン距離](https://ja.wikipedia.org/wiki/%E3%83%AC%E3%83%BC%E3%83%99%E3%83%B3%E3%82%B7%E3%83%A5%E3%82%BF%E3%82%A4%E3%83%B3%E8%B7%9D%E9%9B%A2)を取得します。

```py
# 構文
df.withColumn( <追加するカラム名>, F.levenshtein(<文字列型カラム>, <文字列型カラム>) )

# 例文
from pyspark.sql import functions as F

display( df.withColumn( "levenshtein_distance", F.levenshtein( "left", "right" ) ) )
```
出力例
|  | left | right | octet_length |
|:-:|:-:|:-:|:-:|
| 1 | tuple | apple | 2 |
| 2 | range | orange | 1 |
| 3 | いなご | いちご | 1 |
