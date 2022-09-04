PySparkでこういう場合はどうしたらいいのかをまとめた逆引きPySparkの日付時刻編です。
（随時更新予定です。）

- 原則としてApache Spark 3.3のPySparkのAPIに準拠していますが、一部、便利なDatabricks限定の機能も利用しています（利用しているところはその旨記載しています）。
- Databricks Runtime 11.0 上で動作することを確認しています。
- ノートブックを[こちらのリポジトリ](https://github.com/motokazu-ishikawa-db/gyakubiki_pyspark) から[Repos](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)にてご使用のDatabricksの環境にダウンロードできます。

## 例文の前提条件
- SparkSessionオブジェクトがsparkという変数名で利用可能なこと

# 2-1 現在の取得

## 2-1-1 現在の時刻を取得する
[current_timestamp()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html)関数を使って、現在の時刻を取得します。
```py
# 構文
df.withColumn( <追加するカラム名>, current_timestamp() )

# 例文
from pyspark.sql.functions import current_timestamp

df = df.withColumn( "current_time_utc", current_timestamp() )
display( df.select( "device_id", "current_time_utc" ) )
```
出力例
|  | device_id | current_time_utc |
|:-:|:-:|:-:|
|  1 | 1 |  2022-09-03T13:43:25.514+0000 |


## 2-1-2 現在の日付を取得する
[current_date()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_date.html)関数を使って、現在の日付を取得します。
```py
# 構文
df.withColumn( <追加するカラム名>, current_date() )

# 例文
from pyspark.sql.functions import current_date

df = df.withColumn( "today", current_date() )
display( df.select( "device_id", "today" ) )
```
出力例
|  | device_id | today |
|:-:|:-:|:-:|
| 1 | 1 | 2022-09-03 |

# 2-2 抽出

## 2-2-1 時刻から年の値を取り出す
[year()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html)関数を使って、時刻から年の値を取り出す。
```py
# 構文
df.withColumn( <追加するカラム名>, year(<時刻型のカラム>) )

# 例文
from pyspark.sql.functions import year

df = df.withColumn( "year", year( "time" ) )
display( df.select( "time", "year" ) )
```
|  | time | year |
|:-:|:-:|:-:|
| 1 | 2019-03-14T22:07:35.910+0000 | 2019 |
| 2 | 2016-09-29T07:34:26.932+0000 | 2016 |


## 2-2-2 時刻から月の値を取り出す
[month()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.month.html)関数を使って、時刻から年の値を取り出す。
```py
# 構文
df.withColumn( <追加するカラム名>, month(<時刻型のカラム>) )

# 例文
from pyspark.sql.functions import month

df = df.withColumn( "month", month( "time" ) )
display( df.select( "time", "month" ) )
```
|  | time | month |
|:-:|:-:|:-:|
| 1 | 2019-03-14T22:07:35.910+0000 | 3 |
| 2 | 2016-09-29T07:34:26.932+0000 | 9 |


## 2-2-3 時刻から時の値を取り出す
[month()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hour.html)関数を使って、時刻から時の値を取り出す。
```py
# 構文
df.withColumn( <追加するカラム名>, hour(<時刻型のカラム>) )

# 例文
from pyspark.sql.functions import hour

df = df.withColumn( "hour", hour( "time" ) )
display( df.select( "time", "hour" ) )
```
|  | time | hour |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 19 |
| 2 | 2017-09-04T02:34:43.919+0000 | 2 |


## 2-2-4 時刻から分の値を取り出す
[minute()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.minute.html)関数を使って、時刻から分の値を取り出す。
```py
# 構文
df.withColumn( <追加するカラム名>, minute(<時刻型のカラム>) )

# 例文
from pyspark.sql.functions import minute

df = df.withColumn( "minute", minute( "time" ) )
display( df.select( "time", "minute" ) )
```
|  | time | minute |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 41 |
| 2 | 2017-09-04T02:34:43.919+0000 | 34 |


## 2-2-5 時刻から秒の値を取り出す
[second()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.second.html)関数を使って、時刻から秒の値を取り出す。
```py
# 構文
df.withColumn( <追加するカラム名>, second(<時刻型のカラム>) )

# 例文
from pyspark.sql.functions import second

df = df.withColumn( "second", second( "time" ) )
display( df.select( "time", "second" ) )
```
|  | time | second |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 21 |
| 2 | 2017-09-04T02:34:43.919+0000 | 43 |


## 2-2-6 時刻を特定の単位で切り捨てる
[date_trunc()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_trunc.html)関数を使って、時刻を特定の単位で切り捨てます。例えば、"year"を指定すると、年より小さい単位の情報が切り捨てられ一番小さい値に置き換えられます。
```py
# 構文
df.withColumn( <追加するカラム名>, date_trunc( <切り捨てる単位>, <時刻型のカラム> ) )

# 例文
from pyspark.sql.functions import date_trunc

df = df.withColumn( "date_trunc", date_trunc( "month", "time" ) )
display( df.select( "time","date_trunc" ) )
```
|  | time | date_trunc |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 2016-03-01T00:00:00.000+0000 |
| 2 | 2017-09-04T02:34:43.919+0000 | 2016-03-01T00:00:00.000+0000 |


## 2-2-7 日付を特定の単位で切り捨てる
[trunc()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trunc.html)関数を使って、日付を特定の単位で切り捨てます。例えば、"year"を指定すると、年より小さい単位の情報が切り捨てられ一番小さい値に置き換えられます。
```py
# 構文
df.withColumn( <追加するカラム名>, trunc( <切り捨てる単位>, <時刻型のカラム> ) )

# 例文
from pyspark.sql.functions import trunc

df = df.withColumn( "trunc", trunc( "date", "month" ) )
display( df.select( "date","trunc" ) )
```
|  | date | trunc |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 2018-06-01 |
| 2 | 2017-09-04 | 2017-09-01 |


## 2-2-8 日付が年の何日目かに該当するかを求める
[dayofyear()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofyear.html)関数を使って、日付が年の何日目かに該当するかを求めます。1月1日は1になります。
```py
# 構文
df.withColumn( <追加するカラム名>, dayofyear( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import dayofyear

df = df.withColumn( "dayofyear", dayofyear( "date" ) )
display( df.select( "date", "dayofyear" ) )
```
|  | date | dayofyear |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 153 |
| 2 | 2017-09-04 | 247 |


## 2-2-9 日付が年の何週目に該当するかを求める
[weekofyear()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.weekofyear.html)関数を使って、日付が年の何週目かに該当するかを求めます。1月1日が属する週は1になります。
```py
# 構文
df.withColumn( <追加するカラム名>, weekofyear( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import weekofyear

df = df.withColumn( "weekofyear", weekofyear( "date" ) )
display( df.select( "date", "weekofyear" ) )
```
|  | date | weekofyear |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 22 |
| 2 | 2017-09-04 | 36 |


## 2-2-10 日付が年の第何四半期に該当するかを求める
[quarter()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.quarter.html)関数を使って、日付が年の第何四半期に該当するかを求めます。
```py
# 構文
df.withColumn( <追加するカラム名>, quarter( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import quarter

df = df.withColumn( "quarter", quarter( "date" ) )
display( df.select( "date", "quarter" ) )
```
|  | date | quarter |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 2 |
| 2 | 2017-09-04 | 3 |


## 2-2-11 日付が月の何日目かに該当するかを求める
[dayofmonth()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofmonth.html)関数を使って、日付が年の何日目に該当するかを求めます。
```py
# 構文
df.withColumn( <追加するカラム名>, quarter( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import dayofmonth

df = df.withColumn( "day", dayofmonth( "date" ) )
display( df.select( "date", "day" ) )
```
|  | date | day |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 2 |
| 2 | 2017-09-04 | 4 |


## 2-2-12 日付が週の何日目かに該当するかを求める
[dayofweek()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofweek.html)関数を使って、日付が週の何日目かに該当するかを求めます。日曜日が1となります。
```py
# 構文
df.withColumn( <追加するカラム名>, dayofweek( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import dayofweek

df = df.withColumn( "dayofweek", dayofweek( "date" ) )
display( df.select( "date", "dayofweek" ) )
```
|  | date | dayofweek |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 7 |
| 2 | 2017-09-04 | 2 |


## 2-2-13 その月の月末に該当する日付を求める
[last_day()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.last_day.html)関数を使って、日付から、その月の月末に該当する日付を求める。
```py
# 構文
df.withColumn( <追加するカラム名>, last_day( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import last_day

df = df.withColumn( "last_day_of_month", last_day( "date" ) )
display( df.select( "date", "last_day_of_month" ) )
```
|  | date | dayofweek |
|:-:|:-:|:-:|
| 1 | 2018-06-02 | 2018-06-30 |
| 2 | 2017-09-04 | 2017-09-30 |


# 2-3 変換

## 2-3-1 UNIX時間から時刻へ変換する
[timestamp_seconds()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.timestamp_seconds.html)関数を使って、[UNIX時間](https://ja.wikipedia.org/wiki/UNIX%E6%99%82%E9%96%93)を時刻に変換します。
```py
# 構文
df.withColumn( <追加するカラム名>, timestamp_seconds( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import timestamp_seconds

df = df.withColumn( "time", timestamp_seconds("timestamp") )
display( df.select( "timestamp", "time" ) )
```
|  | timestamp | time |
|:-:|:-:|:-:|
| 1 | 1527968481.2164252 | 2018-06-02T19:41:21.216+0000 |
| 2 | 1504492483.9199414 | 2017-09-04T02:34:43.919+0000 |


## 2-3-2 時刻からUNIX時間へ変換する
[unix_timestamp()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_timestamp.html)関数を使って、時刻を[UNIX時間](https://ja.wikipedia.org/wiki/UNIX%E6%99%82%E9%96%93)に変換します。
```py
# 構文
df.withColumn( <追加するカラム名>, timestamp_seconds( <日付型のカラム> ) )

# 例文
from pyspark.sql.functions import unix_timestamp

df = df.withColumn( "unixtime", unix_timestamp("time") )
display( df.select( "time", "unixtime" ) )
```
|  | time | unixtime |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 1527968481 |
| 2 | 2017-09-04T02:34:43.919+0000 | 1504492483 |


## 2-3-3 時刻をUTCから特定のタイムゾーンへ変換する
[from_utc_timestamp()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_utc_timestamp.html)関数を使って、時刻をUTCから特定のタイムゾーンへ変換します。タイムゾーンは、以下の4つの形式のいずれかで指定できます。

| フォーマット | 例 | 備考 |
|:-:|:-:|:-:|
| <地域>/<都市名> | Asia/Tokyo |   |
| (+\|-)HH:mm | +09:00 |   |
| UTC |   | ‘+00:00’のエイリアス |
| Z |   | ‘+00:00’のエイリアス| 

```py
# 構文
df.withColumn( <追加するカラム名>, from_utc_timestamp( <時刻型のカラム>, <タイムゾーン> ) )

# 例文
from pyspark.sql.functions import from_utc_timestamp

df = df.withColumn( "time_jst", from_utc_timestamp("time", "Asia/Tokyo") )
display( df.select( "time", "time_jst" ) )
```
|  | time | time_jst |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 2018-06-03T04:41:21.216+0000 |
| 2 | 2017-09-04T02:34:43.919+0000 | 2017-09-04T11:34:43.919+0000 |


## 2-3-4 時刻を特定のタイムゾーンからUTCへ変換する
[to_utc_timestamp()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_utc_timestamp.html)関数を使って、時刻を特定のタイムゾーンからUTCへ変換します。タイムゾーンは、以下の4つの形式のいずれかで指定できます。

| フォーマット | 例 | 備考 |
|:-:|:-:|:-:|
| <地域>/<都市名> | Asia/Tokyo |   |
| (+\|-)HH:mm | +09:00 |   |
| UTC |   | ‘+00:00’のエイリアス |
| Z |   | ‘+00:00’のエイリアス| 

```py
# 構文
df.withColumn( <追加するカラム名>, to_utc_timestamp( <時刻型のカラム>, <タイムゾーン> ) )

# 例文
from pyspark.sql.functions import to_utc_timestamp

df = df.withColumn( "time_utc", to_utc_timestamp("time_jst", "Asia/Tokyo") )
display( df.select( "time_jst", "time_utc" ) )
```
|  | time_jst | time_utc |
|:-:|:-:|:-:|
| 1 | 2018-06-03T04:41:21.216+0000 | 2018-06-02T19:41:21.216+0000 |
| 2 | 2017-09-04T11:34:43.919+0000 | 2017-09-04T02:34:43.919+0000 |


## 2-3-5 時刻/日付から文字列へ変換する
[date_format()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html)関数を使って、時刻や日付を、指定した[フォーマット](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)に従って文字列に変換します。

```py
# 構文
df.withColumn( <追加するカラム名>, date_format( <時刻型/日付型のカラム>, <フォーマット> ) )

# 例文
from pyspark.sql.functions import date_format

df = df.withColumn( "timestamp_string", date_format( "time", "yyyyMMdd-HHmmss" ) )
display( df.select( "time", "timestamp_string" ) )
```
|  | time | timestamp_string |
|:-:|:-:|:-:|
| 1 | 2018-06-02T19:41:21.216+0000 | 20180602-194121 |
| 2 | 2017-09-04T02:34:43.919+0000 | 20170904-023443 |


## 2-3-6 文字列から時刻へ変換する
[to_timestamp()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html)関数を使って、文字列を、指定した[フォーマット](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)に従って時刻に変換します。

```py
# 構文
df.withColumn( <追加するカラム名>, to_timestamp( <文字列のカラム>, <フォーマット> ) )

# 例文
from pyspark.sql.functions import to_timestamp

df = df.withColumn( "to_timestamp", to_timestamp( "timestamp_string", "yyyyMMdd-HHmmss" ) )
display( df.select( "timestamp_string", "to_timestamp" ) )
```
|  | timestamp_string | to_timestamp |
|:-:|:-:|:-:|
| 1 | 20180602-194121 | 2018-06-02T19:41:21.216+0000 |
| 2 | 20170904-023443 | 2017-09-04T02:34:43.919+0000 |


## 2-3-7 文字列から日付へ変換する
[to_date()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html)関数を使って、文字列を、指定した[フォーマット](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)に従って日付に変換します。

```py
# 構文
df.withColumn( <追加するカラム名>, to_date( <文字列のカラム>, <フォーマット> ) )

# 例文
from pyspark.sql.functions import to_date

df = df.withColumn( "to_date", to_date( "timestamp_string", "yyyyMMdd-HHmmss" ) )
display( df.select( "timestamp_string", "to_date" ) )
```
|  | timestamp_string | to_date |
|:-:|:-:|:-:|
| 1 | 20180602-194121 | 2018-06-02 |
| 2 | 20170904-023443 | 2017-09-04 |


## 2-3-8 時刻を日付に変換する
[to_date()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html)関数を使って、時刻を日付に変換します。

```py
# 構文
df.withColumn( <追加するカラム名>, to_date( <時刻型のカラム> ) )

# 例文
from pyspark.sql.functions import to_date

df = df.withColumn( "date", to_date( "time_jst" ) )
display( df.select( "time_jst", "date" ) )
```
|  | time_jst | date |
|:-:|:-:|:-:|
| 1 | 2018-06-03T04:41:21.216+0000 | 2018-06-03 |
| 2 | 2017-09-04T11:34:43.919+0000 | 2017-09-04 |


## 2-3-9 年、月、日から、日付を作成する
[make_date()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_date.html)関数を使って、時刻を日付に変換します。

```py
# 構文
df.withColumn( <追加するカラム名>, make_date( <年を表す文字列>, <月を表す文字列>, <日を表す文字列> ) )

# 例文
from pyspark.sql.functions import make_date

df = df.withColumn( "made_date", make_date( "year", "month", "day" ) )
display( df.select( "year", "month", "day", "made_date" ) )
```
|  | year | month | day | make_date |
|:-:|:-:|:-:|:-:|:-:|
| 1 | 2018 | 6 | 3 | 2018-06-03 |
| 2 | 2017 | 9 | 4 | 2017-09-04 |

# 2-4 日付の足し算、引き算

## 2-4-1 ２つの日付の間の日数を求める
[datediff()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.datediff.html)関数を使って、２つの日付の間の日数を求めます。前の日付が2022-01-01で、後の日付が2022-01-02の場合は1になります。前の日付が2022-01-02で、後の日付が2022-01-01の場合は-1になります。

```py
# 構文
df.withColumn( <追加するカラム名>, datediff( <後の日付の日付型カラム>, <前の日付の日付型カラム> ) )

# 例文
from pyspark.sql.functions import datediff

df2 = df2.withColumn( "datediff", datediff( "end_date", "start_date" ) )
display( df2.select( "start_date", "end_date", "datediff" ) )
```
|  | start_date | end_date  | datediff |
|:-:|:-:|:-:|:-:|
| 1 | 2017-11-07 | 2017-10-01 | -37 |
| 2 | 2016-11-17 | 2016-05-30 | -171 |


## 2-4-2 ２つの日付の間の月数を求める
[months_between()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.months_between.html)関数を使って、２つの日付の間の月数を求めます。どちらの日付も、月の中で同じ日、あるいは月末である場合は整数が返ります。それ以外の場合は、月を31日として計算された浮動小数点数が返ります。

```py
# 構文
df.withColumn( <追加するカラム名>, months_between( <後の日付の日付型カラム>, <前の日付の日付型カラム> ) )

# 例文
from pyspark.sql.functions import months_between

df2 = df2.withColumn( "months_between", months_between( "end_date", "start_date" ) )
display( df2.select( "start_date", "end_date", "months_between" ) )
```
|  | start_date | end_date  | datediff |
|:-:|:-:|:-:|:-:|
| 1 | 2017-11-07 | 2017-10-01 | -1.19354839 |
| 2 | 2016-11-17 | 2016-05-30 | -5.58064516 |


## 2-4-3 次の○曜日の日付を求める（例：次の月曜日の日付）
[next_day()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.next_day.html)関数を使って、入力となる日付の次の○曜日(例：月曜日)の日付を求めます。曜日として“Mon”、“Tue”、“Wed”、“Thu”、“Fri”、“Sat”、“Sun”を設定可能です(大文字小文字不問)。

```py
# 構文
df.withColumn( <追加するカラム名>, next_day( <日付型のカラム>, <曜日> ) )

# 例文
from pyspark.sql.functions import next_day

df2 = df2.withColumn( "next_day_of_end_date", next_day( "end_date", "Mon" ) )
display( df2.select( "end_date", "next_day_of_end_date" )  )
```
|  | end_date | next_day_of_end_date |
|:-:|:-:|:-:|
| 1 | 2018-05-23 | 2018-05-28 |
| 2 | 2018-12-22 | 2018-12-24 |


## 2-4-4 ○日後の日付を求める
[date_add()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_add.html)関数を使って、入力となる日付の○日後の日付を求めます。

```py
# 構文
df.withColumn( <追加するカラム名>, date_add( <日付型のカラム>, <日数> ) )

# 例文
from pyspark.sql.functions import date_add

df2 = df2.withColumn( "three_days_after_end_date", date_add( "end_date", 3 ) )
display( df2.select( "end_date", "three_days_after_end_date" )  )
```
|  | end_date | three_days_after_end_date |
|:-:|:-:|:-:|
| 1 | 2018-05-23 | 2018-05-26 |
| 2 | 2018-12-22 | 2018-12-25 |


## 2-4-5 ○日前の日付を求める
[date_sub()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_sub.html)関数を使って、入力となる日付の○日前の日付を求めます。

```py
# 構文
df.withColumn( <追加するカラム名>, date_sub( <日付型のカラム>, <日数> ) )

# 例文
from pyspark.sql.functions import date_sub

df2 = df2.withColumn( "three_days_before_start_date", date_sub( "start_date", 3 ) )
display( df2.select( "start_date", "three_days_before_start_date" )  )
```
|  | end_date | three_days_before_start_date |
|:-:|:-:|:-:|
| 1 | 2018-05-23 | 2018-05-24 |
| 2 | 2018-12-22 | 2018-02-13 |


## 2-4-6 ○ヶ月後の日付を求める
[add_months()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.add_months.html)関数を使って、入力となる日付の○日前の日付を求めます。

```py
# 構文
df.withColumn( <追加するカラム名>, date_sub( <日付型のカラム>, <日数> ) )

# 例文
from pyspark.sql.functions import add_months

df2 = df2.withColumn( "two_months_after_end_date", add_months( "end_date", 2 ) )
display( df2.select( "end_date", "two_months_after_end_date" )  )
```
|  | end_date | two_months_after_end_date |
|:-:|:-:|:-:|
| 1 | 2018-05-23 | 2018-07-23 |
| 2 | 2018-12-22 | 2019-02-22 |


# 2-5 その他

## 2-5-1 特定の長さのウィンドウについて統計値を集計する
[window()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window.html)関数を使って、特定の長さの期間を表すウィンドウを作成します。groupBy()の引数としてウィンドウを与えることで、そのウィンドウについて統計値を集計することができます。引数としてスライディングウィンドウの間隔を設定することで、設定した間隔ごとのスライディングウィンドウが作成されます。設定されない場合はタンブリングウィンドウが作成されます。

```py
# 構文
window( <時刻型のカラム>, <ウィンドウの長さ>[, <スライディングウィンドウの間隔>[, <オフセット>]] )

# 例文
from pyspark.sql.functions import avg, window

df_group = df2.groupBy( window("end_time", "5 days", "1 day" ) ).agg( avg("humidity").alias("average_humidity") )
display( df_group )
```
|  | window | average_humidity |
|:-:|:-:|:-:|
| 1 | {"start": "2017-04-11T00:00:00.000+0000", "end": "2017-04-16T00:00:00.000+0000"} | 61.60972716488731 |
| 2 | {"start": "2016-08-30T00:00:00.000+0000", "end": "2016-09-04T00:00:00.000+0000"} |62.39860950173812 |

(以下略)


## 2-5-2 セッションウィンドウについて統計値を集計する
[session_window()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.session_window.html)関数を使って、セッションウィンドウを作成します。groupBy()の引数としてセッションウィンドウを与えることで、そのセッションウィンドウについて統計値を集計することができます。

参考：[Spark構造化ストリーミングにおけるセッションウィンドウのネイティブサポート](https://qiita.com/taka_yayoi/items/14d8df68f9fd731b9ca0)

```py
# 構文
session_window( <時刻型のカラム>, <セッションのタイムアウト時間> )

# 例文
from pyspark.sql.functions import session_window

df_group = df2.groupBy( session_window( "end_time", "1 hour" ) ).count()
display( df_group )
```
|  | session_window | count |
|:-:|:-:|:-:|
| 1 | {"start": "2016-03-20T03:27:37.620+0000", "end": "2016-04-27T07:21:58.678+0000"} | 6659 |
| 2 | {"start": "2016-04-27T07:42:01.350+0000", "end": "2016-05-01T02:43:31.451+0000"} | 679 |

(以下略)
