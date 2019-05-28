## DataFrame入门
## 使用DataFrame

Apache Spark和Azure Databricks允许使用DataFrame进行数据查询。

DataFrames从Resilient Distributed Datasets(RDD)派生而来，它非常像关系数据库中的表，有列名和数据类型。DataFrame是优化过的表格，比如在读取json文件时可以将数组对象和层状结构作为列的值进行处理。

### DataFrame语法与SQL语法的对应表

| SQL                                     | DataFrame (Python)                    |
| --------------------------------------- | ------------------------------------- |
| `SELECT col_1 FROM myTable`             | `df.select(col("col_1"))`             |
| `DESCRIBE myTable`                      | `df.printSchema()`                    |
| `SELECT * FROM myTable WHERE col_1 > 0` | `df.filter(col("col_1") > 0)`         |
| `..GROUP BY col_2`                      | `..groupBy(col("col_2"))`             |
| `..ORDER BY col_2`                      | `..orderBy(col("col_2"))`             |
| `..WHERE year(col_3) > 1990`            | `..filter(year(col("col_3")) > 1990)` |
| `SELECT * FROM myTable LIMIT 10`        | `df.limit(10)`                        |
| `display(myTable)` (text format)        | `df.show()`                           |
| `display(myTable)` (html format)        | `display(df)`                         |


### 访问数据

从`.parquet`文件创建DataFrame

```sql
ipGeocodeDF = spark.read.parquet("/mnt/training/ip-geocode.parquet")
```

从`.csv`文件创建DataFrame

```sql
bikeShareingDayDF = (spark.read.option("inferSchema","true")
                    .option("header","true")
                    .csv("/mnt/training/bikeSharing/data-001/day.csv"))
```

从`.json`文件创建DataFrame

```sql
databricksBlogDF = spark.read.option("inferSchema","true").option("header","true").json("/mnt/training/databricks-blog.json")
```

DataFrame变量创建完成后， 使用`show()`方法显示DataFrame中的数据，使用`show(n)`方法显示前n行数据。同时，也可以使用`display(df)`方法以HTML格式显示数据表格。

### 应用举例

步骤一：使用`spark.read.parquet()`从`.parquet`文件创建DataFrame变量。

```sql
peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
display(peopleDF)
```

步骤二：按照年份比较给定的两个名字（Donna和Dorthy）在1990年以后出生的女孩子中的使用量。

```sql
from pyspark.sql.functions import col
dordonDF = (peopleDF 
  .select(year("birthDate").alias("birthYear"), "firstName") 
  .filter((col("firstName") == 'Donna') | (col("firstName") == 'Dorothy')) 
  .filter("gender == 'F' ") 
  .filter(year("birthDate") > 1990) 
  .orderBy("birthYear") 
  .groupBy("birthYear", "firstName") 
  .count()
)
display(dordonDF)
```

## DataFrame的常用方法
**`show()`和`display()`**

使用`show()`或`show(n)`显示DataFrame数据

```sql
myDF.show()

-- 显示前10行数据
myDF.show(10)
```

使用`display()`和`display(df.limit(n))`以HTML表格显示DataFrame数据

```sql
display(myDF)

-- 以HTML显示前10行数据
display(myDF.limit(10))
```

**`select()`和`filter()`**

使用`select()`方法选取列，可以使用`alias()`方法修改列名。

```sql
from pyspark.sql.functions import col

myDF = peopleDF 
  	.select("firstName","middleName","lastName",col("birthDate").alias("date"),"gender") 
```

使用`filter()`方法根据条件过滤数据。

```sql
from pyspark.sql.functions import year

myDF = peopleDF
	.filter("gender='F'")
	.filter(year("birthDate") > "1990")
```

**`orderBy()`和`groupBy()`**

使用`orderBy()`方法按照某列排序。

```sql
-- 升序
myDF = peopleDF.orderBy("birthYear")

-- 使用desc方法降序
from pyspark.sql.functions import desc
myDF = peopleDF.orderBy(desc("birthYear"))
```

使用`groupBy()`和`count()`方法按某列进行数量统计。

```sql
myDF = (peopleDF.select(year("birthDate").alias("birthYear")).groupBy("birthYear").count())
```

使用`agg()`方法给统计出的列取别名。

```sql
from pyspark.sql.functions import count
myDF = (peopleDF.select(year("birthDate").alias("birthYear")).groupBy("birthYear").agg(count("birthYear").alias("total")))
```

使用`distinct()`方法获取不重复数据。

```sql
myDF = (peopleDF.select("firstName").distinct())
```

## Exercise 1

### Step 1

`peopleDF`是一个已知的DataFrame，该DataFrame的定义如下：

```
root
 |-- id: integer (nullable = true)
 |-- firstName: string (nullable = true)
 |-- middleName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- birthDate: timestamp (nullable = true)
 |-- ssn: string (nullable = true)
 |-- salary: integer (nullable = true)
```

请从`peopleDF` 中筛选出10个使用最多的女性名字(firstName)，并将结果保存到新的变量名为`top10FemaleFirstNameDF`的DataFrame中。

- firstName, 筛选出的女性名字
- total, 对应名字(firstName)的使用量

**注意：** 该语句需要导入python的`count`方法和`desc`方法。

- 返回前10个名字，使用`limit(10)`方法
- 给使用量的列修改列名为total使用`agg()`方法和`alias()`方法
- 使用量从多到少排序使用`desc()`方法

示例代码：

```sql
from pyspark.sql.functions import count, desc

top10FemaleFirstNamesDF = (peopleDF
                           .select("firstName")
                           .filter("gender='F'")
                           .groupBy("firstName")
                           .agg(count("firstName").alias("total"))
                           .orderBy(desc("total"))
                           .limit(10))
```

### Step 2

基于Step 1中创建的`top10FemaleFirstNamesDF`DataFrame创建一个Temporary View, View的变量名为`Top10FemaleFirstNames`, 并且将View的内容显示出来。

**注意：** Temporary View是的生命周期是一个Spark Session，它不会被保存下来。

- 使用Temporary View可以使用SQL语句直接作用于DataFrame
- 从DataFrame创建Temporary View使用`createOrReplaceTempView()`方法
- 使用`spark.sql()`应用SQL语句到View中

示例代码：

```sql
top10FemaleNamesDF.createOrReplaceTempView("Top10FemaleFirstNames")
resultDF = spark.sql("select * from Top10FemaleFirstNames")
display(resultDF)
```

## Exercise 2

`peopleDF`中的salary列中，有些值是负数，需要使用`abs()`方法将salary列中的值修改为大于0的数。

```sql
from pyspark.sql.functions import abs
peopleWithFixedSalariesDF = peopleDF.withColumn('salary',abs(peopleDF.salary))
```

### Step 1

基于`peopleWithFixedSalariesDF` 创建一个变量名为`peopleWithFixedSalaries20KDF`的DataFrame，要求：

- 过滤掉所有salary在20k以下的数据
- 添加一个新列，列名为salary10k，计算方法
  - 23, 000， 返回"2"
  - 57, 400，返回"6"
  - 1, 231,375， 返回"123"

```sql
from pyspark.sql.functions import col
peopleWithFixedSalaries20KDF = peopleWithFixedSalariesDF.filter("salary >= 20000").withColumn("salary10k", round(col("salary")/10000))

display(peopleWithFixedSalaries20KDF)
```

### Step 2

基于`peopleDF`，返回一个新的变量名为`carenDF`的DataFrame,要求：

- 返回一行一列
- 返回的列名为total
- 返回性别为女性，名字为Caren，出生日期在1980年之前的总数

```sql
from pyspark.sql.functions import month
carenDF = peopleDF.filter("gender='F'").filter("firstName = 'Caren'").filter(year("birthDate") < 1980).agg(count("id").alias("total"))
display(carenDF)
```


