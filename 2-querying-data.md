## Querying Data

使用`spark.read.parquet()`从`.parquet`文件创建DataFrame变量。

```sql
peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
display(peopleDF)
```


比较给定两个名字的使用量，从1990年以后逐年比较。

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


## Exercise

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

