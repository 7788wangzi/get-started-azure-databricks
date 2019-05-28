## pyspark.sql.functions library中的常用方法

### 字符串函数

使用`lower()`和`upper()`方法，将文本转换为大写或小写。

```sql
DF.select(month(col("date")).alias("month"))
.filter(lower(col("primaryType")).contains("homicide"))
```

使用`contains()`方法，过滤某列中包含特定关键字的数据。

```sql
DF.select(month(col("date")).alias("month"))
.filter(lower(col("primaryType")).contains("homicide"))
```

### 日期函数

使用`month()` 和`year()`方法，从timestamp类型的数据中获取年份和月份。

```sql
from pyspark.sql.functions import month, col
DF.select(month(col("date")).alias("month"))
```

使用`to_date()`方法，将string转换为timestamp类型。

```sql
from pyspark.sql.functions import to_date, year, col
          
resultDF = (databricksBlog2DF.select("title", to_date(col("publishedOn"),"MMM dd, yyyy").alias('date'),"link") 
  .filter(year(col("publishedOn")) == '2013') 
  .orderBy(col("publishedOn"))
)

display(resultDF)
```

### 数组元素(json)

使用`sized()`方法获取数组cell中元素的个数。

```sql
from pyspark.sql.functions import size
display(databricksBlogDF.select(size("authors"),"authors"))
```

使用`explode()`方法按照某一个数组列，将一行数据拆分为多行。

```sql
from pyspark.sql.functions import explode
display(databricksBlogDF.select("title","authors",explode(col("authors")).alias("author"), "link"))
```

### 聚集函数

使用`avg()`方法获取平均数。

```sql
from pyspark.sql.functions import avg
avgSalaryDF = peopleDF.select(avg("salary").alias("averageSalary"))

avgSalaryDF.show()
```

使用`round()`方法取整，四舍五入。

```sql
from pyspark.sql.functions import round
roundedAvgSalaryDF = avgSalaryDF.select(round("averageSalary").alias("roundedAverageSalary"))

roundedAvgSalaryDF.show()
```

使用`min()`,`max()`方法获取最小值和最大值。

```sql
from pyspark.sql.functions import min, max
salaryDF = peopleDF.select(max("salary").alias("max"), min("salary").alias("min"), round(avg("salary")).alias("averageSalary"))

salaryDF.show()
```

使用`agg()`给`count()`方法取别名。

```sql
from pyspark.sql.functions import year, count
carenDF = peopleDF.filter("gender='F'").filter("firstName = 'Caren'").filter(year("birthDate") < 1980).agg(count("id").alias("total"))
display(carenDF)
```

使用`desc()`方法降序排列。

```sql
from pyspark.sql.functions import desc
PeopleWithFixedSalariesSortedDF = peopleWithFixedSalariesDF.orderBy(desc("salary"))
display(PeopleWithFixedSalariesSortedDF)
```

