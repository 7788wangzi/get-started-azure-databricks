## 从Azure Blob中读取数据

### 在DBFS中加载Azure Blob

步骤一：创建Shared Access Signature(SAS) URL。

步骤二：在DBFS中使用`dbutils.fs.mount(source=..,mount_point=..,extra_configs=..)`加载Azure Blob

```sql
SasURL = "https://dbtraineastus2.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:32:30Z&st=2018-04-18T22:32:30Z&spr=https&sig=BB%2FQzc0XHAH%2FarDQhKcpu49feb7llv3ZjnfViuI9IWo%3D"
indQuestionMark = SasURL.index('?')
SasKey = SasURL[indQuestionMark:len(SasURL)]
StorageAccount = "dbtraineastus2"
ContainerName = "training"
MountPoint = "/mnt/temp-training"

dbutils.fs.mount(
  source = "wasbs://%s@%s.blob.core.windows.net/" % (ContainerName, StorageAccount),
  mount_point = MountPoint,
  extra_configs = {"fs.azure.sas.%s.%s.blob.core.windows.net" % (ContainerName, StorageAccount) : "%s" % SasKey}
)
```

查看从Azure Blob中的加载的文件

```sql
%fs ls /mnt/temp-training
```

从Azure Blob中的CSV文件创建DataFrame

```sql
val myDF = spark.read.option("header","true").option("inferSchema","true").csv("dbfs:/mnt/temp-training/myfile.csv")
```

### 在DBFS中unmountAzure Blob

调用`dbutils.fs.unmount()`，并将加载时候定义的mount_point作为参数传入。

```sql
dbutils.fs.unmount("/mnt/temp-training")
```

