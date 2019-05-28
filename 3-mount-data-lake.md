## Azure Databricks从Data Lake读取源数据

Azure Data Lake Storage (ADLS)是传统Data Warehouse之外的一个可选方案， ADLS支持多种不同格式的源数据，比如csv， json， parquet等等， 使用Azure Databricks File System(DBFS)命令将数据从ADLS中加载到DBFS中，然后使用Spark对数据进行分析。



**Data Lake的特点：**

- Data Lake源数据格式的多样性。
- 在Data Lake中存放的数据不需要有严格统一的Schema定义，相比Data Warehouse不需要做很多数据格式预定义的工作。

- Data Lake允许存放大文件，借助Spark引擎可以高效地读取处理大文件。



### 在DBFS中加载ADLS

访问Data Lake需要使用Azure Active Directory进行身份验证，在DBFS中使用，可以通过Service Principle服务进行授权。

步骤一：创建新的App Registration, 获取App的`Client Id`和`Client Key`。

步骤二：在Data Lake中授权步骤一中创建的App以读写所有文件及子文件夹的权限。

步骤三：在Azure Databricks File System (DBFS) 中使用App的`Client Id`， `Client Key`和定义的`tenant Id`访问Data Lake。

```sql
-- Data Lake变量
adlsAccountName= "adls527j"

-- client id, client key, tenant id 变量
clientId ="48abef73-e2e1-6666-a2a0-59779ba1a7e1"
clientKey ="VkNQoHvYNC2qN-_iAuDF}0D/8H02OOmk"
tenantId ="72f911bf-86f2-42af-92ab-2d7cd033db47"

-- 拼接配置文件字符串
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": clientId,
           "dfs.adls.oauth2.credential": clientKey,
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"}
           
-- 使用配置文件加载Data Lake           
dbutils.fs.mount(
  source = "adl://" + adlsAccountName + ".azuredatalakestore.net/",
  mount_point = "/mnt/adls",
  extra_configs = configs)
```

查看从Data Lake中的加载的文件

```sql
%fs ls mnt/adls/
```

### 在DBFS中unmount ADLS

调用`dbutils.fs.unmount()`，并将加载时候定义的mount_point作为参数传入。

```sql
dbutils.fs.unmount("/mnt/adls")
```

