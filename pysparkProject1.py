# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

#CREATE DATAFRAME
data = spark.read.csv("/FileStore/tables/googlestores.csv", header=True, inferSchema=True)


# COMMAND ----------

data.count()

# COMMAND ----------

data.show(n=10)

# COMMAND ----------

print(data.columns)

# COMMAND ----------

data.printSchema()

# COMMAND ----------

# DBTITLE 1,#DATA CLEANING

data=data.drop("size","Content Rating","Last Updated","Android Ver","Current Ver")
print(data.columns)

# COMMAND ----------

data.show(2)

# COMMAND ----------

data.printSchema()

# COMMAND ----------

# DBTITLE 1,#DATA CLEANING BY CHANGING THE DATA TYPES 

from pyspark.sql.functions import regexp_replace,col
data=data.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
    .withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
.withColumn("Installs",col("Installs").cast(IntegerType()))\
.withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
.withColumn("Price",col("Price").cast(IntegerType()))

# COMMAND ----------

data.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM apps;

# COMMAND ----------

# DBTITLE 1,Top Reviews Given To The App
# MAGIC %sql
# MAGIC
# MAGIC select App, sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Top 10 Installed Apps
# MAGIC %sql
# MAGIC select  App,sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC limit 10;

# COMMAND ----------

# DBTITLE 1,Category based Installed
# MAGIC %sql
# MAGIC select  Category,sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc;

# COMMAND ----------

# DBTITLE 1,Top Paid Apps
# MAGIC %sql
# MAGIC select  App,sum(Price) from apps
# MAGIC group by 1
# MAGIC order by 2 desc;

# COMMAND ----------


