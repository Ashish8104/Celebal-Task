# Databricks notebook source
# DBTITLE 1,Creating Spark Instance
#/FileStore/tables/dataset0.csv

from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("test1").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Getting dataframe according to column delimiter
df = sc.read.format("csv").option("header", True).option("delimiter",";").csv('/FileStore/tables/dataset0.csv')
dfs= df
df.show()



# COMMAND ----------

# DBTITLE 1,concatinate two column having 1 text delimiter each [manually]
from pyspark.sql.functions import concat_ws
dfs=dfs.withColumn("$names$",(concat_ws(";", "$name", "surname$"))).drop("$name", "surname$")
dfs.show()

# COMMAND ----------

# DBTITLE 1,Creating only the perfect name from dataframe
listed=[]
for x in dfs.columns:
  #print(x)
  if "$" in x:
    #listed.append(x)
    z=x.split("$")[1]
    print(z)
    listed.append(z)
    

# COMMAND ----------

# DBTITLE 1,Making new dataframe
from pyspark.sql.functions import *

for x in range(len(listed)):

  # new column with all data between ["$ __ $"]
  dfs = dfs.withColumn(listed[x], split(dfs[x], "[$]")[1] )
#droping unneccesary column  
dfs=dfs.drop("$class$", "$age$", "$names$")
dfs.show()

# COMMAND ----------

# DBTITLE 1,Using ["modules "] to read dataframe according to delimiter
# But doesn't work well but it's also a option can be handy at some time


dftest = spark.read.format("csv").option("header", "true").option("modules","$").csv('/FileStore/tables/dataset0.csv')
dftest.show()
