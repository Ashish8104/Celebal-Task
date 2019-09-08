// Databricks notebook source
// DBTITLE 1,SQLZOO for chapter 5
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,creating dataframe "df"
val df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("/FileStore/tables/chapterno1.csv")

// COMMAND ----------

df.show()

// COMMAND ----------

// THIS IS FOR THE part 7
val condition_7 =  df.select("*").filter($"population" >=  10000000)
condition_7.groupBy($"continent").agg(count($"name")).show()

// COMMAND ----------

// DBTITLE 1,Query to watch out for.
// THIS IS FOR PART 8 
val condition_8 = df.groupBy($"continent").agg( (sum($"population") < 100000000).alias("population_greater") ).drop("population_greater")
condition_8.show()
