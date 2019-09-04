// Databricks notebook source
// DBTITLE 1,Null one
// IMPORTING ALL FUNCTIONS IN SPARK
import org.apache.spark.sql.functions._

// COMMAND ----------

//  File uploaded to /FileStore/tables/teacher.csv
// File uploaded to /FileStore/tables/dept.csv

val teacher_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/teacher.csv")
teacher_df.show()

// COMMAND ----------


val dept_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/dept.csv")
dept_df.show()

// COMMAND ----------

// Checking the null values
teacher_df.select($"name").where($"dept".isNull).show()

// COMMAND ----------

// THIS IS PART 4
dept_df.join(teacher_df, dept_df("id")===teacher_df("dept") )
.select(dept_df("name"), teacher_df("name")).show()

// COMMAND ----------

// THIS IS PART 8
teacher_df.join(dept_df, teacher_df("dept")===dept_df("id") )
.groupBy(dept_df("name")).agg( count(teacher_df("name")) ).show()

// COMMAND ----------

// DBTITLE 1,CASES Function implementation in spark dataframe
// THIS IS FOR PART 10.
// CHECKING THE FIRST CASE INSIDE SELECT AND USING WHEN
teacher_df.select( $"name", when($"dept".isin(1,2), "THI")
                  // CHECKING SECOND CASE AND ELSE CONDITION
.when($"dept"===3, "Art").otherwise("None")).show()



/*SQL CODE
select name, 
case
  when dept in (1,2) Then 'Sci'
  when dept=3 Then 'Art'
else
   'None'
end
 from teacher
*/


