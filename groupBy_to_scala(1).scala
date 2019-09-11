// Databricks notebook source
import org.apache.spark.sql.functions._
import spark.implicits._
import scala.collection.mutable.Map 

// COMMAND ----------

var A: Map[String,Int] = Map()

// COMMAND ----------

// TRYING MAP TO MAKE DATAFRAME FROM KEY VALUE PAIR
val colors = Map("lot" -> Seq(1,2,3,3,2), "val" -> Seq(10,9,8,7,6) )

// COMMAND ----------

// colors.groupBy("lot").agg("val".count)
val df = colors.toSeq.toDF("lot","val").show()

// COMMAND ----------

// DBTITLE 1,ONLY FOR TEST PURPOSE
"""val valu = List(1,2,4,4,3,3)
var out: Map[Int, Int] = Map()

for((k,v ) <- valu.groupBy(_ % 3))
{
  printf("key:", k ,"\n" )
  printf("value :",v )
  
}
// var out: output[]
// for (x,a <- in ){
//   println(a) }


"""

// COMMAND ----------

// DBTITLE 1,FINALIZE DATAFRAME FOR TASK
val dfs = Seq(
(1,10),
(2,9),
(3,8),
(2,7),(3,6)).toDF("lot","val")
dfs.show()

// COMMAND ----------

dfs.groupBy("lot").agg(count($"val")).show()

// COMMAND ----------

import sqlContext.implicits._

// COMMAND ----------

dfs.groupBy("lot").agg(count("val")).collect()

// COMMAND ----------

// We are now selecting the disitct values Like we do in "GROUP BY "
val lot_val =  dfs.select("lot").distinct.collect().flatMap(_.toSeq)
print(lot_val)

//WE get the Array[Int] 

// COMMAND ----------

// now with using map we are getting Integer number and [Dataset value like arrray]

//this returns Seq[(Any, Dataframe)] as (lot, Dataframe)
val val_val = lot_val.map(d => (d -> dfs.where($"lot" === d)))

print(val_val)

// COMMAND ----------

// DBTITLE 1,Importing dataframe class
import org.apache.spark.sql.DataFrame


// COMMAND ----------

// DBTITLE 1,Generarting each value key value pair for key and there respective values.
// Creating the Empty map will take any and dataframe as value
var A: Map[Any, DataFrame] = Map()

// Iterate over each value of group by column
for (a <- lot_val)
{
  //   println(a)  This will print the distinct value of Array
  
  
  A.put(a , dfs.where($"lot"===a))  // Appending the key and datafrane in spark.
}
print(A)

// COMMAND ----------

A(2).show()

// COMMAND ----------

// Now for each row we are printing the 
val_val.foreach(a => a.class ) 

// COMMAND ----------

val_val.foreach(a => a._2.show())
