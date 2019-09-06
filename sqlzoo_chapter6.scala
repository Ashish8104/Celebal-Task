// Databricks notebook source
//  File uploaded to /FileStore/tables/eteam.csv
// File uploaded to /FileStore/tables/goal.csv
//  File uploaded to /FileStore/tables/game.csv

import org.apache.spark.sql.functions._


// COMMAND ----------

val eteam_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/eteam.csv")
eteam_df.show()

// COMMAND ----------

val goal_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/goal.csv")
goal_df.show()

// COMMAND ----------

val game_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/game.csv")
game_df.show()

// COMMAND ----------

// DBTITLE 1,Inner join between two tables
// THIS IS FOR PART 3
// APPLYING THE INNER JOIN BETWEEN TWO TABLES

game_df.select($"*").join(goal_df, $"id"===$"matchid", "inner").where($"teamid" === "GRE").show()

// COMMAND ----------

// THIS IS FOR PART 4

game_df.join(goal_df, $"id"===$"matchid").select($"team1",$"team2", $"player").where($"player".like("Robert%")).show()

// COMMAND ----------

// THIS IS FOR PART 5
goal_df.join(eteam_df, $"id"===$"teamid", "inner").filter($"gtime"  <= 18).select($"player",$"teamid",$"coach",$"gtime").show()

// COMMAND ----------

// THI IS FOR PART 6
//   FIRST JOINNING THE TWO DATAFRAME
game_df.join(eteam_df).where( game_df("team1") === eteam_df("id") )
//     FILTERING THE DATA
.where(eteam_df("coach") === "Fernando Santos" )
.select($"mdate",$"teamname").show()

// COMMAND ----------

 import sqlContext.implicits._

// COMMAND ----------

// THIS IS FOR PART 8
game_df.join(goal_df, $"matchid"===$"id")
.where( ($"team1" ==="GRE" || $"team2" ==="GRE") && $"teamid"!="GER" )
.select($"player").distinct().show()

// COMMAND ----------

// THIS IS FOR PART 10
game_df.join(goal_df, $"id"===$"matchid").groupBy($"stadium").agg(count("*")).show()

// COMMAND ----------

// THIS IS FOR PART 11
game_df.join(goal_df,$"id"===$"matchid").where($"team1"==="POL" || $"team2"==="POL")
.groupBy($"mdate",$"matchid").agg(count($"gtime")).show()
