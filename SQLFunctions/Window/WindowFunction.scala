// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # The Window Function
// MAGIC 
// MAGIC There are a lot of SQL functions that Spark has adopted in Spark SQL and the Dataframe/Dataset API. 
// MAGIC 
// MAGIC One of these functions which snuck up on me once when converting SQL Subroutines to Spark was the Window Function. 
// MAGIC 
// MAGIC Window enables a function to perform over all of the data in a Dataframe but independently by Windows. This removes the need to split the dataframe up and applying the function on the different parts and having to union them back together. 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The Window function exists in the `Expressions` package at `org.apache.spark.sql.expressions.Window`

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Here we can read in one of the example datasets that Spark provides:

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/asa/airlines/

// COMMAND ----------

val df = spark.read.option("header", true).csv("/databricks-datasets/asa/airlines/*").select($"Year", $"Month", $"FlightNum", $"ActualElapsedTime")

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The Window expression is formed similar to how it would be formed in SQL. 
// MAGIC 
// MAGIC We take the Window class, add some columns to partition by which creates the subset of rows, and within that subset we also select some columns to order by.

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
val window = Window.partitionBy("Year", "Month", "FlightNum").orderBy("Year", "Month")

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Now that the window function is setup we can utilize it within a select on the Dataframe. 
// MAGIC 
// MAGIC Here we select Year, Month, and FlightNum, then we calculate a Row Number over the `window` and alias that to the column `row_num`
// MAGIC 
// MAGIC We can see in the results that it added a incrementing row_number starting at 1 for each of the unique combinations of the columns the window was partitioned by

// COMMAND ----------

import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window

val window = Window.partitionBy("Year", "Month", "FlightNum").orderBy("Year", "Month")
val withWindowRowNum = df.select($"Year", $"Month", $"FlightNum", row_number().over(window).as("row_num"))
display(withWindowRowNum.orderBy($"Year", $"Month", $"FlightNum", $"row_num"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Here we do the same thing, but instead of calculating a row number we count the number of records in each partition.
// MAGIC 
// MAGIC This is good if you want to know how many rows exist for a unique combination of columns but without changing the table with a Group By. 

// COMMAND ----------

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.expressions.Window

val window = Window.partitionBy("Year", "Month", "FlightNum").orderBy("Year", "Month")
val withWindowCount = df.select($"Year", $"Month", $"FlightNum", count($"FlightNum").over(window).as("count"))
display(withWindowCount.orderBy($"Year", $"Month", $"FlightNum", $"count"))

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC This can also be performed with a `withColumn` instead of within a `select`

// COMMAND ----------

import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window

val window = Window.partitionBy("Year", "Month", "FlightNum").orderBy("Year", "Month")
val withColumn = df.withColumn("row_num", row_number().over(window))
display(withColumn)
