// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC #Scatter Matrix
// MAGIC 
// MAGIC Generating a Scatter Matrix of features in your dataset is an easy way to visualize some of the relationships that exist.
// MAGIC 
// MAGIC Below are ways to create a scatter matrix natively in Databricks and utilizing MatPlotLib.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### With Databricks

// COMMAND ----------

/**   
   1. vendor name: 30 
      (adviser, amdahl,apollo, basf, bti, burroughs, c.r.d, cambex, cdc, dec, 
       dg, formation, four-phase, gould, honeywell, hp, ibm, ipl, magnuson, 
       microdata, nas, ncr, nixdorf, perkin-elmer, prime, siemens, sperry, 
       sratus, wang)
   2. Model Name: many unique symbols
   3. MYCT: machine cycle time in nanoseconds (integer)
   4. MMIN: minimum main memory in kilobytes (integer)
   5. MMAX: maximum main memory in kilobytes (integer)
   6. CACH: cache memory in kilobytes (integer)
   7. CHMIN: minimum channels in units (integer)
   8. CHMAX: maximum channels in units (integer)
   9. PRP: published relative performance (integer)
  10. ERP: estimated relative performance from the original article (integer)
**/

val columns = Array("vender_name", "model_name", "myct", "mmin", "mmax", "cach", "chmin", "chmax", "prp", "erp")

// Read in the machine data from the ics.uci.edu website, read as CSV and apply the above headers
val df = spark.read
  .option("inferSchema", "true")
  .csv(
      scala.io.Source.fromURL("https://archive.ics.uci.edu/ml/machine-learning-databases/cpu-performance/machine.data")
      .mkString
      .split("\n")
      .toSeq
      .toDF
      .as[String]
    ).toDF(columns: _*)

df.printSchema
df.createOrReplaceTempView("machine")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We can use databricks to display a single scatter plot using the `display` function and the plot options on the cell

// COMMAND ----------

display(df.select( "myct", "erp"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Here, using just a single scatter plot, we can see that as the `MYCT`, or machine cycle time in nanoseconds, goes down, the estimated performance rises.
// MAGIC 
// MAGIC Lets do the same thing, but select multiple columns to compare

// COMMAND ----------

display(df.select( "myct", "mmax", "chmax", "prp", "erp"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC These tell us more about the relationships of the multiple columns. We can see that as `PRP` goes up so does `ERP` which means the estimated performance was a pretty good indicator of the machines performance. Here if we are trying to do the estimation on the performance a it is good that the data we have looks so linear.
// MAGIC 
// MAGIC We can also see that as the `MMAX` goes up so does both performance metrics, and `CHMAX` also has a similar effect. `MYCT` is the opposite and as it rises the perfomance falls fast and bottoms out.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### With Python and Pandas
// MAGIC 
// MAGIC We can build a similar matrix with MatPlotLib and Pandads
// MAGIC 
// MAGIC <p><span style="color:red">Again remember that the `toPandas` method on a dataframe will collect the data to the driver memory.</span></p>

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import pandas as pd
// MAGIC 
// MAGIC df = spark.table("machine").select( "erp", "prp", "chmax", "mmax", "myct")
// MAGIC 
// MAGIC fig, ax = plt.subplots()
// MAGIC pandasDF = df.toPandas()
// MAGIC 
// MAGIC pd.scatter_matrix(pandasDF, ax=ax)
// MAGIC display(fig.figure)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC I personally like the look and labeling of the python scatter plot better. 
// MAGIC 
// MAGIC But the Databricks implementation provides some ease of use and extra features like the LOESS lines.
