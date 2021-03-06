// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC #Heatmaps
// MAGIC 
// MAGIC Generating a heatmap based on your feature correlations is a good way to show the relationships in your data.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Databricks supplied bike sharing dataset

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/bikeSharing/data-001/

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/bikeSharing/README.md

// COMMAND ----------

val bikeDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/databricks-datasets/bikeSharing/data-001/day.csv").drop("dteday")

// COMMAND ----------

display(bikeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Utilize the VectorAssembler to turn the features into a Vector for use in the correlation object.
// MAGIC 
// MAGIC Also create a temp view to be able to read this dataset from Python as well.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()  
    .setInputCols(bikeDF.columns)
    .setOutputCol("features")

val featureDF = assembler.transform(bikeDF)
featureDF.createOrReplaceTempView("bike_features")

// COMMAND ----------

// MAGIC %md 
// MAGIC Calculate the correlation between all of the features using the spark Correlation object.
// MAGIC 
// MAGIC Also turn this dataframe of correlations into a temp view of transfering to Python.

// COMMAND ----------

import org.apache.spark.ml.stat.Correlation

val correlations = Correlation.corr(featureDF, "features")
correlations.createOrReplaceTempView("bike_correlations")
display(correlations)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Take the correlations and the bike columns and create a Pandas Dataframe
// MAGIC 
// MAGIC The act of creating a Pandas dataframe will pull your entire dataframe into the driver memory, so be careful doing this with large datasets.

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC 
// MAGIC featureDF = spark.table("bike_features")
// MAGIC correlationDF = spark.table("bike_correlations")
// MAGIC bikeCoor = correlationDF.collect()[0][0]
// MAGIC pandasDF = pd.DataFrame(bikeCoor.toArray())
// MAGIC 
// MAGIC pandasDF.index, pandasDF.columns = featureDF.drop("features").columns, featureDF.drop("features").columns
// MAGIC print(pandasDF.index)
// MAGIC print(pandasDF.columns)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC With Matplotlib and seaborn we can create a heatmap with the Pandas dataframe and view the correlations of the data

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import seaborn as sns
// MAGIC 
// MAGIC fig, ax = plt.subplots()
// MAGIC sns.heatmap(pandasDF)
// MAGIC display(fig.figure)
