// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #Catalyst Optimizer

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC The Spark Catalyst Optimizer is used to turn a query, whether created in Spark SQL or from utilizing the Dataframe/Dataset API, into a tree structure which is operated on by predefined Rules which transform the trees into more optimized versions.
// MAGIC 
// MAGIC These Rules as they stand in Spark 2.4 can be seen in the package [org.apache.spark.sql.catalyst](https://github.com/apache/spark/tree/branch-2.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst)
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC In Spark SQL the Catalyst Optimizer operates in 4 stages:
// MAGIC 
// MAGIC 1. Analyzing a logical plan to resolve references
// MAGIC 2. Logical plan optimization
// MAGIC 3. Physical planning
// MAGIC 4. Code generation to compile parts of the query to Java bytecode.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Here we are using a provided dataset in parquet to show the output of the 4 steps of the optimizer

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/amazon/data20K/

// COMMAND ----------

val df = spark.read.parquet("/databricks-datasets/amazon/data20K/")

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Here we can see all of the steps for the read explained. 
// MAGIC 
// MAGIC Since we are doing no work with the dataframe, the 3 plans are very similar resulting in a physical plan with no pushed down filters or optimizations

// COMMAND ----------

df.explain(extended = true)

// COMMAND ----------

// MAGIC %md 
// MAGIC If we perform something as simple as select on a parquet dataset we can see some action happen within the plans

// COMMAND ----------

df.select("rating").explain(extended = true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Here in the Parsed Logical Plan:
// MAGIC 
// MAGIC ```
// MAGIC == Parsed Logical Plan ==
// MAGIC 'Project [unresolvedalias('rating, None)]
// MAGIC +- Relation[rating#92,review#93] parquet
// MAGIC ```
// MAGIC We can see that it knows we want the column we called "rating" but it marks the fact that it is unresolved, meaning that at this point it is unsure what that column is, if it exists, what its type is, and so on.
// MAGIC 
// MAGIC 
// MAGIC In the Analyzed Logical Plan:
// MAGIC 
// MAGIC ```
// MAGIC == Analyzed Logical Plan ==
// MAGIC rating: double
// MAGIC Project [rating#92]
// MAGIC +- Relation[rating#92,review#93] parquet
// MAGIC ```
// MAGIC 
// MAGIC The resolution of that column was completed, it was given a type as well as an id of #92.
// MAGIC 
// MAGIC Then the Optimized plan is no different than the previous steps since we don't actually use those columns for anything after that.
// MAGIC 
// MAGIC But if you look at the physical plan:
// MAGIC 
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC *(1) FileScan parquet [rating#92] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[dbfs:/databricks-datasets/amazon/data20K], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<rating:double>
// MAGIC ```
// MAGIC 
// MAGIC You can see the pushed down select `FileScan parquet [rating#92] ` instead of `FileScan parquet [rating#92,review#93]`, meaning the file scan will only read the ratings column.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now lets perfom multiple filters with the rating column so we can see an optimization at work.

// COMMAND ----------

df.select("rating").filter($"rating" > 3d).filter($"rating" < 4d).explain(extended = true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Here in the Parsed Logical Plan:
// MAGIC 
// MAGIC ```
// MAGIC == Parsed Logical Plan ==
// MAGIC 'Filter ('rating < 4.0)
// MAGIC +- Filter (rating#92 > 3.0)
// MAGIC    +- Project [rating#92]
// MAGIC       +- Relation[rating#92,review#93] parquet
// MAGIC ```
// MAGIC 
// MAGIC We can see our two filters performing separately, with only one of the filters containing the `rating` columns id tied to the column read from the parquet 
// MAGIC 
// MAGIC In the Analyzed Logical Plan:
// MAGIC 
// MAGIC ```
// MAGIC == Analyzed Logical Plan ==
// MAGIC rating: double
// MAGIC Filter (rating#92 < 4.0)
// MAGIC +- Filter (rating#92 > 3.0)
// MAGIC    +- Project [rating#92]
// MAGIC       +- Relation[rating#92,review#93] parquet
// MAGIC ```
// MAGIC 
// MAGIC It has analyzed the columns and determined that both filters are operating on the same column `ratings#92` and that rating is infact a double so it doesnt need to  perform any type of casting.
// MAGIC 
// MAGIC Now for this case in the Optimized Logical Plan:
// MAGIC 
// MAGIC ```
// MAGIC == Optimized Logical Plan ==
// MAGIC Project [rating#92]
// MAGIC +- Filter ((isnotnull(rating#92) && (rating#92 > 3.0)) && (rating#92 < 4.0))
// MAGIC    +- Relation[rating#92,review#93] parquet
// MAGIC ```
// MAGIC 
// MAGIC We can see it made some changes. It took the two filters, combined them with a logical and, and also threw in a not null for good measure since these operations can't be performed on null doubles.
// MAGIC 
// MAGIC And finally we can see the work of this Optimized Logical Plan pushed down to the Physical Plans File Scan utilizing Predicate Pushdown:
// MAGIC 
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC *(1) Project [rating#92]
// MAGIC +- *(1) Filter ((isnotnull(rating#92) && (rating#92 > 3.0)) && (rating#92 < 4.0))
// MAGIC    +- *(1) FileScan parquet [rating#92] Batched: true, DataFilters: [isnotnull(rating#92), (rating#92 > 3.0), (rating#92 < 4.0)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/databricks-datasets/amazon/data20K], PartitionFilters: [], PushedFilters: [IsNotNull(rating), GreaterThan(rating,3.0), LessThan(rating,4.0)], ReadSchema: struct<rating:double>
// MAGIC ```
// MAGIC 
// MAGIC Meaning that all of the work for those filters will be performed in the FileScan itself as supported by Parquet. Limiting the data we even attempt to read by the filters we have defined.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now all of the optimizations, aside from the Pushdown and type determinations, could have been made manaually by performing the ANDs in one filter call.

// COMMAND ----------

df.select("rating").filter(($"rating" > 3d).and($"rating" < 4d).and($"rating".isNotNull)).explain(extended = true)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Here we can see the three plans are pretty much identical which all lead up to a filescan with the pushed down filters.
// MAGIC 
// MAGIC So why don't we just write our code like this all of the time. 
// MAGIC 
// MAGIC Well for large queries this can be very hard to read if we chained all of the filters with ands and ors within one filter call.
// MAGIC 
// MAGIC Another reason is we may not be able to easily with a switch in our code. Take below for example.
// MAGIC 
// MAGIC Here the result of the random number dictates what we are to be doing to the Dataframe. Here they are just filters but maybe we only need certain columns depending on the random number, or we need to join in other tables. Here the Optimizer is able to perform the same optimizations as we manually performed above but do it with the dynamically created dataframe.

// COMMAND ----------

val r = scala.util.Random
var temp = r.nextInt(2)

var df2 = df
if(temp == 0) {
  df2 = df2.filter($"rating" > 3d)
} else if (temp == 1){
  df2 = df2.filter($"rating" > 4d)
}

temp = r.nextInt(2)
if(temp == 0) {
  df2 = df2.filter($"rating" < 5d)
} else if (temp == 1){
  df2 = df2.filter($"rating" < 6d)
}
df2.explain(extended = true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC As much of a help the catalyst optimizer can be there may be a situation where you have to turn off some of the optimal optimizations. 
// MAGIC 
// MAGIC In Spark 2.4 they added a feature [SPARK-24802](https://issues.apache.org/jira/browse/SPARK-24802) which allows for the disabling of certain Optimizations. It can be set using the line `sparl.conf.set("spark.sql.optimizer.excludedRules", "")` and giving it a comma separated list of the fully qualified Rule name. 
// MAGIC 
// MAGIC I have never had the need to disable any of the optimizations but it is good to know this feature exists.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Below we exclude the Combine and InferFilter optimization rules:

// COMMAND ----------

spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.CombineFilters,org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Here we can check the optimizer batches and their rules to make sure they no longer contain the rules we excluded

// COMMAND ----------

spark.sessionState.optimizer.batches.foreach(batch => batch.rules.foreach(rule => if(rule.ruleName.contains("CombineFilters")) {println(rule.ruleName)}))

// COMMAND ----------

spark.sessionState.optimizer.batches.foreach(batch => batch.rules.foreach(rule => if(rule.ruleName.contains("Infer")) {println(rule.ruleName)}))

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC So if we run the same code from before and look at the plan

// COMMAND ----------

val df = spark.read.parquet("/databricks-datasets/amazon/data20K/")
df.select("rating").filter($"rating" > 3d).filter($"rating" < 4d).explain(extended = true)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Now we can see the Optimized Logical Plan:
// MAGIC 
// MAGIC ```
// MAGIC == Optimized Logical Plan ==
// MAGIC Project [rating#78]
// MAGIC +- Filter (rating#78 < 4.0)
// MAGIC    +- Filter (rating#78 > 3.0)
// MAGIC       +- Relation[rating#78,review#79] parquet
// MAGIC ```
// MAGIC 
// MAGIC And how it no longer contains the Optimizations we saw above and is missing the isNotNull filter all together
