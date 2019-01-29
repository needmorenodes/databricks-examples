// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #Types of Spark Joins
// MAGIC 
// MAGIC This notebook is to provide examples of:
// MAGIC 
// MAGIC 1. The different types of joins in spark
// MAGIC 1. How the type of join is determined
// MAGIC 1. Some of the DAGs created by the Joins.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Shuffles and shuffle boundries
// MAGIC 
// MAGIC Just a quick refresher on join performance and Shuffles/Stage boundries.
// MAGIC 
// MAGIC A join will almost always cause a shuffle of data over the network. The one of the reasons the different types of joins exist is to allow for the best performance around this fact. 
// MAGIC 
// MAGIC Broadcast joins exist to know of an upfront cost to the shuffle that the join will incure and to shuffle the entire smaller dataset instead of potentially both datasets. 
// MAGIC 
// MAGIC Since Stage boundries are built on shuffles, a join will almost always cause a new stage to be created. *Apparently unless you are in community edition and only have 1 executor*
// MAGIC 
// MAGIC This will be apperent by the DAG generated, if I can run this notebook on a proper cluster I will link the DAG images.

// COMMAND ----------

// MAGIC %md 
// MAGIC If we take a look into the Spark SQL execution code, specifically in [org.apache.spark.sql.execution.SparkStrategies](https://github.com/apache/spark/blob/branch-2.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala), we can see where Spark makes its determination on the type of join to use. 
// MAGIC 
// MAGIC The JavaDoc comment on the [JoinSelection](https://github.com/apache/spark/blob/branch-2.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala#L148) Strategy provides an overview of the code.
// MAGIC 
// MAGIC ```
// MAGIC   /**
// MAGIC    * Select the proper physical plan for join based on joining keys and size of logical plan.
// MAGIC    *
// MAGIC    * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
// MAGIC    * predicates can be evaluated by matching join keys. If found, join implementations are chosen
// MAGIC    * with the following precedence:
// MAGIC    *
// MAGIC    * - Broadcast hash join (BHJ):
// MAGIC    *     BHJ is not supported for full outer join. For right outer join, we only can broadcast the
// MAGIC    *     left side. For left outer, left semi, left anti and the internal join type ExistenceJoin,
// MAGIC    *     we only can broadcast the right side. For inner like join, we can broadcast both sides.
// MAGIC    *     Normally, BHJ can perform faster than the other join algorithms when the broadcast side is
// MAGIC    *     small. However, broadcasting tables is a network-intensive operation. It could cause OOM
// MAGIC    *     or perform worse than the other join algorithms, especially when the build/broadcast side
// MAGIC    *     is big.
// MAGIC    *
// MAGIC    *     For the supported cases, users can specify the broadcast hint (e.g. the user applied the
// MAGIC    *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame) and session-based
// MAGIC    *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold to adjust whether BHJ is used and
// MAGIC    *     which join side is broadcast.
// MAGIC    *
// MAGIC    *     1) Broadcast the join side with the broadcast hint, even if the size is larger than
// MAGIC    *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. If both sides have the hint (only when the type
// MAGIC    *     is inner like join), the side with a smaller estimated physical size will be broadcast.
// MAGIC    *     2) Respect the [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold and broadcast the side
// MAGIC    *     whose estimated physical size is smaller than the threshold. If both sides are below the
// MAGIC    *     threshold, broadcast the smaller side. If neither is smaller, BHJ is not used.
// MAGIC    *
// MAGIC    * - Shuffle hash join: if the average size of a single partition is small enough to build a hash
// MAGIC    *     table.
// MAGIC    *
// MAGIC    * - Sort merge: if the matching join keys are sortable.
// MAGIC    *
// MAGIC    * If there is no joining keys, Join implementations are chosen with the following precedence:
// MAGIC    * - BroadcastNestedLoopJoin (BNLJ):
// MAGIC    *     BNLJ supports all the join types but the impl is OPTIMIZED for the following scenarios:
// MAGIC    *     For right outer join, the left side is broadcast. For left outer, left semi, left anti
// MAGIC    *     and the internal join type ExistenceJoin, the right side is broadcast. For inner like
// MAGIC    *     joins, either side is broadcast.
// MAGIC    *
// MAGIC    *     Like BHJ, users still can specify the broadcast hint and session-based
// MAGIC    *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold to impact which side is broadcast.
// MAGIC    *
// MAGIC    *     1) Broadcast the join side with the broadcast hint, even if the size is larger than
// MAGIC    *     [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. If both sides have the hint (i.e., just for
// MAGIC    *     inner-like join), the side with a smaller estimated physical size will be broadcast.
// MAGIC    *     2) Respect the [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold and broadcast the side
// MAGIC    *     whose estimated physical size is smaller than the threshold. If both sides are below the
// MAGIC    *     threshold, broadcast the smaller side. If neither is smaller, BNLJ is not used.
// MAGIC    *
// MAGIC    * - CartesianProduct: for inner like join, CartesianProduct is the fallback option.
// MAGIC    *
// MAGIC    * - BroadcastNestedLoopJoin (BNLJ):
// MAGIC    *     For the other join types, BNLJ is the fallback option. Here, we just pick the broadcast
// MAGIC    *     side with the broadcast hint. If neither side has a hint, we broadcast the side with
// MAGIC    *     the smaller estimated physical size.
// MAGIC    */
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Below two dataframes are created:
// MAGIC 
// MAGIC `cowsDF` is a list of cows with their name, id, and age. 
// MAGIC 
// MAGIC `isFluffyDF` contains a reference to the cows by id and a boolean to denote if they are... fluffy.
// MAGIC 
// MAGIC <img src="https://needmorenodes.github.io/databricks-examples/SparkInternals/Images/FluffyCow.jpg" alt="drawing" width="200"/>
// MAGIC 
// MAGIC Here is the classic fluffy cow picture incase your imagination was wandering.

// COMMAND ----------

val r = new scala.util.Random(42)
def cowNameGenerator(): String = {
  val cowNames = Array("Bertha", "Betty", "Bessie", "Netty", "Nellie", "Rose", "Meg", "Dahlia", "Margie", "Lois", "Flower", "Maggie", "Jasmine", "Minnie", "Esmeralda", "Bella", "Daisy", "Shelly", "Candie")
  cowNames(r.nextInt(cowNames.length))
}

case class Cow(id:Long, name:String, age:Integer)
case class IsFluffy(id:Long, is_fluffy:Boolean)

val tenThousandCows = (1 to 10000).map(id => Cow(id, cowNameGenerator(), r.nextInt(15))).toSeq
val tempCowsDF = tenThousandCows.toDF
tempCowsDF.write.parquet("/tmp/cows")

val cowsDF = spark.read.parquet("/tmp/cows")
val isFluffyDF = Seq(
  IsFluffy(5L, true),
  IsFluffy(50L, true),
  IsFluffy(100L, true),
  IsFluffy(500L, true),
  IsFluffy(600L, true)
).toDF


//Hiding large output
displayHTML("")

// COMMAND ----------

display(cowsDF)

// COMMAND ----------

display(isFluffyDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Broadcast Joins

// COMMAND ----------

//Reset autoBroadcastJoinThreshold to the default of 10M for repeat runs
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10L * 1024 * 1024)

// COMMAND ----------

val nameFluffyDF = cowsDF.join(isFluffyDF, Seq("id"))
nameFluffyDF.explain(extended = true)

// COMMAND ----------

display(nameFluffyDF)

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Even with the threshold set to -1 we can force a broadcast join by supplying a hint to the join with the `org.apache.spark.sql.functions.broadcast` function.
// MAGIC 
// MAGIC This may still result in a `SortMergeJoin` if the hinted table doesn't fit the boradcast requirements.

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast

val nameFluffyDF = cowsDF.join(broadcast(isFluffyDF), Seq("id"))
nameFluffyDF.explain(extended = true)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Sort Merge Joins
// MAGIC 
// MAGIC Even though they come after the `ShuffleHashJoin` in the case statement, `SortMergeJoin` will normally be the next option after checking for a `BroadcastJoin`. This is partly due to the setting `spark.sql.join.preferSortMergeJoin` which defaults to `true` in the configuration.

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

// COMMAND ----------

val nameFluffyDF = cowsDF.join(isFluffyDF, Seq("id"))
nameFluffyDF.explain(extended = true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Shuffle Hash Join
// MAGIC 
// MAGIC Shuffle Hash joins are technically next in line after a Broadcast join, though there exists a specific spark setting to skip over them called `spark.sql.join.preferSortMergeJoin`.
// MAGIC 
// MAGIC As of this point I still have not been able to generate a dataset to show the utilization of the Shuffle Hash Join for this notebook...
// MAGIC 
// MAGIC Even with `preferSortMergeJoin` set to false I need to dig deeper into the following case statements to generate a dataset to take this fork. 
// MAGIC 
// MAGIC ```
// MAGIC case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
// MAGIC    if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
// MAGIC      && muchSmaller(right, left) ||
// MAGIC      !RowOrdering.isOrderable(leftKeys) =>
// MAGIC   Seq(joins.ShuffledHashJoinExec(
// MAGIC     leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))
// MAGIC 
// MAGIC case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
// MAGIC    if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
// MAGIC      && muchSmaller(left, right) ||
// MAGIC      !RowOrdering.isOrderable(leftKeys) =>
// MAGIC   Seq(joins.ShuffledHashJoinExec(
// MAGIC     leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))
// MAGIC ```

// COMMAND ----------

spark.conf.set("spark.sql.join.preferSortMergeJoin", false)
spark.conf.get("spark.sql.join.preferSortMergeJoin")
spark.sessionState.conf.preferSortMergeJoin

// COMMAND ----------

val nameFluffyDF = cowsDF.join(isFluffyDF, Seq("id"))
nameFluffyDF.explain(extended = true)
