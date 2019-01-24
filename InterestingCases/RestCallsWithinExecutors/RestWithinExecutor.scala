// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #Rest Within an Executor
// MAGIC 
// MAGIC I once had a use case where we needed to obtain data from a rest endpoint using the details of data which was stored within a Dataframe. 
// MAGIC 
// MAGIC Below is a simplified example of Utilizing a restcall from within a dataframe as well as taking the JSON result and converting those into a Dataframe with a Schema.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We are utilizing the GuildWars 2 API because it contains a lot of entries, is well documented, and is fairly forgiving in terms of restrictions. 
// MAGIC 
// MAGIC Below is just the URI and endpoints we will be utilizing

// COMMAND ----------

val api = "https://api.guildwars2.com"
val recipes = "/v2/recipes"
val items = "/v2/items"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC First we need to get the list of all Recipes in the game, the Recipes endpoint returns an array of IDs when called without passing it a list of IDs.
// MAGIC 
// MAGIC We can see here I am just utilizing the basic `scala.io.Source` class to get the response and perform some String manipulation toget an array of ids.

// COMMAND ----------

val allRecipieURI = s"${api}${recipes}"
val allRecipeArray = scala.io.Source.fromURL(allRecipieURI).mkString.replaceAll("\\s|\\[|\\]","").split(",")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Using the Spark Implicits we can convert the `Array` of Ids into a `Seq` of Ids and call `.toDF` on it to obtain a dataframe which contains a row for each id.

// COMMAND ----------

val allRecipeDF = allRecipeArray.toSeq.toDF
display(allRecipeDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC This ends up giving us about 12 thousand ids in a dataframe

// COMMAND ----------

allRecipeDF.count

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Here is where the fun begins. Each ID can be used to call the same endpoint to obtain the json description of that recipe and all the information that goes along with it.
// MAGIC 
// MAGIC In this case, with only 12 thousand IDs, we could do this in memory. But in the case where you have billions of ids and need it all to perform some amount of work you can either download the data in parts and store it in a distributed store... or you could just download it right into the dataframe. 
// MAGIC 
// MAGIC So here we defined a UDF which takes the id and utilizes the `scala.io.Source` class to call the endpoint and retrieve the json object. 

// COMMAND ----------

import org.apache.spark.sql.functions.udf

val getItem = (id: String) => {
  scala.io.Source.fromURL(s"${allRecipieURI}?ids=${id}").mkString
}
val getItemUDF = udf(getItem)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We can utilize the UDF in a `withColumn` passing in the `value` column and assigning it to a column with the name `json`

// COMMAND ----------

// Used a Limit 10 to limit run time on Community Edition

val withItemJsonWithColumn = allRecipeDF.limit(10).withColumn("json", getItemUDF($"value"))
display(withItemJsonWithColumn)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We can also utilize the UDF directly within the select, passing in the `value` column and aliasing it to a column with the name `json`

// COMMAND ----------

// Used a Limit 10 to limit run time on Community Edition

val withItemJsonSelect = allRecipeDF.limit(10).select(getItemUDF($"value").as("json"))
display(withItemJsonSelect)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now that is great and all, we have a Dataframe of json objects as Strings. But to fully utilize Spark to work on these objects we will want them in a dataframe with the Json object as a Row with a Schema that represents the object and its internals. 
// MAGIC 
// MAGIC Here we take the Dataframe of 1 column, and convert it to a `Dataset[String]`, we do this with the `.as[String]` method on the Dataframe. 
// MAGIC 
// MAGIC Once it is a Dataset we can utilize the `spark.read.json` reader to parse the Json within the rows and turn them into a proper Dataframe.

// COMMAND ----------

val parsedJsonDF = spark.read.json(withItemJsonSelect.as[String])
display(parsedJsonDF)

// COMMAND ----------

parsedJsonDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC As I mentioned above, the GW2 API is very forgiving. And even though it is forgiving we limited the calls to 10 for this example. 
// MAGIC 
// MAGIC There are Rest endpoints who limit the connections to a certain amount within a timeframe in order to not overload the server.
// MAGIC 
// MAGIC When this came up in the past I have added artificial sleeps into the UDFs which are randomized between 50ms and 150ms. 
// MAGIC 
// MAGIC This will at least keep the Rest calls stagered when tasks kick off at the same time, and even more so when tasks are started at different times.

// COMMAND ----------

// Sleep the call between 50-150ms to stager the API calls
def getItem(id:String): String = {
  val r = new scala.util.Random()
  Thread.sleep(r.nextInt(100) + 50)
  scala.io.Source.fromURL(s"${allRecipieURI}?ids=${id}").mkString
}

val getItemUDF = udf(getItem _)
val itemsDF =  spark.read.json(
    allRecipeDF
    .limit(10)
    .select(getItemUDF($"value").as("json"))
    .as[String]
  )
display(itemsDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC We can see this about doubled the time it took to retrieve the json values
