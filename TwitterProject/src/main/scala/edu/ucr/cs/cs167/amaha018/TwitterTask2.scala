package edu.ucr.cs.cs167.amaha018

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.expressions.Window

object TwitterTask2 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Twitter Task 2")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT

    val inputFile: String = args(0)
    val outputFile: String = args(1)

    val tweetsDFView = sparkSession.read.format("json")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputFile)
      .createOrReplaceTempView("tweetsDF")

    // Remove records with hashtags that aren't in the top 20 array ( or empty )
    val noEmptyHashtagDF = sparkSession.sql(
      s"""SELECT *
         |FROM tweetsDF
         |WHERE exists(hashtags , x -> array_contains(array("ALDUBxEBLoveis","FurkanPalalı","no309","LalOn","chien","job","Hiring","sbhawks","Top3Apps","perdu","trouvé","CareerArc","Job","trumprussia","trndnl","Jobs","ShowtimeLetsCelebr8","hiring","impeachtrumppence","music"), x))
         |""".stripMargin
    ).withColumn("temp1", lit(1)).withColumn("row1",row_number.over(Window.partitionBy("temp1").orderBy("temp1")))
    noEmptyHashtagDF.createOrReplaceTempView("noEmptyHashtag")

    // Get column with first element of intersection of relevant hashtags array with the top 20 array
    val topicDF = sparkSession.sql(
      s"""SELECT element_at(
         |  array_intersect(
         |   hashtags,
         |   array("ALDUBxEBLoveis","FurkanPalalı","no309","LalOn","chien","job","Hiring","sbhawks","Top3Apps","perdu","trouvé","CareerArc","Job","trumprussia","trndnl","Jobs","ShowtimeLetsCelebr8","hiring","impeachtrumppence","music")
         |  ) ,
         |  1
         |)
         |AS topic FROM noEmptyHashtag
         |""".stripMargin
    ).withColumn("temp2", lit(1)).withColumn("row2",row_number.over(Window.partitionBy("temp2").orderBy("temp2")))

    // Join topic column with main DataFrame
    val newDF = noEmptyHashtagDF.join(topicDF, noEmptyHashtagDF("row1") === topicDF("row2"), "inner")

    // 1) Drop hashtags column,
    // 2) Reorder columns
    val finalDF = newDF.drop("hashtags")
      .select("id","text","topic","user_description","retweet_count","reply_count","quoted_status_id")

    // Output file
    finalDF.write.json(outputFile)
  }
}