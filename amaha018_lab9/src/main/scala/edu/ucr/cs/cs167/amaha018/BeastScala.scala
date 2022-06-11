package edu.ucr.cs.cs167.amaha018

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession= spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val tweetsDF = sparkSession.read.format("csv")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputFile)

      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "count-by-county" =>
          tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry")
          val tweetsRDD: SpatialRDD = tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry").toSpatialRDD
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
          val countyTweet: RDD[(IFeature, IFeature)] = countiesRDD.spatialJoin(tweetsRDD)
          val tweetsByCounty: Map[String, Long] = countyTweet
            .map({ case (county, tweet) => (county.getAs[String]("NAME"), 1) })
            .countByKey()
          println("County\tCount")
          for ((count, county) <- tweetsByCounty)
            println(s"$county\t$count")
        case "convert" =>
          val outputFile = args(2)
          tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry")
          val tweetsRDD: SpatialRDD = tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry").toSpatialRDD
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
          val tweetCountyRDD: RDD[(IFeature, IFeature)] = tweetsRDD.spatialJoin(countiesRDD)
          val tweetCounty: DataFrame = tweetCountyRDD.map({ case (tweet, county) => Feature.append(tweet, county.getAs[String]("GEOID"), "CountyID") })
            .toDataFrame(sparkSession)
          val convertedDF: DataFrame = tweetCounty.selectExpr("CountyID", "Longitude", "Latitude", "split(lower(text), ',') AS keywords", "Timestamp")
          convertedDF.write.mode(SaveMode.Overwrite).parquet(outputFile)
        case "count-by-keyword" =>
          val keyword: String = args(2)
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("tweets")
          println("CountyID\tCount")
          sparkSession.sql(
            s"""
           SELECT CountyID, count(*) AS count
              FROM tweets
             WHERE array_contains(keywords, "$keyword")
            GROUP BY CountyID
            """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))
        case "choropleth-map" =>
          val keyword: String = args(2)
          val outputFile: String = args(3)
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("tweets")
          println("CountyID\tCount")

          sparkContext.shapefile("tl_2018_us_county.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")
          val tempDataFrame = sparkSession.sql(
            s"""
           SELECT CountyID, NAME, g, count
            FROM (SELECT CountyID, count(*) AS count
              FROM tweets
             WHERE array_contains(keywords, "$keyword")
            GROUP BY CountyID),counties
            WHERE CountyID = GEOID
            """)
          tempDataFrame.toSpatialRDD
            .coalesce(1)
            .saveAsShapefile(outputFile)
        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}