package edu.ucr.cs.cs167.amaha018
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object AppSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167_Lab6_AppSQL")
      .config(conf)
      .getOrCreate()

    val command: String = args(0)
    val inputfile: String = args(1)
    try {
      val input = spark.read.format("csv")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputfile)
      import spark.implicits._
      input.createOrReplaceTempView("log_lines")
      val t1 = System.nanoTime
      var validCommand = true
      command match {
        case "count-all" =>
          // Count total number of records in the file
          val count = spark.sql(
            """SELECT count(*)
             FROM log_lines""")
            .first()
            .getAs[Long](0)
          println(s"Total count for file '$inputfile' is $count")

        case "code-filter" =>
          // Filter the file by response code, args(2), and print the total number of matching lines
          val responseCode: String = args(2)
          val query: String = """SELECT count(*)
                                |FROM log_lines
                                |WHERE response=200;""".stripMargin
          val count: Long = spark.sql(query).first().getAs[Long](0)
          println(s"Total count for file '$inputfile' with response code $responseCode is $count")

      case "time-filter" =>
        // Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
        val from: Long = args(2).toLong
        val to: Long = args(3).toLong
        val query: String = s"""SELECT count(*)
                              |FROM log_lines
                              |WHERE time BETWEEN $from AND $to;""".stripMargin
        val count: Long = spark.sql(query).first().getAs[Long](0)
        println(s"Total count for file '$inputfile' in time range [$from, $to] is $count")

        case "count-by-code" =>
          // Group the lines by response code and count the number of records per group
          println(s"Number of lines per code for the file '$inputfile'")
          println("Code,Count")
          val query: String = """SELECT response, COUNT(response)
                                |FROM log_lines
                                |GROUP BY response
                                |ORDER BY response;""".stripMargin
            spark.sql(query).foreach(row => println(s"${row.get(0)},${row.get(1)}"))

         case "sum-bytes-by-code" =>
           // Group the lines by response code and sum the total bytes per group
           println(s"Total bytes per code for the file '$inputfile'")
           println("Code,Sum(bytes)")
           val query: String = """SELECT response, SUM(bytes)
                                 |FROM log_lines
                                 |GROUP BY response
                                 |ORDER BY response;""".stripMargin
             spark.sql(query).foreach(row => println(s"${row.get(0)},${row.get(1)}"))

        case "avg-bytes-by-code" =>
           // Group the liens by response code and calculate the average bytes per group
           println(s"Average bytes per code for the file '$inputfile'")
           println("Code,Avg(bytes)")
           val query: String = """SELECT response, AVG(bytes)
                                 |FROM log_lines
                                 |GROUP BY response
                                 |ORDER BY response;""".stripMargin
             spark.sql(query).foreach(row => println(s"${row.get(0)},${row.get(1)}"))

        case "top-host" =>
           // print the host the largest number of lines and print the number of lines
           println(s"Top host in the file '$inputfile' by number of entries")
           val query: String = """SELECT host, COUNT(*) AS cnt
                                 |FROM log_lines
                                 |GROUP BY host
                                 |ORDER BY cnt DESC
                                 |LIMIT 1;""".stripMargin
           val topHost: Row = spark.sql(query).first()
           println(s"Host: ${topHost.get(0)}")
           println(s"Number of entries: ${topHost.get(1)}")

          case "comparison" =>
            // Given a specific time, calculate the number of lines per response code for the
            // entries that happened before that time, and once more for the lines that happened at or after
            // that time. Print them side-by-side in a tabular form.

          val filterTimestamp: Long = args(2).toLong
          println(s"Comparison of the number of lines per code before and after $filterTimestamp on file '$inputfile'")
          println("Code,CountBefore,CountAfter")

          val countsBefore: DataFrame = input.filter($"time" < filterTimestamp).groupBy($"response").count().withColumnRenamed("count", "count_before")
          val countsAfter: DataFrame = input.filter($"time" >= filterTimestamp).groupBy($"response").count().withColumnRenamed("count", "count_after")
          val comparedResults: DataFrame = countsBefore.join(countsAfter, "response").orderBy("response")
          comparedResults.foreach(row => println(s"${row.get(0)},${row.get(1)},${row.get(2)}"))

          case _ => validCommand = false

      }
      val t2 = System.nanoTime
      if (validCommand)
        println(s"Command '$command' on file '$inputfile' finished in ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid command '$command'")
    } finally {
      spark.stop
    }
  }
}