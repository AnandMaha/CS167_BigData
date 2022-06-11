package edu.ucr.cs.cs167.amaha018
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.Map
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]) {
    val command: String = args(0)
    val inputfile: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    conf.setAppName("CS167_Lab6")
    val sparkContext = new SparkContext(conf)
    try {
      val inputRDD: RDD[String] = sparkContext.textFile(inputfile)
      val validLines: RDD[String] = inputRDD.filter( x => !(x.contains("host\tlogname")) )
      val parsedLines: RDD[Array[String]] = validLines.map(x => x.split("\t"))
      val t1 = System.nanoTime
      var valid_command = true
      command match {
        case "count-all" =>
          // Count total number of records in the file
          val count: Long = parsedLines.count()
            println(s"Total count for file '$inputfile' is $count")

        case "code-filter" =>
          // Filter the file by response code, args(2), and print the total number of matching lines
          val responseCode = args(2)
          val filteredLines: RDD[Array[String]] = parsedLines.filter(_(5) == responseCode)
          val count = filteredLines.count()
          println(s"Total count for file '$inputfile' with response code $responseCode is $count")

         case "time-filter" =>
           // Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
           val from = args(2).toLong
           val to = args(3).toLong
           val filteredLines: RDD[Array[String]] = parsedLines.filter(x => x(2).toLong >= from  && x(2).toLong <= to)
           val count = filteredLines.count()
           println(s"Total count for file '$inputfile' in time range [$from, $to] is $count")

        case "count-by-code" =>
           // Group the lines by response code and count the number of records per group
           val loglinesByCode: RDD[(String, Int)] = parsedLines.map(x => (x(5),1))
           val counts: Map[String, Long] = loglinesByCode.countByKey()
             println(s"Number of lines per code for the file '$inputfile'")
           println("Code,Count")
           counts.foreach(pair => println(s"${pair._1},${pair._2}"))

        case "sum-bytes-by-code" =>
           // Group the lines by response code and sum the total bytes per group
           val loglinesByCode: RDD[(String, Long)] = parsedLines.map(x => (x(5),x(6).toLong))
           val sums: RDD[(String, Long)] = loglinesByCode.reduceByKey((a,b) => (a + b)).sortByKey()
             println(s"Total bytes per code for the file '$inputfile'")
           println("Code,Sum(bytes)")
           sums.foreach(pair => println(s"${pair._1},${pair._2}"))

         case "avg-bytes-by-code" =>
           // Group the lines by response code and calculate the average bytes per group
           val loglinesByCode: RDD[(String, Long)] = parsedLines.map(x => (x(5),x(6).toLong))
           val averages: RDD[(String, (Int, Int))] =
             loglinesByCode.aggregateByKey((0,0))((resPair,nextVal) => (resPair._1 + nextVal.toInt , resPair._2 + 1),(resPair1,resPair2) => (resPair1._1 + resPair2._1 , resPair1._2 + resPair2._2))
             println(s"Average bytes per code for the file '$inputfile'")
           println("Code,Avg(bytes)")
           averages.sortByKey().collect().foreach(pair => println(s"${pair._1},${pair._2._1.toDouble/pair._2._2}"))

         case "top-host" =>
           // Print the host the largest number of lines and print the number of lines
           val loglinesByHost: RDD[(String, Int)] = parsedLines.map(x => (x(0),1))
           val counts: RDD[(String, Int)] = loglinesByHost.reduceByKey((a,b)=>(a+b))
           val sorted: RDD[(String, Int)] = counts.sortBy(_._2, ascending = false)
           val topHost: (String, Int) = sorted.first()
             println(s"Top host in the file '$inputfile' by number of entries")
           println(s"Host: ${topHost._1}")
           println(s"Number of entries: ${topHost._2}")
         case _ => valid_command = false
      }
      val t2 = System.nanoTime
      if (valid_command)
        println(s"Command '$command' on file '$inputfile' finished in ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid command '$command'")
    } finally {
      sparkContext.stop
    }
  }
}