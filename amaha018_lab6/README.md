# Lab 6

## Student information
* Full name: Anand Mahadevan
* E-mail: amaha018@ucr.edu
* UCR NetID: amaha018
* Student ID: 862132182

## Answers

* (Q1) What are these two arguments?
  - The first argument, command, specifies what operation to perform on the second argument, inputfile, which is the name of the input file.
* (Q2) (B) If you do this bonus part, copy and paste your code in the README file as an answer to this question. 
  - The following code has 3 parts, Zero Value of (0,0) that represents initial (current sum, current count),
    seqOp is a function that takes in an aggregate value resPair that's (current sum, current count) and a Long nextVal
    that will be incorporated into a new resPair by adding nextVal to current sum and +1 to current count, 
    combOp is a function that takes in 2 aggregate value resPair and combines them together by adding together 
    each resPair's current sum and current count into another resPair. 
    The result is an RDD[(String, (Int, Int))] with each String (response code) having a corresponding pair representing its (total number of bytes, number of records). 
  - Code:
    val averages: RDD[(String, (Int, Int))] =
    loglinesByCode.aggregateByKey((0,0))((resPair,nextVal) => (resPair._1 + nextVal.toInt , resPair._2 + 1),(resPair1,resPair2) => (resPair1._1 + resPair2._1 , resPair1._2 + resPair2._2))
* (Q3) What is the type of the attributes `time` and `bytes` this time? Why?
  - They are both type string this time. This is because the default typing is string, and we have not specified to spark SQL to infer a schema.
* (Q4) (B) If you do this bonus part, copy and paste your code in the README file as an answer to this question.
  - I used dataframes. input is filtered twice, once with times less than the argument time, the other greater
    than or equal to the argument time. They are then both grouped by response and produce the count for each response.
  - val countsBefore: DataFrame = input.filter($"time" < filterTimestamp).groupBy($"response").count().withColumnRenamed("count", "count_before")
    val countsAfter: DataFrame = input.filter($"time" >= filterTimestamp).groupBy($"response").count().withColumnRenamed("count", "count_after")