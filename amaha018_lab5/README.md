# Lab 5

## Student information
* Full name: Anand Mahadevan
* E-mail: amaha018@ucr.edu
* UCR NetID: amaha018
* Student ID: 862132182

## Answers

* (Q1) Do you think it will use your local cluster? Why or why not?
  - Yes, because nothing appeared on the web interface.
* (Q2) Does the application use the cluster that you started? How did you find out?
  - Yes, it does. There's a completed application in the web interface.
* (Q3) What is the Spark master printed on the standard output on IntelliJ IDEA?
  - Using Spark master 'local[*]'
    Number of lines in the log file 30970
* (Q4) What is the Spark master printed on the standard output on the terminal?
  - Using Spark master 'spark://127.0.0.1:7077'
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Number of lines in the log file 30970
* (Q5) For the previous command that prints the number of matching lines, list all the processed input splits.
  - 22/04/29 17:25:32 INFO HadoopRDD: Input split: file:/mnt/c/ProgrammingFolder/CS167-BigData/workspace/amaha018_lab5/nasa_19950801.tsv:0+1169610
    22/04/29 17:25:32 INFO HadoopRDD: Input split: file:/mnt/c/ProgrammingFolder/CS167-BigData/workspace/amaha018_lab5/nasa_19950801.tsv:1169610+1169610
* (Q6) For the previous command that counts the lines and prints the output, how many splits were generated?
  - 4 splits.
* (Q7) Compare this number to the one you got earlier.
  - 4 splits now compared to 2 splits.
* (Q8) Explain why we get these numbers.
  - The input file is being read two times.
* (Q9) What can you do to the current code to ensure that the file is read only once?
  - Put .cache() after textFile(inputPath) in the code