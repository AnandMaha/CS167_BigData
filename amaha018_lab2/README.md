# Lab 2

## Student information
* Full name: Anand Mahadevan
* E-mail: amaha018@ucr.edu
* UCR NetID: amaha018
* Student ID: 862132182

## Answers

* (Q1) Verify the file size and record the running time.
  * AREAWATER.csv File Size is 2,271,210,910 bytes, Running time is 43.118275 seconds
* (Q2) Record the running time of the copy command.
  * The copy command took 13.6517255 seconds to complete.
* (Q3) How do the two numbers compare? (The running times of copying the file through your program and the operating 
  system.) Explain IN YOUR OWN WORDS why you see these results.
  * The hadoop copy takes 3-4 times longer to complete compared to the operating system. The reason why this happens 
   is that the operating system has direct access to the file to copy, while hadoop must write to multiple nodes and  
   read from them.
* (Q4) Does the program run after you change the default file system to HDFS? What is the error message, 
if any, that you get?
  * No, the error message is "Input file 'AREAWATER.csv' does not exist!".
* (Q5) Use your program to test the following cases and record the running time for each case.
  Case number | Runtime (seconds)
  --- | --- 
  1 | 36.541792
  2 | 21.355809
  3 | 31.163079