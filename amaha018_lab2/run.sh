mvn clean package
hadoop jar target/amaha018_lab2-1.0-SNAPSHOT.jar file://`pwd`/AREAWATER.csv hdfs:///output.csv/
hadoop jar target/amaha018_lab2-1.0-SNAPSHOT.jar hdfs:///output.csv file://`pwd`/output.csv/
hadoop jar target/amaha018_lab2-1.0-SNAPSHOT.jar hdfs:///output.csv hdfs:///output2.csv/
