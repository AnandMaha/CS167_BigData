#!/usr/bin/env sh
mvn clean package

# All tasks
spark-submit --class edu.ucr.cs.cs167.amaha018.TwitterProject --master "local[*]" target/TwitterProject-1.0-SNAPSHOT.jar Tweets_10k.json tweets_clean.json task2output.json