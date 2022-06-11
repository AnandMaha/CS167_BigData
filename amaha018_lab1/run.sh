#!/usr/bin/env sh
mvn clean package
hadoop jar target/amaha018_lab1-1.0-SNAPSHOT.jar edu.ucr.cs.cs167.amaha018.App input.txt output.txt
$SHELL