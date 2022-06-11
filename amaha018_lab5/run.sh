#!/usr/bin/env sh
mvn clean package
spark-submit --class edu.ucr.cs.cs167.amaha018.Filter target/amaha018_lab5-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv filter_output 302
spark-submit --class edu.ucr.cs.cs167.amaha018.Aggregation target/amaha018_lab5-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv aggregation_output
