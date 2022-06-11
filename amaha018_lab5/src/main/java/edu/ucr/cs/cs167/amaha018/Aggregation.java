package edu.ucr.cs.cs167.amaha018;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class Aggregation {
    public static void main(String[] args) throws IOException {
        final String inputPath = args[0];
        final String outputPath = args[1];
        FileWriter fw = new FileWriter(outputPath);
        SparkConf conf = new SparkConf();
        if (!conf.contains("spark.master"))
            conf.setMaster("local[*]");
        System.out.printf("Using Spark master '%s'\n", conf.get("spark.master"));
        conf.setAppName("CS167-Lab5");
        try (JavaSparkContext spark = new JavaSparkContext(conf)) {
            JavaRDD<String> logFile = spark.textFile(inputPath).cache();
            JavaPairRDD<String, Integer> codes = logFile.mapToPair((PairFunction<String, String, Integer>) s -> {
                String code = s.split("\t")[5];
                return new Tuple2<String,Integer>(code,1);
            });
            Map<String, Long> counts = codes.countByKey();
            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                String out = "Code '" + entry.getKey() + "' : number of entries " + entry.getValue() + "\n";
                System.out.printf(out);
                fw.write(out);
            }
            fw.close();
        }
    }
}