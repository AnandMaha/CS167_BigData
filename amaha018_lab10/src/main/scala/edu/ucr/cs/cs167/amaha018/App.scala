package edu.ucr.cs.cs167.amaha018

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object App {

  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Usage <input file>")
      println("  - <input file> path to a CSV file input")
      sys.exit(0)
    }
    val conf = new SparkConf().setAppName("CS167_Lab10")
    val inputfile = args(0)
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Lab10")
      .config(conf)
      .getOrCreate()

    val t1 = System.nanoTime
    try {
      val sentimentData: DataFrame = spark.read.format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .load(inputfile)
      sentimentData.printSchema()
      sentimentData.show()

      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

      val hashingTF = new HashingTF()
        .setInputCol("words").setOutputCol("features")

      val stringIndexer = new StringIndexer()
        .setInputCol("sentiment")
        .setOutputCol("label")
        .setHandleInvalid("skip")

      val logisticRegression = new LogisticRegression()

      val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, stringIndexer, logisticRegression))

      val paramGrid: Array[ParamMap] = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
        .addGrid(logisticRegression.regParam, Array(0.01, 0.1, 0.3, 0.8))
        .build()

      val cv = new TrainValidationSplit()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator().setLabelCol("label"))
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
        .setParallelism(2)

      val Array(trainingData: Dataset[Row], testData: Dataset[Row]) = sentimentData.randomSplit(Array(0.8, 0.2))

      // Run cross-validation, and choose the best set of parameters.
      val logisticModel: TrainValidationSplitModel = cv.fit(trainingData)

      val numFeatures: Int = logisticModel.bestModel.asInstanceOf[PipelineModel].stages(1).asInstanceOf[HashingTF].getNumFeatures
      val regParam: Double = logisticModel.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LogisticRegressionModel].getRegParam
      println(s"Number of features in the best model = $numFeatures")
      println(s"RegParam the best model = $regParam")

      val predictions: DataFrame = logisticModel.transform(testData)
      predictions.select("text", "label", "prediction", "probability").show()

      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("prediction")

      val accuracy: Double = binaryClassificationEvaluator.evaluate(predictions)
      println(s"Accuracy of the test set is $accuracy")

      val t2 = System.nanoTime
      println(s"Applied sentiment analysis algorithm on input $inputfile in ${(t2 - t1) * 1E-9} seconds")
    } finally {
      spark.stop
    }
  }
}
