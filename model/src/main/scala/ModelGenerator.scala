// Import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler, StringIndexerModel}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml._


object ModelGenerator
{
    // Define main method (Spark entry point)
    def main(args: Array[String])
    {
        // Create new spark conf
        val sparkConf = new SparkConf()
          .setAppName("Model Generator")

        // Create new spark session
        val sparkSession = SparkSession
          .builder()
          .appName("Model Generator")
          .config(sparkConf)
          .getOrCreate()


      if (args(0) == "create") {
        createAndSaveAircraftModel(sparkConf, sparkSession)
      } else if (args(0) == "use") {
        useAircraftModel(sparkSession, args(1), args(2))
      }
    }

    def useAircraftModel(sparkSession: SparkSession, inputFile: String, outputFile: String) = {
      val useDF = sparkSession
        .read
        .format("csv")
        .option("header", "true")
        .load(inputFile)

        val model = PipelineModel.read.load("hdfs://columbia:47481/data/model-airplane")
        val predictions = model.transform(useDF)
        
        
        predictions.select("PREDICTED_AIRCRAFT", "features").coalesce(1).write.text(outputFile)
    }

  /*
    def useTargetModel(sparkSession: SparkSession) = {
      val useDF = sparkSession
        .read
        .format("csv")
        .option("header", "true")
        .load("input.csv")


      val model = PipelineModel.read.load("target-model")
      val predictions = model.transform(useDF)
      predictions.select("PREDICTED_TARGET", "features").show()
    }*/

    def createAndSaveAircraftModel(sparkConf: SparkConf, sparkSession: SparkSession) = {
      //Load csv file
      val rawDataFrame = sparkSession
        .read
        .format("csv")
        .option("header", "true")
        .load("hdfs://columbia:47481/data/THOR_Vietnam_Bombing_Operations.csv")

      //And transform it....
      val filtered = rawDataFrame.filter("COUNTRYFLYINGMISSION is not null")
        .filter("VALID_AIRCRAFT_ROOT is not null")
        .filter("WEAPONTYPE is not null")
        .filter("FLTHOURS is not null")
        .filter("PERIODOFDAY is not null")
        .filter("TGTTYPE is not null")
        .filter("WEAPONSLOADEDWEIGHT is not null")


      //By selecting features...
      val countryIndexed = new StringIndexer()
        .setInputCol("COUNTRYFLYINGMISSION")
        .setOutputCol("countryIndexed")
        .fit(filtered)

      val targetIndexed = new StringIndexer()
        .setInputCol("TGTTYPE")
        .setOutputCol("targetIndexed")
        .fit(filtered)

      val weaponIndexed = new StringIndexer()
        .setInputCol("WEAPONTYPE")
        .setOutputCol("weaponIndexed")
        .fit(filtered)

      val hoursIndexed = new StringIndexer()
        .setInputCol("FLTHOURS")
        .setOutputCol("hoursIndexed")
        .fit(filtered)

      val periodIndexed = new StringIndexer()
        .setInputCol("PERIODOFDAY")
        .setOutputCol("periodIndexed")
        .fit(filtered)

      val weaponWeightIndexed = new StringIndexer()
        .setInputCol("WEAPONSLOADEDWEIGHT")
        .setOutputCol("weaponWeightIndexed")
        .fit(filtered)

      //Add indexing on the labels...
      val labelIndexer = new StringIndexer()
        .setInputCol("VALID_AIRCRAFT_ROOT")
        .setOutputCol("labels")
        .fit(filtered)

      val assembler  = new VectorAssembler()
        .setInputCols(Array("countryIndexed", "targetIndexed", "weaponIndexed", "hoursIndexed", "periodIndexed", "weaponWeightIndexed"))
        .setOutputCol("features")


      //Transforming data complete
      //-----------------------------------------------------


      val Array(trainingData, testData) = filtered.randomSplit(Array(0.7, 0.3))


      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("PREDICTED_AIRCRAFT")
        .setLabels(labelIndexer.labels)

      val trainer = new MultilayerPerceptronClassifier()
        .setLayers(Array[Int](6, 30, 30, 50))
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(1000)
        .setLabelCol("labels")
        .setFeaturesCol("features")

      val transformPipeline = new Pipeline()
        .setStages(Array(countryIndexed, targetIndexed, weaponIndexed, hoursIndexed, periodIndexed, weaponWeightIndexed))
        .fit(trainingData)

      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, transformPipeline, assembler, trainer, labelConverter))

      val model = pipeline.fit(trainingData)


      val predictions = model.transform(testData)

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("labels")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")

      val accuracy = evaluator.evaluate(predictions)
      model.write.overwrite().save("hdfs://columbia:47481/data/model-airplane")


      println("The model is " + accuracy + "% accurate.")
    }


  /*
  def createAndSaveTargetModel(sparkConf: SparkConf, sparkSession: SparkSession) = {
    //Load csv file
    val rawDataFrame = sparkSession
      .read
      .format("csv")
      .option("header", "true")
      .load("small.csv")
    //.load("hdfs://columbia:47481/data/THOR_Vietnam_Bombing_Operations.csv")

    //And transform it....
    val filtered = rawDataFrame.filter("COUNTRYFLYINGMISSION is not null")
      .filter("VALID_AIRCRAFT_ROOT is not null")
      .filter("WEAPONTYPE is not null")
      .filter("FLTHOURS is not null")
      .filter("PERIODOFDAY is not null")
      .filter("TGTTYPE is not null")
      .filter("WEAPONSLOADEDWEIGHT is not null")


    //By selecting features...
    val countryIndexed = new StringIndexer()
      .setInputCol("COUNTRYFLYINGMISSION")
      .setOutputCol("countryIndexed")
      .fit(filtered)

    val airplaneIndexed = new StringIndexer()
      .setInputCol("VALID_AIRCRAFT_ROOT")
      .setOutputCol("aircraftIndexed")
      .fit(filtered)

    val weaponIndexed = new StringIndexer()
      .setInputCol("WEAPONTYPE")
      .setOutputCol("weaponIndexed")
      .fit(filtered)

    val hoursIndexed = new StringIndexer()
      .setInputCol("FLTHOURS")
      .setOutputCol("hoursIndexed")
      .fit(filtered)

    val periodIndexed = new StringIndexer()
      .setInputCol("PERIODOFDAY")
      .setOutputCol("periodIndexed")
      .fit(filtered)

    val weaponWeightIndexed = new StringIndexer()
      .setInputCol("WEAPONSLOADEDWEIGHT")
      .setOutputCol("weaponWeightIndexed")
      .fit(filtered)

    val labelIndexed = new StringIndexer()
      .setInputCol("TGTTYPE")
      .setOutputCol("labels")
      .fit(filtered)

    val assembler  = new VectorAssembler()
      .setInputCols(Array("countryIndexed", "aircraftIndexed", "weaponIndexed", "hoursIndexed", "periodIndexed", "weaponWeightIndexed"))
      .setOutputCol("features")


    //Transforming data complete
    //-----------------------------------------------------


    val Array(trainingData, testData) = filtered.randomSplit(Array(0.7, 0.3))


    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("PREDICTED_TARGET")
      .setLabels(labelIndexed.labels)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(Array[Int](6, 30, 30, 50))
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
      .setLabelCol("labels")
      .setFeaturesCol("features")

    val transformPipeline = new Pipeline()
      .setStages(Array(countryIndexed, airplaneIndexed, weaponIndexed, hoursIndexed, periodIndexed, weaponWeightIndexed))
      .fit(trainingData)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexed, transformPipeline, assembler, trainer, labelConverter))

    val model = pipeline.fit(trainingData)


    val predictions = model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("labels")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    model.write.overwrite().save("target-model")


    println("The model is " + accuracy + "% accurate.")
  }*/
}
