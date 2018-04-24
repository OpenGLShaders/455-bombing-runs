// Import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}


object ModelGenerator
{
    // Define main method (Spark entry point)
    def main(args: Array[String])
    {
        // Create new spark conf
        val sparkConf = new SparkConf()
            .setMaster("spark://columbia:47401")
            .setAppName("Model Generator")
        
        // Create new spark session
        val sparkSession = SparkSession
            .builder()
            .appName("QualityMapper")
            .config(sparkConf)
            .getOrCreate()
        
        // Load in data set csv (argument 1) as a data frame
        val rawDataFrame = sparkSession
            .read
            .format("csv")
            .option("header", "true")
            .load("hdfs://columbia:47481/data/THOR_Vietnam_Bombing_Operations.csv")
        val validPredictors = rawDataFrame.na.drop()

        
        val labelIndexer = new StringIndexer()
                        .setInputCol("VALID_AIRCRAFT_ROOT")
                        .setOutputCol("labels")
                        .fit(validPredictors)
                        
        val countryIndexed = new StringIndexer()
                        .setInputCol("COUNTRYFLYINGMISSION")
                        .setOutputCol("countryIndexed")
                        //.setMaxCategories(4)
                        
        val assembler  = new VectorAssembler()
                        .setInputCols(Array("countryIndexed"))
                        .setOutputCol("features")
                        
        /*val targetIndexed = new VectorIndexer()
                                .setInputCol("TGTTYPE")
                                .setOutputCol("targetIndexed")*/
        /*val weaponIndexed = new VectorIndexer()
                                .setInputCol("WEAPONTYPE")
                                .setOutputCol("weaponIndexed")
        val countryIndexed = new VectorIndexer()
                                .setInputCol("FLTHOURS")
                                .setOutputCol("countryIndexed")
        val countryIndexed = new VectorIndexer()
                                .setInputCol("PERIODOFDAY")
                                .setOutputCol("countryIndexed")
        val countryIndexed = new VectorIndexer()
                                .setInputCol("TGTWEATHER")
                                .setOutputCol("countryIndexed")
        val countryIndexed = new VectorIndexer()
                                .setInputCol("TGTCLOUDCOVER")
                                .setOutputCol("countryIndexed")
        val countryIndexed = new VectorIndexer()
                                .setInputCol("WEAPONSLOADEDWEIGHT")
                                .setOutputCol("countryIndexed")*/
                                
                                
                                
        
        
        val Array(trainingData, testData) = validPredictors.randomSplit(Array(0.7, 0.3))
        
        // Train a RandomForest model.
        val rf = new RandomForestClassifier()
                        .setLabelCol("labels")
                        .setFeaturesCol("features")
                        .setNumTrees(10)

        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString()
                        .setInputCol("prediction")
                        .setOutputCol("PREDICTED_AIRCRAFT")
                        .setLabels(labelIndexer.labels)
                        
        val pipeline = new Pipeline()
                        .setStages(Array(labelIndexer, countryIndexed, assembler, rf, labelConverter))
                        
        val model = pipeline.fit(trainingData)
        val predictions = model.transform(testData)
        
        predictions.select("PREDICTED_AIRCRAFT", "VALID_AIRCRAFT_ROOT", "features").show(5)
        
                        
                        
        // Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
                        .setLabelCol("PREDICTED_AIRCRAFT")
                        .setPredictionCol("VALID_AIRCRAFT_ROOT")
                        .setMetricName("accuracy")
                        
        val accuracy = evaluator.evaluate(predictions)
        println("Test set accuracy = " + accuracy)
        
        //val rmse = evaluator.evaluate(predictions)
        //println("Root Mean Squared Error (RMSE) on test data = " + rmse)

        //val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
        //println("Learned regression tree model:\n" + treeModel.toDebugString)
        /*(
        //First model
        val aircraftTargets = rawDataFrame.select("VALID_AIRCRAFT_ROOT").show();
        
        val aircraftPredictors = rawDataFrame.select("COUNTRYFLYINGMISSION", 
                                             "TGTTYPE", 
                                             "WEAPONTYPE", 
                                             "FLTHOURS",
                                             "PERIODOFDAY", 
                                             "TGTWEATHER",
                                             "TGTCLOUDCOVER",
                                             "WEAPONSLOADEDWEIGHT")
                                             
        validPredictors.show()
        println(validPredictors.count())
            */
        
        /*
        //Second model
        val weaponTarget = rawDataFrame.select("WEAPONTYPE").show();
        
        val weaponPredictors = rawDataFrame.select("COUNTRYFLYINGMISSION", 
                                             "TGTTYPE", 
                                             "VALID_AIRCRAFT_ROOT", 
                                             "FLTHOURS",
                                             "PERIODOFDAY", 
                                             "TGTWEATHER",
                                             "TGTCLOUDCOVER",
                                             "WEAPONSLOADEDWEIGHT").show(); */
    }
    
}
