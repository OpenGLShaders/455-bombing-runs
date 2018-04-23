// Import required spark classes
import org.apache.hadoop.fs._;
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.Adapter
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geosparkviz.extension.visualizationEffect.HeatMap
import org.datasyslab.geosparkviz.core.{ImageGenerator, RasterOverlayOperator}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.Envelope

object QualityMapper
{
    // Define main method (Spark entry point)
    def main(args: Array[String])
    {
        // Parse raw data
        
        // Create new spark conf
        val sparkConf = new SparkConf().set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
        
        // Create new spark context
        val sparkContext = new SparkContext(sparkConf)
        
        // Create new spark session
        val sparkSession = SparkSession
            .builder()
            .appName("QualityMapper")
            .config("spark.serializer", classOf[KryoSerializer].getName)
            .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
            .getOrCreate()
        
        // Load in data set csv (argument 1) as a data frame
        val rawDataFrame = sparkSession
            .read
            .format("csv")
            .option("header", "true")
            .load(args(0))
        
        // Register raw data frame as global temp view
        rawDataFrame.createGlobalTempView("raw")
        
        // Create sql statement to grab all rows from specified column (argument 2) that meet
        // given condition (argument 3) and have a valid longitude and latiude value
        val sqlStatement = "SELECT TGTLATDD_DDD_WGS84, TGTLONDDD_DDD_WGS84, " + args(1) + " FROM global_temp.raw WHERE " + args(1) + " = '" + args(2) + "' AND TGTLATDD_DDD_WGS84 <> '' AND TGTLONDDD_DDD_WGS84 <> ''"
        
        // Execute sql statement from above, filtering data
        val filteredDataFrame = sparkSession.sql(sqlStatement)

	filteredDataFrame.show()	

	// Write filtered data as csv
	filteredDataFrame.coalesce(1).write.csv("filtered")
        
        // Plot parsed data on heat map
        
	// Create spatial rdd from csv file
	val spatialRDD = new PointRDD(sparkContext, "/user/jordantp/filtered/*.csv", 0, FileDataSplitter.CSV, true)

        // Create boundary for vietnam view
        val vietnamBoundary = new Envelope(24.034746, 100.605803, 8.120920, 110.982506)
        
        // Create new heat map
        val heatMap = new HeatMap(1000, 600, vietnamBoundary, false, 2)
        
        // Visualize spatial rdd onto heat map
        //heatMap.Visualize(sparkContext, spatialRDD)
        
        // Create new image generator
        val imageGenerator = new ImageGenerator
    }
}
