// Import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object QualityMapper
{
    // Define main method (Spark entry point)
    def main(args: Array[String])
    {
        // Create new spark conf
        val sparkConf = new SparkConf()
        
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
            .load(args(0))
        
        // Register raw data frame as global temp view
        rawDataFrame.createGlobalTempView("raw")
        
        // Create sql statement to grab all rows from specified column (argument 2) that meet
        // given condition (argument 3) and have a valid longitude and latiude value
        val sqlStatement = "SELECT " + args(1) + ",TGTLATDD_DDD_WGS84, TGTLONDDD_DDD_WGS84 FROM global_temp.raw WHERE " + args(1) + " = '" + args(2) + "' AND TGTLATDD_DDD_WGS84 <> '' AND TGTLONDDD_DDD_WGS84 <> ''"
        
        // Execute sql statement from above
        sparkSession.sql(sqlStatement).show()
    }
}
