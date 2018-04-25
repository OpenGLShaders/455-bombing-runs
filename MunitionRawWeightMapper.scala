// Import required spark classes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

object MunitionRawWeightMapper
{
    // Define main method (Spark entry point)
    def main(args: Array[String])
    {
        // Parse raw data
        
        // Create new spark session
        val sparkSession = SparkSession
            .builder()
            .appName("QualityMapper")
            .getOrCreate()
        
        // Load in data set csv (argument 1) as a data frame
        val rawDataFrame = sparkSession
            .read
            .format("csv")
            .option("header", "true")
            .load(args(0))
        
        // Register raw data frame as global temp view
        rawDataFrame.createGlobalTempView("raw")
        
        // Create sql statement to grab all rows from specified column (argument 2) that meet given condition (argument 3) and have a valid longitude and latiude value
        val sqlStatement = "SELECT TGTLATDD_DDD_WGS84, TGTLONDDD_DDD_WGS84, NUMWEAPONSDELIVERED * WEAPONTYPEWEIGHT, WEAPONTYPE FROM global_temp.raw WHERE WEAPONTYPE = '" + args(1) + "' AND TGTLATDD_DDD_WGS84 <> '' AND TGTLONDDD_DDD_WGS84 <> ''"
        
        // Execute sql statement from above, filtering data
        val filteredDataFrame = sparkSession.sql(sqlStatement)

        // Write filtered data as csv   
        filteredDataFrame.coalesce(1).write.csv("filtered")
    }
}
