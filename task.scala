// import required spark classes
// import org.apache.spark.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Define main method (Spark entry point)
object AircraftCounter
{
    // Main method
    def main(args: Array[String])
    {
        // Create spark session
        /*val spark = SparkSession
                    .builder()
                    .appName("task")
                    .config("spark.some.config.option", "some-value")
                    .getOrCreate()
        */
        
        // initialise spark context
        val conf = new SparkConf().setAppName("AircraftCounter")
        val sc = new SparkContext(conf)
        
        // Create new buffered source using main data set
        //val mainSetBufferedSource = scala.io.Source.fromFile("hdfs:///data/THOR_Vietnam_Bombing_Operations.csv")
        
        // Process main data set
        
        // Declare string to hold aircraft
        val aircraft = ""
        
        // For each line in main data set
        /*for (line <- mainSetBufferedSource.getLines)
        {
            // Split columns
            val columns = line.split(",").map(_.trim)
            
            // Concat current aircraft column to aircraft string
            aircraft.concat(" " + columns(5))
        }
        */
        // Convert aircraft to RDD
        //val aircraftRDD = sc.parallelize(List(aircraft))
        
        val aircraftRDD = sc.parallelize("(-+-)")
        
        // Write aircraft RDD to file
        aircraftRDD.saveAsTextFile("output")
    }
}
