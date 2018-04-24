import sbt.Keys.{libraryDependencies, version}



lazy val root = (project in file(".")).
  settings(
    name := "HW4",

    version := "0.1.0",

    scalaVersion := "2.11.11",
  )

val SparkVersion = "2.2.1"

val SparkCompatibleVersion = "2.2"

val HadoopVersion = "2.7.2"

val GeoSparkVersion = "1.1.2"

val dependencyScope = "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-mllib" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope
)

