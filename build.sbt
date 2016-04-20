name := "spark_test"

version := "1.0"

scalaVersion := "2.11.1"

resolvers += DefaultMavenRepository
//resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "com.youdao" % "quipu-avro" % "0.0.9",
  "joda-time" % "joda-time" % "2.9.2",
  "org.apache.spark" % "spark-core_2.11" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.1",
  "com.databricks" % "spark-avro_2.11" % "2.0.1",
  "org.apache.spark" % "spark-streaming_2.11" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0"
)