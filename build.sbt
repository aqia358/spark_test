name := "spark_test"

version := "1.0"

scalaVersion := "2.11.1"

resolvers += DefaultMavenRepository
//resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "com.databricks" %% "spark-avro" % "2.0.1",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0"
)