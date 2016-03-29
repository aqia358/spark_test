package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._

/**
 * Created by liuhl on 16-3-22.
 */
case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]) {
    println("RDDRelation")
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    df.registerTempTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sqlContext.sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = sqlContext.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = sqlContext.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    // Write out an RDD as a parquet file with overwrite mode.
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    // Read in parquet file.  Parquet files are self-describing so the schema is preserved.
    val parquetFile = sqlContext.read.parquet("pair.parquet")

    // Queries can be run using the DSL on parquet files just like the original RDD.
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerTempTable("parquetFile")
    sqlContext.sql("SELECT * FROM parquetFile").collect().foreach(println)

    // The Avro records get converted to Spark types, filtered, and
    // then written back out as Avro records
    val df2 = sqlContext.read.avro("src/main/resources/episodes.avro")
    df2.filter("doctor > 5").write.avro("/tmp/output")
    df2.filter("doctor > 5").foreach(println)
    sc.stop()
  }
}
