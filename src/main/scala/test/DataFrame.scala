package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._

/**
 * Created by liuhl on 16-3-22.
 */

object DataFrame {
  def main(args: Array[String]) {
    println("DataFrame")
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // The Avro records get converted to Spark types, filtered, and
    // then written back out as Avro records
    val df2 = sqlContext.read.avro("src/main/resources/episodes.avro")
    //    df2.filter("doctor > 5").write.avro("/tmp/output")
    println(df2.count)
    df2.filter("doctor > 5").take(10).foreach(println)
    sc.stop()
  }
}
