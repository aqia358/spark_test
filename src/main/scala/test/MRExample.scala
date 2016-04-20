package test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


import com.databricks.spark.avro._
import scala.io.Source._
import scala.collection.mutable


/**
 * Created by liuhl on 16-4-6.
 */
class MRExample {
  def readFile(filename: String): Set[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    lineIter.toSet
  }
  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def main(args: Array[String]) {
    println(getYesterday())
  }

  def test(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    val file1 = readFile("/home/liuhongliang/lost_user_1.txt")
    val file2 = readFile("/home/liuhongliang/lost_user_2.txt")
    val scfile = sc.textFile("")


    println(file1.size)
    val pv = sqlContext.read.avro("hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/04/01/01/*.avro")
    //    val pv = sqlContext.read.avro("hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/04/*/*/*.avro")


    var exist = collection.mutable.Set.empty[String]
    var exist1 = collection.mutable.Set.empty[String]
    var exist2 = collection.mutable.Set.empty[String]
    val appids = Set("252", "462")

    pv.map { x =>
      val udid = x.getMap[String, Object](21).get("udid").fold("")(y => y.toString)
      val appid = x.getString(5)
      val pvDeviceId: String = x.getString(14)
      val inyoudao = appids.contains(appid)
      val inf1 = if (file1.contains(pvDeviceId) && !exist1.contains(pvDeviceId)) {
        exist1 += pvDeviceId
        1
      } else if (file1.contains(udid) && !exist1.contains(udid)) {
        exist1 += udid
        1
      } else 0
      val inf2 = if (file2.contains(pvDeviceId) && !exist2.contains(pvDeviceId)) {
        exist2 += pvDeviceId
        1
      } else if (file2.contains(udid) && !exist2.contains(udid)) {
        exist2 += udid
        1
      } else 0
      (s"$inyoudao", (inf1, inf2))
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).collect().foreach(println)




    val file = file1 ++ file2
    val pv2 = sqlContext.read.avro("hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/03/*/*/*.avro")


    pv2.foreachPartition(x => {
      x.foreach({ y =>
        println
      })
    })

    sc.accumulator[collection.mutable.Set[String]](collection.mutable.Set.empty[String])
    //count
    pv2.map[String] {
      x =>
        val udid = x.getMap[String, Object](21).get("udid").fold("")(y => y.toString)
        val auid = x.getMap[String, Object](21).get("auid").fold("")(y => y.toString)
        val pvDeviceId: String = x.getString(14)
        val inf1 = if (file.contains(pvDeviceId) && !exist1.contains(pvDeviceId)) {
            exist1 += pvDeviceId
            pvDeviceId
          } else if (file.contains(udid) && !exist1.contains(udid)) {
            exist1 += udid
            udid
          } else if (file.contains(auid) && !exist1.contains(auid)) {
            exist1 += auid
            auid
          }  else ""
        inf1
    }.filter(_ != "").distinct().count()
    pv2.map[String] {
      x =>
        val udid = x.getMap[String, Object](21).get("udid").fold("")(y => y.toString)
        val auid = x.getMap[String, Object](21).get("auid").fold("")(y => y.toString)
        val pvDeviceId: String = x.getString(14)
        val inf1 = if (file.contains(pvDeviceId) && !exist1.contains(pvDeviceId)) {
            exist1 += pvDeviceId
            pvDeviceId
          } else if (file.contains(udid) && !exist1.contains(udid)) {
            exist1 += udid
            udid
          } else if (file.contains(auid) && !exist1.contains(auid)) {
            exist1 += auid
            auid
          }  else ""
        inf1
    }.filter(_ != "").saveAsTextFile("hdfs://ws030:8000/user/liuhongliang/tmp.txt")

    val i4 = pv2.map[String] {
      x =>
        val udid = x.getMap[String, Object](21).get("udid").fold("")(y => y.toString)
        val auid = x.getMap[String, Object](21).get("auid").fold("")(y => y.toString)
        val pvDeviceId: String = x.getString(14)
        val pvslotId: String = x.getString(6)
        val inf1 = if (file.contains(pvDeviceId) && !exist1.contains(pvDeviceId)) {
            exist1 += pvDeviceId
            pvslotId+"::"+pvDeviceId
          } else if (file.contains(udid) && !exist1.contains(udid)) {
            exist1 += udid
            pvslotId+"::"+udid
          } else if (file.contains(auid) && !exist1.contains(auid)) {
            exist1 += auid
            pvslotId+"::"+auid
          }  else ""
        inf1
    }.filter(_ != "").distinct().cache()
    i4.groupBy(x => x.substring(0, x.indexOf("::"))).collect().foreach(y => println(y._1, y._2.toSet.size))



    val c = i4.groupBy(x => x.substring(0, x.indexOf("::"))).collect().foreach(y => (y._1, y._2.size))

    c.asInstanceOf[Array[(String, Int)]].filter(a => a._2 > 100).foreach(println)

    pv.take(1)


    i4.count()

    val tmp: Array[String] = Array("a::1", "a::2", "b::3", "a::1")
    tmp.groupBy(x => x.substring(0, x.indexOf("::"))).foreach(y => println(y._1, y._2.size))


    var result = sc.accumulableCollection(collection.mutable.HashSet.empty[String])

    // foreach
    (4 to 4).foreach(m => {
      var end = 31
      var start = 1
      if (m == 3) start = 29
      if (m == 4) end = 11
      (start to end).foreach(d => {
        val path = "hdfs://ws001:8000/quipu/camus/data/sdk_land_page_avro/hourly/2016/0%d/%s/*/*.avro".format(m, d.formatted("%02d"))
        val land = sqlContext.read.avro(path)
        val ht = sc.accumulator(0l)
        val lt = sc.accumulator(0l)
        val pt = sc.accumulator(0l)
        val fd = sc.accumulator(0l)

        land.foreachPartition(rows => rows.filter(tmp => tmp.getMap[String, String](9).get("fd") != "None").foreach(row => {
          val sponsorId = row.get(1)
          val slotId = row.get(4)
          val ht_tmp = row.getLong(6)
          val lt_tmp = row.getLong(7)
          val pt_tmp = row.getLong(8)
          val ext = row.getMap[String, String](9)
          val fd_tmp = ext.get("fd").toString
          if (fd_tmp == null || fd_tmp == "" || fd_tmp == "null" || fd_tmp == "None") {

          } else {
            ht += ht_tmp
            lt += lt_tmp
            pt += pt_tmp
            fd += 1
          }
        }))
        val str = "2016-0%d-%s: ht_avg:%f,lt_avg:%f,pt_avg:%f,fd_count:%d".format(m, d.formatted("%02d"), ht.value / fd.value.toFloat, lt.value / fd.value.toFloat, pt.value / fd.value.toFloat, fd.value)
        println(str)
        result.value += str
        land.unpersist()
      })
    })

    val land = sqlContext.read.avro("hdfs://ws001:8000/quipu/camus/data/sdk_land_page_avro/hourly/2016/04/01/*/*.avro")

    // map reduce
    var rr = sc.accumulableCollection(collection.mutable.HashSet.empty[String])

    (3 to 3).foreach(m => {
      var end = 31
      var start = 1
      if (m == 3) start = 29
      if (m == 4) end = 11
      (start to end).foreach(d => {
        val path = "hdfs://ws001:8000/quipu/camus/data/sdk_land_page_avro/hourly/2016/0%d/%s/*/*.avro".format(m, d.formatted("%02d"))
        val land = sqlContext.read.avro(path)
        land.mapPartitions(rows => rows.filter(tmp => tmp.getMap[String, String](9).get("fd") != "None").map(row => {
          val sponsorId = row.getLong(1)
          val slotId = row.getString(4)
          val ht_tmp = row.getLong(6)
          val lt_tmp = row.getLong(7)
          val pt_tmp = row.getLong(8)
          val ext = row.getMap[String, String](9)
          val fd_tmp = ext.get("fd").toString
          if (fd_tmp == null || fd_tmp == "" || fd_tmp == "null" || fd_tmp == "None") {
            ("2016/0%d/%s,%s,%s".format(m, d.formatted("%02d"), sponsorId, slotId), (0l, 0l, 0l, 0l))
          } else {
            ("2016/0%d/%s,%s,%s".format(m, d.formatted("%02d"), sponsorId, slotId), (ht_tmp, lt_tmp, pt_tmp, 1l))
          }
        })).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)).foreach(x => {
          val value = x._2
          val count = value._4
          val ht_avg = value._1 / value._4.toFloat
          val lt_avg = value._2 / value._4.toFloat
          val pt_avg = value._3 / value._4.toFloat
          rr += "%s,ht_sum:%d,lt_sum:%d,pt_sum:%d,count:%d,ht_avg:%f,lt_avg:%f,pt_avg:%f".format(x._1, value._1, value._2, value._3, value._4, ht_avg, lt_avg, pt_avg)
        })
        land.unpersist()
      })
    })


    //    val file = sc.broadcast(file1 ++ file2)
    val scFile = sc.broadcast(file1 ++ file2)
    //    val accExist = sc.accumulableCollection(new mutable.HashSet[String]())
    val accExist3 = sc.accumulableCollection(new mutable.HashSet[String]())
    val count = sc.accumulableCollection(new mutable.ArrayBuffer[Int]())

    val hourlyExist = sc.accumulableCollection(new mutable.HashMap[String, mutable.HashSet[String]]())
    for (i <- 5 to 5) {
      val date = if (i < 10) s"0$i" else s"$i"
      for (h <- 0 to 23) {
        val hour = if (h < 10) s"0$h" else s"$h"
        val ace = sc.accumulableCollection(new mutable.HashSet[String]())
        val pv = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_dsp/hourly/2016/04/11/$hour/*.avro")
        pv.foreachPartition {
          rows =>
            val exist = new mutable.HashSet[String]
            rows.foreach(
              row => {
                val syndId = row.getLong(2)
                val idfa = row.getMap[String, Object](25).get("IDFA").fold("")(y => y.toString)
                val deviceId = row.getMap[String, Object](25).get("DEVICEID").fold("")(y => y.toString)
                if (syndId == 112) {
                  if (scFile.value.contains(idfa) && !exist.contains(idfa)) {
                    exist += idfa
                    ace += idfa
                  } else if (scFile.value.contains(deviceId) && !exist.contains(deviceId)) {
                    exist += deviceId
                    ace += deviceId
                  }
                }
              })
        }
        hourlyExist.value.put(hour, ace.value)
        pv.unpersist()
      }
    }
    pv.count()
    accExist3.value.size

    count.value.size

    pv.take(1).foreach(row => println(row.getMap[String,Object](25).get("IDFA"), row.getMap[String,Object](25).get("DEVICEID")))
    pv.take(1).foreach(row => println(row.getMap[String,Object](25).foreach(println(_))))


    var a = new mutable.HashSet[String]

    hourlyExist.value.foreach(x => {
      a = a ++ x._2
    })
    var t = 0
    print("0_")
    for (x <- 0 to 17) {
      var a = new mutable.HashSet[String]
      for (y <- 0 to x) {
        val hour = if (y < 10) s"0$y" else s"$y"
        a = a ++ hourlyExist.value.get(hour).get
      }
      print(a.size - t + "_")
      t = a.size
    }
    println("|")

  }

}
