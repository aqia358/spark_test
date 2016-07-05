package test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._
import scala.io.Source._
import scala.collection.mutable._


/**
 * Created by liuhl on 16-4-6.
 */
class MRExample {

  def readFile(filename: String) = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    lineIter.toSet
  }

  val file1 = readFile("/home/liuhongliang/lost_user_1.txt")
  val file2 = readFile("/home/liuhongliang/lost_user_2.txt")


  val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def getYesterday(): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def main(args: Array[String]) {
    println(getYesterday())
  }

  def pv_bid_impr_click_join(): Unit = {
    (0 to 23).map(hour => {
      val pvPath = "hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/06/29/%s/*.avro".format(hour.formatted("%02d"))
      val pv = sqlContext.read.avro(pvPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
        var vendor, nsv, av, mcc = ""
        val bid_id = row.getString(3)
        val array = row.getAs[WrappedArray[String]](7)
        mcc = row.getMap[String, Object](22).get("mcc").fold("")(y => y.toString)
        nsv = row.getMap[String, Object](22).get("nsv").fold("")(y => y.toString)
        av = row.getMap[String, Object](22).get("av").fold("")(y => y.toString)
        if (array != null) {
          array.foreach(next => {
            val tmp = next.split(":")
            if (tmp.length == 2) {
              if ("vendor".equals(tmp(0)))
                vendor = tmp(1)
            }
          })
        }
        (bid_id, (mcc + "," + vendor + "," + nsv + "," + av, 1, 0, 0, 0, 0))
      })
      val bidPath = "hdfs://ws030:8000/quipu/camus/data/bid_sdk/hourly/2016/06/29/%s/*.avro".format(hour.formatted("%02d"))
      val bid = sqlContext.read.avro(bidPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
        val bid_id = row.getString(3)
        (bid_id, ("", 0, 1, 0, 0, 0))
      })
      val imprPath = "hdfs://ws030:8000/quipu/camus/data/impr_sdk/hourly/2016/06/29/%s/*.avro".format(hour.formatted("%02d"))
      val impr = sqlContext.read.avro(imprPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
        val bid_id = row.getString(3)
        (bid_id, ("", 0, 0, 1, 0, 0))
      })
      val clickPath = "hdfs://ws030:8000/quipu/camus/data/click_sdk_charged/hourly/2016/06/29/%s/*.avro".format(hour.formatted("%02d"))
      val click = sqlContext.read.avro(clickPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
        val bid_id = row.getString(3)
        val charge = row.getInt(33)
        (bid_id, ("", 0, 0, 0, 1, charge))
      })
      val group = pv.cogroup(bid, impr, click)
      val res0 = group.map(x => {
        var key = ""
        var pv, bid, impr, click, charge = 0
        val list = List(x._2._1, x._2._2, x._2._3, x._2._4)
        list.foreach(iter =>
          iter.foreach(x => {
            key = if ("".equals(x._1)) key else x._1
            pv += x._2
            bid += x._3
            impr += x._4
            click += x._5
            charge += x._6
          }))
        (key, (pv, bid, impr, click, charge))
      }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
      pv.unpersist()
      bid.unpersist()
      impr.unpersist()
      click.unpersist()
      res0
    }).reduce((a, b) => a.union(b)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5)).repartition(1).saveAsTextFile("hdfs://ws030:8000/user/liuhongliang/join/all")

  }

  def pv_bid_impr_click_join_abtest(): Unit = {
    (7 to 7).map(month => {
      (1 to 3).map(day => {
        val resultPath = "2016%s%s".format(month.formatted("%02d"), day.formatted("%02d"))
        (0 to 23).map(hour => {
          val timePath = "2016/%s/%s/%s".format(month.formatted("%02d"), day.formatted("%02d"), hour.formatted("%02d"))
          val pvPath = "hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/%s/*.avro".format(timePath)
          val pv = sqlContext.read.avro(pvPath).filter("slot_id = '537910e95c64557b8734535eaffc4bb8'").map(row => {
            var abtest, vendor, nsv, av = ""
            val bid_id = row.getString(3)
            val array = row.getAs[WrappedArray[String]](7)
            if (array != null)
              array.foreach(next => {
                print(next)
                val tmp = next.split(":")
                if (tmp.length == 2) {
                  if ("abtest".equals(tmp(0)))
                    abtest = tmp(1)
                }
              })
            (bid_id, (abtest + "," + vendor + "," + nsv + "," + av, 1, 0, 0, 0, 0))
          })
          val bidPath = "hdfs://ws030:8000/quipu/camus/data/bid_sdk/hourly/%s/*.avro".format(timePath)
          val bid = sqlContext.read.avro(bidPath).filter("slot_id = '537910e95c64557b8734535eaffc4bb8'").map(row => {
            val bid_id = row.getString(3)
            (bid_id, ("", 0, 1, 0, 0, 0))
          })
          val imprPath = "hdfs://ws030:8000/quipu/camus/data/impr_sdk/hourly/%s/*.avro".format(timePath)
          val impr = sqlContext.read.avro(imprPath).filter("slot_id = '537910e95c64557b8734535eaffc4bb8'").map(row => {
            val bid_id = row.getString(3)
            (bid_id, ("", 0, 0, 1, 0, 0))
          })
          val clickPath = "hdfs://ws030:8000/quipu/camus/data/click_sdk_charged/hourly/%s/*.avro".format(timePath)
          val click = sqlContext.read.avro(clickPath).filter("slot_id = '537910e95c64557b8734535eaffc4bb8'").map(row => {
            val bid_id = row.getString(3)
            val charge = row.getInt(33)
            (bid_id, ("", 0, 0, 0, 1, charge))
          })
          val group = pv.cogroup(bid, impr, click)
          val res0 = group.map(x => {
            var key = ""
            var pv, bid, impr, click, charge = 0
            val list = List(x._2._1, x._2._2, x._2._3, x._2._4)
            list.foreach(iter =>
              iter.foreach(x => {
                key = if ("".equals(x._1)) key else x._1
                pv += x._2
                bid += x._3
                impr += x._4
                click += x._5
                charge += x._6
              }))
            (key, (pv, bid, impr, click, charge))
          }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
          pv.unpersist()
          bid.unpersist()
          impr.unpersist()
          click.unpersist()
          res0
        }).reduce((a, b) => a.union(b)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
          .repartition(20).saveAsTextFile("hdfs://ws030:8000/user/liuhongliang/abtest/%s".format(resultPath))
      })
    })

  }

  def pv_bid_impr_click_join_version(): Unit = {
    (6 to 6).map(month => {
      (1 to 30).map(day => {
        val resultPath = "2016%s%s".format(month.formatted("%02d"), day.formatted("%02d"))
        (0 to 23).map(hour => {
          val timePath = "2016/%s/%s/%s".format(month.formatted("%02d"), day.formatted("%02d"), hour.formatted("%02d"))
          val pvPath = "hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/%s/*.avro".format(timePath)
          val pv = sqlContext.read.avro(pvPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
            var abtest, vendor, nsv, av = ""
            val bid_id = row.getString(3)
            av = row.getMap[String, Object](22).get("av").fold("")(y => y.toString)
            abtest = resultPath
            (bid_id, (abtest + "," + vendor + "," + nsv + "," + av, 1, 0, 0, 0, 0))
          })
          val bidPath = "hdfs://ws030:8000/quipu/camus/data/bid_sdk/hourly/%s/*.avro".format(timePath)
          val bid = sqlContext.read.avro(bidPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
            val bid_id = row.getString(3)
            (bid_id, ("", 0, 1, 0, 0, 0))
          })
          val imprPath = "hdfs://ws030:8000/quipu/camus/data/impr_sdk/hourly/%s/*.avro".format(timePath)
          val impr = sqlContext.read.avro(imprPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
            val bid_id = row.getString(3)
            (bid_id, ("", 0, 0, 1, 0, 0))
          })
          val clickPath = "hdfs://ws030:8000/quipu/camus/data/click_sdk_charged/hourly/%s/*.avro".format(timePath)
          val click = sqlContext.read.avro(clickPath).filter("slot_id = '68cecd628fb86c72793660846b81617a'").map(row => {
            val bid_id = row.getString(3)
            val charge = row.getInt(33)
            (bid_id, ("", 0, 0, 0, 1, charge))
          })
          val group = pv.cogroup(bid, impr, click)
          val res0 = group.map(x => {
            var key = ""
            var pv, bid, impr, click, charge = 0
            val list = List(x._2._1, x._2._2, x._2._3, x._2._4)
            list.foreach(iter =>
              iter.foreach(x => {
                key = if ("".equals(x._1)) key else x._1
                pv += x._2
                bid += x._3
                impr += x._4
                click += x._5
                charge += x._6
              }))
            (key, (pv, bid, impr, click, charge))
          }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
          pv.unpersist()
          bid.unpersist()
          impr.unpersist()
          click.unpersist()
          res0
        }).reduce((a, b) => a.union(b)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
          .repartition(20).saveAsTextFile("hdfs://ws030:8000/user/liuhongliang/version/%s".format(resultPath))
      })
    })

  }

  def game_top_slotId(): Unit = {
    val file1 = readFile("/mfs/zhaown/game/sy_udid_30_89_dev_limit.txt")
    val file2 = readFile("/mfs/zhaown/game/sy_udid_90.txt")
    val file = sc.broadcast(readFile("/home/liuhongliang/game.exist"))
    file.value.contains("xx")

    val scfile1 = sc.textFile("hdfs://ws030:8000/user/liuhongliang/sy_udid_30_89_dev_limit.txt")
    val scfile2 = sc.textFile("hdfs://ws030:8000/user/liuhongliang/sy_udid_90.txt")
    var scfile = scfile1.++(scfile2)

    //    val scfile3 = sc.textFile("hdfs://ws030:8000/user/liuhongliang/game.exist")
    //    scfile = scfile.++(scfile3)
    val accExist = sc.accumulableCollection(new HashSet[String]())
    scfile.foreach(x => accExist += x)
    accExist.value.size
    //    val file = sc.broadcast(accExist.value)
    val slotAndDeviceExist = sc.accumulableCollection(new HashSet[String]())
    //    val uv = sc.accumulableCollection(new HashSet[String]())


    for (h <- 0 to 23) {
      val hour = if (h < 10) s"0$h" else s"$h"
      val pv = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/04/26/$hour/*.avro")
      //      val pv = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/04/26/00/*.avro")
      pv.foreachPartition(rows => rows.foreach(row => {
        var udid = row.getMap[String, Object](21).get("udid").fold("")(y => y.toString)
        udid = if (udid.contains("ifa:")) udid.substring(5) else udid
        val auid = row.getMap[String, Object](21).get("auid").fold("")(y => y.toString)
        val pvDeviceId: String = row.getString(14)
        val slotId = row.getString(6)
        if (file.value.contains(pvDeviceId)) {
          slotAndDeviceExist += slotId + "," + pvDeviceId
        } else if (file.value.contains(udid)) {
          slotAndDeviceExist += slotId + "," + udid
        } else if (file.value.contains(auid)) {
          slotAndDeviceExist += slotId + "," + auid
        }
      }))
      pv.unpersist()
    }
    var map = new HashMap[String, Int]()
    var uv1 = new HashSet[String]()

    val t = uv1.par
    val itr = slotAndDeviceExist.value.toIterator
    while (itr.hasNext) {
      val x = itr.next()
      val array = x.split(",")
      if (array.size == 2) {
        uv1.+=(array(1))
        map.put(array(0), map.get(array(0)).getOrElse(0) + 1)
      }
    }

    slotAndDeviceExist.value.foreach(x => {
      val array = x.split(",")
      if (array.size == 2) {
        uv1.+=(array(1))
        map.put(array(0), map.get(array(0)).getOrElse(0) + 1)
      }
    })
    map.foreach(println)
    println(uv1.size)

  }

  val t2 = new HashSet[String]()

  def add2(x: String): Unit = {
    println("add")
    t2 += x
  }

  def click_imei_tofile(): Unit = {
    val accExist = sc.accumulableCollection(new HashSet[String]())
    val counter = sc.accumulator(0, "tt")
    for (i <- 15 to 15) {
      val date = if (i < 10) s"0$i" else s"$i"
      val click = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/click_sdk_charged/hourly/2016/06/%s/*/*.avro".format(date))
      click.foreachPartition(rows => {
        rows.foreach(row => {
          val timestamp = row.getLong(0)
          val device: String = row.getString(14)
          counter += 1
          accExist += (timestamp + " " + device)
        })
      })
      click.unpersist()
    }
    accExist.value.size
  }

  def click_imei(): Unit = {
    val accExist = sc.accumulableCollection(new HashSet[String]())
    val counter = sc.accumulator(0, "tt")
    for (i <- 7 to 22) {
      val date = if (i < 10) s"0$i" else s"$i"
      val click = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/click_sdk_charged/hourly/2016/06/%s/*/*.avro".format(date))
      //      click.mapPartitions(rows => {
      //        rows.map(row => {
      //          val device: String = row.getString(14)
      //          val sponsorId = row.getLong(20)
      //          val timestamp = row.getLong(0)
      //          if (sponsorId == 286304) {
      //            return timestamp + " " + device
      //          }
      //        })
      //      }).saveAsTextFile("hdfs://ws030:8000/user/liuhongliang/imei%s.txt".format(date))

      click.foreachPartition(rows => {
        rows.foreach(row => {
          val device: String = row.getString(14)
          val timestamp = row.getLong(0)
          val sponsorId = row.getLong(20)
          if (sponsorId == 286304) {
            counter += 1
            accExist += (timestamp + " " + device)
          }
        })
      })
      click.unpersist()
    }
    accExist.value.size
    sc.parallelize(accExist.value.toList, 1).saveAsTextFile("hdfs://ws030:8000/user/liuhongliang/imei.txt")

    accExist.value.foreach(println(_))
  }

  def pv_sdk(): Unit = {

    val file = sc.broadcast(file1 ++ file2)
    val accExist = sc.accumulableCollection(new HashSet[String]())

    for (i <- 5 to 5) {
      val date = if (i < 10) s"0$i" else s"$i"
      for (h <- 0 to 23) {
        val hour = if (h < 10) s"0$h" else s"$h"
        val pv = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/04/12/$hour/*.avro")
        pv.foreachPartition {
          rows =>
            val exist = new HashSet[String]
            rows.foreach(
              row => {
                val udid = row.getMap[String, Object](21).get("udid").fold("")(y => y.toString)
                val auid = row.getMap[String, Object](21).get("auid").fold("")(y => y.toString)
                val pvDeviceId: String = row.getString(14)
                if (file.value.contains(pvDeviceId) && !exist.contains(pvDeviceId)) {
                  exist += pvDeviceId
                  accExist += pvDeviceId
                } else if (file.value.contains(udid) && !exist.contains(udid)) {
                  exist += udid
                  accExist += udid
                } else if (file.value.contains(auid) && !exist.contains(auid)) {
                  exist += auid
                  accExist += auid
                }
              })
        }
        pv.unpersist()
      }
    }
    accExist.value.size

  }

  def pv_dsp() {

    val scFile = sc.broadcast(file1 ++ file2)
    val accExist3 = sc.accumulableCollection(new HashSet[String]())

    val hourlyExist = sc.accumulableCollection(new HashMap[String, HashSet[String]]())
    for (i <- 5 to 5) {
      val date = if (i < 10) s"0$i" else s"$i"
      for (h <- 0 to 23) {
        val hour = if (h < 10) s"0$h" else s"$h"
        val ace = sc.accumulableCollection(new HashSet[String]())
        val pv = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_dsp/hourly/2016/04/11/$hour/*.avro")
        pv.foreachPartition {
          rows =>
            val exist = new HashSet[String]
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
    accExist3.value.size


    var a = new HashSet[String]
    hourlyExist.value.foreach(x => {
      a = a ++ x._2
    })
    var t = 0
    print("0_")
    for (x <- 0 to 17) {
      var a = new HashSet[String]
      for (y <- 0 to x) {
        val hour = if (y < 10) s"0$y" else s"$y"
        a = a ++ hourlyExist.value.get(hour).get
      }
      print(a.size - t + "_")
      t = a.size
    }
    println("|")
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


    var exist = collection.Set.empty[String]
    var exist1 = collection.Set.empty[String]
    var exist2 = collection.Set.empty[String]
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

    sc.accumulator[collection.Set[String]](collection.Set.empty[String])
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


    val scFile = sc.broadcast(file1 ++ file2)
    val accExist3 = sc.accumulableCollection(new HashSet[String]())

    val hourlyExist = sc.accumulableCollection(new HashMap[String, HashSet[String]]())
    for (i <- 5 to 5) {
      val date = if (i < 10) s"0$i" else s"$i"
      for (h <- 0 to 23) {
        val hour = if (h < 10) s"0$h" else s"$h"
        val ace = sc.accumulableCollection(new HashSet[String]())
        val pv = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_dsp/hourly/2016/04/11/$hour/*.avro")
        pv.foreachPartition {
          rows =>
            val exist = new HashSet[String]
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
    accExist3.value.size


    pv.take(1).foreach(row => println(row.getMap[String,Object](25).get("IDFA"), row.getMap[String,Object](25).get("DEVICEID")))
    pv.take(1).foreach(row => println(row.getMap[String,Object](25).foreach(println(_))))


    var a = new HashSet[String]

    hourlyExist.value.foreach(x => {
      a = a ++ x._2
    })
    var t = 0
    print("0_")
    for (x <- 0 to 17) {
      var a = new HashSet[String]
      for (y <- 0 to x) {
        val hour = if (y < 10) s"0$y" else s"$y"
        a = a ++ hourlyExist.value.get(hour).get
      }
      print(a.size - t + "_")
      t = a.size
    }
    println("|")

  }


  def landpage(): Unit = {
    var slotId: String = "b65143e622ec5936e38a47ed365e2384"
    slotId = "780dc5048214ef5f0c94f366d2554d40"

    var map = new HashMap[Int, Int]()
    for (d <- 13 to 27) {

      var set = sc.accumulableCollection(new HashSet[String]())
      val land = sqlContext.read.avro(s"hdfs://ws001:8000/quipu/camus/data/sdk_land_page_avro/hourly/2016/04/%d/*/*.avro".format(d))
      land.foreachPartition(rows => rows.filter(tmp => slotId.endsWith(tmp.getString(4))).foreach(row => {
        set += row.getMap[String, String](9).getOrElse("fd", "null")
      }))
      val result = land.mapPartitions(rows => rows.filter(tmp => slotId.endsWith(tmp.getString(4))).map(row => {
        (row.getMap[String, String](9).getOrElse("fd", "null"), 1)
      })).reduceByKey((a, b) => a + b)
      result.collect().foreach(x => if (x._2 > 1) println(x._1 + " : " + x._2))
      set.value.foreach(println(_))


      val c = land.map(row => {
        if (slotId.endsWith(row.getString(4))) {
          val ext = row.getMap[String, String](9)
          val fd: String = ext.getOrElse("fd", "null_fd")
          (fd, 1)
        } else {
          ("else", 1)
        }
      }).reduceByKey((a, b) => a + b)

      //      c.collect().foreach(x => println(x._1 + ":" + x._2))
      var count = 1;
      c.collect().foreach(x => {
        var array = x._1.split("school=")
        if (array.size > 1 && !array(0).contains("=&")) {
          count += 1
        }
      })
      map.put(d, count)
      land.unpersist()
    }


  }

  def land_page(): Unit = {
    val landPage = sqlContext.read.avro(s"hdfs://ws030:8000/quipu/camus/data/pv_sdk/hourly/2016/04/25/00/*.avro")
    val accExist = sc.accumulableCollection(new HashSet[String]())

    landPage.foreachPartition(rows => rows.filter(tmp => tmp.getString(4) == "780dc5048214ef5f0c94f366d2554d40").foreach(row => {
      accExist += row.getMap[String, String](9).getOrElse("fd", "null")
    }))
    accExist.value.size
    accExist.value.foreach(println(_))

  }


}
