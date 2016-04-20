package com.youdao.tonetease

import java.text.SimpleDateFormat
import java.util.Calendar

import com.youdao.quipu.avro.schema.{SdkClick, SdkPv, SdkImpr}
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.avro.util.Utf8
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by liuhl on 16-4-19.
 */
class NeteaseDaily {

  def getYesterday(formatStr:String): String = {
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat(formatStr).format(cal.getTime())
  }

  def main(args: Array[String]) {
    println(getYesterday("yyyy/MM/dd"))
  }

  def test(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val yesterday = getYesterday("yyyy/MM/dd")
    val yesterdayOutput = getYesterday("yyyy-MM-dd")
    val impr = sc.hadoopFile[AvroWrapper[SdkImpr], NullWritable, AvroInputFormat[SdkImpr]]("/quipu/camus/data/impr_sdk/hourly/%s/*/*.avro".format(yesterday))
    val pv = sc.hadoopFile[AvroWrapper[SdkPv], NullWritable, AvroInputFormat[SdkPv]]("/quipu/camus/data/pv_sdk/hourly/%s/*/*.avro".format(yesterday))
    val i = impr.filter {
      _._1.datum().getSponsorId == 262090
    }.map {
      x =>
        val obj = x._1.datum()
        val ext = obj.getExt
        val ip = String.valueOf(obj.getIp)
        (obj.getBidId.toString, (obj.getTimestamp, ip, String.valueOf(obj.getSlotId), String.valueOf(obj.getAppId)))
    }
    val p = pv.filter {
      x => Set("1042", "1070", "252", "380", "438", "462", "542", "554", "794", "974").contains(String.valueOf(x._1.datum().getAppId))
    }.map {
      x =>
        val obj = x._1.datum()
        val ext = obj.getExt
        val auid = ext.get(new Utf8("auid"))
        val udid = ext.get(new Utf8("udid"))
        val dn = String.valueOf(obj.getDeviceName)
        val did = String.valueOf(if (auid == null) udid else auid)
        (obj.getBidId.toString, (did, dn))
    }

    val ip = i.cogroup(p)
    val output = ip.flatMap {
      x =>
        val v = x._2
        val imprs = v._1
        val pvs = v._2
        val (did, dn) = if (pvs.isEmpty) ("", "") else (pvs.head._1, pvs.head._2)
        imprs.map {
          impr =>
            (new DateTime(impr._1).toString,
              if (did.contains(":")) did.substring(4)
              else did
              , dn, impr._2, impr._3, impr._4)
        }
    }
    output.repartition(1).map(x => s"${x._1};${x._2};${x._3};${x._4};${x._5};${x._6};").saveAsTextFile("/user/eadata/%s_impr".format(yesterdayOutput))

    val click = sc.hadoopFile[AvroWrapper[SdkClick], NullWritable, AvroInputFormat[SdkClick]]("/quipu/camus/data/click_sdk_charged/hourly/%s/*/*.avro".format(yesterday))
    val c = click.filter {
      _._1.datum().getSponsorId == 262090
    }.map {
      x =>
        val obj = x._1.datum()
        val ext = obj.getExt
        val ip = String.valueOf(obj.getIp)
        (obj.getBidId.toString, (obj.getTimestamp, ip, String.valueOf(obj.getSlotId), String.valueOf(obj.getAppId)))
    }
    val cp = c.cogroup(p)
    val outputc = cp.flatMap {
      x =>
        val v = x._2
        val clicks = v._1
        val pvs = v._2
        val (did, dn) = if (pvs.isEmpty) ("", "") else (pvs.head._1, pvs.head._2)
        clicks.map {
          click => (new DateTime(click._1).toString,
            if (did.contains(":")) did.substring(4)
            else did
            , dn, click._2, click._3, click._4)
        }
    }
    outputc.repartition(1).map(x => s"${x._1};${x._2};${x._3};${x._4};${x._5};${x._6};").saveAsTextFile("/user/eadata/%s_click".format(yesterdayOutput))


  }
}
