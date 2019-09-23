package com.label

import com.util.LabelUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object APPLabel {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("文件输入错误")
      sys.exit()
    }
    val Array(in, out, docs) = args

    val spark = SparkSession.builder()
      .appName("APPLabel")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val rdd: RDD[String] = spark.sparkContext.textFile(docs)
    val docMap = rdd.map(_.split("\\s", -1)).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()
    val broadcast = spark.sparkContext.broadcast(docMap)

    val df = spark.read.parquet(in)
    df.rdd.map(row => {
      val userid = row.getAs[String]("userid")
      var appname = row.getAs[String]("appname")
      if (StringUtils.isBlank(appname)) {
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"), "unknow")
      }
      (userid,LabelUtils.appLebal(appname))
    }).map(t=>t._1+","+t._2)
      .saveAsTextFile(out)
  }
}
