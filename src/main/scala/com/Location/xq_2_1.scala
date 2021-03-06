package com.Location

import com.util.RptUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object xq_2_1 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("文件输入错误")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder()
      .appName("xq2")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet(inputPath)

    df.rdd.map(row=>{
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val rptList = RptUtils.ReqPt(requestmode,processnode)
      val clictList = RptUtils.clickPt(requestmode,iseffective)
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

      val allList = rptList++clictList++adList

      (row.getAs[String]("ispname"),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)


  }
}
