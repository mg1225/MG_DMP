package com.label

import com.util.LabelUtils
import org.apache.spark.sql.SparkSession

object kwLabel {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("文件输入错误")
      sys.exit()
    }

    val Array(in,out) = args

    val spark = SparkSession.builder()
      .appName("adLabel")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df = spark.read.parquet(in)

    df.rdd.map(row=>{
      val userid = row.getAs[String]("userid")
      val keywords = row.getAs[String]("keywords").split("|").map((_,1))

    })
  }
}
