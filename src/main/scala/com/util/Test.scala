package com.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("test")
      .getOrCreate()

    val arr = Array("     ")
    val rdd: RDD[String] = spark.sparkContext.makeRDD(arr)
    rdd.map(t=>{
      HttpUtil.get(t)
    }).foreach(println)
  }
}
