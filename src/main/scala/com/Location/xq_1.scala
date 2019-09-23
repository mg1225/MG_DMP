package com.Location

import org.apache.spark.sql.{DataFrame, SparkSession}

object xq_1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("xq1")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = spark.read.parquet("H:\\log")

    df.createTempView("log")
    val df2 = spark.sql(
      """
       select
       |provincename,
       |cityname,
       |sum(case when ysqq = 1 then 1 else 0 end) as cnt_ysqq,
       |sum(case when yxqq = 1 then 1 else 0 end) as cnt_yxqq,
       |sum(case when gkqq = 1 then 1 else 0 end) as cnt_gkqq,
       |sum(case when cyjj = 1 then 1 else 0 end) as cnt_cyjj,
       |sum(case when cgjj = 1 then 1 else 0 end) as cnt_cgjj,
       |sum(case when zss = 1 then 1 else 0 end) as cnt_zss,
       |sum(case when djs = 1 then 1 else 0 end) as cnt_djs,
       |sum(case when gkxf = 1 then winprice end) as cnt_kgxf,
       |sum(case when gkcb = 1 then adpayment end) as cnt_kgcb
       |from
       |(
       |select
       |provincename,
       |cityname,
       |winprice,
       |adpayment,
       |case when requestmode = 1 and processnode >= 1 then 1 else 0 end as ysqq,
       |case when requestmode = 1 and processnode >= 2 then 1 else 0 end as yxqq,
       |case when requestmode = 1 and processnode = 3 then 1 else 0 end as gkqq,
       |case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end as cyjj,
       |case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end as cgjj,
       |case when requestmode = 2 and iseffective = 1 then 1 else 0 end as zss,
       |case when requestmode = 3 and iseffective = 1 then 1 else 0 end as djs,
       |case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 end as gkxf,
       |case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 end as gkcb
       |from log
       |) as t
       |group by provincename,cityname
      """.stripMargin)
    df2.show()
    spark.close()
  }
}
