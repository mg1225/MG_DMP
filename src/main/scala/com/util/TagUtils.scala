package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {
  def getOneUserId(row:Row):String={
    row match {
      case t if StringUtils.isNoneBlank(t.getAs[String]("imei"))=>"IM" + t.getAs[String]("imei")
      case t if StringUtils.isNoneBlank(t.getAs[String]("mac"))=>"MC" + t.getAs[String]("mac")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfa"))=>"ID" + t.getAs[String]("idfa")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudid"))=>"OD" + t.getAs[String]("openudid")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androidid"))=>"AD" + t.getAs[String]("androidid")
      case t if StringUtils.isNoneBlank(t.getAs[String]("imeimd5"))=>"IM" + t.getAs[String]("imeimd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("macmd5"))=>"MC" + t.getAs[String]("macmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfamd5"))=>"ID" + t.getAs[String]("idfamd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudidmd5"))=>"OD" + t.getAs[String]("openudidmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androididmd5"))=>"AD" + t.getAs[String]("androididmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("imeisha1"))=>"IM" + t.getAs[String]("imeisha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("macsha1"))=>"MC" + t.getAs[String]("macsha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfasha1"))=>"ID" + t.getAs[String]("idfasha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudidsha1"))=>"OD" + t.getAs[String]("openudidsha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androididsha1"))=>"AD" + t.getAs[String]("androididsha1")
    }
  }
}
