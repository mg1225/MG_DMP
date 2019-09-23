package com.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

object AmapUtil {
  /*https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=c8611d7a2b60cea0355e6c464a17e170&extensions=all*/
  def getBusinessFromAmap(long:Double,lat:Double):String = {
    val loaction = long+","+lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+loaction+"&key=c8611d7a2b60cea0355e6c464a17e170"

    val jsonstr: String = HttpUtil.get(url)

    val jSONObject: JSONObject = JSON.parseObject(jsonstr)

    val status: Int = jSONObject.getIntValue("status")
    if(status == 0) return ""
    val jSONObject1: JSONObject = jSONObject.getJSONObject("regeocode")
    if (jSONObject1==null) return ""
    val jsonObject2: JSONObject = jSONObject1.getJSONObject("addressComponent")
    if(jsonObject2 == null) return "" 
    val jSONArray: JSONArray = jsonObject2.getJSONArray("businessAreas")

    if(jSONArray == null) return ""

    val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    for (item <- jSONArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val name: String = json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }
}
