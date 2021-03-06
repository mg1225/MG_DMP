package Tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, String2Type, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    if(String2Type.toDouble(row.getAs[String]("long"))>=73
      && String2Type.toDouble(row.getAs[String]("long"))<=136
      && String2Type.toDouble(row.getAs[String]("lat"))>=3
      && String2Type.toDouble(row.getAs[String]("lat"))>=53
    ){
      val long = String2Type.toDouble(row.getAs[String]("long"))
      val lat = String2Type.toDouble(row.getAs[String]("lat"))

      val business = getBusiness(long,lat)
      if (StringUtils.isNoneBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list:+=(str,1)
        })
      }
    }
    list
  }

  def getBusiness(long: Double,lat: Double):String={
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(long,lat,6)
    var business = redis_queryBussiness(geohash)
    if(business==null){
      business = AmapUtil.getBusinessFromAmap(long,lat)
      if(business != null && business.length>0){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }

  def redis_queryBussiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  def redis_insertBusiness(geohash:String,business:String):Unit={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
  
}
