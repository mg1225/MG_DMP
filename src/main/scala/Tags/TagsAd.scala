package Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsAd extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]
    val adType: Int = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list:+=("LC"+v,1)
      case v if v > 0 && v <= 9 => list:+=("LC0"+v,1)
    }
    val adName: String = row.getAs[String]("adspacetypename")
    if (StringUtils.isBlank(adName)){
      list:+=("LN"+adName,1)
    }

    val channel = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channel,1)
    list
  }
}
