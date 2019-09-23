package Tags

import com.util.Tag
import org.apache.spark.sql.Row

object TagsApp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    args(1).asInstanceOf[]
    val app = row.getAs[String]("appname")

  }
}
