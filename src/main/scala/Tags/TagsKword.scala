package Tags

import com.util.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKword extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    val arr: Array[String] = row.getAs[String]("keywords").split("\\|")

    val arr2: Array[String] = arr.filter(word=>word.length>=3 && word.length<=8 && !stopWords.value.contains(word))

    arr2.foreach(word=>list:+=("K"+word,1))
    list
  }
}
