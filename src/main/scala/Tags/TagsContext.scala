package Tags

import com.util.TagUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath) = args

    val spark: SparkSession = SparkSession.builder()
      .appName("tags")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.read.parquet(inputPath)

    df.rdd.map(row=>{
      val userId: String = TagUtils.getOneUserId(row)

    })
  }
}

