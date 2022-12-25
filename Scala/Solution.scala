import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD

class Solution {

  System.setProperty("hadoop.home.directory","C://hadoop")

  val spark: SparkSession = SparkSession.builder
    .appName("Solution")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def mostCommonWord(paragraph: String, banned: Array[String]):  Unit = {
    //val specialChars = Set('.',',')
    val b = banned.toList
         for(x <- b){
          println(x)
        }
    val p = paragraph.replaceAll("[-+.^:,]", "").toLowerCase
    val words = p.split(" ").filterNot(x => x == b(0)).toList.groupBy(identity)
    println(words)
    val mapvalues = words.mapValues(_.size)
    println(mapvalues)
    val maxBy = mapvalues.maxBy(_._2)
    println(maxBy)
    val result = maxBy._1
    println(result)
//    val df = words.toList.toDF("values")
//    val windowSpec = Window.partitionBy("values").orderBy($"values" desc)
//    val df1 = df.withColumn("Rank", row_number().over(windowSpec)).filter(col("Rank") === 2)
//    val result = df1.selectExpr("values")
//     for(x <- words){
//       println(x)
//     }
  }

  }

object Solution {
  def main(args: Array[String]): Unit = {
    val paragraph = "Bob hit a ball, the hit BALL flew far after it was hit."
    val banned = Array("hit")
    val a = new Solution
    print(a.mostCommonWord(paragraph,banned))
    //print(a.numberlength(num))

  }
}

//The Answer for this example: "ball"
