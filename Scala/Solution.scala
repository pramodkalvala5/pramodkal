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
  
  //Swap Adjacent in LR String
//LeetCode 777 In a string composed of ‘L’, ‘R’, and ‘X’ characters, like “RXXLRXRXL”, a move consists of either replacing one occurrence of “XL” with “LX”, 
// or replacing one //occurrence of “RX” with “XR”. Given the starting string start and the ending string end, return True if and only if there exists a sequence of moves to transform one string to the other.
//The problem could be solved based on 3 conditions:
//Both strings have same length
//Both strings have same number of ‘L’s and ‘R’s
//All ‘L’s indices in end are smaller or equal to their indices in start and all ‘R’s indices in end are greater or equal to their indices in start
  
    //RXXLRXRXL
  def canTransform(start: String, end: String): Boolean= {
    val sv = start.zipWithIndex
    println(s"value of sv with zipWithIndex is  $sv")
    val sv_filter = sv.filterNot(_._1 == 'X')
    println(s"value of sv with filter not is  $sv_filter")
    val sv_filter_length = sv_filter.length
    println(s"value of sv_filter_length is  $sv_filter_length")
    val ev = end.zipWithIndex.filterNot(_._1 == 'X')
    println(s"value of ev is  $ev")
    val ev_length = ev.length
    println(s"value of ev length is  $ev_length")
    val test = (sv_filter zip ev)
    println(s"value of sv_filter zip ev is  $test")
    // (sv_filter zip ev).foreach {
      // case (('L', i1), ('L', i2)) => println(s"value of L's $i1 & $i2")
      // case (('R', i1), ('R', i2)) => println(s"value of R's $i1 & $i2")
        start.length == end.length && sv_filter.length == ev.length &&
          (sv_filter zip ev).forall {
            case (('L', i1), ('L', i2)) => i1 >= i2
            case (('R', i1), ('R', i2)) => i1 <= i2
            case _ => false
          }
    }

}

  

object Solution {
  def main(args: Array[String]): Unit = {
    val paragraph = "Bob hit a ball, the hit BALL flew far after it was hit."
    val banned = Array("hit")
    val a = new Solution
    print(a.mostCommonWord(paragraph,banned))
    print(a.canTransform("RXXLRXRXL", "RXXLRXRXL"))
    //print(a.numberlength(num))

  }
}

//The Answer for mostCommonWord: "ball"

// The Answer for canTransform is -
// value of sv with zipWithIndex is  Vector((R,0), (X,1), (X,2), (L,3), (R,4), (X,5), (R,6), (X,7), (L,8))
// value of sv with filter not is  Vector((R,0), (L,3), (R,4), (R,6), (L,8))
// value of sv_filter_length is  5
// value of ev is  Vector((R,0), (L,3), (R,4), (R,6), (L,8))
// value of ev length is  5
// value of sv_filter zip ev is  Vector(((R,0),(R,0)), ((L,3),(L,3)), ((R,4),(R,4)), ((R,6),(R,6)), ((L,8),(L,8)))
// value of R's 0 & 0
// value of L's 3 & 3
// value of R's 4 & 4
// value of R's 6 & 6
// value of L's 8 & 8
