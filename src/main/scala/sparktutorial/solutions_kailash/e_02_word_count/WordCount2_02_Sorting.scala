package sparktutorial.solutions_kailash.e_02_word_count

import org.apache.spark.rdd.RDD

/**
  * Created by kneupane on 7/24/18.
  */
object WordCount2_02_Sorting {

  /**
    * Exercise: See the Scaladoc page for `OrderedRDDFunctions`:
    * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.OrderedRDDFunctions
    * Sort the output by word, try both ascending and descending.
    * Note this can be expensive for large data sets! (Why??)
    */

  var wc: RDD[(String, Int)] = WordCount2_01_OtherVersionsOfBible.wordCountOfBible()

  def sortWordCount(ascending: Boolean): RDD[(String, Int)] = {
    //true for ascending, false for descending
    var sorted = wc.sortBy(x => x._1, ascending)
    var asc_desc = if (ascending) "/ascending/" else "/descending/"
    sorted.saveAsTextFile(WC2Resources.ioPath._2 + asc_desc)
    sorted
  }

  def main(args: Array[String]): Unit = {
    sortWordCount(true)
  }

}