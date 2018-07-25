package sparktutorial.solutions_kailash.e_01_word_count

import org.apache.spark.rdd.RDD

/**
  * Created by kneupane on 7/25/18.
  */
object WordCount2_05_GroupCount {

  /**
    * Exercise (Hard): Group the word-count pairs by count. In other words,
    * All pairs where the count is 1 are together (i.e., just one occurrence
    * of those words was found), all pairs where the count is 2, etc. Sort
    * ascending or descending. Hint: Is there a method for grouping?
    */

  var wc = WordCount2_04_SortByCount.mostFrequentNotStopWord()._2

  def groupedWC(): RDD[(Int, List[String])] = {
    var grp = wc.groupBy(x => x._2).sortBy(x => x._1).map(x => (x._1, (x._2.map(u => u._1).toList))).coalesce(1)
    //grp.foreach(println)
    grp
  }

  def main(args: Array[String]) = {
    groupedWC().foreach(println)
  }
}
