package sparktutorial.solutions_kailash.e_02_word_count

import org.apache.spark.rdd.RDD

/**
  * Created by kneupane on 7/24/18.
  */
object WordCount2_04_SortByCount {

  /**
    * Exercise (Hard): Sort the output by count. You can't use the same
    * approach as in the previous exercise. Hint: See RDD.keyBy
    * (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD)
    * What's the most frequent word that isn't a "stop word".
    *
    */

  private var lc = WordCount2_03_LetterCount.letterCount()

  /*  def sortLCByCount(): Unit = {
      var countAsKey: RDD[(Int, (String, Int))] = lc.keyBy(x => x._2)
      println("KeyBy")
      var removedCountFromValue = countAsKey.map(x => (x._1, x._2._1))
        .sortByKey(true).coalesce(1)
      removedCountFromValue.foreach(println)
      removedCountFromValue.saveAsTextFile(WC2Setup.ioPath._2 + "/keyBy/")
      removedCountFromValue
    }*/

  //What's the most frequent word that isn't a "stop word".
  def mostFrequentNotStopWord(): ((String, Int), RDD[(String, Int)]) = {
    var words = WC2Resources.input.flatMap(line => line.toLowerCase.split("""[^\p{IsAlphabetic}]+"""))
    var wordsWithoutStopWords = words.subtract(WC2Resources.stopWordsRDD)
    var wc = wordsWithoutStopWords.map(x => (x, 1)).reduceByKey(_ + _).sortBy(x => x._2, false).coalesce(1)
    println("sssss")
    wc.foreach(println)
    var str_val = wc.first()
    (str_val, wc)
  }

  def main(args: Array[String]): Unit = {
    // sortLCByCount()
    println(s"Most frequent word after stop words: ${mostFrequentNotStopWord()._1._1}\nIts count:${mostFrequentNotStopWord()._1._2}")
  }

}
