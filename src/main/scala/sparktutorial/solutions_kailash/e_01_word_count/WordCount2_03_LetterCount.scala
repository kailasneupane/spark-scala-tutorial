package sparktutorial.solutions_kailash.e_01_word_count

/**
  * Created by kneupane on 7/24/18.
  */
object WordCount2_03_LetterCount {
  /**
    * Exercise: Take the output from the previous exercise and count the number
    * of words that start with each letter of the alphabet and each digit.
    */

  var sortedWC = WordCount2_02_Sorting.sortWordCount(true)

  def letterCount() = {
    var lc = sortedWC.map(x => (x._1, x._1.length))
    lc.foreach(println)
    lc.saveAsTextFile(WC2Resources.ioPath._2 + "/letter_count/")
    lc
  }

  def main(args: Array[String]): Unit = {
    letterCount()
  }

}
