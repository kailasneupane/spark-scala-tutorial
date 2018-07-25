package sparktutorial.solutions_kailash.e_01_word_count

import org.apache.spark.rdd.RDD

/**
  * Created by kneupane on 7/24/18.
  */
object WordCount2_01_OtherVersionsOfBible {

  /**
    * Exercise: Use other versions of the Bible:
    * The data directory contains similar files for the Tanach (t3utf.dat - in Hebrew),
    * the Latin Vulgate (vuldat.txt), the Septuagint (sept.txt - Greek)
    */

  def wordCountOfBible(): RDD[(String, Int)] = {

    //word count
    var wc = WC2Resources.input.flatMap(x => x.split("""[^\p{IsAlphabetic}]+"""))
      .map(x => (x, 1))
      .reduceByKey((x, y) => (x + y))
    wc.foreach(println)
    wc.saveAsTextFile(WC2Resources.ioPath._2)
    println("completed")
    wc
  }

  def main(args: Array[String]): Unit = {
    wordCountOfBible()
  }

}
