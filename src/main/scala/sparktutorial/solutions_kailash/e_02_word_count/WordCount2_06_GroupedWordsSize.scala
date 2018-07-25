package sparktutorial.solutions_kailash.e_02_word_count

/**
  * Created by kneupane on 7/25/18.
  */
object WordCount2_06_GroupedWordsSize {

  /**
    * Exercise (Thought Experiment): Consider the size of each group created
    * in the previous exercise and the distribution of those sizes vs. counts.
    * What characteristics would you expect for this distribution? That is,
    * which words (or kinds of words) would you expect to occur most
    * frequently? What kind of distribution fits the counts (numbers)?
    */
  var gg = WordCount2_05_GroupCount.groupedWC()

  def getGroupSize() = {
    var ggSize = gg.map(x => (x._1, x._2.size, x._2)).sortBy(x => x._2,true).coalesce(1)
    ggSize
  }

  def main(args: Array[String]): Unit = {
    getGroupSize().foreach(println)
  }

}
