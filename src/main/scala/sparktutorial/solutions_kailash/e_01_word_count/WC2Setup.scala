package sparktutorial.solutions_kailash.e_01_word_count

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

/**
  * Created by kneupane on 7/24/18.
  */
object WC2Setup {

  val conf = new SparkConf()
    .setAppName("Different version of bible")
    .setMaster("local[*]")
    .set("spark.hadoop.validateOutputSpecs", "false")
    .set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(conf)

  //bible input files path
  private val input_kjvdat = "data/kjvdat.txt" //King James Version of the Bible
  private val input_t3utf = "data/t3utf.dat" //Tanach in Hebrew
  private val input_vuldat = "data/vuldat.txt" //Latin Vulgate
  private val input_sept = "data/sept.txt" //Septuagint

  //bible output files path
  private val out_kjv = "output/kjv-wc2"
  private val out_t3utf = "output/t3utf-wc2"
  private val out_vuldat = "output/vuldat-wc2"
  private val out_sept = "output/sept-wc2"

  var ioPath: (String, String) = {
    println("Which Bible for word count?\n1. KJV \n2. Tanach \n3. Vulgate\n4. Septuagint\n")
    var input = StdIn.readLine
    var in_out = input match {
      case "1" => (input_kjvdat, out_kjv)
      case "2" => (input_t3utf, out_t3utf)
      case "3" => (input_vuldat, out_vuldat)
      case "4" => (input_sept, out_sept)
    }
    in_out
  }

  val input = sc.textFile(ioPath._1)

}
