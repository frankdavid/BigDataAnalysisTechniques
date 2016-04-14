package hu.frankdavid.bigdata_mit

import org.apache.spark.SparkContext

object MenAndWomen {

  def main(args: Array[String]) {
    val sc = new SparkContext(master = "local", appName = "men_and_women")

    val profiles = sc.textFile("/user/hadoop/mit/profiles.tsv")
    val result = profiles.map {_.split('\t')(1)}.countByValue()
    println(result)

  }
}
