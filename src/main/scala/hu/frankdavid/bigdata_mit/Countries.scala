package hu.frankdavid.bigdata_mit

import org.apache.spark.SparkContext

object Countries {

  def main(args: Array[String]) {
    val sc = new SparkContext(master = "local", appName = "countries")

    val profiles = sc.textFile("/user/hadoop/mit/profiles.tsv")

    val result = profiles.map {_.split('\t')(3)}.countByValue()

    result.toSeq.sortBy(-_._2).foreach { case (country, count) =>
      println(s"$country\t$count")
    }

  }
}
