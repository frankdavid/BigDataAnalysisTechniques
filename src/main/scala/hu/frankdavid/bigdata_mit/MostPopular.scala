package hu.frankdavid.bigdata_mit

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MostPopular {

  def main(args: Array[String]) {
    val sc = new SparkContext(master = "local", appName = "most_popular")

    val plays = sc.textFile("/user/hadoop/mit/plays.tsv")

    val topArtists = plays.map { row =>
      val values = row.split('\t')
      val plays = try {values(3).toLong} catch {case _ => 0}
      (values(2), plays)
    }.reduceByKey(_ + _).top(10)(Ordering.by(_._2))

    topArtists.foreach { case (artist, plays) =>
      println(s"$artist: $plays")
    }

  }
}

object MostPopularUnder30 {

  def main(args: Array[String]) {
    val sc = new SparkContext(master = "local", appName = "most_popular")

    //read data files and group by user id
    val plays = sc.textFile("/user/hadoop/mit/plays.tsv").keyBy(_.split('\t')(0))
    val profiles = sc.textFile("/user/hadoop/mit/profiles.tsv").keyBy(_.split('\t')(0))

    // join by user id and calculate the sum of play count
    val topArtists = plays.join(profiles).flatMap { case (user, (play, profile)) =>
      // split tab separated rows
      val profileValues = profile.split('\t')
      val playValues = play.split('\t')
      try {
        // if age < 30, emit artist and play count
        if (profileValues(2).toInt < 30) {
          val playCount = playValues(3).toLong
          Some((playValues(2), playCount))
        } else {
          None
        }
      } catch {
        // handle wrong values
        case _: NumberFormatException => None
      }
    }
   // for each artist calculate the sum of play count
   .reduceByKey(_ + _)
   // pick top 10 by the sum of play count
   .top(10)(Ordering.by(_._2))

    //display the result
    topArtists.foreach { case (artist, plays) =>
      println(s"$artist: $plays")
    }

  }
}
