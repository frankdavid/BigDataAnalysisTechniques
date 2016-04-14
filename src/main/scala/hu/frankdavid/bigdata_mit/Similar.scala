package hu.frankdavid.bigdata_mit

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Similar {

  def main(args: Array[String]) {
    val sc = new SparkContext(master = "local", appName = "similar")

    val targetId = "00000c289a1829a808ac09c00daf10bc3c4e223b"
    val plays = sc.textFile("/user/hadoop/mit/plays.tsv")

    // create tuples of (artist, user)
    val artistUsers = plays.map { row =>
      val values = row.split('\t')
      (values(2), values(0))
    }

    // collect the target's favorite artists
    val targetFavorite = artistUsers.filter(_._2 == targetId).map(_._1).collect()

    // for each artist-user pair, the user should not equal the target and the artist must be in the targetFavorite
    val results = artistUsers.filter(artistUser => artistUser._2 != targetId && targetFavorite.contains(artistUser._1))
                  // group by user
                  .keyBy(_._2).groupByKey()
                  // select top 3 by the number of shared artists
                  .top(3)(Ordering.by(_._2.size))

    // display the result
    results.foreach(result => println(result._1))

  }
}
