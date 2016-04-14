package hu.frankdavid.bigdata_mit

import com.xeiam.xchart.StyleManager.ChartType
import com.xeiam.xchart.VectorGraphicsEncoder.VectorGraphicsFormat
import com.xeiam.xchart.{ChartBuilder, VectorGraphicsEncoder}
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.immutable.SortedMap

object NumberOfDifferentArtistsPerUser {

  val BinSize = 1

  def main(args: Array[String]) {
    val sc = new SparkContext(master = "local", appName = "different_artists")

    val playHistory = sc.textFile("/user/hadoop/mit/plays.tsv")
    val result =
      SortedMap.empty[Int, Long] ++  // sort the result by keys
      playHistory.groupBy(_.split('\t')(0)) // group by first column (userid)
      .map { case (user, results) =>
        (results.size / BinSize) * BinSize  // round to bin size
      }.countByValue()  // count the occurrences of each value

    //rint results
    result.foreach{case(size, count) => println(s"$size\t$count")}

    //save as chart
    val chart = new ChartBuilder().chartType(ChartType.Bar).build()
    chart.addSeries(
      "Different artists per user",
      asJavaCollection[Integer](result.keys.asInstanceOf[Iterable[Integer]]),
      asJavaCollection[Integer](result.values.asInstanceOf[Iterable[Integer]]))

    VectorGraphicsEncoder.saveVectorGraphic(chart, "different_artists_per_user2", VectorGraphicsFormat.PDF)
  }
}
