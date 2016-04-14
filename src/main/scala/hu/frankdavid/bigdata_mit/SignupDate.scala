package hu.frankdavid.bigdata_mit

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.LongSumReducer

import scala.io.Source

class SignupDate
object SignupDate {

  val BinSizeDays = 1
  val DateInputFormat = new SimpleDateFormat("MMM d, yyyy")
  val DateOutputFormat = new SimpleDateFormat("yyyy-MM-dd")

  class Mapper extends MapReduceBase with org.apache.hadoop.mapred.Mapper[LongWritable, Text, Text, LongWritable] {
    val One = new LongWritable(1)
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, LongWritable], reporter: Reporter) {
      val parts = value.toString.split('\t') //split into columns
      val signupDate = DateInputFormat.parse(parts(4))  //extract signup date
      val binSizeMillis = BinSizeDays * 24 * 3600 * 1000
      val date = DateOutputFormat.format(new Date((signupDate.getTime / binSizeMillis) * binSizeMillis)) //put into bin by date
      output.collect(new Text(date), One)
    }
  }

  def main(args: Array[String]) {
    val conf: JobConf = new JobConf(classOf[SignupDate])
    conf.setJobName("signup_date")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[LongWritable])
    conf.setMapperClass(classOf[Mapper])
    conf.setReducerClass(classOf[LongSumReducer[Text]])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[_, _]])
    FileInputFormat.setInputPaths(conf, new Path("/user/hadoop/mit/profiles.tsv"))
    FileOutputFormat.setOutputPath(conf, new Path("/user/hadoop/mit/signup_date2"))
    JobClient.runJob(conf)
  }
}

object SignupDateOutliers {
  def main(args: Array[String]) {
    val dateSignups = readSignups()
    var sum = 0L
    var squareSum = 0L
    val windowSize = 100
    val frontIterator = dateSignups.iterator
    for(_ <- 0 until windowSize) {
      val v = frontIterator.next()._2
      sum += v
      squareSum += v * v
    }
    val middleIterator = dateSignups.iterator
    for(_ <- 0 until windowSize / 2) {
      middleIterator.next()
    }
    val backIterator = dateSignups.iterator

    while(frontIterator.hasNext) {
      val back = backIterator.next()._2
      val middle = middleIterator.next()
      val front = frontIterator.next()._2

      sum -= back
      sum += front
      squareSum -= back * back
      squareSum += front * front

      val mean = sum / windowSize.toDouble
      val deviation = math.sqrt(squareSum / windowSize.toDouble - mean * mean)
      val sensitivity = 2.698
      if (math.abs(middle._2 - mean) >= deviation * sensitivity) {
        println(middle._1)
      }
    }


  }

  def readSignups(): Seq[(String, Long)] = {
    val source = Source.fromFile("results/signup_dates2.txt")
    val dateSignups = source.getLines().map { row =>
      val values = row.split('\t')
      values(0) -> values(1).toLong
    }.toSeq
    dateSignups
  }
}
