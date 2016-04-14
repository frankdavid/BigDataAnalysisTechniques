package hu.frankdavid.bigdata_mit

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, LongWritable, Text}
import org.apache.hadoop.mapred._

import scala.collection.JavaConversions._

class ManAndWomen2
object ManAndWomen2 {

  class Mapper extends MapReduceBase with org.apache.hadoop.mapred.Mapper[LongWritable, Text, Text, Text] {
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter) {
      val parts = value.toString.split('\t')
      // filter for f and m values only
      if (parts(1) == "m" || parts(1) == "f") {
        output.collect(new Text(parts(3)), new Text(parts(1)))
      }
    }
  }

  class Reducer extends MapReduceBase with org.apache.hadoop.mapred.Reducer[Text, Text, Text, DoubleWritable] {
    def reduce(k2: Text, iterator: util.Iterator[Text], outputCollector: OutputCollector[Text, DoubleWritable], reporter:
    Reporter) = {
      var female, male = 0
      iterator.foreach {
        _.toString match {
          case "m" => male += 1
          case "f" => female += 1
          case _ => // should not happen in theory
        }
      }
      outputCollector.collect(k2, new DoubleWritable(female / (female + male).toDouble))
    }
  }

  def main(args: Array[String]) {
    val conf: JobConf = new JobConf(classOf[SignupDate])
    conf.setJobName("men_and_women_2")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Mapper])
    conf.setReducerClass(classOf[Reducer])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[_, _]])
    FileInputFormat.setInputPaths(conf, new Path("/user/hadoop/mit/profiles.tsv"))
    FileOutputFormat.setOutputPath(conf, new Path("/user/hadoop/mit/men_and_women_2"))
    JobClient.runJob(conf)
  }
}