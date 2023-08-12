import org.apache.spark.{SparkConf, SparkContext}
import ScanLeft.ScanLeft._
import org.apache.spark.rdd._


object Main extends App {


  val conf: SparkConf = new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("scan")
  
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val seq: Seq[Int] = (0 to 20).toSeq
  val seqSum = seq.sum
  val rdd = sc.parallelize(seq, 30)
  println(rdd.count())

  val s: RDD[(Int, Double)] = rdd
    .keyBy(t => t)
    .mapValues(_.toDouble / seqSum)
    .sortByKey()

  val func: (Double, Double) => Double = (acc: Double, e: Double) => acc + e
  println("Spark version")
  s.scanLeft(0.0)(func)
    .collect()
    .foreach(println)

  seq
    .map(_.toDouble / seqSum)
    .scanLeft(0.0)(op = (acc: Double, e: Double) => acc + e)
    .tail
    .foreach(println)
  println("Hello, World!")
}


