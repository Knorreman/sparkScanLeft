package ScanLeft

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuiteLike
import ScanLeft._

import scala.annotation.tailrec

class ScanLeftTest extends AnyFunSuiteLike {

  lazy val fixture: Object {val conf: SparkConf; val sc: SparkContext} =
    new {
      val conf = new SparkConf()
      conf.setMaster("local[2]")
      conf.setAppName("Testing")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")
    }

  test("testScanLeftRDD_cumSum") {

    val testData = (0 to 4200).map(_.toDouble).toList

    def testFunc: (Double, Double) => Double = (acc, e) => acc + e
    val testZero = 0.0

    val expected = testData.scanLeft(testZero)(testFunc).tail
    val sc = fixture.sc

    val actual = sc.parallelize(testData, 5)
      .keyBy(t => t)
      .sortByKey()
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

      assert(expected.equals(actual))
  }
  test("testScanLeftRDD_stringConcat") {

    val testData = List("a", "b", "c", "d", "e")
    def testFunc: (String, String) => String = (acc, e) => acc + e
    val testZero = ""

    val expected = testData.scanLeft(testZero)(testFunc).tail
    val sc = fixture.sc

    val actual = sc.parallelize(testData, 2)
      .keyBy(t => t)
      .sortByKey()
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

    assert(expected.equals(actual))

  }
  test("testScanLeftRDD_multiplicationByZero") {
    def testFunc: (Double, Double) => Double = (acc, e) => acc * e

    val testData = (0 to 42000).map(_.toDouble).toList
    val testZero = 0.0

    val expected = testData.scanLeft(testZero)(testFunc).tail
    val sc = fixture.sc

    val actual = sc.parallelize(testData, 5)
      .keyBy(t => t)
      .sortByKey()
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

    assert(expected.equals(actual))
  }
  test("testScanLeftRDD_cumDiff") {
    def testFunc: (Double, Double) => Double = (acc, e) => acc - e

    val testData = (0 to 42000).map(_.toDouble).toList
    val testZero = 0.0

    val expected = testData.scanLeft(testZero)(testFunc).tail
    val sc = fixture.sc

    val actual = sc.parallelize(testData, 5)
      .keyBy(t => t)
      .sortByKey()
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

    assert(expected.equals(actual))
  }

  test("testScanLeftRDD_directions") {
    val directions = List("North", "East", "South", "West", "North")

    val testZero = (0, 0)
    def testFunc: ((Int, Int), String) => (Int, Int) = {
      case ((x, y), "North") => (x, y + 1)
      case ((x, y), "South") => (x, y - 1)
      case ((x, y), "East") => (x + 1, y)
      case ((x, y), "West") => (x - 1, y)
    }

    val path = directions.scanLeft(testZero)(testFunc).tail
    val sc = fixture.sc

    val actual = sc.parallelize(directions, 5)
      .keyBy(t => t)
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

    assert(path.equals(actual))
  }

  test("testScanLeftRDD_runningAverage"){
    val numbers = List(1.0, 2.0, 3.0, 4.0, 5.0)

    def testFunc: ((Double, Double), Double) => (Double, Double) = {
      case ((avg, n), x) => ((avg * n + x) / (n + 1), n + 1)
    }
    val testZero = (0.0, 0.0)
    val runningAverage = numbers.scanLeft(testZero)(testFunc).tail
      .map(_._1)
    val sc = fixture.sc

    val actual = sc.parallelize(numbers, 3)
      .keyBy(t => t)
      .scanLeft(testZero)(testFunc)
      .values
      .keys
      .collect()
      .toList

    assert(runningAverage.equals(actual))
  }
  test("testScanLeftRDD_exponentialMovingAverage") {
    val alpha = 0.2
    val prices = List(100.0, 101.0, 98.0, 99.0, 100.0)

    def testFunc: (Double, Double) => (Double) = (prevEma, price) => alpha * price + (1 - alpha) * prevEma
    val testZero = prices.head
    val ema = prices.tail.scanLeft(testZero)(testFunc)
    val sc = fixture.sc

    val actual = sc.parallelize(prices, 3)
      .keyBy(t => t)
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

    assert(ema.equals(actual))
  }

  test("testScanLeftRDD_factorial") {
    def testFunc: (Int, Int) => (Int) = _ * _
    val numbers = 1 to 10
    val testZero = 1
    val factorials = numbers.scanLeft(testZero)(testFunc).tail

    val sc = fixture.sc

    val actual = sc.parallelize(numbers, 3)
      .keyBy(t => t)
      .scanLeft(testZero)(testFunc)
      .values
      .collect()
      .toList

    assert(factorials.equals(actual))
  }

  test("testScanLeftRDD_gcd") {


    val numbers = Seq(48, 18, 24, 30, 50, 75, 15, 34)
    val testZero = numbers.head
    val result = numbers.tail.scanLeft(testZero){ case (a, b) => GCDUtils.gcd(a, b) }

    val sc = fixture.sc

    val actual = sc.parallelize(numbers, 4)
      .keyBy(t => t)
      .scanLeft(testZero){ case (a, b) => GCDUtils.gcd(a, b) }
      .values
      .collect()
      .toList

    assert(result.equals(actual))
  }

}

object GCDUtils {
  @tailrec
  def gcd(x: Int, y: Int): Int = if (y == 0) x else gcd(y, x % y)
}
