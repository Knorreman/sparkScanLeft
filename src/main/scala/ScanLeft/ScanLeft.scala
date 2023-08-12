package ScanLeft
import org.apache.spark.TaskContext
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.reflect.ClassTag

object ScanLeft {

  implicit class ScanLeftRDD[K : Ordering : ClassTag, T: ClassTag ](self: RDD[(K, T)]) {

    def scanLeft[A](z: A)(f: (A, T) => A): RDD[(K, A)] = {
      val sc = self.context

      val numPartitions = self.getNumPartitions

      if (self.getStorageLevel == StorageLevel.NONE) {
        println("rdd is not cached, caching it")
        self.cache()
      }

      var _z = z
      val fbc = sc.broadcast(f)

      def partFunc = (taskContext: TaskContext, iter: Iterator[(K, T)]) => {
        val index = taskContext.partitionId()
        val partitionResult: Seq[A] = iter
          .map(_._2)
          .scanLeft(_z)((acc, kv) => {
            fbc.value(acc, kv)
          })
          .toSeq

        (index, partitionResult.last)
      }

      val partitionResultMap = collection.mutable.Map[Int, A]()

      // index 0 should have the original zero value
      partitionResultMap.update(0, z)

      (0 until numPartitions)
        .foreach(i => {
          val res = sc.runJob(self, partFunc, Seq(i))

          partitionResultMap.update(i+1, res.head._2)
          _z = res.head._2
        })

      val partitionResultMapBc = sc.broadcast(partitionResultMap)

      val scannedRDD = self.mapPartitionsWithIndex((index: Int, iter: Iterator[(K, T)]) => {
          val iterSeq = iter.toSeq // So I can iterate over it twice
          val startValue = partitionResultMapBc.value(index)
          val partitionResult: Iterator[(K, A)] = iterSeq
            .map(_._2)
            .scanLeft(startValue)((acc, kv) => {
             fbc.value(acc, kv)
            })
            .tail
            .zip(iterSeq.map(_._1))
            .map(_.swap)
            .iterator
          partitionResult
        })
      scannedRDD
    }
  }

}
