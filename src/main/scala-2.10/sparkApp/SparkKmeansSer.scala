package sparkApp

import breeze.linalg.{squaredDistance, DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zx on 2016/4/27.
  */
object SparkKmeansSer {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split("\\s+").map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[DenseVector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val lines = sc.objectFile(args(0)).asInstanceOf[RDD[ObjectDenseVector]]
    val data = lines.map(_.getValue()).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val K = args(1).toInt
    val iter = args(2).toInt

    var kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    var tempDist = 1.0
    var step = 0

    val D = kPoints(0).length

    while (step < iter) {

      val startTime = System.currentTimeMillis()

      //      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))
      //
      //      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
      //
      //      val newPoints = pointStats.map {pair =>
      //        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()


      val pointParts = data.mapPartitions { iter =>
        val results = new Array[DenseVector[Double]](K)
        for (i <- 0 until K) {
          results(i) = DenseVector.zeros(D)
        }
        val counts = new Array[Int](K)

        while (iter.hasNext) {
          val p = iter.next()
          val index = closestPoint(p, kPoints)
          results(index) = results(index) + p
          counts(index) += 1
        }
        Iterator(results.zip(counts))
      }.collect()

      val newPoints = new Array[DenseVector[Double]](K)
      for (i <- 0 until K) {
        newPoints(i) = DenseVector.zeros(D)
      }
      val newCounts = new Array[Int](K)
      for (pointPart <- pointParts) {
        for (i <- 0 until K) {
          newPoints(i) += pointPart(i)._1
          newCounts(i) += pointPart(i)._2
        }
      }
      for (i <- 0 until K) {
        newPoints(i) *= 1.0 / newCounts(i)
      }

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      kPoints = newPoints

      val endTime = System.currentTimeMillis()
      step += 1
      println("Finished iteration " + step + " (delta = " + tempDist + ") while time is " + (endTime - startTime) / 1000.0 + "s")

    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }

}
