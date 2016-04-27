package sparkApp

import breeze.linalg.{Vector, DenseVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import sparkApp.SparkLR.DataPoint

import scala.util.Random

/**
  * Created by zx on 2016/4/27.
  */
object SparkLRSer {

  val rand = new Random(42)

  def main(args: Array[String]) {

    if(args.length<4){
      System.err.println("Usage of Parameters: input numpartition iter numDest name")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(4))
    val sc = new SparkContext(sparkConf)
    val length = args(3).toInt
    val numPartitions = args(1).toInt
    val points = sc.textFile(args(0)).repartition(numPartitions).map{line =>
      val parts = line.split(" ")
      val y = parts(0).toDouble
      val data = new Array[Double](parts.length-1)
      for(i <- 1 until parts.length){
        data(i-1) = parts(i).toDouble
      }
      val x = DenseVector(data)
      new DataPoint(x,y)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    points.foreach(x => Unit)
    val iterations = args(2).toInt

    // Initialize w to a random value
    var w = DenseVector.fill(length){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to iterations) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        p.x * (1 / (1 + Math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)

    sc.stop()
  }

}
