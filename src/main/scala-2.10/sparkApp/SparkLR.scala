package sparkApp

import breeze.linalg.{Vector, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
  * Created by zx on 2016/4/27.
  */
object SparkLR {

  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def main(args: Array[String]) {

    if(args.length<4){
      System.err.println("Usage of Parameters: ApplicationName inputPath iterations dimOfVector")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val length = args(1).toInt
    val points = sc.objectFile(args(0)).asInstanceOf[RDD[DataPoint]].persist(StorageLevel.MEMORY_AND_DISK)
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
