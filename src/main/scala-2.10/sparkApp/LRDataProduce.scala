package sparkApp

import breeze.linalg.DenseVector
import org.apache.spark.{SparkContext, SparkConf}
import sparkApp.SparkLR.DataPoint

import scala.util.Random

/**
  * Created by zx on 2016/5/14.
  */
object LRDataProduce {

  val rand = new Random(42)

  def generateData(size:Int,numDest:Int): Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if(i % 2 == 0) -1 else 1
      val x = DenseVector.fill(numDest){rand.nextGaussian + y*0.7}
      new DataPoint(x,y)
    }
    Array.tabulate(size)(generatePoint)
  }

  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("LRDataProduce");
    val sc = new SparkContext(sparkConf)
    val numDest = args(0).toInt
    val numSimplesEach = args(1).toInt
    val numSimplesSlices = args(2).toInt
    val numSlices = args(3).toInt

    val result = sc.parallelize(0 until numSimplesSlices, numSlices).flatMap(i => generateData(numSimplesEach,numDest))

    result.saveAsObjectFile(args(4))

  }

}
