package decaApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import breeze.linalg.DenseVector
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import sparkApp.SparkLR

import scala.util.Random

/**
  * Created by zx on 2016/4/27.
  */

class PointChunk(dimensions: Int,size: Int = 4196)
  extends ByteArrayOutputStream(size)
    with Serializable { self =>


  def show():Unit={
    var address = 0
    while(address<size){
      println(WritableComparator.readDouble(buf, address))
      address+=8
    }

  }

  def getVectorValueIterator(w: Array[Double]) = new Iterator[Array[Double]] {
    var offset = 0
    var currentPoint=new Array[Double](dimensions)
    var i = 0
    var y = 0.0
    var dotvalue = 0.0

    override def hasNext = offset < self.count

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        //read data from the chunk
        i=0
        while (i < dimensions) {
          currentPoint(i)= WritableComparator.readDouble(buf, offset)
          offset += 8
          i += 1
        }
        y = WritableComparator.readDouble(buf, offset)
        offset += 8
        //calculate the dot value
        i=0
        dotvalue = 0.0
        while (i < dimensions) {
          dotvalue += w(i)*currentPoint(i)
          i += 1
        }
        //transform to values
        i=0
        while (i < dimensions) {
          currentPoint(i) *= (1 / (1 + Math.exp(-y * dotvalue)) - 1) * y
          i += 1
        }
        currentPoint.clone()
      }
    }
  }
}
/**
  * Logistic regression based classification.
  * Usage: SparkLR [slices]
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
  * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
  */

object DecaLR {
  val rand = new Random(42)

  def testOptimized(points: RDD[SparkLR.DataPoint],iterations:Int,numDests:Int,w:DenseVector[Double]): Unit = {
    val cachedPoints = points.mapPartitions ({ iter =>
      val (iterOne ,iterTwo) = iter.duplicate
      val chunk = new PointChunk(numDests,8*iterOne.length*(1+numDests))
      val dos = new DataOutputStream(chunk)
      for (point <- iterTwo) {
        point.x.foreach(dos.writeDouble)
        dos.writeDouble(point.y)
      }
      Iterator(chunk)
    },true).persist(StorageLevel.MEMORY_AND_DISK)

    cachedPoints.foreach(x => Unit)

    val w_op=new Array[Double](numDests)
    for(i <- 0 until numDests)
      w_op(i) = w(i)

    val startTime = System.currentTimeMillis
    for (i <- 1 to iterations) {
      println("On iteration " + i)
      val gradient= cachedPoints.mapPartitions{ iter =>
        val chunk = iter.next()
        chunk.getVectorValueIterator(w_op)
      }.reduce{(lArray, rArray) =>
        val result_array=new Array[Double](lArray.length)
        for(i <- 0 to numDests-1)
          result_array(i) = lArray(i) + rArray(i)
        result_array
      }

      for(i <- 0 to numDests-1)
        w_op(i) = w_op(i) - gradient(i)
      //println("w is :"+w_op.mkString(";"))
    }
    val duration = System.currentTimeMillis - startTime
    println("result:"+w_op.mkString(";"))
    println("Duration is " + duration / 1000.0 + " seconds")

  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val iterations = args(1).toInt
    val numDests = args(2).toInt
    val points = sc.objectFile(args(0)).asInstanceOf[RDD[SparkLR.DataPoint]]

    val w = DenseVector.fill(numDests){2*rand.nextDouble() - 1}
    println("Initial w:"+w)

    testOptimized(points,iterations,numDests,w)
    System.gc()
    System.gc()
    System.gc()

    sc.stop()
  }

}
