package decaApp

import java.io.DataOutputStream

import breeze.linalg.DenseVector
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import sparkApp.SparkLR

import scala.util.Random

/**
  * Created by iceke on 16/9/12.
  */

class UnsafeChunk(dimensions: Int,size: Long = 4196)
extends Serializable
{self=>
  import UnsafeLR._
  private val baseAddress = UNSAFE.allocateMemory(size)
  private var curAddress = baseAddress
  def address = baseAddress
  def free:Unit={
    UNSAFE.freeMemory(baseAddress)
  }

  def writeDouble(num:Double):Unit={
    UNSAFE.putDouble(curAddress,num)
    curAddress+=8
  }


   def show():Unit={
     var address = baseAddress
     while(address<curAddress){
     //  println(UNSAFE.getDouble(address))
       address+=8
     }

  }


  def getVectorValueIterator(w: Array[Double]) = new Iterator[Array[Double]] {
    var offset = baseAddress
    var currentPoint = new Array[Double](dimensions)
    var i = 0
    var y = 0.0
    var dotValue = 0.0
    override def hasNext = offset<self.curAddress

    override def next() = {
      if(!hasNext) Iterator.empty.next()
      else{
        //read data from chunk
        i = 0
        while(i<dimensions){
          currentPoint(i) = UNSAFE.getDouble(offset)
          offset += 8
          i += 1
        }
        y = UNSAFE.getDouble(offset)
        offset += 8

        //caculate the dot value
        i=0
        dotValue = 0.0
        while(i<dimensions) {
          dotValue += w(i) * currentPoint(i)
          i += 1
        }
        //transform to values
        i = 0
        while(i < dimensions){
          currentPoint(i) *= (1 / (1 + Math.exp(-y * dotValue)) - 1) * y
          i+=1
        }
        currentPoint.clone()
      }

    }
  }




}


object UnsafeLR{
  val rand = new Random(42)
  final val UNSAFE = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }

  def testOptimized(points: RDD[SparkLR.DataPoint],iterations:Int,numDests:Int,w:DenseVector[Double]): Unit = {
    val cachedPoints = points.mapPartitions ({ iter =>
      val (iterOne ,iterTwo) = iter.duplicate
      val chunk = new UnsafeChunk(numDests,8*iterOne.length*(1+numDests))

      for (point <- iterTwo) {
        point.x.foreach(chunk.writeDouble)
        chunk.writeDouble(point.y)
      }
      chunk.show()

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
      println("w is :"+w_op.mkString(";"))
    }
    val duration = System.currentTimeMillis - startTime
    println("result:"+w_op.mkString(";"))
    println("Duration is " + duration / 1000.0 + " seconds")

  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(args(3)).setMaster("local")
    val sc = new SparkContext(sparkConf)
    val iterations = args(1).toInt
    val numDests = args(2).toInt
    val points = sc.objectFile(args(0)).asInstanceOf[RDD[SparkLR.DataPoint]]
    points.foreach(println)

    val w = DenseVector.fill(numDests){2*rand.nextDouble() - 1}
    println("Initial w:"+w)

    testOptimized(points,iterations,numDests,w)
    System.gc()
    System.gc()
    System.gc()

    sc.stop()
  }
}
