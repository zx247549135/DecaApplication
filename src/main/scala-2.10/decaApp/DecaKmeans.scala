package decaApp

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import sparkApp.ObjectDenseVector

/**
  * Created by zx on 2016/4/27.
  */
class Kmeans2Chunk(K:Int,dimensions:Int,size:Int=4096)
  extends ByteArrayOutputStream(size)
    with Serializable {self=>
  def getOrigin=new Iterator[Array[Double]]{
    var offset=0
    override  def hasNext=offset<self.count
    override def next():Array[Double]={
      if(!hasNext) Iterator.empty.next()
      else{
        val result=new Array[Double](dimensions)
        for(i<-0 until dimensions){
          result(i)=WritableComparator.readDouble(buf,offset)
          offset+=8
        }
        result
      }
    }
  }

  def getVectorValueIterator=new Iterator[Array[Double]]{
    var offset=0
    var currentPoint=new Array[Double](dimensions)
    var i=0
    override def hasNext=offset<self.count
    override def next()={
      if(!hasNext) Iterator.empty.next()
      else{
        i=0
        while(i<dimensions){
          currentPoint(i)=WritableComparator.readDouble(buf,offset)
          i+=1
          offset+=8
        }
        currentPoint.clone()
      }
    }
  }

  def squDis(p:Array[Double],k:Array[Double]):Double={
    var tempDist=0.0
    for(i<-0 until p.length){
      tempDist+=(p(i)-k(i))*(p(i)-k(i))
    }
    tempDist
  }

  def closestPoint(p:Array[Double],kpoints:Array[Array[Double]]):Int={
    var bestIndex=0
    var closest=Double.PositiveInfinity
    for(i<-0 until kpoints.length){
      val tempDist=squDis(p,kpoints(i))
      if(tempDist<closest){
        closest=tempDist
        bestIndex=i
      }
    }
    bestIndex
  }

  def getNewData(kpoints:Array[Array[Double]]):Iterator[Array[Array[Double]]] = {
    var offset = 0
    val currentPoint = new Array[Double](dimensions)
    var i = 0
    val pointArr=new Array[Array[Double]](K).map(_ => new Array[Double](dimensions+1))

    while(offset < self.count){
      i=0
      while(i<dimensions){
        currentPoint(i)=WritableComparator.readDouble(buf,offset)
        i+=1
        offset+=8
      }

      val index=closestPoint(currentPoint,kpoints)
      for(i<-0 until dimensions){
        pointArr(index)(i)+=currentPoint(i)
      }
      pointArr(index)(dimensions)+=1.0

    }
    Iterator(pointArr)
  }

}

object DecaKmeans {
  def parseArray(line:String):Array[Double]={
    line.split(' ').map(_.toDouble)
  }
  def squDis(p:Array[Double],k:Array[Double]):Double={
    var tempDist=0.0
    for(i<-0 until p.length){
      tempDist+=(p(i)-k(i))*(p(i)-k(i))
    }
    tempDist
  }
  def closestPoint(p:Array[Double],kpoints:Array[Array[Double]]):Int={
    var bestIndex=0
    var closest=Double.PositiveInfinity
    for(i<-0 until kpoints.length){
      val tempDist=squDis(p,kpoints(i))
      if(tempDist<closest){
        closest=tempDist
        bestIndex=i
      }
    }
    bestIndex
  }
  def main(args:Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val lines = sc.objectFile(args(0)).asInstanceOf[RDD[ObjectDenseVector]]
    val data = lines.map(_.getValue().toArray) //.persist(StorageLevel.MEMORY_AND_DISK)

    val k = args(1).toInt
    val D = args(4).toInt
    val iter = args(2).toInt
    //val kpoints=data.takeSample(withReplacement = false,k,42)
    val cachedPoints = data.mapPartitions { iter =>
      val (iterone, itertwo) = iter.duplicate
      val chunk = new Kmeans2Chunk(k, D, 8 * iterone.length * D)
      val dos = new DataOutputStream(chunk)
      for (point <- itertwo) {
        for (i <- 0 until D) {
          dos.writeDouble(point(i))
        }
      }
      Iterator(chunk)
    }.persist(StorageLevel.MEMORY_AND_DISK)
    cachedPoints.foreach(_ => Unit)
    val kpoints = cachedPoints.mapPartitions(dIter => {
      val chunk = dIter.next()
      chunk.getOrigin
    }, true).takeSample(withReplacement = false, k, 42).toArray

    var tempDist = 1.0
    var step = 0
    while (iter > step) {

      val startTime = System.currentTimeMillis()

      val newData = cachedPoints.mapPartitions { iter => {
        val chunk = iter.next()
        chunk.getNewData(kpoints)
      }
      }
      val dataArray = newData.collect()
      val newPoint = new Array[Array[Double]](k).map(_ => new Array[Double](D + 1))
      for (i <- 0 until dataArray.length) {
        for (j <- 0 until k) {
          for (k <- 0 until D + 1) {
            newPoint(j)(k) += dataArray(i)(j)(k)
          }
        }
      }
      for (i <- 0 until k) {
        for (j <- 0 until D) {
          newPoint(i)(j) /= newPoint(i)(D)
        }
      }
      tempDist = 0.0
      for (i <- 0 until k) {
        tempDist += squDis(kpoints(i), newPoint(i))
      }
      for (i <- 0 until k) {
        for (j <- 0 until D) {
          kpoints(i)(j) = newPoint(i)(j)
        }
      }

      val endTime = System.currentTimeMillis()
      step += 1
      println("Finished iteration " + step + " (delta = " + tempDist + ") while time is " + (endTime - startTime) / 1000.0 + "s")


    }
    println("Final centers:")
    for (i <- 0 until k) {
      for (j <- 0 until D) {
        print(kpoints(i)(j))
        print(" ")
      }
      println()
    }
    sc.stop()
  }


}
