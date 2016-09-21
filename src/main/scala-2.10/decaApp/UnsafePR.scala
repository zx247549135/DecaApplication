package decaApp

import java.io.DataOutputStream

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by iceke on 16/9/12.
  */
class UnsafeEdge (size: Int = 4196){self =>
  import UnsafePR._
  private val baseAddress = UNSAFE.allocateMemory(size)
  private var curAddress = baseAddress
  def address = baseAddress
  def free:Unit={
    UNSAFE.freeMemory(baseAddress)
  }

  def writeInt(num:Int):Unit={
    UNSAFE.putInt(curAddress,num)
    curAddress += 4
  }


  def show(): Unit ={

  }

  def getInitValueIterator(value: Float) = new Iterator[(Int, Float)] {
    var offset = baseAddress

    override def hasNext = offset < self.curAddress

    override def next()={
      if (!hasNext) Iterator.empty.next()
      else{
        val srcId = UNSAFE.getInt(offset)
        offset += 4
        val numDests = UNSAFE.getInt(offset)
        offset += 4+4*numDests
        (srcId, value)
      }
    }

  }

  def getMessageIterator(vertices: Iterator[(Int, Float)]) = new Iterator[(Int, Float)] {
    var changeVertex = true
    var offset = baseAddress
    var currentDestIndex = 0
    var currentDestNum = 0
    var currentContrib = 0.0f

    private def matchVertices(): Boolean = {
      assert(changeVertex)
      if(offset >= self.curAddress) return false

      var matched = false
      while (!matched && vertices.hasNext) {
        val currentVertex = vertices.next()
        while (currentVertex._1 > UNSAFE.getInt(offset)) {
          offset += 4
          val numDests = UNSAFE.getInt(offset)
          offset += 4 + 4 * numDests

          if (offset >= self.curAddress) return false
        }
        if (currentVertex._1 == UNSAFE.getInt(offset)) {
          matched = true
          offset += 4
          currentDestNum = UNSAFE.getInt(offset)
          offset += 4
          currentDestIndex = 0
          currentContrib = currentVertex._2 / currentDestNum
          changeVertex = false
        }
      }
      matched
    }


    override def hasNext = !changeVertex || matchVertices()

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        currentDestIndex += 1
        if (currentDestIndex == currentDestNum) changeVertex = true

        val destId = UNSAFE.getInt(offset)
        offset += 4

        (destId, currentContrib)
      }
    }

  }


}

object UnsafePR{
  private val ordering = implicitly[Ordering[Int]]

  final val UNSAFE = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }


  def testOptimized(groupedEdges: RDD[(Int, Iterable[Int])],iters:Int, save: String) {
    val cachedEdges = groupedEdges.mapPartitions ( iter2 => {
      val (iter1, iter) = iter2.duplicate
      var realSize = 0
      for((src, dests) <- iter1){
        realSize += 2
        realSize += dests.size
      }
      val chunk = new UnsafeEdge(realSize * 4)
      for ((src, dests) <- iter) {
        chunk.writeInt(src)
        chunk.writeInt(dests.size)
        dests.foreach(chunk.writeInt)
      }
      Iterator(chunk)
    }, true).cache()

    cachedEdges.foreach(_ => Unit)

    val initRanks = cachedEdges.mapPartitions( iter => {
      val chunk = iter.next()
      chunk.getInitValueIterator(1.0f)
    }, true)

    var ranks = initRanks

    for (i <- 1 to iters) {
      val contribs = cachedEdges.zipPartitions(ranks) { (EIter, VIter) =>
        val chunk = EIter.next()
        chunk.getMessageIterator(VIter)
      }
      ranks = contribs.reduceByKey(cachedEdges.partitioner.get, _ + _).asInstanceOf[ShuffledRDD[Int, _, _]].
        setKeyOrdering(ordering).
        asInstanceOf[RDD[(Int, Float)]].
        mapValues(0.15f + 0.85f * _)
    }
    ranks.saveAsTextFile(save)

  }

  def main(args: Array[String]) {
    println("directmemory: "+sun.misc.VM.maxDirectMemory())
    val conf = new SparkConf().setAppName(args(2))
    val spark = new SparkContext(conf)

    //Logger.getRootLogger.setLevel(Level.FATAL)

    val lines = spark.textFile(args(0))
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }.groupByKey().
      asInstanceOf[ShuffledRDD[Int, _, _]].
      setKeyOrdering(ordering).
      asInstanceOf[RDD[(Int, Iterable[Int])]]

    val iters = args(1).toInt
    testOptimized(links, iters, args(3))

    spark.stop()
  }

}
