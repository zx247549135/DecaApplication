package decaApp

import java.io.DataOutputStream

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by iceke on 16/9/12.
  */
class UnsafePR {

}

object UnsafePR{
  private val ordering = implicitly[Ordering[Int]]

  def testOptimized(groupedEdges: RDD[(Int, Iterable[Int])],iters:Int, save: String) {
    val cachedEdges = groupedEdges.mapPartitions ( iter2 => {
      val (iter1, iter) = iter2.duplicate
      var realSize = 0
      for((src, dests) <- iter1){
        realSize += 2
        realSize += dests.size
      }
      val chunk = new EdgeChunk(realSize * 4)
      val dos = new DataOutputStream(chunk)
      for ((src, dests) <- iter) {
        dos.writeInt(src)
        dos.writeInt(dests.size)
        dests.foreach(dos.writeInt)
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
    val conf = new SparkConf().setAppName(args(2)).setMaster("local");
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
