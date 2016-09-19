package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 15-10-9.
  */
object CCDataDistinct {
  def main(args:Array[String]) {

    val sparkConf = new SparkConf().setAppName("CCDataDistinct")
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), args(1).toInt)

    val edges = lines.flatMap{ s =>
      val parts = s.split("\\s+")
      List((parts(0).toInt, parts(1).toInt),(parts(1).toInt,parts(0).toInt))
    }.distinct()

    edges.map(eMsg => eMsg._1+" "+eMsg._2).saveAsTextFile(args(2))
  }
}
