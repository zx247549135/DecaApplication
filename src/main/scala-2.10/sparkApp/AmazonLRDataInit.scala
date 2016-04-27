package sparkApp

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import sparkApp.SparkLR.DataPoint

import scala.util.Random

/**
  * Created by zx on 2016/4/27.
  */
object AmazonLRDataInit {

  val rand = new Random(42)

  def main(args:Array[String]) {
    val sparkConf = new SparkConf().setAppName("LRDataInit")
    val sparkContext = new SparkContext(sparkConf);

    val lines = sparkContext.textFile(args(0))
    val dim = args(1).toInt
    val out = lines.map(line_init => {
      val line = line_init.toString
      val parts = line.replace("\\s+","").replace("(","").replace(")","").split(",,")
      val result = parts.map(_.trim)
      val outResult = new Array[Double](dim)
      for (i <- 0 until dim) {
        outResult.update(i, result(i+1).toDouble)
      }
      val denseResult = DenseVector.tabulate(dim)(i => outResult(i))
      val label_1 = rand.nextInt()
      val label = if(label_1 % 5 == 0) 1 else -1
      val objectResult = new DataPoint(denseResult, label)
      objectResult
    })
    out.saveAsObjectFile(args(2))

  }

}
