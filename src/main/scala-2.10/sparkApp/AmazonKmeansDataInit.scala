package sparkApp

import breeze.linalg.DenseVector
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zx on 2016/4/27.
  */

class ObjectDenseVector(denseVector: DenseVector[Double]) extends Serializable{
  def getValue(): DenseVector[Double] = this.denseVector
}

object AmazonKmeansDataInit {
  def main(args:Array[String]) {
    val sparkConf = new SparkConf().setAppName("AmazonDataInit")
    val sparkContext = new SparkContext(sparkConf);

    val lines = sparkContext.textFile(args(0))
    val dim = args(1).toInt
    val out = lines.map(line_init => {
      val line = line_init.toString
      val parts = line.replace("\\s+", "").replace("(", "").replace(")", "").split(",,")
      val result = parts.map(_.trim)
      val outResult = new Array[Double](dim)
      for (i <- 0 until dim) {
        outResult.update(i, result(i + 1).toDouble)
      }
      val denseResult = DenseVector.tabulate(dim)(i => outResult(i))
      val objectResult = new ObjectDenseVector(denseResult)
      objectResult
    })
    out.saveAsObjectFile(args(2))
  }
}
