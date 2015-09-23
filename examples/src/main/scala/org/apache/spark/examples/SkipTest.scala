package org.apache.spark.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liwen on 9/22/15.
 */
object SkipTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Skip Test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val table = sqlContext.read.parquet(args(0))
    val res = sqlContext.sql("select l_shipdate from table limit 100");
    res.foreach(println)

    sc.stop()
  }
}
