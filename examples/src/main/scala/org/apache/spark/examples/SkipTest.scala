package org.apache.spark.examples

/**
 * Created by liwen on 9/22/15.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import parquet.hadoop.util.counters.BenchmarkCounter

import scala.collection.mutable.ArrayBuffer

object SkipTest {
  val sparkConf = new SparkConf().setAppName("Skip Test")
  val sc = new SparkContext(sparkConf)
//  val hc = new HiveContext(sc)
  val sqlContext = new SQLContext(sc)

  import org.apache.spark.deploy.SparkHadoopUtil
  import scala.sys.process._

  def setConfParameters() {
    SparkHadoopUtil.get.conf.setBoolean("parquet.task.side.metadata", false)
    SparkHadoopUtil.get.conf.set("mapred.min.split.size", "2000000000")
    SparkHadoopUtil.get.conf.set("mapreduce.input.fileinputformat.split.minsize", "2000000000")
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
  }

  def callPurge(): Unit = {
    val c = System.getenv("MASTER")
    val output = if (c == null || c.startsWith("local")) {
//      Seq("bash", "-c", "purge").!
      Seq("sudo", "sh", "-c", "'echo 3 >/proc/sys/vm/drop_caches'").!
    } else {
      Seq("bin/clear-os-cache.py").!
    }
    if (output.toInt == 0) {
      println("cleared OS cache by " + (if (c == null || c == "local") "drop_caches" else "bin/clear-os-cache.py"))
    } else {
      println("failed to clear OS cache")
    }
  }

  def testQuery(parentPath: String, queryId: Int): Unit = {
    import java.io.{FileWriter, PrintWriter, File}
    import java.nio.file.{Paths, Files}

    val queryPath: String = parentPath + "_meta/metadata.workload/newtest10"
    val colGroups = scala.io.Source.fromFile(parentPath + "_meta/metadata.grouping").getLines
    val outputPath: String = parentPath + "_meta/results/"

    SparkHadoopUtil.get.conf.setBoolean("parquet.column.crack", true)

    val queryContent: String = new String(Files.readAllBytes(Paths.get(queryPath)))
    val queries: Array[String] = queryContent.split(";")

    val statsPath: java.io.File = new java.io.File(outputPath + "/times")
    statsPath.getParentFile.mkdirs
    val pw: PrintWriter = new java.io.PrintWriter(new FileWriter(statsPath, true))

    val query: String = queries(queryId).trim
    val lines: Array[String] = query.split("\n")
    println(query)
    val filterString: String = lines(0).substring(2)
    val columnString: String = lines(1).substring(2)
    val queryName: String = lines(3).substring(2)

    //val weight: Double = lines(2).substring(4).toDouble
    val weight = 1

    val countGroups = colGroups.filter(x => x.split(",").intersect(columnString.split(",")).size > 0).size
    if (countGroups == 1) {
      SparkHadoopUtil.get.conf.setBoolean("parquet.column.single", true)
    }
    callPurge
    setConfParameters
    SparkHadoopUtil.get.conf.set("parquet.filter.bitset", filterString)
    val data = sqlContext.read.parquet(parentPath)
    data.registerTempTable("denorm")
    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    val startTime = System.currentTimeMillis
    val res = sqlContext.sql(query).collect
    val end2end = System.currentTimeMillis - startTime

//  val loadTime = BenchmarkCounter.loadTimeCounter.getCount
//  val sortTime = BenchmarkCounter.sortTimeCounter.getCount
    val countValue = SparkHadoopUtil.get.conf.getLong("parquet.read.count.val", -1)
    println("count value: " + countValue)
    val countRid = SparkHadoopUtil.get.conf.getLong("parquet.read.count.rid", -1)
    println("count rid: " + countRid)

    pw.write(
      queryName + "\t" +
        (end2end * weight).toLong +
//        (loadTime * weight).toLong + "\t" +
//       (sortTime * weight).toLong + "\t" +
      "\n")


    val respath: java.io.File = new java.io.File(outputPath + "/" + queryName)
    val pw2: PrintWriter = new java.io.PrintWriter(new FileWriter(respath, true))
    res.map(_.toString).sortBy(x => x).foreach(x => pw2.write(x + "\n"))

    pw.close
    pw2.close
  }

  def countCells(parentPath: String, queryId: Int): Unit = {
    import java.io.{FileWriter, PrintWriter, File}
    import java.nio.file.{Paths, Files}

    val queryPath: String = parentPath + "_meta/metadata.workload/newtest10"
    val colGroups = scala.io.Source.fromFile(parentPath + "_meta/metadata.grouping").getLines
    val outputPath: String = parentPath + "_meta/results/"

    SparkHadoopUtil.get.conf.setBoolean("parquet.column.crack", true)

    val queryContent: String = new String(Files.readAllBytes(Paths.get(queryPath)))
    val queries: Array[String] = queryContent.split(";")

    val statsPath: java.io.File = new java.io.File(outputPath + "/counts")
    statsPath.getParentFile.mkdirs
    val pw: PrintWriter = new java.io.PrintWriter(new FileWriter(statsPath, true))

    val query: String = queries(queryId).trim
    val lines: Array[String] = query.split("\n")
    println(query)
    val filterString: String = lines(0).substring(2)
    val columnString: String = lines(1).substring(2)
    val queryName: String = lines(3).substring(2)
    //val weight: Double = lines(2).substring(4).toDouble
    val weight = 1


    val countGroups = colGroups.filter(x => x.split(",").intersect(columnString.split(",")).size > 0).size
    if (countGroups == 1) {
      SparkHadoopUtil.get.conf.setBoolean("parquet.column.single", true)
    }
    setConfParameters
    SparkHadoopUtil.get.conf.set("parquet.filter.bitset", filterString)
    sqlContext.setConf("spark.sql.shuffle.partitions", "1")

    val dir = new java.io.File(parentPath)
    var totValue = 0L
    var totRid = 0L
    val countValues = ArrayBuffer[Long]()
    val countRids = ArrayBuffer[Long]()

    for (path <- dir.listFiles) {
      val data = sqlContext.read.parquet(path.getAbsolutePath)
      data.registerTempTable("denorm")
      sqlContext.sql(query).foreach(x => x)
      val countValue = SparkHadoopUtil.get.conf.getLong("parquet.read.count.val", -1)
      println("count value: " + countValue)
      val countRid = SparkHadoopUtil.get.conf.getLong("parquet.read.count.rid", -1)
      println("count rid: " + countRid)

      countValues.append(countValue)
      countRids.append(countRid)

      totValue += countValue
      totRid += countRid
    }

    pw.write(
      queryName + "\t" +
        ((totValue + totRid) * weight).toLong +  "\t" +
        (totValue * weight).toLong + "\t" +
        (totRid * weight).toLong + "\t" +
        (countValues.mkString(" ")) + "\t" +
        (countRids.mkString(" ")) + "\t" +
        "\n")

    pw.close
  }

////  def adhoc(): Unit = {
//    SparkHadoopUtil.get.conf.setBoolean("parquet.task.side.metadata", false)
//    SparkHadoopUtil.get.conf.set("mapred.min.split.size", "2000000000")
//    SparkHadoopUtil.get.conf.setBoolean("parquet.column.crack", true)
//    SparkHadoopUtil.get.conf.set("mapreduce.input.fileinputformat.split.minsize", "2000000000")
//    sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
//    val parentPath = "/Users/liwen/work/skip-legacy/data/files/tpch/denorm1-1993-03_g64_1000"
//    val data = sqlContext.read.parquet(parentPath + "/pq/data.parquet")
//    data.registerTempTable("denorm")
//    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
//    val res = sqlContext.sql(
//      """
//        |select
//        |	l_orderkey,
//        |	sum(l_extendedprice * (1 - l_discount)) as revenue,
//        |	o_orderdate,
//        |	o_shippriority
//        |from
//        |  denorm
//        |where
//        |c_mktsegment = 'MACHINERY'
//        |group by
//        |	l_orderkey,
//        |	o_orderdate,
//        |	o_shippriority
//        |order by
//        |	revenue desc,
//        |	o_orderdate
//        |limit 10
//      """.stripMargin)
//
//    res.foreach(println)
////
//  }

  def main(args: Array[String]) {
    val parentPath = args(1).reverse.dropWhile(_ == '/').reverse
    if (args(0) == "time") {
      testQuery(parentPath, args(2).toInt)
    } else if (args(0) == "count") {
      countCells(parentPath, args(2).toInt)
    } else {
      println("unknown command " + args(0))
    }
  }
}
