package com.mvad.spark.demo.sql

/**
 * Created by zhugb on 15-5-4.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

object SessionLogParquetDemo {

  def toNormalCookie(cookiebytes: ArrayBuffer[Int]) = if (cookiebytes.length != 12) {
    "???????????????????????"
  } else {

    val firstPart = (
      for (i <- 0 until cookiebytes.length - 1) yield
      ((cookiebytes(i).toByte & 0xf0) >>> 4).toString + ((cookiebytes(i).toByte & 0x0f)).toString
      ).mkString
    val lastPart = ((cookiebytes(11).toByte & 0xFF) >>> 4)

    firstPart + lastPart
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("SparkSQL:[demo][SparkSQLUsingSessionLogParquet]"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    // Create a DataFrame from Parquet files
    val df = sqlContext.read.parquet("/mvad/warehouse/session/dspan/date=2015-05-01/")
    // Your can alsoCreate a DataFrame from data sources
    //    val df = sqlContext.load("/mvad/warehouse/session/dspan/date=2015-05-01/","parquet")
    df.registerTempTable("sessionlog")
    sqlContext.tableNames.foreach(println)
    df.printSchema()

    sqlContext.udf.register("toNormalCookie", toNormalCookie _)
    val sql1 =
      """
        |select toNormalCookie(cookie) as cookiestr,eventTime,eventType,geoInfo.country as country,
        |geoInfo.province as province from sessionlog limit 10
      """.stripMargin
    val sample = sqlContext.sql(sql1)
    sample.show()

    val sql2 =
      """
        |select eventType, count(cookie) as count from sessionlog group by eventType
      """.stripMargin
    val result = sqlContext.sql(sql2)
    result.cache()

    // only show 20 records
    result.show()
    result.show(100)

    // collect to driver for post processing
    result.collect().map(_.mkString(",")).foreach(println)

    //save result
    result.write.save("/tmp/result-parquet")
    result.rdd.saveAsTextFile("/tmp/result-txt")
    result.rdd.saveAsObjectFile("/tmp/result-sequence")

    // bug api, only create to default DB
    //    result.saveAsTable("result")
    //    result.insertInto("result")

    sc.stop()
  }
}
